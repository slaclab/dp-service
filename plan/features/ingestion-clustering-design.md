# Design: clustered data platform service with per-source ingestion and PV-partitioned writers

## Problem

Running many ingestion instances today means any instance can receive any PV, from any number of
connections. Each instance builds and writes its own buckets, so:

1. **The public API leaks storage.** Clients build `IngestionDataFrame`s with a timestamp axis and
   columns, which is effectively the bucket layout. Storage cannot change (Mongo buckets to Arrow /
   Parquet) without changing every client.
2. **One PV is written by many writers.** Two instances receiving the same PV produce two
   interleaved streams of small buckets/files. Nothing accumulates a PV in one place, so files stay
   small, compaction has to merge across writers, and the single-owner assumption in
   `parquet-atomic-flush-design.md` ("each partition is owned by one ingester") does not hold.

Goal: any client may connect to any instance and send any PV. At any moment every sample of a
given PV goes to exactly one partition, owned by one writer, which accumulates it into large
time-ordered files. A PV may change partition over time (see "Moving a PV between partitions"),
and readers then read every partition it has lived in.

## Overview of the solution

The service has two roles, **ingestion** and **query**. The ingestion service has two internal
stages: a **receive stage** that takes client requests on any node and routes each PV to its
owning partition, and a **writer stage** that owns partitions and turns their data into caches
and files. There is no separate gateway service. Between the stages sits a durable log: NATS
JetStream in a cluster, or a local write-ahead log behind the same interface (see "Ingest log:
NATS optional"). The diagram shows the JetStream mode:

```
clients ──gRPC──▶ ingestion service, receive stage (any node: validate, route, append)
                         │ append to dp.ingest.p.<partition> (live lane)
                         │        or dp.ingest.h.<partition> (historical lane)
                         ▼
                 [NATS JetStream stream DP_INGEST, R3, file storage]
                         │ two pull consumers per partition (one per lane), held by the lease owner
                         ▼
                  ingestion service, writer stage (partition owner: per-PV buffers → Parquet + manifest)
```

![Ingestion data path: receive stage, ingest log, writer double buffer, Arrow IPC hot cache, Parquet, and query path](ingestion-data-path.svg)

*Figure: from a received sample to Parquet. T0 is the in-memory double buffer, T1 the Arrow IPC
hot cache on local disk, T2 the committed Parquet files plus manifest. Queries merge all three
tiers, taking cached rows only where their sequence is not yet committed (see Commit bookkeeping). The figure shows the live lane only; the historical lane and late cache are described in the text.*

- Each PV is placed on a partition, recorded on its `pvMetadata` document (see "Cluster
  management"). A new PV goes to a partition of the least-loaded node, and a rate-limited
  rebalancer may move PVs later. A moved PV's data is the union of its partitions. Writers own partitions, not PVs, so failover moves whole partitions.
- The receive stage acks the client only after the log append is durable. With JetStream the log is
  replicated, so the writer needs no local WAL, which removes the failover gap the flush design
  calls out ("with a node-local WAL, unflushed data is only recoverable when that node comes
  back"). The local mode accepts that gap or puts the WAL on shared storage.
- The log position (`seq`, per partition and lane) is the `seq` of the flush design. Because the live lane, the
  historical lane, and the per-day late windows commit independently, the partition manifest records
  the **committed sequence set** (`committedRanges`, a compacted interval set), not one watermark.
  See "Commit bookkeeping". Recovery re-fetches every sequence of the partition's subjects that the
  set does not cover.

**Decided: one binary, roles by config.** The ingestion service and the query engine ship as one
executable. A small site runs both roles in one process, and a large one runs each on its own
nodes. The protocol between them is the same in both cases. See "Deployment" for the shapes.

## Part 1: per-source public ingestion API

The client describes **what it measured**, never how it will be stored.

```proto
service DpIngestionService {
  // Long-lived; one stream may carry many sources.
  rpc ingestSamples(stream IngestSamplesRequest) returns (stream IngestSamplesAck);
}

// One request carries 1..N source blocks. Single sample, batch of one PV, and
// batch of many PVs are all the same shape with different counts.
message IngestSamplesRequest {
  string providerId = 1;
  repeated SourceSamples sources = 2;   // batch form: many samples per PV
  repeated Sample samples = 3;          // single-sample form: one sample each, any PVs
}

// Single sample: the minimal, self-describing unit.
message Sample {
  string sourceName = 1;                // 2. pvName
  Timestamp timestamp = 2;              // 1. ts
  uint64 sourceSeq = 3;                 // retry key, same sequence space as SourceSamples
  oneof value {                         // 3. value
    double doubleValue = 10;
    float  floatValue  = 11;
    sint64 longValue   = 12;
    sint32 intValue    = 13;
    bool   boolValue   = 14;
    string stringValue = 15;
    int32  enumValue   = 16;
    DoubleArray doubleArray = 17;       // ... other array/struct/image arms as in the column types
    bytes  serialized  = 20;
  }
  repeated TypedAttribute attributes = 30;   // 4. additional key/value for THIS sample
}

// Per-sample key/value with a typed value. A key may repeat in one sample (multi-valued).
message TypedAttribute {
  string key = 1;
  oneof value {
    sint64 intValue    = 2;
    double doubleValue = 3;
    string stringValue = 4;
    bool   boolValue   = 5;
  }
}

message SourceSamples {
  string sourceName    = 1;   // the PV
  uint64 sourceSeq     = 2;   // per (providerId, sourceName), strictly increasing; idempotency key
  oneof timestamps {          // per block: PVs in one request need not share an axis or count
    SamplingClock clock    = 3;
    TimestampList list     = 4;
  }
  oneof values {              // typed column, reusing the existing column messages
    DoubleColumn  doubles  = 10;
    FloatColumn   floats   = 11;
    // ... int, bool, string, enum, arrays, struct, image, serialized
  }
  ColumnMetadata metadata = 30;         // block-level: applies to every sample in the block
  repeated RowAttributes rowAttributes = 31; // sparse per-sample K/V, only rows that have any
}

message RowAttributes {
  uint32 row = 1;                       // index into this block's timestamps
  repeated TypedAttribute attributes = 2;
}

message IngestSamplesAck {
  repeated SourceAck sources = 1;
}

message SourceAck {
  string sourceName = 1;
  uint64 ackedThroughSeq = 2;     // everything <= this is durable
  ExceptionalResult rejected = 3; // set only when this block was rejected; other blocks unaffected
}
```

| Client sends | Shape |
|---|---|
| Single sample | 1 `Sample`: ts, pvName, value, optional K/V |
| Several single samples | 1 request, N `Sample`s, any PVs, any types |
| Batch, one PV | 1 block, N timestamps + N values |
| Batch, many PVs | 1 request, M blocks, each with its own timestamps/length/type |
| Continuous feed | any mix over one long-lived stream |

**Two kinds of key/value, kept apart on purpose:**

- `ColumnMetadata` on a block describes the source over that block (provenance, units, tags).
  Stored once per block, as today.
- `Sample.attributes` / `rowAttributes` annotate one sample (e.g. `{beamMode: "tuning"}`,
  `{gain: 4}`, `{temp: 21.5}`). Values are **typed** (int, double, string, bool), and a key may
  appear more than once in one sample, meaning it is multi-valued. Different samples, and
  different PVs, may carry entirely different keys or none. Storage is one column for all of
  them (see "Sample layout: values and key/values"). Limits: at most 20 entries per sample, keys
  and string values at most 256 characters, the shared `ColumnMetadataValidationUtility` caps.
- Per-sample K/V is returned with the value. Filtering on it is supported but is not an index:
  see "Searching key/values" for its cost and the opt-in accelerators. Labels that must be
  searched fast across the archive belong in the Sample Status API (#238).
- If a client puts the same key/values on every sample of a block, they belong in block-level
  `ColumnMetadata`, which is stored once. The client library steers users that way.

The receive stage converts `Sample`s to the same internal per-PV blocks before routing, so a
single sample and a batch take one path after validation.

Client batching is transport only: the receive stage splits a request into blocks and routes each to its
PV's partition, so storage layout is the same whether a client sends 1 or 1000 samples per block.

Properties:

- **No frame, no bucket.** A block holds samples of one source; a request holds any number of blocks. Bucket size, span, file layout,
  partitioning, and flush cadence are server decisions and may change without a client release.
  `maxBucketSpanSeconds` stops being a client-visible limit; the writer cuts buckets itself.
- **Batching stays possible** for efficiency: a message carries many samples of one source, and a
  stream multiplexes sources. A thin client library can still accept a multi-PV frame and split it
  per source, so existing client code can migrate gradually.
- **Idempotent retries.** `(providerId, sourceName, sourceSeq)` identifies a block. Dedup happens
  in the **writer stage**, not in JetStream: the receive stage coalesces many blocks into one
  message, and a retry after a receiving node crashes coalesces differently, so a `Nats-Msg-Id` cannot identify a block.
  The writer keeps a `lastSeq` high-watermark per `(providerId, sourceName, lane)` in the partition
  manifest and drops any block at or below it. The watermark is per lane because the two lanes are
  consumed independently: a shared one would let a newer live block drop an older historical block
  of the same source. A reconnecting client resends everything after its last `ackedThroughSeq`.
- **Cumulative acks** per source replace per-request acks, matching streaming flow control. A
  block split across both lanes is acked only when both parts are durable in the log.
- **Ack means durable.** Today ingestion acks on enqueue and persists later on a worker (see
  CLAUDE.md, "Ingestion RPC duration is not ingestion latency"); a crash loses acked data. Here an
  ack follows the durable log append (replicated in JetStream mode).
- The legacy `ingestData*` RPCs become an adapter on the new service (phase 7), split per column
  into the same internal blocks, then deprecated.
- **Owner-aware clients (optimization).** A client library may ask any node where its PVs live
  (`resolvePlacement(pvNames) → owner endpoints`) and send each PV straight to its owner, skipping
  the forwarding hop. This is only an optimization: a stale or wrong guess is routed correctly by
  the receive stage, so correctness never depends on the client.

## Part 2: one writer per PV, via partitioned log subjects

### Receive stage (any node, stateless)

Any node running the ingestion role accepts any client and any PV. For each PV it receives:

1. Validate (the existing `IngestionValidationUtility` and `ColumnValueLimits`, minus frame-level
   checks that no longer apply).
2. Look up the PV's partition (cached `pvMetadata.placement`, placing it if new) and the **lane**: samples with `ts ≥ now − H` go live, older
   ones go historical (H = hot window, see "Hot data visibility"). A block that straddles the
   boundary is split in two. The receive stage is the only place that classifies. The writer stage
   obeys the lane and never reclassifies, so one sample cannot be routed two ways.
3. **Convert once to Arrow and coalesce** per owning partition for up to a few ms or ~1 MB (see
   "Wire format"). Per-sample messages would cap throughput far below the 4000 PV × 1 kHz
   baseline; coalescing keeps the message rate to thousands per second.
4. **Route:**
   - partition owned by this node → append to the local log and cache, no network hop;
   - otherwise → JetStream mode: append to the partition's subject, and the owner pulls it;
     local mode: forward over the internal node service (`Forward`) to the owner.
5. Await the durable append, then ack the contained `sourceSeq`s to the clients.

**The receiving node never writes a PV it does not own.** If the owner cannot be reached in local
mode, the block is rejected with a retryable error. Writing it locally would put the PV in a
partition its placement does not name.

Payloads above the NATS message limit (images, large arrays; keep `max_payload` ≤ 8 MB) use a
**claim check**: write the blob to object storage under a content hash, publish a reference. The
writer stage treats the reference as the value.

Back-pressure: when the log rejects or slows the append (stream `max_bytes` reached, consumer lag
over threshold, or a slow owner on the `Forward` stream), the receive stage stops reading from the
client's gRPC stream, and gRPC flow control propagates it to the client. Never buffer unboundedly
in the receive stage.

### JetStream layout

- One stream `DP_INGEST`, subjects `dp.ingest.p.*` (live) and `dp.ingest.h.*` (historical), file
  storage, replicas 3. Retention is `workqueue` (a message is removed once its consumer acks it),
  with `max_bytes` and `discard: new`. **No `max_age`**: an age limit would delete acked-to-client
  data that a writer has not committed yet, which breaks "ack means durable". A full stream fails
  the publish instead, and that reaches the client as back-pressure.
- Per-subject ordering is guaranteed, so a single partition is totally ordered and its stream
  sequences increase monotonically, which keeps `committedRanges` a short list of intervals.
- Two durable pull consumers per partition (`dp.ingest.p.<k>` and `dp.ingest.h.<k>`), explicit
  ack, each with its own in-flight limit. Only the partition's lease owner pulls from them. A
  message is acked only when every row it carries is in a committed file. A historical message
  whose rows fall into several days is acked after the last of those days commits.

### Writer stage (partition owner, stateful)

- Holds partition leases (`partitionLease {owner, epoch, expiresAt}` from the flush design).
  Assignment, rebalance, and failover are described in "Cluster management".
- For each owned partition: pull, decode, append samples into **per-PV** buffers (Arrow builders).
  Because every sample of a PV arrives at this one buffer regardless of which node or connection
  received it, the PV accumulates in one place.
- Live rows go through the hot cache, and historical rows through the late cache (both in "Hot
  data visibility"). Each flush writes one Parquet file and commits it with one manifest CAS that
  adds the file, adds the flushed rows' sequences to `committedRanges`, and checks `ownerEpoch`.
  The JetStream messages it fully covers are acked after that.
- Takeover (JetStream mode; local mode in "Ingest log"): new owner increments epoch, reads the
  manifest, and re-fetches every message of the partition's two subjects whose sequence is not in
  `committedRanges` (with workqueue retention,
  these are exactly the unacked messages still in the stream). A zombie old owner fails its CAS on
  epoch. Redelivered rows are filtered by `committedRanges` and by the per-source `lastSeq`. No
  local disk state is needed to recover.
- `pvStats` (#232) is updated by the writer in the same place it is today, **before** the commit
  that makes the data visible, preserving "stats cover every stored bucket".

### Internal node service

Nodes talk to each other through **one internal service on one port**, built on Arrow Flight
(which is itself a gRPC service). It carries three operations:

| Operation | Flight call | Direction | Used for |
|---|---|---|---|
| `Forward` | `DoPut` stream | receiving node → owner | ingest batches for partitions the receiving node does not own (local mode) |
| `ReadCached` | `DoGet` stream | query node → owner | cached rows for a partition and time range, with `ver`, only `seq ∉ committedRanges` |
| `Subscribe` | `DoExchange` | subscriber's node → owner | live push of newly appended rows (local mode) |

In JetStream mode `Forward` and `Subscribe` are not used, because the log routes data and serves
subscriptions. `ReadCached` is the same in both modes.

Rules:

- **Separate limits per operation.** The owner runs `Forward` and `ReadCached` on separate thread
  pools with separate in-flight limits. A heavy query cannot delay forwarded writes, and an ingest
  burst cannot starve reads. Over its limit, `ReadCached` answers `BUSY` and the caller backs off.
  `Forward` relies on gRPC flow control instead, so the back-pressure reaches the client.
- **Fencing on every call.** Each request carries the partition id and the lease epoch the caller
  saw. A node that no longer owns the partition, or holds a newer epoch, answers `NOT_OWNER`. The
  caller refreshes the lease from MongoDB and retries.
- **Loopback when local.** When the owner is the calling process, both operations call the
  in-process implementation of the same interface: identical checks, no network hop. This is how a
  single node runs the same code as a cluster.
- **Not exposed to clients.** The client-facing ingestion and query APIs are a separate service
  definition. The internal service shares the binary and transport but uses its own listener and
  node-to-node credentials (mTLS), so a client cannot call `Forward` or `ReadCached`.
- **Endpoint discovery.** Each node publishes `flightEndpoint` in its `clusterNodes` document.
  Callers resolve partition → `partitionLease.owner` → endpoint, and cache the result.

### Wire format

After the receive stage, data is **Arrow end to end**. The client's protobuf is converted exactly
once, on the receiving node, and never again.

```
client ──protobuf ingestSamples──▶ receive stage: validate, convert once to Arrow
   jetstream: Arrow IPC stream as the NATS message payload ─┐
   local:     Arrow IPC stream over Flight DoPut (Forward) ──┴─▶ owner
owner: append batches to per-PV builders (RAM) · WAL and IPC segments keep the same bytes
       · Parquet written from Arrow
query node ◀── Arrow over Flight DoGet (ReadCached) ── owner cache
query client ◀── Arrow Flight, or protobuf through the querySamples adapter
```

- **One record batch per PV block.** A batch holds the samples of one PV, from a single sample up
  to thousands. Its columns follow the sample layout below: `ts`, `ver`, the one typed value
  column the PV uses, and `attrs`. The PV name, `providerId`, `sourceSeq`, and lane travel
  as batch metadata. One PV per batch keeps a single type per value column, with no union types,
  and lets the owner append the batch straight into that PV's builder.
- **One IPC stream per owning partition.** The receive stage groups the batches of every PV that
  maps to the same partition into one Arrow IPC stream, which can carry several PVs, and sends it
  as one NATS message or one `DoPut` write. It is cut at about 1 MB or a few milliseconds.

  ```
  node-2 receives: BPM:01:X ×3, MAG:Q1:I ×1, BPM:07:Y ×500
  placement:       BPM:01:X, MAG:Q1:I → p-0001 (node-1);  BPM:07:Y → p-0004 (node-2 itself)
  → p-0001: one IPC stream [ batch(BPM:01:X, 3 rows), batch(MAG:Q1:I, 1 row) ]
  → p-0004: local append, no network
  ```

- **Same bytes in both log modes.** The NATS payload and the `DoPut` stream carry the same IPC
  format, so the owner decodes both with the same code. In local mode the owner appends the
  received IPC bytes to its WAL unchanged, and replay reads them back as record batches.
- **A single sample costs a one-row batch.** That is correct but has a fixed per-batch overhead,
  which the per-partition grouping amortizes across PVs.
- Protobuf remains only at the edges: the client `ingestSamples` request, and the optional
  `querySamples` adapter.

### Same PV from several sources

Several connections may legitimately send the same PV (redundant IOCs, a re-sent backfill). The
writer sees both in one buffer, so the conflict is resolvable. **Decided policy: last write wins.**
A sample's identity is `(pvName, timestamp)`. When two samples share it, the one with the higher
write version `ver` (see "Ingest log") is kept as a correction of the other, in every tier: the seal sort, the query
merge, and compaction. An identical value is a no-op. Every overwrite with a different value is
counted in a metric (`dp.ingest.overwrites`, labeled by service only, per the closed attribute
vocabulary), and the overwritten value's `providerId` is logged, so a misconfigured duplicate
source is visible rather than silent.

## Scope: a new service, written from scratch

**Decided:** ingestion and query are rewritten from scratch as one new service, not adapted from
the current code. The same service also absorbs the annotation domain: annotations, datasets,
calculations, PV metadata, configurations and configuration activations, and sample statuses.

- **Why together.** The query engine needs PV metadata (wide-table column metadata),
  configuration activations (time selectors), and sample statuses (row filters) on the same
  read path as the data. Inside one service they are local lookups instead of calls across
  services, and one lease and partition model covers everything.
- **Storage is a backend detail.** Clients never see Mongo buckets, Parquet files, or partitions.
  The writer flushes through a `ColdStore` interface. The first implementation may write Mongo
  buckets, managed entirely by the backend (bucket size and span chosen by the writer, `pvStats`
  maintained, indexes created). Parquet is the target implementation. Switching changes only the
  `ColdStore` behind the writer and the cold reader behind the planner.
- **Caches from the start.** The hot cache, late cache, commit bookkeeping, and query planner are
  part of the first data-path release, whichever cold store sits behind them.
- **Invariants to carry over.** The current code's hard-won rules are requirements for the new
  service, not legacy: the closed metrics attribute vocabulary (#212), reject vs error
  classification (#235), blank-criterion guards (#243), the `pvStats` span bound if Mongo buckets
  are the cold store (#232), time-sliced pages accepted whole (#274), outbound flow control (#274),
  fail-closed schema migration (#254). CLAUDE.md is the checklist.
- **Mongo-bucket idempotency.** If Mongo buckets are the cold store, the commit must still be
  atomic. Bucket `_id` is derived from `(partition, first seq, pvName)` so a replayed flush produces
  the same ids and the insert ignores duplicate keys. The `committedRanges` CAS follows the insert.

## Ingest log: NATS optional

The receive and writer stages never call NATS directly. They use an `IngestLog` interface with two
implementations, chosen by config (`ingestLog: jetstream | local`). Everything above the
interface is the same in both modes: placement, caches, manifests, commit bookkeeping, the query
planner, and the merge.

### The interface

| Operation | Contract |
|---|---|
| `append(partition, lane, batch) → seq` | Returns only when the batch is durable. The receive stage acks the client after it returns. |
| `read(partition, lane, fromSeq)` | Ordered stream of `(seq, batch)` for one partition and lane. |
| `release(partition, lane, seqs)` | The rows are committed. The log may discard them. |
| `uncommitted(partition, lane)` | Everything appended and not released, for replay after a takeover. |

`seq` is a log position **per partition and lane**. It is used for commit bookkeeping
(`committedRanges`) and replay, and **never for ordering writes across partitions**.

### Ordering without a global stream: the write version

Last write wins previously relied on the single JetStream stream's sequence being global. A local
log has no global sequence, so ordering moves to a value both modes produce the same way:

- The receive stage stamps every block with a **write version** `ver`, a hybrid logical clock
  `(physical ns, logical counter, nodeId)`. It is monotonic per node, and close to real time
  across nodes.
- Every tier resolves the same `(pvName, ts)` by the highest `ver`: the seal sort, the query
  merge, and compaction. `ver` is stored as a column in caches and Parquet files.
- Clock skew between nodes only matters for two writes to the same `(pvName, ts)` that arrive
  within the skew. That case is a correction race with no meaningful winner anyway. The HLC
  guarantees that a correction sent after a client saw the original wins, because the client's
  later request reaches a node whose clock has passed the original `ver` (the client echoes the
  highest `ver` it was acked, and the receiving node advances its clock past it).

With `ver` carrying the order, JetStream no longer has to be one stream. It can be split per lane
or per partition range for throughput.

### Mode `jetstream`

As described in Part 2: replicated, durable across node loss, takeover by replay from the stream,
cluster-wide subscriptions through JetStream consumers.

### Mode `local`

No broker. Each writer keeps a **local WAL per partition and lane** (segment files, fsync on
append with group commit, a few ms of batching).

- **Routing.** The receive stage forwards each batch to the partition owner through the internal
  node service's `Forward` operation (in-process when the owner is the receiving node). `append` returns after the owner's WAL
  fsync. This keeps one partition per PV at any moment, exactly as in `jetstream` mode.
- **Back-pressure.** The forwarding stream uses gRPC flow control. A slow owner pauses the
  receiving node, which pauses the client.
- **Owner unreachable.** The receive stage rejects the block with a retryable error. It never writes the
  block somewhere else, which would place one PV in two partitions without a placement change.
- **Takeover.** The WAL lives on the owner's disk, so a crashed owner's uncommitted rows are
  recoverable only from that disk. Two options, by deployment:
  - **WAL on shared storage** (NFS, a replicated volume): the new owner opens the dead owner's
    WAL for those partitions and replays it. The lease epoch fences the old owner if it returns.
  - **WAL on local disk:** the partition stays unowned until its node returns. While it is
    unavailable, new PVs are not placed on it, ingest for its PVs is rejected (retryable), and
    queries whose cache summary overlaps it fail with an error rather than return partial data.
    The operator can force a takeover, accepting the loss of its uncommitted rows. That is logged
    and counted, never silent.
- **Durability statement.** `local` with local disk is durable across process crashes, not across
  disk loss. The service logs the mode at startup, as for single-replica NATS.
- **Subscriptions.** There is no shared log to consume, so a subscriber connects to the owners of
  the requested PVs' partitions through the internal node service's `Subscribe` operation. The owner pushes rows as they
  are appended. The query engine resolves owners the same way as for cached reads.

### Optional variant: write where received

In `local` mode a receiving node may write to a partition **it** owns, instead of forwarding
(`placement: receiver`). This drops the forwarding hop and makes ingest depend only on the
receiving node. A PV sent to several nodes then lives in several partitions at once, and its
`placement.history` holds several open entries. The read side already handles that through the
union rule and the `ver` ordering. The cost is more files per PV and window when several clients
send the same PV to different nodes. It suits sites where each PV has one client and a load
balancer keeps clients on one node.

### Choosing a mode

| Site | Mode |
|---|---|
| Single node, lab, test stand | `local` |
| Cluster with shared storage, no broker wanted | `local`, WAL on shared storage |
| Cluster needing durability across node loss and cluster-wide subscriptions | `jetstream`, R3 |

Switching mode is a restart with the logs drained: flush all caches so `uncommitted` is empty in
the old log, then start with the new one. Placement, manifests, and files are unchanged.

## Hot data visibility

Parquet files cannot be written per seal without producing small files, so recent data must be
queryable before it reaches Parquet. The writer therefore keeps a **hot cache** in two tiers (see
the figure):

- **T0, RAM double buffer.** Buffer A takes appends (per-PV Arrow builders); on seal (e.g. 64 MB or
  10 s) it swaps with buffer B, which is frozen, sorted by `(pvName, time)`, and still queryable
  while it is written out. A swap waits until B is durable on disk; meanwhile the JetStream pull
  pauses, giving back-pressure instead of a third buffer.
- **T1, Arrow IPC segments on the local FS.** Each sealed buffer becomes one immutable IPC segment
  (write tmp, fsync, rename, fsync dir), memory-mapped for reads, carrying its seq range, time range,
  and PV list. Segments live for the **hot window** H.
- **Flush to T2.** Independently of sealing, the flusher merges one window's segments into one large
  Parquet file, updates `pvStats`, and commits the manifest CAS adding its sequences to `committedRanges`. Hot window and
  flush window are separate settings: flushing every 10 min with a 1 h hot window gives large files
  and keeps recent queries off cold storage.
- **Release.** After the commit, the JetStream messages the file fully covers are acked and the window's
  segments marked obsolete. They are deleted once no query snapshot pins them (refcount).

**H, the hot window**, is one setting (decided: **1 hour**, so an IOC replaying its buffer after a
reconnect stays in the live lane and only real backfills go historical) with two uses: the receive stage's lane boundary (`ts ≥ now − H`
is live) and how long hot IPC segments stay on the owner. The flush window (e.g. 10 min) is a
separate, shorter setting.

Rules:

- The IPC segments are a **cache, not the durable copy**. A JetStream message is acked only after
  its rows are committed, so losing a writer's disk costs a JetStream replay, never data.
- A query reads the manifest snapshot first (files and `committedRanges = R`), then the Parquet
  files overlapping its range, then the owner's cached rows (hot and late) keeping only rows whose
  `seq ∉ R`, and merges by `(pvName, time)`. Each sample appears exactly once, even while a flush
  commits.

### Commit bookkeeping

A single `committedSeq` watermark is not enough. The live lane flushes every few minutes, while a
late window can stay cached for an hour, so a high watermark either stalls behind the oldest
cached late row or advances past it. If it advances, the query filter `seq > C` would hide that
cached late row, which is a silent wrong answer. The manifest therefore stores
`committedRanges`, the set of committed log positions as sorted, merged intervals. It stays
small because each lane commits mostly contiguous runs, and the intervals merge back together
once late windows commit. Each commit CAS adds the flushed rows' sequences to the set. Queries
and replay both test membership in the set.

Cached rows are read from the partition owner over Arrow Flight (see "Query engine"). A query over
only the recent window never touches Parquet.

Subscriptions (`SourceMonitorManager`) should consume from JetStream directly (an ephemeral consumer
on the relevant partition subjects), which gives every subscriber all PVs regardless of the
receiving node; today a subscription sees only data its own instance ingested.

### Late and historical data (initial idea)

A PV's samples can arrive with timestamps in the past: a reconnecting IOC replaying its buffer, a
backfill from an older archive, or a re-import that corrects data. The writer must accept them
without breaking the sort order of committed files or the exactly-once rule.

- **Ingestion does not care about time order.** The writer appends by sequence, not by time. A
  sealed buffer is sorted by `(pvName, ts)` whatever the arrival order, so a late sample is just a
  row with an old `ts` and a new `seq`.
- **Order in RAM.** The active buffer is append-only in arrival (log position) order, so
  ingestion never shifts data. Each per-PV builder tracks the last appended `ts` and a
  `sorted` flag. In the common case (one source per PV, monotone clock) the flag stays true and
  sealing costs nothing. An out-of-order append clears the flag, and the seal does a stable sort
  of that PV only, by `(ts, ver)`. A query reading the active buffer gets a sorted view of the
  same kind (sort on read for unsorted PVs; the buffer is bounded at a few seconds of data).
- **Two clients, reversed arrival.** Client 1 sends `ts=10:00:02`, client 2 sends `ts=10:00:01`,
  and client 1's message reaches JetStream first. Both land in the same partition and the same
  writer, so they get seq 100 and 101. Different `ts` means two distinct samples: both are kept
  and the seal sorts them to `10:00:01, 10:00:02`. If the earlier one arrives after the buffer
  sealed, it goes into the next buffer. Those two IPC segments overlap in time, which the query
  merge and the window flush both handle by merging the window's segments in `(pv, ts)` order.
  Only a sample older than the whole hot window takes the late-file path below. The same `ts`
  from both clients is a conflict, resolved by the policy in "Same PV from several sources".
- **Route by age on arrival.** The receive stage's lane classification (step 2) decides: rows
  with `ts ≥ now − H` take the live lane, and older rows take the historical lane into the
  partition's late cache (below), which
  writes them to Parquet as separate late files with their true `tMin/tMax`. They are never forced into the
  current window's file, which would make that file's time range span years and ruin pruning.
- **Separate late cache.** Late rows do not go through the hot double buffer. Each partition has a
  second cache, the **late cache**, keyed by cold time window (e.g. one day):
  `lateCache[partition][day] → per-PV Arrow builders`. Late data is usually sparse (a few samples
  spread over many days), so flushing it on the hot cadence would produce many tiny files. The
  late cache accumulates longer and flushes **one late file per window**, never one file mixing
  windows, so every file keeps a narrow `tMin/tMax`.
  - Flush a window when its rows reach a size target (e.g. 32 MB), when it is older than a max
    age (e.g. 1 h), or when the late cache's total memory hits its cap (flush the largest window
    first). Sealed late windows spill to IPC segments like hot ones.
  - Late rows are queryable while cached, with the same rule as hot rows (`seq ∉ committedRanges`,
    pinned by snapshot). The lease document lists cached late windows (`cache.lateWindows`), so the
    planner asks the writer only when the request overlaps the cache summary.
  - Durability costs JetStream space. A late row's message stays unacked until its day commits.
    `committedRanges` keeps that from holding back the live lane, and the stream's `max_bytes` must
    cover the late cache's maximum age at the expected backfill rate.
  - A late file has `kind: late` in the manifest. Compaction prefers windows with late files,
    merges them into the base file, and removes them, as described below.
- **Overlapping files are allowed, and the query handles them.** A late file overlaps older files
  of the same partition in time. The planner already selects files by `tMin/tMax` overlap, so it
  picks both, and the k-way merge on `(pvName, ts)` interleaves them. Correctness does not need
  non-overlapping files. Only read cost does.
- **Duplicates and corrections.** A late sample with the same `(pvName, ts)` as a stored one
  follows the last-write-wins policy in "Same PV from several sources": the higher `ver` wins. The
  losing row stays in its file until compaction removes it. The merge keeps one row per
  `(pvName, ts)`, so a query never shows both.
- **Background merge (compaction) restores locality.** A compactor picks a partition's time
  window that holds late files, rewrites those files plus the originals they overlap into one
  sorted file, applies the duplicate rule, and swaps them in with one manifest CAS, as in
  `parquet-atomic-flush-design.md`. It runs off the ingest path and can be throttled. Until it
  runs, queries are correct but read more files.
- **Discovery of cached late data.** Late rows stay in the late cache until their day's file
  commits. The lease document lists those days in `cache.lateWindows`, so a backfill of last year makes
  the planner ask the writer for queries on last year until it commits. That is correct and costs
  one Flight call.
- **Historical data: one endpoint, classified by the server.** There is no separate import RPC.
  Clients always use `ingestSamples`, and every sample follows the same path as live data:
  receive stage, log, partition owner, cache, Parquet. A historical sample is queryable as soon as
  it is acked, through the same planner and cache read. The server classifies each block by its
  timestamps, in the receive stage (step 2):
  - **Live:** `ts ≥ now − H` → live lane → hot double buffer.
  - **Historical:** `ts < now − H` → historical lane → late cache for its day. A dense backfill
    fills a day quickly and flushes at a larger size target (e.g. 128 MB). A day file built
    entirely from one sorted backfill becomes the day's base file when none exists yet, and
    needs no compaction.
  - **Isolation without a new endpoint.** Historical blocks travel on the internal subject
    `dp.ingest.h.<k>` (same partition as the live lane), consumed by the partition's second pull consumer with
    its own in-flight limit. A large backfill then cannot delay live acks, and back-pressure on it
    pauses only the client streams sending historical blocks. This is invisible in the public
    API, which keeps it free to change.
  - **Same commit, same result.** The same manifest CAS and `pvStats` update apply. A query
    returns identical rows whether the data arrived live or in a backfill.
- **Per-PV statistics.** `pvStats.maxBucketSpanSeconds` (#232) still only grows, so a backfill
  with long buckets widens the query bound for that PV, and nothing breaks. A late file updates
  `pvStats` before its commit, like any flush.

Open questions for this section are collected in Open decisions.

## Partition files and metadata

A partition is the unit that owns a cache and a stream of Parquet files. Each server owns one or
more partitions. Every partition keeps its own hot and late cache and writes its own files when a
size or age limit is reached.

### Partition ids

- A partition has a **stable id** (`p-0213`) that never encodes the owning server. The owner is a
  field on `partitionLease` and changes on failover or a move. An id such as `instance-2-p3` would
  be wrong after the first owner change, and it appears in the PV placement, file paths, subjects,
  and query plans.
- Ids are **allocated on demand**: a server that needs a new open partition takes the next id from
  an atomic counter (`clusterConfig.nextPartitionId`, `$inc`) and creates its lease, manifest, and
  JetStream consumers. Ids are never reused. There is no fixed partition count.
- A server can run with a single active partition, so it has one cache and one file stream. It
  claims more partitions as its open ones reach their caps.

### File layout and the manifest

```
<root>/p-0213/2026-10-31/01JB....parquet       base file, one per flush window
<root>/p-0213/2025-03-10/01JC....parquet       late file, one per cold day
```

The date folder is for people browsing storage. The **manifest is the only file list**, and
nothing lists storage to find files. One document per partition:

```js
partitionManifest {
  _id: "p-0213",
  version: 42, ownerEpoch: 7,            // CAS key and fencing token
  committedRanges: [[1,9100],[9200,9350]],
  lastSeq: { "<provider>|<pv>|<lane>": 1234, ... },   // per-source dedup watermarks
  files: [
    { id: "01JB...", path: "p-0213/2026-10-31/01JB....parquet", kind: "base" | "late",
      tMin, tMax,                        // time range of the file
      pvMin, pvMax,                      // name range; files are sorted by pvName
      pvs: { "BPM:01:X": { tMin, tMax, rows, rowGroups: [3, 4],
                           attrKeys: ["k1", "k2"], metadata: {...} }, ... },
      rows, bytes, crc32c }
  ]
}
```

- **Per-PV index.** `pvs` is what makes pruning effective. A partition's PVs are scattered over
  the name space, so `pvMin/pvMax` alone excludes little. The planner reads a file only if the
  requested PV is in `pvs` with an overlapping time range, and reads only its `rowGroups`.
- **Document size.** MongoDB caps a document at 16 MB. When a file holds more than a few thousand
  PVs, its `pvs` map moves to a `fileIndex` collection (`{fileId, pvName, tMin, tMax, rows,
  rowGroups}`, indexed on `(pvName, tMin)`), and the manifest keeps only the file-level fields.

### Row groups: mixed PVs, sorted

A row group holds several PVs. One row group per PV would create thousands of tiny row groups,
which bloats the footer, hurts compression, and adds per-row-group read overhead. Instead:

- Each file is sorted by `(pvName, ts)`, so every PV is one contiguous run of rows.
- Row groups are cut by size (64 to 128 MB). Low-rate PVs share a row group, and a high-rate PV
  fills whole row groups by itself.
- Column statistics and the page index are written for `pvName` and `ts`, so a reader skips row
  groups and pages outside the requested PV and range even without the manifest index.

Reading one PV from a mixed row group therefore costs close to reading a per-PV one.

### Sample layout: values and key/values

Caches and Parquet files hold many PVs of different types, each with its own key/values or none.
They all share **one fixed schema**. Sparseness is handled by nulls and encoding, never by schema
changes, so the column count never grows with the number of PVs or keys.

```
pv          dictionary<int32, utf8>
ts          int64 (ns)
ver         int64            write version
v_f64       float64          nullable ┐
v_f32       float32          nullable │
v_i64       int64            nullable │  typed value columns:
v_i32       int32            nullable │  exactly one is set per row,
v_bool      bool             nullable │  the one matching the PV's type
v_str       utf8             nullable │
v_enum      int32            nullable │
v_bin       binary           nullable │  images, structs, serialized, claim-check refs
v_f64_list  list<float64>    nullable │  arrays, one column per element type
...                                   ┘
attrs       list< struct< k:    dictionary<int32, utf8>,
                          i64:  int64   nullable,
                          f64:  float64 nullable,
                          str:  utf8    nullable,
                          bool: bool    nullable > >   nullable (null = no key/values)
```

**Typed value columns.**

- A row sets only its PV's column. Files are sorted by `(pv, ts)`, so each PV is one long run of
  rows and every column it does not use is one long null run, which Parquet run-length encodes
  to a few bytes. Each PV in effect pays only for its own column.
- Each column holds one type, so its min/max statistics are meaningful and prune row groups.
- The count is fixed by the type system (about 12 columns), never by the data.
- A PV that changes type simply fills another column from then on; the reader takes whichever
  column is set.
- Rejected: an Arrow union column (no Parquet equivalent, weak engine support) and separate files
  per type (more files per partition, against the large-file goal).

**Key/values: one list-of-struct column.**

```
row1  attrs = [ {k:"k1", i64:A}, {k:"k1", i64:B} ]     k1 repeated: multi-valued
row2  attrs = [ {k:"k1", i64:C}, {k:"k2", f64:D} ]     different keys, different types
row3  attrs = null                                     no key/values
```

- **List, not map**, so a repeated key is kept in order. In query results a key that appears once
  comes back as a value and a repeated key as a list (`k1: [A, B]`).
- **Struct with typed fields**, so every entry keeps its own type, and the same key may be an int
  in one sample and a float in another. Exactly one value field is set per entry.
- **No key/values is nearly free**: a null list costs a validity bit and an offset in Arrow and
  almost nothing in Parquet.
- **Different keys across PVs cost nothing extra.** Keys are data, not schema. Parquet stores the
  column as flat leaves (`attrs.k`, `attrs.i64`, `attrs.f64`, `attrs.str`, `attrs.bool`) plus
  repetition levels, and dictionary-encodes `attrs.k`, so a key repeated across rows costs a
  small code. Only nearly unique keys (an id inside the key name) defeat the dictionary; size then
  stays proportional to the data, and the fix is to move the varying part into the value.
- Rejected: one column per key. With keys that differ across many PVs the schema would grow
  without bound, every row group would carry a chunk per key column, footers and writer memory
  would grow with the key count, and files would disagree on schema.

**Promoted keys (opt-in, capped).** An operator may promote a key that is frequent across the
cluster, or that queries filter on, to its own nullable column (`attr_<key>`, typed by its
dominant type). The cap is about 8 keys cluster-wide, chosen by measured frequency, never per PV
and never automatically. The writer splits the entry out on append; entries of another type stay
in `attrs`. The reader merges promoted columns and `attrs` back into one `<pv>.attrs`, so clients
see no difference. Promotion applies to files written afterwards; older files keep the key in
`attrs`, and the reader handles both.

**Block metadata stays out of the rows.** `ColumnMetadata` (units, provenance, tags) is stored
once per PV per file, in the manifest's per-PV entry and the Parquet footer key/value metadata.

### Searching key/values

| Query shape | Speed |
|---|---|
| Filter inside a query that names PVs and a time range | Fast. PV and time pruning select a few row groups first; the engine then reads only `attrs.k` and the needed value leaf for those rows, and `k = "…"` is an integer compare on dictionary codes. The extra cost is about the size of those columns for the selected rows. |
| Search by key or value across PVs and history | Slow by default. Min/max statistics are useless on `attrs.k`, which mixes keys, so the engine scans the attribute leaves of every file in range. The cost grows with archive size: minutes on a multi-year archive, even at hundreds of millions of entries per second per core (an unmeasured estimate). |

Accelerators, cheapest first:

| Technique | Skips | Default |
|---|---|---|
| Key list per PV per file in the manifest (`pvs[pv].attrKeys`) | files where that PV never had the key | **always on** |
| Parquet Bloom filter on `attrs.k` | row groups without the key | **always on** |
| Parquet Bloom filters on `attrs.str` / `attrs.i64` | row groups without the value | opt-in, per key set |
| Promoted key column | row groups and pages by min/max | opt-in, about 8 keys max |
| Inverted index collection (`key, value → file, rowGroups`), written at flush | everything except matching row groups | opt-in, only for keys searched often |
| Sample Status API (#238) | designed for labels you filter on | use it for searchable labels |

Key/values are not a search index by default. Benchmark attribute-column scan throughput and
Bloom filter hit rates on real data before fixing defaults (see "Validation before committing").

### Cache metadata

The lease document carries what the planner needs to decide whether to ask the owner:

```js
partitionLease {
  _id: "p-0213",
  owner: "node-7f3a", epoch: 7, expiresAt,
  cache: {
    tMin, tMax,                // time range of every cached row (hot and late)
    lateWindows: ["2025-03-10"],
    pvBloom: <bytes>           // Bloom filter of cached PV names, rebuilt on each seal
  }
}
```

- The cache changes every few seconds, so the lease holds only a compact summary. Exact per-PV
  ranges come from the owner in the Flight response.
- A Bloom-filter false positive costs one unneeded Flight call that returns no rows. A false
  negative is impossible, because names are only added until their rows commit.

### What a query reads for one PV

```
pvMetadata.placement.history        → partitions ["p-0213", "p-0517"]
  partitionManifest p-0213          → files whose pvs contain the PV in range → read row groups directly
  partitionLease p-0213             → cache overlaps range and pvBloom hits? → Flight to owner
  (same for p-0517)
  → merge by (pvName, ts), last write wins on ver
```

Reading a PV from two partitions is not slower in itself: both reads run in parallel. The cost
grows with the number of files that hold few rows of the PV, which the history limits and
compaction keep small.

## Query engine: hot/cold planning and wide-table assembly

The query engine is a separate, stateless role (it can scale independently of ingestion).
Any node with the query role can take any request: it reads files itself and fetches cached rows
from their owners through `ReadCached`. For each
request it must answer three questions: which writer holds the hot data, which part of the
requested range is hot and which is cold, and how to turn many per-PV streams into one table.

```
request(PVs, [begin,end), extras)
   │
   ├─ 1 PV → partitions (every entry of cached pvMetadata.placement.history)
   ├─ 2 per partition: read lease doc  → owner, epoch, cache summary
   │                   read manifest   → snapshot (version v, committedRanges R), files
   ├─ 3 plan per partition:
   │      cold = manifest files whose per-PV index overlaps the request
   │      cached = ask owner only if the cache summary overlaps the request
   ├─ 4 execute in parallel:
   │      cold → read Parquet directly from storage (no writer involved)
   │      cached → ReadCached (Flight DoGet) to owner: (PVs, range, v, epoch) → rows with seq ∉ R
   ├─ 5 per-PV sorted streams → k-way merge on ts → wide Arrow record batches
   └─ 6 stream pages to client (byte budget, time-sliced)
```

### Discovery: who owns the cache, and what does it hold

The partition lease document (shape in "Partition files and metadata") is the routing entry.
Its `owner` resolves to a Flight endpoint through `clusterNodes`.

- The query engine caches the lease table and refreshes it from a change stream, falling back to a
  short TTL. A stale cache is safe because every hot request carries the epoch. A writer that no
  longer owns the partition answers `NOT_OWNER`, and the planner refreshes and retries.
- `cache.tMin/tMax`, `cache.lateWindows`, and `cache.pvBloom` let the planner skip the network
  call when the request does not overlap the cache, which is the common case for historical queries. They are pruning hints, kept
  conservative (a range only widens until its rows commit), not correctness boundaries.
- The split between hot and cold data is by **sequence, not time**: cold = files in snapshot v,
  cached = rows with `seq ∉ R`. Splitting by a time watermark would lose or duplicate data around late
  arrivals and around a flush that commits during the query.

### Consistency between the two reads

1. Read the manifest snapshot first, which gives (v, R).
2. Ask the owner for cached rows with `seq ∉ R`. The owner keeps obsolete segments for a grace period, so
   it can serve them even after a newer flush.
3. If the owner can no longer serve every row outside R (segments already deleted, or a takeover
   whose new owner replayed against a newer manifest), it returns `STALE_SNAPSHOT`. The
   planner rereads the manifest and retries that partition.
4. The owner serves `ReadCached` from its own thread pool with an in-flight limit (see "Internal
   node service"), so heavy queries cannot slow ingest. Over the limit it answers `BUSY`, and the
   planner retries with backoff.
5. If the owner is unreachable, the query returns an **error**. It never returns cold data alone.
   Missing hot rows would be a silent wrong answer, the same failure class as #232 and #274.

### Cold read: files directly, no writer involved

- The engine opens Parquet from object storage or NFS itself. It verifies crc32c and size from the
  manifest and fails on a mismatch.
- It uses projection (only requested columns) and predicate pushdown on `(pvName, ts)`. The
  manifest's per-PV row-group index (see "Partition files and metadata") tells it which row groups
  hold a PV, so it reads only those, then skips pages inside them through the Parquet page index.
- Compute is Arrow-native: the SQL engine's executor (see "SQL layer") runs scan, filter and merge,
  with no row-by-row conversion. Rows from the hot cache already arrive as Arrow over Flight, so both
  sources feed one execution plan.

### Iceberg: where it fits

Iceberg is the format for the **cold table only**, and the hot cache stays outside it.

- Adopting Iceberg instead of the custom manifest means any engine (Spark, Trino, DuckDB,
  pyiceberg) can read the archive directly, which matters for machine-learning users. The commit
  is still one atomic CAS, now on Iceberg's catalog pointer. Partitioning is by
  `(hash bucket of pvName, day)`, and the files keep the same sort order.
- The cost is that iceberg-cpp is less mature than the Java and Python implementations. Commit
  frequency is limited (each commit writes a metadata file and a manifest list), and that is
  acceptable only because flushes are large and infrequent.
- Recommendation: keep the custom manifest for the first version, and make it **Iceberg-shaped**
  (immutable files, snapshot id, per-file statistics) so that switching later only changes
  metadata and does not rewrite data. Decide when external engines need direct access. Either way
  dp-service still does the hot + cold merge, because no external engine can see the hot cache.

### Building the wide table

A request names many PVs plus extra data, and the result is one wide table.

| Column | Source |
|---|---|
| `timestamp` | one per row; which timestamps become rows depends on the row mode below |
| `<pv>` | value, typed as stored (scalar, list for arrays, binary for images/structs) |
| `<pv>.attrs` | per-sample K/V as `list<struct<k, typed value>>`, promoted keys merged back; emitted only if requested |
| `<pv>.status` | Sample Status join (#238), optional |
| schema field metadata | column metadata, units, `pvMetadata` description and tags |

Assembly:

- Internally every source is **long** (`pv, ts, ver, v_*, attrs`, the sample layout), sorted by `(pv, ts)`. Pivoting
  to wide is a k-way merge on `ts` across the per-PV streams, which needs no hash join and no
  global sort, because every input is already ordered.
- The result is **time-sliced**, not materialized. It keeps the #274 slice drain: a slice covers
  every PV and is accepted or discarded whole, its length adapts to the row target, and a byte
  budget ends the page. This lets a request for 4000 PVs over hours stream with bounded memory.
- **Row semantics: one row per timestamp.** The output has a single `timestamp` column and one
  column per PV. Every PV sample with that exact timestamp (nanosecond precision) lands in the same
  row. A PV with no sample at that instant gets null. The request picks how rows are formed:

  | Mode | Rows | Use |
  |---|---|---|
  | `UNION` (default) | every distinct ts of any PV; missing → null | lossless, same-clock PVs (one IOC, one SamplingClock) |
  | `INTERSECT` | only ts where **all** requested PVs have a sample | strictly simultaneous, e.g. synchronous BSA/beam-synchronous data |
  | `ASOF(tolerance)` | ts of a reference PV (or union); each other PV takes its latest sample ≤ ts within tolerance | different IOCs, slightly skewed clocks |
  | `RESAMPLE(period, agg)` | fixed grid; each cell = last/mean/min/max in the bin | ML features, plotting |

  ```
  UNION, ts in ns:
  timestamp            | BPM:01:X | BPM:01:Y | MAG:Q1:I
  10:00:00.000000000   |  0.12    |  -0.03   |  null
  10:00:00.001000000   |  0.13    |  -0.02   |  152.4
  10:00:00.001000400   |  null    |   null   |  152.5     ← different IOC, 400 ns skew
  ```

  Exact matching is cheap: the inputs are sorted, so a k-way merge emits one row per distinct ts
  and fills each column whose head equals it. `ASOF` and `RESAMPLE` run in the same merge (keep the
  last value per PV) and need no join. The value of `ASOF`/`RESAMPLE` is never invented outside
  tolerance: a cell with no qualifying sample stays null, and the response states the mode so a
  filled value is never mistaken for a measured one.
- Per-sample attributes follow the row: `<pv>.attrs` in a row belongs to the sample that filled
  `<pv>` in that row (under `ASOF`, the sample chosen; under `RESAMPLE`, omitted unless `agg=last`).
- Duplicate `(pv, ts)` rows from the cached/cold overlap cannot occur, because of the `seq ∉ R` rule.
  Same-`(pv, ts)` rows across tiers and partitions are resolved by last write wins (higher `ver`).
- Transport: Arrow Flight SQL is the main path for large tables, because it avoids per-value
  protobuf encoding. The existing gRPC query API stays as an adapter: translated to SQL, executed
  by the same engine, converted back to the current responses (see "SQL layer").

## SQL layer: parser, planner, executor

The query service gets one SQL engine, and **every query goes through it**. The existing gRPC
query API is translated to SQL and executed by the same engine, and SQL is also offered directly
as a new API. There is one planner and one executor, so the gRPC API and SQL can never disagree
on results.

```
gRPC querySamples / queryBuckets / queryTable ──translate──┐
Arrow Flight SQL (new, direct SQL)  ───────────────────────┼──▶ parse → bind → logical plan
                                                           │      → optimizer rules (DP-specific)
                                                           │      → physical plan → executor
                                                           │           scans: DpSampleScan (hot + cold)
                                                           │           joins: pvMetadata, activations, statuses
                                                           └──◀── Arrow record batches ──▶ adapter → gRPC response
```

### Engine choice

Use an existing embeddable SQL engine and extend it, rather than writing a parser and planner.
The DP-specific parts below (scan, rules, table functions, guards) are ours whatever the engine.

| | Apache DataFusion | DuckDB | Apache Calcite |
|---|---|---|---|
| Language | Rust | C++ | Java |
| Extending scans and rules | very open; it is built to be the core of other databases | table functions and extensions; optimizer less open | planner only, the executor is ours |
| Single-node speed | very good | usually the fastest | depends on our executor |
| Flight SQL server | framework in arrow-rs | not built in | not built in |
| Embedding in a multi-threaded server | designed for it (async streams on Tokio) | in-process, tuned for one embedded database | yes |

**Recommended: DataFusion, if the service is Rust.** InfluxDB 3.0 runs on the same stack (Arrow,
Parquet, Flight SQL, DataFusion), which shows the architecture works in production. What it gives
this design:

- **`TableProvider`** for `DpSampleScan`: it receives pushed-down filters, projection, and limit,
  and does placement, manifests, `ReadCached`, Parquet reads, and last write wins inside.
- **Pluggable logical and physical optimizer rules** for the mandatory rules below.
- **User-defined table, scalar, and aggregate functions** for `wide`, `during`, `attr`,
  `time_bucket`.
- **Parquet reader** with projection, row-group pruning by statistics and by our own index,
  page-index pruning, Bloom filters, and late materialization.
- **Streaming execution** as async record-batch streams, matching time-sliced pages and outbound
  flow control.
- **Memory pool per query and spilling** for sorts, aggregates, and joins, covering the resource
  guards.

Weak points to plan for:

- **Rust only.** From C++ or Java it would be an FFI dependency. Choosing DataFusion largely
  means choosing Rust (open decision 3).
- **Fast-moving API.** Major versions ship about monthly and extension APIs change between them.
  Pin a version and budget regular upgrades.
- **Simpler cost-based join planning** than Calcite or a mature database. Our queries are mostly
  scan, filter, pivot, and aggregate, and our own rules carry the decisions that matter.
- **As-of join** may not be built in; `ASOF` mode is our own operator inside `wide(...)` either way.
- **Nested types** work but are less polished than flat columns. The `attrs` list-of-struct
  column, its functions, and pushdown into its Parquet leaves need explicit tests.

Engine facts above are from current knowledge and must be re-checked against the release used.
If the service is C++, DuckDB is the fallback, with more work to customize planning.

### Engine spike before committing

Two weeks, Rust, DataFusion, to confirm the fit before phase 3:

1. `DpSampleScan` over a handful of Parquet files in the sample layout, with the per-PV row-group
   index, plus one fake owner serving cached rows over Flight.
2. The mandatory-time-bound reject rule and PV/time pushdown, verified with `EXPLAIN`.
3. `wide(...)` in `UNION` mode, and `attr()` over `attrs`.
4. A Flight SQL endpoint, queried from Python through ADBC.
5. Benchmark: 4000 PVs over one hour, cold only and cold plus cached, against the same query on
   the current service.

Exit criteria: the pushdown reaches the scan unmodified, the merge is correct across cached and
cold rows, and the benchmark is no slower than the current service for the same query.

### Tables exposed to SQL

| Table | Shape | Backed by |
|---|---|---|
| `samples` | long: `pv, ts, ver, value` (typed, see below), `attrs` | `DpSampleScan`: placement, partition manifests, cached rows over `ReadCached`, Parquet files, last-write-wins merge |
| `pv_metadata` | one row per PV: name, aliases, tags, attributes, description, units | `pvMetadata` collection |
| `configurations`, `configuration_activations` | as the annotation API | Mongo collections |
| `sample_statuses` | `pv, ts, domain, layer, status` | Sample Status storage (#238) |
| `annotations`, `datasets` | as the annotation API | Mongo collections |

`samples` exposes the typed value columns as one logical `value` column with a `value_type`, plus
the physical `v_f64`, `v_i64`, … for typed filters. Key/values are available as `attrs` and through
functions: `attr(attrs, 'k1')` (first value), `attr_all(attrs, 'k1')` (list), `has_attr(attrs,
'k1')`.

### DP-specific table functions

The wide table and row modes are not plain SQL, so they are table functions that the planner
implements with the k-way merge:

```sql
-- wide table, one row per timestamp
SELECT * FROM wide(
  pvs   => ['BPM:01:X', 'BPM:01:Y', 'MAG:Q1:I'],
  begin => TIMESTAMP '2026-10-31 10:00:00',
  end   => TIMESTAMP '2026-10-31 11:00:00',
  mode  => 'ASOF', tolerance => INTERVAL '1 ms');

-- PV selection through metadata, then data
SELECT s.pv, s.ts, s.value
FROM samples s JOIN pv_metadata m ON s.pv = m.pv_name
WHERE 'bpm' = ANY(m.tags)
  AND s.ts >= TIMESTAMP '2026-10-31 10:00' AND s.ts < TIMESTAMP '2026-10-31 10:05';

-- only while a configuration was active
SELECT * FROM samples
WHERE pv = 'BPM:01:X' AND during(ts, 'config:linac-tune');

-- aggregation runs in the engine, next to the data
SELECT pv, time_bucket(INTERVAL '1 s', ts) AS t, avg(value) FROM samples
WHERE pv LIKE 'BPM:%' AND ts >= now() - INTERVAL '1 hour'
GROUP BY pv, t;
```

`wide(...)` takes `mode => UNION | INTERSECT | ASOF | RESAMPLE` with the same semantics as
"Building the wide table". `during(ts, ...)` resolves configuration activations into time
fragments and keeps the #207 clamp invariant (fragments clamped once, used by both the scan bound
and the per-sample filter).

### Optimizer rules that make it fast

Generic SQL engines do not know the storage, so these rules are ours and are mandatory:

- **PV and time pushdown into `DpSampleScan`.** `pv = / IN / LIKE` and `ts` ranges, including
  those reached through a join with `pv_metadata`, become the scan's PV list and time range. The
  scan then prunes through placement, the per-PV row-group index, and the cache summary. A query
  that reaches the scan **without a time bound is rejected**, not run: an unbounded scan of the
  archive is the multi-minute query #232 exists to prevent.
- **Join reordering for metadata first.** A filter on `pv_metadata` runs first and its PV list is
  pushed into the scan (a dynamic filter), never a full scan joined afterwards.
- **Key/value pushdown.** `has_attr`/`attr` predicates use the per-file key lists, the `attrs.k`
  Bloom filter, and promoted columns when they exist.
- **Aggregation pushdown.** `count/min/max` on value columns can be answered from row-group
  statistics where the row group lies wholly inside the filter; partial aggregates can run on
  the owner for cached rows, so only results cross the network.
- **Merge stays in the scan.** Last write wins on `ver` and the `seq ∉ committedRanges`
  cached/cold split happen inside `DpSampleScan`, so no SQL a user writes can see a duplicate or a
  half-committed row.

### gRPC API translated to SQL

Each existing method maps to a fixed SQL template with bound parameters, never string
concatenation:

| gRPC method | SQL |
|---|---|
| `querySamples` (V2) | `SELECT * FROM wide(pvs => ?, begin => ?, end => ?, mode => 'UNION')` with selectors added as `during(...)` and status filters |
| `queryBuckets` (V2) | `SELECT pv, ts, value FROM samples WHERE pv IN (?) AND ts >= ? AND ts < ? ORDER BY pv, ts` returned as bucket-shaped batches |
| `queryTable` (V1) | `wide(...)` with the V1 column/row options |
| `queryPvMetadata` | `SELECT … FROM pv_metadata WHERE …` built from the criteria |

- The adapter converts Arrow batches to the existing protobuf responses, keeping their paging
  tokens, the #274 time-sliced pages accepted whole, and the reject vs error classification
  (#235). Translation rejects what it cannot express as `RESULT_STATUS_REJECT` before planning.
- The adapters carry the golden-test contract: the same request through the old service and the
  new SQL path must return identical results. This is how the rewrite is validated (phase 6).

### Direct SQL API

- **Transport: Arrow Flight SQL.** It is the standard SQL-over-Flight protocol, so JDBC and ADBC
  drivers, Python (`adbc_driver_flightsql`, pyarrow), DBeaver, and BI tools connect without a
  custom client. Results stream as Arrow record batches.
- **Read-only.** Only `SELECT`, `WITH`, `EXPLAIN`. DDL and DML are rejected at parse time. Writes
  stay on the ingestion and annotation APIs, which own validation and placement.
- **Prepared statements** with bound parameters are the recommended form, and the only one the
  client libraries emit.
- **`EXPLAIN`** shows the pruning outcome: partitions, files, row groups, owners asked. It is the
  user-facing counterpart of `MongoBucketQueryPlanTest`.

### Guards, since users now write arbitrary queries

- **Mandatory time bound** on every `samples` scan (above), and a maximum time span per query by
  config.
- **Resource limits per query:** memory, rows, bytes, wall time, and concurrent queries per user.
  Spilling to disk for large sorts and aggregates where the engine supports it; over the limit is
  a reject with the limit named, not an error.
- **Outbound flow control** (#274): the Flight SQL stream honors client readiness, and a stalled
  or cancelled client frees its worker.
- **Metrics:** the closed attribute vocabulary (#212) applies; the SQL text is never an attribute.
  Slow queries log the normalized SQL and the plan summary, like today's slow-query log.
- **Access control:** per-user row filters (e.g. hidden PV prefixes) are applied as a mandatory
  predicate on `pv_metadata` and `samples` by the binder, so no query text can bypass them.

## Deployment

One executable, `roles: [ingestion, query]` in config. Either or both per process. A site that
wants dedicated ingest front ends can run `ingestion` with `ownPartitions: false`: the node runs
the receive stage only and owns no partitions.

| Shape | Processes | Typical use |
|---|---|---|
| Single node | 1 process, all roles, `ingestLog: local` | lab, test stand, small facility, dev |
| Small cluster | 3 nodes, all roles each, NATS R3, or `local` with the WAL on shared storage | redundancy without role planning |
| Split | ingestion ×N, query ×K, NATS R3 (optionally some receive-only ingestion nodes) | large facility; scale ingest and query independently |

Rules that keep every shape the same system:

- **Roles talk through the same interfaces in every shape.** Receive stage to writer stage is
  always the `IngestLog` interface, and query to owner is always `ReadCached` on the internal node
  service (in-process when they share a process). There is
  no in-process shortcut that a split deployment lacks, so the single node exercises the same code
  paths as a cluster, and moving to a split is configuration, not a migration.
- **Single node is a cluster of one.** It holds every partition lease. An added node allocates
  its own partitions and receives new PVs first, and the rebalancer moves existing PVs gradually.
  No data moves: cold files are in shared storage and the JetStream stream is the replay source.
- **Durability depends on the log mode, not the role layout.** `local` on local disk and
  single-replica NATS both lose acked but uncommitted data if that disk dies. The service logs
  either at startup as a degraded durability mode. NATS R3, or `local` with the WAL on replicated
  shared storage, is required for the "ack means durable" guarantee across node loss.
- **No broker on a single node.** `local` mode makes a test stand one process with no NATS at all.
- **Per-role metrics.** `dp.service` stays the service attribute. Roles are distinguished by the
  existing `dp.job`/`rpc.method` attributes, so the closed vocabulary (#212) is unchanged.

## Cluster management

![Cluster topology: nodes, roles, ingest log, MongoDB cluster state, and shared storage](cluster-topology.svg)

*Figure: three nodes of the same binary. Nodes 1 and 2 run both roles and own partitions; node 3
runs only the query role. Receive stages append to the ingest log, writer stages pull their
own partitions and flush Parquet to shared storage, and query engines read files directly and
cached rows from owners over Flight. All nodes share the cluster state in MongoDB.*

MongoDB is the cluster's shared state. Every node reads and writes it, so no separate coordination
service (ZooKeeper, etcd, Raft) is needed. All coordination writes are single-document conditional
updates, which MongoDB makes atomic.

### Collections

```js
clusterNodes {            // one per running process
  _id: "node-7f3a",       // generated at start
  host, roles: ["ingestion","query"], ownPartitions: true,
  flightEndpoint: "10.0.1.7:8815",   // internal node service (Forward, ReadCached, Subscribe)
  grpcEndpoint: "10.0.1.7:50051",    // client-facing APIs
  startedAt, heartbeatAt, // renewed every 2 s
  load: { bytesPerSec, pvCount, partitions }, state: "ACTIVE" | "DRAINING"
}

partitionLease {          // one per partition, created when the id is allocated
  _id: "p-0213",
  owner: "node-7f3a", epoch: 7, expiresAt,     // lease, renewed every 2 s, TTL 10 s
  cache: {...}                                 // query routing summary (see Partition files and metadata)
}

partitionManifest {       // one per partition: file list and commit state (same section)
  _id: "p-0213", ...
}

clusterConfig {           // one document
  _id: "cluster", nextPartitionId: 214, hotWindowSec: 3600, openPartitionCap: {...}, ...
}

pvMetadata {              // existing catalog collection, one per PV
  pvName, ...,             // existing fields
  placement: {...}         // new server-owned routing field, see Placement policy
}
```

Cluster-wide settings in `clusterConfig` are read by every node at start. A node whose own config
disagrees refuses to start (the #254 fail-closed rule).

### Placement policy: new PVs to the least-loaded node, gradual moves later

Two rules, decided in review:

1. **No mass move on join.** When a node joins, no existing PV moves at once. Existing PVs move
   only through the rate-limited rebalancer (see "Moving a PV between partitions").
2. **A new PV goes to the least-loaded node.** The first time the cluster sees a PV, it is
   placed on a partition owned by the node with the lowest current load.

A pure hash cannot do this, because the hash ignores load and a new node would receive nothing.
Placement is therefore a **stored assignment**, made once per PV, and it lives in the existing
**`pvMetadata`** collection, which is already the PV catalog (unique `pvName` index, aliases,
tags, attributes, description). No second PV collection is added.

```js
pvMetadata {                       // existing document, one new server-owned field
  pvName: "BPM:NEW:X",             // unique index: the first insert wins any race
  aliases, tags, attributes, description, modifiedBy, createdAt, updatedAt,   // as today
  placement: {                     // new, written by the cluster, never by clients
    partition: 213, placedAt, placedBy: "node-7f3a",
    version, history: [ ... ]            // see "Moving a PV between partitions"
  }
}
```

Rules that come with reusing the collection:

- **`placement` is server-owned.** `savePvMetadata` is an upsert by `pvName` that replaces
  the client-editable fields. It must preserve `placement` the same way it preserves
  `createdAt`, and it must reject a request that tries to set it. Otherwise a client saving a
  description would silently erase the PV's routing.
- **Routing uses the canonical `pvName` only, never an alias.** An alias resolving to a
  different document would route one PV's samples to two partitions.
- **First sight creates the document.** A PV that arrives before anyone saved metadata for it
  gets a `pvMetadata` document holding only `pvName`, `createdAt`, and `placement`. Every
  ingested PV therefore appears in the catalog, which also answers "which PVs exist".
- **Index.** The existing unique `pvName` index serves both the lookup and the race. A new index
  on `placement.partition` lets a takeover or an operator list a partition's PVs.

Partitions are still the unit of ownership, leasing, files, and replay. A node owns a set of
partitions, allocated on demand, and new PVs fill its open ones.

### A new PV arrives: who gets it

```
receive stage gets a block for "BPM:NEW:X"
  1 local cache: pvName → partition?            hit → route to partition k         (normal case;
                                                 a stale entry after a move is harmless)
  2 miss → pvMetadata.findOne({pvName})          has placement → cache it, publish
  3 absent, or no placement → place it:
       node = least loaded ACTIVE partition owner (from clusterNodes load figures)
       k    = one of node's open partitions       (an owned partition below its cap)
       updateOne({pvName, placement: {$exists: false}},
                 {$set: {placement: {partition: k, ...}}, $setOnInsert: {createdAt}},
                 {upsert: true})
       → matched or inserted: we placed it
       → duplicate key, or matched 0 because placement now exists:
         someone else placed it first; read theirs and use it
  4 route to k: local append if this node owns k, else log subject or Forward to the owner
```

- **Race-free.** The unique `pvName` index plus the `placement: {$exists: false}` condition mean
  two nodes placing the same new PV at once cannot both succeed. The loser reads the winner's
  value, and every block of that PV goes to one partition.
- **Load figure.** Each partition owner publishes its load in its `clusterNodes` heartbeat: ingest bytes/s
  over the last minute plus PV count. The least-loaded choice is made on bytes/s, with PV count
  as the tiebreak. A brand-new PV has no rate yet, so this balances future load by current load,
  which is good enough when PVs arrive one by one. A burst of new PVs is spread round-robin
  across the lowest few nodes, so they do not all land on one node before its heartbeat shows
  the extra load.
- **Open partitions.** A node keeps a few partitions open for new PVs. When one reaches a cap (PV
  count or bytes/s), the node closes it to new PVs and allocates a fresh partition id. A new node joins
  with zero load, so it receives new PVs until it catches up with the others.
- **Cache.** Receive stages cache `pvName → partition`, and query engines cache `pvName → history`.
  Both are updated from the `pvMetadata` change stream. A process starts cold and fills the cache
  on demand. A stale receive-stage entry delays a move. A stale query entry could miss a partition, so
  the query engine compares the placement `version` it planned with against the version the
  writer reports and replans on a mismatch.
- **Failure modes.** If MongoDB is unreachable, known PVs keep flowing from the cache, and blocks
  for an unplaced PV are rejected with a retryable error. They are never routed by a fallback
  hash, which would put the PV in two partitions.

### Node join, crash, and drain

- **Join.** The node registers in `clusterNodes`, allocates a few new partitions as its open
  partitions, and takes no existing partition. New PVs start flowing to it immediately, because
  its load is lowest.
- **Crash.** A crashed node's leases expire after
  the TTL, and surviving nodes claim its partitions whole (least-loaded first), replaying
  uncommitted messages from JetStream. The PVs keep their partition, so `placement` is untouched
  and no cache goes stale. Only the partition's owner changes.
- **Planned drain.** The node sets `DRAINING`, stops receiving new PVs, flushes, and releases its
  leases. Other nodes claim those partitions the same way as after a crash, without the TTL wait.
- **Restart.** A node that comes back under the same identity before its leases expire renews
  them and resumes. After expiry, it rejoins as a new node.
- **Lease claim.** `updateOne({_id, $or: [{owner: null}, {expiresAt: {$lt: now}}]},
  {$set: {owner: me, expiresAt}, $inc: {epoch: 1}})`. A matched count of 1 means this node won.
  Nodes claim after a short random delay so a crash does not cause a stampede.

### Moving a PV between partitions

A PV may move to another partition, and therefore to another node, at any time: automatically
for balance or by an operator. Its data then lives in more than one partition's files. That is
allowed, under one rule that every component follows:

> **A PV's data is the union of every partition in its placement history.** Writers never copy
> data on a move. Readers always read every partition the PV has lived in, and the merge resolves
> overlaps by last write wins on the write version `ver`.

Because of that rule, a move needs no cutover point, no pause in ingest, and no data copy.

**Placement document after moves:**

```js
placement: {
  version: 3,                       // incremented by every move; CAS key
  partition: 913,                   // current: where receive stages route
  history: [                        // every partition that may hold data for this PV
    { partition: 213, from: T0, until: T1 },
    { partition: 517, from: T1, until: T2 },
    { partition: 913, from: T2 }    // open entry = current
  ],
  movedAt: T2, movedBy: "rebalancer@node-2c11"
}
```

`from`/`until` are the move times, used only as pruning hints. Data in an old partition is not
limited to that interval: a straggler or late sample can land there after `until`, and an old
entry is still read for any time range where its partition's files hold the PV (file PV lists
and `tMin/tMax` decide, not the interval).

**Move protocol:**

1. **Choose.** The rebalancer picks a PV and a target partition owned by a less-loaded node.
2. **Commit the move** with one CAS on the `pvMetadata` document:
   `updateOne({pvName, "placement.version": v}, {$set: {partition: new, version: v+1,
   movedAt}, $push: {history: ...}})`. A matched count of 0 means a concurrent move, so it aborts.
3. **Carry the dedup watermark.** The per-source `lastSeq` watermarks for that PV are copied to
   the target partition's manifest in the same step (or the target reads them from the source
   on first sight). Without this, a client retry of a block already committed in the old
   partition would be stored again in the new one. It would still be harmless (identical value,
   last-write-wins no-op), but it would cost space.
4. **Propagate.** Receive stages see the change through a change stream on `pvMetadata` and update
   their caches. No acknowledgment is needed.
5. **Stragglers are fine.** A receiving node with a stale cache keeps routing to the old partition
   until it sees the change. The old owner accepts those blocks as normal, and they are part of
   the PV's data because the old partition is in its history. No sample is lost or rejected.

**What each component does about moved PVs:**

| Component | Behavior |
|---|---|
| Receive stage | Routes to `placement.partition`. A stale value only delays the switch. |
| Writer stage | Accepts any PV that arrives on its partition. It does not check placement, because a straggler is valid data. |
| Query planner | Resolves PV → every partition in `history`, prunes by the per-PV file index and the cache summary, and reads all survivors, including both owners' caches during the move. |
| Merge | Deduplicates `(pv, ts)` across partitions by highest `ver`, which the receive stage stamps in both log modes. |
| Compaction | Works per partition. A day can hold the PV's rows in two partitions, and the merge handles that. |
| `pvStats` | Per PV, not per partition, so it is unaffected. |

**Ordering across a move.** The last-write-wins rule needs an order on writes to one `(pv, ts)`
across partitions. The write version `ver` provides it in both log modes (see "Ingest log").

**Keeping history bounded.** Every history entry is one more partition per query, so moves are
limited:

- **Minimum dwell time** per PV (e.g. 6 h) before it may move again, which prevents flapping.
- **Hysteresis.** Rebalance only when node load differs by more than a threshold (e.g. 25 %),
  and move the fewest PVs that close the gap, largest bytes/s first.
- **Rate limit.** At most one move per node per interval (e.g. 30 s), so a rebalance never
  floods change streams or caches.
- **History pruning.** An old entry is dropped when no committed file of its partition holds the
  PV and no cached row is pending there. A background **re-home** job may also rewrite an old
  partition's rows for a PV into the current partition's files (a compaction across
  partitions, committed with a CAS on both manifests in order: target first, then source).
  This is optional and runs only when a PV's history exceeds a cap (e.g. 4 entries).

**Who runs the rebalancer.** A single active instance, elected through a lease document
(`clusterLease {_id: "rebalancer", owner, epoch, expiresAt}`) on any node. Two rebalancers
would be safe anyway, because each move is a CAS on the placement version, but they would
waste moves.

**Node join, revisited.** Joining still moves nothing at once: new PVs go to the new node first
because its load is lowest. If load stays uneven after that, the rebalancer moves existing PVs
gradually under the limits above.

### Who the other roles talk to

- **Receive stages** need `clusterConfig`, the placement cache, the `clusterNodes` load figures
  (only to place new PVs), and, in local mode, owner endpoints for `Forward`. In JetStream mode
  they append to subjects, and JetStream delivers to whoever owns the partition.
- **Query engines** cache `partitionLease` and `clusterNodes` to resolve partition → owner →
  `flightEndpoint`. They refresh from a change stream and fall back to a short polling interval.
  A stale entry is safe, because the owner answers `NOT_OWNER` and the epoch check rejects it.
- **Operators** read the same collections: one query shows every node, its roles, and its
  partitions. An admin RPC can move a partition by asking its owner to release it with a target
  node hint.

## Alternatives rejected

| Alternative | Why not |
|---|---|
| Pure hash routing (`hash(pv) mod P`) | No lookup, but ignores load, so a new node would receive no PVs. Replaced in review by stored placement. |
| Instance number in the partition name (`instance-2-p3`) | Wrong after the first owner change, and the name is stored in placements, paths, and plans. Owner is a lease field instead. |
| One row group per PV | Thousands of tiny row groups: large footers, poor compression, per-group overhead. Mixed row groups sorted by PV with a per-PV index read almost as cheaply. |
| Separate PV placement collection | Duplicates the catalog `pvMetadata` already is; two PV records could disagree. |
| Client-side routing (client hashes PV to an instance) | Leaks topology into the API, breaks on rebalance, every client language reimplements it. |
| Separate gateway service | An extra deployable with no state of its own. Routing is a stage of the ingestion service instead, so any ingestion node receives any PV. |
| No routing: every node writes what it receives | Brings back problem 2 (one PV written by many nodes, many small files) and makes recent-data queries ask every node. Kept only as the opt-in `placement: receiver` variant of local mode. |
| One column per key/value key | Schema grows without bound when keys differ across PVs: a chunk per key column in every row group, large footers, writer memory per column, files with different schemas. Replaced by one `attrs` list-of-struct column plus at most ~8 promoted keys. |
| `map<string,string>` for key/values | Loses value types and drops repeated keys. Replaced by `list<struct<k, i64, f64, str, bool>>`. |
| One `value` union column | No Parquet union type, weak engine support. Replaced by fixed typed value columns. |
| One record batch mixing several PVs | Needs union-typed value columns and a split on the owner. One batch per PV, grouped per partition in one IPC stream, avoids both. |
| Kafka | Same model, works, but heavier to run (JVM, ZooKeeper/KRaft) for a facility install; NATS is a single Go binary, already common in controls environments. Keep the writer's log interface narrow so Kafka remains a swap-in. |
| Broker required in every deployment | Forces a NATS cluster on single-node and small sites that do not need it. Replaced by the `IngestLog` interface with `jetstream` and `local` modes. |
| Every instance writes, compaction merges later | Small files forever, cross-writer compaction conflicts on the manifest CAS, and duplicate resolution across files at read time. |
| Mongo change streams as the bus | Not designed for this rate; ties the bus to the storage being replaced. |

## Implementation phases

1. **API (dp-grpc).** New `ingestSamples`, `resolvePlacement` for owner-aware clients, and the
   Flight SQL query contract (tables, table functions, guards). The annotation, PV
   metadata, and configuration APIs move under the unified service, unchanged where possible. A
   client library splits multi-PV frames into blocks.
2. **New service core.** One binary with the ingestion role (receive and writer stages) and the
   query role, the internal node service, Arrow wire format end to end, and: JetStream with
   both lanes, placement on `pvMetadata`, on-demand partitions with leases and manifests, hot
   cache, late cache, commit bookkeeping, and the query planner with Flight reads of cached data.
   The rebalancer comes later in this phase, after the base path works. Cold storage is the first `ColdStore` implementation (see
   Open decisions).
3. **SQL layer.** Engine spike first (see "Engine spike before committing"), then the engine embedded, `DpSampleScan` and the metadata tables, pushdown rules, `wide`
   and `during` table functions, guards, Flight SQL endpoint, and the gRPC-to-SQL translation for
   the existing query methods.
4. **Unified domain.** Annotations, datasets, calculations, PV metadata, configurations and
   activations, and sample statuses, used by the query engine for column metadata, time
   selectors, and status filters.
5. **Parquet cold store** with compaction, if phase 2 started on Mongo buckets.
6. **Subscriptions on JetStream.** Cluster-wide subscriptions.
7. **Legacy adapters and migration.** `ingestData*` adapter on the new service, golden tests
   comparing old and new query results through the SQL translation, then migration of the existing
   archive and retirement of the old services.
8. **Iceberg cold table** (optional), once external engines need direct archive access.

Running N instances is safe for both performance and the API from phase 2 on.

## Validation before committing

- Benchmark both log modes: local WAL fsync with group commit, and JetStream R3, on facility hardware at the baseline (4000 PV × 1 kHz doubles, ≈ 64 MB/s
  with timestamps) and at 3× headroom, with coalesced ~1 MB messages. Measure publish latency p99
  and consumer lag.
- Kill-tests: kill an owner mid-flush and while late windows are cached, kill a receiving node
  mid-batch (with and without forwarding),
  partition the NATS cluster; assert no loss and no duplicates by `(pvName, timestamp)` count.
- Late-data test: interleave live ingest with a backfill and with out-of-order samples from two
  clients; assert every query, including those running across a flush, returns each sample once.
- Forwarding test: send every PV to a non-owner node; assert throughput and that no node writes a
  PV it does not own.
- Key/value test: PVs with no, repeated, typed, and widely differing keys; measure file size
  against a no-attribute baseline, attribute-leaf scan throughput, and `attrs.k` Bloom filter
  skip rate on a full-archive key search.
- SQL tests: every gRPC query method returns identical results through the translation; `EXPLAIN`
  pins pruning (PV, time, key/value pushdown) for the documented query shapes; an unbounded
  `samples` scan is rejected; resource limits reject rather than error.
- Rebalance test: add and remove ingestion nodes under load; measure takeover time (lease expiry +
  replay).

## Open decisions

Decided in review:

- Scope: a from-scratch unified service (ingestion, query, annotation domain), with caches in the
  first release and storage hidden behind `ColdStore`.
- API: a single `ingestSamples` endpoint for live and historical data, classified by the receive
  stage; owner-aware clients are an optional optimization.
- Hot window H: 1 hour.
- Conflicts: last write wins on `(pvName, ts)`.
- Placement: stored on `pvMetadata`; new PVs go to the least-loaded node; PVs may move, and a
  PV's data is the union of its partitions.
- Partitions: stable ids allocated on demand, one cache and one file stream each; the manifest is
  the file list, with per-PV row-group indexes; row groups mix PVs sorted by `(pvName, ts)`.
- Deployment: one binary with two roles, ingestion and query; no separate gateway service.
- Internal node service: one Flight service for `Forward`, `ReadCached`, `Subscribe`.
- Sample layout: fixed schema with typed value columns (one set per row) and one `attrs`
  `list<struct<k, i64, f64, str, bool>>` column; typed and repeatable keys; promoted keys opt-in,
  ~8 max; per-file key lists and an `attrs.k` Bloom filter always on, other search accelerators
  opt-in.
- SQL layer: one embedded SQL engine in the query service (DataFusion recommended if Rust); the gRPC query API is translated to SQL
  and executed by it; direct SQL over Arrow Flight SQL, read-only, with a mandatory time bound.
- Wire format: Arrow end to end after the receive stage; one record batch per PV, one IPC stream
  per owning partition, same bytes in both log modes.
- Ingest log: NATS optional behind `IngestLog` (`jetstream` or `local`); last write wins orders by
  the write version `ver` stamped by the receive stage, not by a log sequence.

1. Open-partition caps, lease TTL and heartbeat intervals.
2. Flush window length.
3. Implementation language for the new service (see `cpp-migration-analysis.md`,
   `rust-migration-analysis.md`, `cpp-columnar-storage-rationale.md`).
4. First cold store: Mongo buckets managed by the backend, or Parquet directly.
5. Custom Iceberg-shaped manifest vs real Iceberg for the cold table.
6. SQL engine: DataFusion (recommended if Rust), DuckDB, or Calcite, following the
   implementation language and the engine spike.
7. Default row mode (`UNION` proposed) and default `ASOF` tolerance; whether `INTERSECT` should use
   timing-system pulse ids instead of timestamps where available.
8. Late data: the compaction trigger (number of overlapping files per window, or ratio of rows
   read to rows returned), and the late cache's maximum age and memory cap.
9. Migration of the existing Mongo archive and annotation collections into the new service.
10. Rebalancer limits: dwell time, load threshold, move rate, history cap; whether re-home is needed
    in the first release.
11. Local-mode defaults: whether `placement: receiver` is allowed in the first release, and the
    WAL group-commit interval.
12. Threshold for moving a file's per-PV index to `fileIndex`, and the Bloom filter size per lease.
13. Promoted-key list and cap, and which keys get value Bloom filters or an inverted index.
14. SQL limits defaults (max time span, memory, rows, wall time, concurrency per user) and whether
    `samples` without a PV filter is allowed when the time span is small.
