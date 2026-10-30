# Analysis: moving dp-service (MLDP) to Rust, fully async

## Goal

Rewrite the three services (ingestion, query, annotation) in Rust with an end-to-end async
model: gRPC handling, MongoDB access, and response streaming, with no thread-per-request or
blocking worker pools on the hot path.

## Current state (baseline)

| Item | Value |
|---|---|
| Main code | 329 files, ~47.7k LOC Java 21 |
| Test code | 150 files, ~51k LOC (unit + Failsafe ITs over gRPC) |
| Concurrency model | `QueueHandlerBase`: capacity-1 queue + fixed worker pool, jobs block on the **sync** Mongo driver |
| Streaming backpressure | hand-built `OutboundReadinessGate` over `ServerCallStreamObserver.isReady()` (#274) |
| Dependencies to replace | grpc-java, mongodb-driver-sync, bson POJO codec, OpenTelemetry SDK + Prometheus/OTLP exporters, jhdf5 (HDF5), Apache POI (xlsx), fastcsv, snakeyaml, log4j2 |

## MongoDB: native async

- The official `mongodb` crate (maintained by MongoDB Inc.) is **async-native on tokio**. Its sync
  API is a wrapper around the async one.
- `collection.find(filter).await` really yields while the query is in flight, and cursors are
  `Stream`s. Sessions/transactions, bulk writes, `$merge` aggregations, hints, and `explain`
  are all supported.
- BSON mapping uses `serde` derive. A missing or mistyped field is a compile or decode error,
  never the silent drop the Java POJO codec produces.
- No database thread pool is needed. Concurrency is bounded by the driver's connection pool
  (`max_pool_size`) and by a semaphore per request class if needed.

## Architecture

```
tonic gRPC server (tokio multi-thread runtime, N workers ≈ cores)
   └─ async task per request
        ├─ mongodb async calls (.await yields)
        ├─ response Stream → HTTP/2 flow control gives backpressure
        └─ spawn_blocking for HDF5/xlsx writes only
```

### Rules

1. **Streaming responses are `Stream`s.** When a client is slow, HTTP/2 flow control stops
   polling the stream, which replaces `OutboundReadinessGate`. When a client cancels, the future
   is dropped, which replaces the cancel handler and the 300 s parked-worker trade-off. Count a
   dropped stream as `dp.outcome=abandoned` with a drop guard.
2. **Errors are `Result`.** Map them once to the wire status (reject vs error, #235). A panic in a
   task surfaces as `JoinError` and must be converted to an error response in one central place,
   so no request can hang.
3. **The samples slice drain (#274)** stays: a slice is drained from the cursor stream fully
   before it is accepted, and is discarded whole on a budget trip.
4. **Blocking libraries (libhdf5) run only in `spawn_blocking`**, with a bounded semaphore so
   exports cannot exhaust the blocking pool.
5. **Per-request state is not shared across requests** (the span-class holder rule, #232/#274).
   The borrow checker makes the per-request scope explicit.

## Library stack

| Concern | Crate | Notes |
|---|---|---|
| gRPC | `tonic` | Streaming plus flow control built in. |
| Protobuf | `prost` + `tonic-build` from the dp-grpc `.proto` files | |
| Runtime | `tokio` | |
| MongoDB | `mongodb` + `bson` (serde) | Use `#[serde(tag = "_t")]` enums for column discriminators. |
| Telemetry | `opentelemetry`, `opentelemetry-prometheus`/OTLP, `tracing` | Keep the metric names and closed attribute vocabulary from `doc/metrics.md`. |
| HDF5 | `hdf5` (hdf5-metno, binds libhdf5) | Blocking, so it runs in `spawn_blocking`. Removes the jhdf5 vendoring. |
| xlsx | `rust_xlsxwriter` (write), `calamine` (read) | |
| CSV / config / logging | `csv`, `serde_yaml` or `figment`, `tracing-subscriber` | |
| Build | cargo with a lockfile | Reproducible, fast CI. |

## Trade-offs

**Gains**
- The only fully async stack with an officially supported MongoDB driver.
- Removes three bug classes recorded in CLAUDE.md:
  - swallowed unchecked exceptions that hang streams (replaced by `Result`)
  - silent POJO field drops (replaced by serde)
  - BSON date type mismatches (typed `bson::DateTime`)
- Data races are caught at compile time (`Send`/`Sync`), the class of the `QueryTelemetry`
  synchronization bug.
- Native performance and memory footprint, with no GC pauses.

**Costs**
- Steeper team learning curve: ownership and async lifetimes.
- HDF5 binding coverage is narrower than the C/C++ API. Verify that the export layout
  (`dataColumnEncoding` attributes, nested groups) is reachable before committing.

## Risks specific to this codebase

1. **Stored-format compatibility.** Rust must read every existing document unchanged:
   - `_t` discriminators (legacy `dataColumn` stamped by migration v4)
   - Embedded protobuf `bytes` fields
   - BSON `Date` timestamps
   - `pvStats` `_id` keying
   - The `serviceMetadata.schemaVersion` marker and claim protocol (port it fail-closed, or keep
     it Java-owned during coexistence)
2. **Silent wrong-answer invariants.** These fail as plausible wrong results, not errors, so a
   port can break them without any test failing loudly. Each needs a port checklist item:
   - #232/#274: span bound, span-class partition, never caching spans on the read side
   - #207: fragment clamp shared by DB and assembly
   - #274: slice-based samples drain
   - #243/#245: blank criteria, match-all, bounded queries
   - `(pvName, firstTime)` sort with an index hint
3. **Plan-shape test.** `MongoBucketQueryPlanTest` asserts `explain` output, so the filters must
   match in shape, not only in meaning.
4. **Mixed-version operation.** Java and Rust services writing the same DB must agree on the
   `pvStats` `$max`-before-insert ordering. Rust `to_lowercase()` differs from Java
   `toLowerCase(Locale.ROOT)` for some Unicode, so pin tag normalization by test.
5. **Client libraries.** `com.ospreydcs.dp.client.*` stays Java. Only the servers move.
6. **Effort.** About 48k LOC of service logic, estimated at 9–15 person-months for parity, with
   query and samples assembly as the largest part.

## Migration strategy (strangler, per service)

1. **Conformance harness first.** Point the existing Java ITs (`src/test/integration`) at an
   external server address. They become the black-box acceptance suite.
2. **Shared crates:**
   - `dp-proto`
   - `dp-bson` (serde types plus round-trip tests against BSON dumps from the Java build)
   - `dp-telemetry`
   - `dp-config` (same YAML keys)
3. **Ingestion service first.** It benefits most from async bulk writes and must keep the
   `pvStats` ordering invariant.
4. **Query service second.** Port the plan-shape test before the implementation.
5. **Annotation service last.**
6. The migration runner stays Java-owned until the annotation service is ported, then moves to
   Rust with the same marker semantics.
