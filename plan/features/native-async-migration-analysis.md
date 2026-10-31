# Analysis: moving dp-service (MLDP) to C++ or Rust, fully async

## Goal

Rewrite the three services (ingestion, query, annotation) in a native language, with an
end-to-end async model: gRPC handling, MongoDB access, and response streaming, with no
thread-per-request or blocking worker pools on the hot path.

## Current state (baseline)

| Item | Value |
|---|---|
| Main code | 329 files, ~47.7k LOC Java 21 |
| Test code | 150 files, ~51k LOC (unit + Failsafe ITs over gRPC) |
| Concurrency model | `QueueHandlerBase`: capacity-1 queue + fixed worker pool, jobs block on the **sync** Mongo driver |
| Async Mongo today | `MongoAsyncClient` / `MongoAsyncIngestionClient` (reactivestreams) exist but are off the production path and skip the schema version check |
| Streaming backpressure | hand-built `OutboundReadinessGate` over `ServerCallStreamObserver.isReady()` (#274) |
| Dependencies to replace | grpc-java, mongodb-driver-sync/reactivestreams, bson POJO codec, OpenTelemetry SDK + Prometheus/OTLP exporters, jhdf5 (HDF5), Apache POI (xlsx), fastcsv, snakeyaml, log4j2 |

Many of the invariants in `CLAUDE.md` exist to work around this model: unchecked exceptions
escaping into `QueueHandlerBase` and hanging the caller's stream, the POJO codec silently
dropping fields without getters/setters, `Instant`↔`Date` round-trips, a ready gate plus
cancel handler to stop slow readers filling the heap. A rewrite has to reproduce the
behavior these invariants protect. It does not need to reproduce the workarounds themselves.

## The deciding factor: async MongoDB

| | C++ | Rust |
|---|---|---|
| Official driver | `mongocxx` (on top of `libmongoc`) | `mongodb` crate (official, MongoDB Inc.) |
| Async API | **None.** Both mongocxx and libmongoc are synchronous/blocking. | **Native async** on tokio. The sync API is a wrapper around it. |
| How you get "async" | Blocking calls offloaded to a thread pool behind futures/coroutines. That is the current Java model with more steps. | `collection.find(filter).await`, with cursors as `Stream`s. Change streams, sessions/transactions, bulk writes, `$merge` aggregations, and hints are all supported. |
| BSON mapping | Manual builder/view API, no reflection mapping | `serde` derive. A missing field is a compile or decode error, never a silent drop. |

**C++ cannot meet the "all async including MongoDB" requirement with an official driver.** The
options are a blocking pool, which isn't async, or an unofficial or abandoned driver, which is
too risky for the archive that every service depends on. This requirement alone decides between
the two languages.

## Full ecosystem comparison

| Concern | C++ | Rust |
|---|---|---|
| gRPC server | grpc++ async: CompletionQueue or callback API. Verbose, easy to get wrong in lifetime and cancellation handling. | `tonic` on tokio/hyper. Streaming responses are `Stream`s, and HTTP/2 flow control gives backpressure for free, which replaces `OutboundReadinessGate`. Cancellation drops the future, so no parked worker. |
| Protobuf | protoc C++ | `prost` + `tonic-build` (generated from the same dp-grpc `.proto` files) |
| Mongo | sync only (see above) | async native |
| Telemetry | opentelemetry-cpp (usable, heavier build) | `opentelemetry` + `opentelemetry-prometheus`/OTLP, `tracing` |
| HDF5 export | libhdf5 C++ API (native) | `hdf5` crate (hdf5-metno, binds libhdf5). It's blocking, so it runs in `spawn_blocking`. Removes the jhdf5 vendoring problem. |
| xlsx export/import | OpenXLSX / libxlsxwriter | `rust_xlsxwriter` (write, streaming-capable), `calamine` (read) |
| CSV / YAML config | fine | `csv`, `serde_yaml`/`figment` |
| Error model | exceptions. The same "escaped exception hangs the stream" class remains possible. | `Result` everywhere. A dropped error is a compile warning. Panics in tasks surface as `JoinError` and must be handled once, centrally. |
| Memory/thread safety | manual; data races possible (cf. `QueryTelemetry` synchronization bug) | enforced at compile time (`Send`/`Sync`) |
| Build/deps | CMake + vcpkg/conan; grpc + mongocxx + otel builds are slow and fragile | cargo, with reproducible lockfiles |
| Team ramp-up | familiar to SLAC controls (EPICS) developers | steeper learning curve (ownership, async lifetimes) |

## Recommendation

**Rust** (tokio + tonic + prost + official `mongodb` crate + serde BSON).

- It is the only option with an officially supported async MongoDB driver.
- tonic streaming plus HTTP/2 flow control removes the hand-built readiness gate, the
  cancel-handler wake-up, and the "worker parked for 300 s" trade-off.
- `Result` and serde remove three recurring bug classes recorded in CLAUDE.md: swallowed unchecked
  exceptions that hang streams, silent POJO field drops, and BSON date type mismatches.
- C++ would give the same performance range, but only with blocking Mongo calls, a harder async
  gRPC API, and the same exception-escape risk.

### Alternative worth costing before committing

**Stay on Java 21 and go async in place.** Use the existing reactivestreams driver, or the sync
driver on virtual threads (`Executors.newVirtualThreadPerTaskExecutor()` replacing the
`QueueHandlerBase` pools). Virtual threads give async-equivalent scalability with the current
code and its roughly 100k LOC of tests intact. Weeks of work instead of a year. If the motivation
is concurrency and scalability, not memory footprint, GC pauses, or deployment size, this option
delivers most of the benefit at a small fraction of the risk.

## Risks specific to this codebase

1. **Stored-format compatibility.** Rust must read every existing document unchanged:
   - `_t` discriminators on column documents → serde `#[serde(tag = "_t")]` enums. Legacy
     `dataColumn` entries were stamped by migration v4.
   - Embedded protobuf `bytes` fields
   - `Date`-typed timestamps
   - The `pvStats` `_id` keying
   - The `serviceMetadata.schemaVersion` marker and claim protocol

   The migration runner must be ported with fail-closed semantics, or kept single-owner in Java
   during coexistence.
2. **Silent wrong-answer invariants.** These all fail as plausible wrong results, not errors, so
   a port can break them without any test failing loudly:
   - #232/#274: span bound, span-class partition, never caching spans on the read side
   - #207: fragment clamp shared by DB and assembly
   - #274: slice-based samples drain
   - #243/#245: blank criteria, match-all, bounded queries
   - `(pvName, firstTime)` sort with an index hint

   Each needs an explicit port checklist item.
3. **Plan-shape tests.** `MongoBucketQueryPlanTest` asserts `explain` output and must be ported.
   The filters have to be byte-for-byte equivalent in shape, not just semantically equal.
4. **Mixed-version operation.** Java and Rust services writing the same DB during the transition
   must agree on the `pvStats` `$max`-before-insert ordering and on tag normalization
   (`Locale.ROOT` lowercase → Rust `to_lowercase()` differs for some Unicode; must match Java's
   behavior or be pinned by test).
5. **Client libraries.** `com.ospreydcs.dp.client.*` (Java clients, `DataImportUtility`) are
   consumed by users and stay Java. Only the servers move.
6. **Effort.** About 48k LOC of service logic. A realistic estimate is 9–15 person-months for
   feature parity, with query and samples assembly as the largest part.

## Suggested migration strategy (strangler, per service)

The gRPC contract (dp-grpc) is the seam: clients cannot tell which language serves them.

1. **Conformance harness first.** Point the existing Java ITs (`src/test/integration`) at an
   external server address instead of the in-process server. They become the black-box
   acceptance suite for the Rust services. This is the most valuable step, and it should be done
   before writing any Rust.
2. **Shared Rust crates:**
   - `dp-proto` (tonic-build from dp-grpc)
   - `dp-bson` (document types plus round-trip tests against BSON dumps from the Java build)
   - `dp-telemetry` (same metric names and closed attribute vocabulary as `doc/metrics.md`)
   - `dp-config` (same YAML keys)
3. **Ingestion service first.** It has the smallest surface, a write path only, is
   throughput-bound, and benefits most from async bulk writes. It must keep the `pvStats` ordering
   invariant.
4. **Query service second.** It is the largest risk. Port the plan-shape test before the
   implementation.
5. **Annotation service last.** It has the most CRUD surface and HDF5/xlsx export, but the least
   performance pressure.
6. Retire Java servers one at a time. Migrations stay owned by the Java runner until the
   annotation service is ported, then move to a Rust runner with the same marker semantics.

## Decision summary

| Criterion | C++ | Rust | Java 21 (virtual threads/reactive) |
|---|---|---|---|
| Fully async incl. MongoDB | ✗ (no async driver) | ✓ | ✓ (reactive) / ≈ (virtual threads) |
| gRPC streaming backpressure | manual | built-in | manual (exists) |
| Safety against current bug classes | low | high | unchanged |
| Effort | very high | high | low |
| Risk to stored data / invariants | high | medium (with conformance suite) | low |

**Choose Rust if going native. Cost the Java virtual-thread option first if the driver is
scalability only.**

## Addendum: C++ with coroutines plus a dedicated database thread pool

Decision under consideration: accept that `mongocxx` is blocking, and isolate it behind a bounded
thread pool. Everything else stays async: gRPC, streaming, HDF5/xlsx assembly, telemetry.

### Shape

```
gRPC (callback API / asio) ──► coroutine per request on N io threads (N ≈ cores)
                                   │  co_await db.run([](mongocxx::client&){ ... })
                                   ▼
                              DB pool: M threads, each owns a mongocxx::client
                              taken from mongocxx::pool (M ≈ maxPoolSize)
                                   │  result posted back to the io executor
                                   ▼
                              coroutine resumes, streams the response with backpressure
```

- **One `mongocxx::instance` per process, one `mongocxx::pool`.** `mongocxx::client` is not thread
  safe, so each DB thread acquires its own client from the pool.
- **The coroutine never holds a cursor across a `co_await`.** A cursor belongs to one
  client/thread. Drain whole batches (or whole samples slices, #274) on the DB thread and hand
  back owned data. This matches the "accept or discard a slice whole" invariant.
- **Bounded queue in front of the pool**, with a deadline per job, and cancellation checked
  before a DB job starts. Queue wait and pool saturation are exported as metrics, the same role
  `queue.wait` and `workers.active` play today.
- **Exceptions are caught on the DB thread and returned in the result** (for example
  `std::expected<T, DbError>`). Nothing may escape a pool thread. This is the C++ equivalent of
  the #235 and #274 "never let an exception hang the stream" rule.

### Trade-offs against Rust/tokio

- Concurrency on database I/O is capped at M in-flight queries. In practice MongoDB's own
  connection pool caps it anyway (default `maxPoolSize` 100), so the real loss is small. The
  cost is one OS thread per connection rather than a scalability ceiling.
- Everything that is not the database scales on coroutines: request decode, response streaming
  to slow clients (no worker parked for 300 s), and export file writing.
- The `mongocxx` API is lower-level than serde mapping. Document types need hand-written
  to/from-BSON code with round-trip tests against BSON dumps from the Java build.

### Sizing

- **M (DB threads):** equal to `maxPoolSize`, starting at 32–64.
- **N (io threads):** equal to the core count.
- **HDF5 writes:** a separate small blocking pool, so exports cannot starve queries.
