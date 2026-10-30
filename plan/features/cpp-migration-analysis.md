# Analysis: moving dp-service (MLDP) to C++, fully async

## Goal

Rewrite the three services (ingestion, query, annotation) in C++ with an async model for gRPC
handling, response streaming, and request processing. MongoDB access is isolated behind a
dedicated thread pool, because no official async C++ MongoDB driver exists (see below).

## Current state (baseline)

| Item | Value |
|---|---|
| Main code | 329 files, ~47.7k LOC Java 21 |
| Test code | 150 files, ~51k LOC (unit + Failsafe ITs over gRPC) |
| Concurrency model | `QueueHandlerBase`: capacity-1 queue + fixed worker pool, jobs block on the **sync** Mongo driver |
| Streaming backpressure | hand-built `OutboundReadinessGate` over `ServerCallStreamObserver.isReady()` (#274) |
| Dependencies to replace | grpc-java, mongodb-driver-sync, bson POJO codec, OpenTelemetry SDK + Prometheus/OTLP exporters, jhdf5 (HDF5), Apache POI (xlsx), fastcsv, snakeyaml, log4j2 |

## MongoDB: no async driver

- `mongocxx`, and the `libmongoc` C driver under it, are **synchronous only**. Socket I/O happens
  inside the driver with blocking `recv()`/`send()` calls.
- A C++20 coroutine can only yield at a `co_await` on a non-blocking operation. Calling `mongocxx`
  directly from a coroutine blocks the whole event-loop thread and stalls every other task on it.
- A true async alternative would mean writing a custom wire-protocol client on asio: OP_MSG
  messages, SCRAM/x509 auth, TLS, replica-set discovery and failover, retryable writes, cursors,
  and sessions. That is months of work with no vendor support. **Rejected.**
- **Decision: coroutines for everything else, plus a bounded thread pool for database access.**

## Architecture

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

### Rules

1. **One `mongocxx::instance` per process, one `mongocxx::pool`.** `mongocxx::client` is not
   thread safe, so each DB thread acquires its own client.
2. **A cursor is never held across a `co_await`.** A cursor belongs to one client/thread. Drain
   whole batches (or whole samples slices, #274) on the DB thread and hand back owned data. This
   matches the "accept or discard a slice whole" invariant.
3. **Bounded queue in front of the pool**, with a deadline per job, and cancellation checked
   before a DB job starts. Export queue wait and pool saturation as metrics, the same role
   `queue.wait` and `workers.active` play today.
4. **No exception escapes a pool thread.** Return `std::expected<T, DbError>`. This is the C++
   equivalent of the #235 and #274 rule that an escaped exception must never hang a stream.
5. **Separate small blocking pool for HDF5/xlsx writes**, so exports cannot starve queries.

### Sizing

- **M (DB threads):** equal to `maxPoolSize`, starting at 32–64.
- **N (io threads):** equal to the core count.

## Library stack

| Concern | Choice | Notes |
|---|---|---|
| gRPC | grpc++ callback API (or asio-grpc for coroutine integration) | Verbose; lifetime and cancellation handling must be designed carefully. Backpressure is manual, like `OutboundReadinessGate` today. |
| Protobuf | protoc C++ from the dp-grpc `.proto` files | |
| MongoDB | `mongocxx` + `bsoncxx`, behind the DB pool | Each document type needs hand-written BSON mapping code. There is no reflection mapping. |
| Async runtime | C++20 coroutines on asio | |
| Telemetry | opentelemetry-cpp + Prometheus/OTLP exporters | Heavy build. |
| HDF5 | libhdf5 C++ API | Native, removes the jhdf5 vendoring. |
| xlsx | libxlsxwriter (write), OpenXLSX (read) | |
| CSV / YAML / logging | fast-cpp-csv-parser or hand-written, yaml-cpp, spdlog | |
| Build | CMake + vcpkg or conan | grpc + mongocxx + otel builds are slow and fragile. Pin versions. |

## Trade-offs

**Gains**
- Native performance and memory footprint, with no GC pauses.
- Request handling and slow streaming clients scale on coroutines. No worker is parked for 300 s.
- Familiar to SLAC controls (EPICS) developers.

**Costs**
- In-flight DB calls are capped at M. MongoDB's connection pool caps them anyway, so the real
  cost is one OS thread per connection, not a scaling ceiling.
- Exceptions remain. The "escaped exception hangs the stream" bug class needs strict discipline
  at every pool and coroutine boundary.
- No compile-time data-race checking (cf. the `QueryTelemetry` synchronization bug).
- Hand-written BSON mapping per document type. The POJO "silent field drop" bug turns into a
  "forgot to map a field" bug, so round-trip tests are mandatory.

## Risks specific to this codebase

1. **Stored-format compatibility.** C++ must read every existing document unchanged:
   - `_t` discriminators on column documents
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
4. **Mixed-version operation.** Java and C++ services writing the same DB must agree on the
   `pvStats` `$max`-before-insert ordering and on tag normalization (Java `Locale.ROOT`
   lowercasing).
5. **Client libraries.** `com.ospreydcs.dp.client.*` stays Java. Only the servers move.
6. **Effort.** About 48k LOC of service logic, estimated at 11–18 person-months for parity. That
   is higher than Rust because of the hand-written BSON mapping and async gRPC plumbing.

## Migration strategy (strangler, per service)

1. **Conformance harness first.** Point the existing Java ITs (`src/test/integration`) at an
   external server address. They become the black-box acceptance suite.
2. **Shared libraries:**
   - `dp_proto`
   - `dp_bson` (mapping plus round-trip tests against BSON dumps from the Java build)
   - `dp_db_pool`
   - `dp_telemetry` (same metric names and closed attribute vocabulary as `doc/metrics.md`)
   - `dp_config` (same YAML keys)
3. **Ingestion service first**, keeping the `pvStats` ordering invariant.
4. **Query service second.** Port the plan-shape test before the implementation.
5. **Annotation service last.**
6. The migration runner stays Java-owned until the annotation service is ported.
