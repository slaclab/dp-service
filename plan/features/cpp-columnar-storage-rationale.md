# Rationale: C++ for the columnar (Arrow + Parquet) data platform

## Scenario

- **Metadata only in MongoDB:** the PV catalog, `pvStats`, the Parquet file catalog, annotations,
  configurations, and sample statuses.
- **Bulk time-series data leaves MongoDB.** Ingestion buffers it in an **Arrow hot cache** (backed
  by a WAL) and flushes it to **Parquet** files partitioned by time window × PV group.
- **Queries merge hot Arrow batches with cold Parquet files**, then return the result over gRPC
  (dp-grpc) and, optionally, as Arrow IPC or Arrow Flight.
- **Everything is async.** Request handling runs on C++20 coroutines.

This document explains why C++ is the right implementation language for this scenario, and
records the conditions that choice depends on.

## 1. The MongoDB limitation stops mattering

The main argument against C++ in the earlier analysis (`cpp-migration-analysis.md`) was that
`mongocxx` has no async API. In this scenario MongoDB is off the data path:

- Metadata lookups are small: a catalog lookup, a `pvStats` read, a file-list resolution.
  Each is a sub-millisecond-to-millisecond point query returning kilobytes, not megabytes of
  buckets.
- A small DB thread pool (8–16 threads) behind `co_await db.run(...)` serves that load easily.
  The cost of "one OS thread per in-flight metadata query" is negligible.
- The heavy I/O is now Parquet and object-store reads, and Arrow C++ already does those
  asynchronously (`arrow::Future`, async `RecordBatchReader`, `arrow::fs` with async S3 reads,
  and the I/O thread pool).

The decisive weakness of C++ applied to a design where MongoDB carried the bulk data. In this
design it carries only metadata.

## 2. Arrow and Parquet C++ are the reference implementations

| Capability | Arrow/Parquet C++ |
|---|---|
| Arrow format | Reference implementation. New features usually land here first. |
| Parquet | The most complete writer and reader: page indexes, bloom filters, modular encryption, all encodings, statistics, predicate and column pushdown. |
| Compute kernels | `arrow::compute`: SIMD-vectorized filter, take, sort, aggregate, cast, temporal functions. |
| Query engine | **Acero**: streaming execution plans (scan → filter → project → aggregate → order by) over datasets. |
| Datasets | `arrow::dataset`: partitioned Parquet datasets, partition pruning, fragment-level statistics, async scanning. |
| Transport | **Arrow Flight** and Flight SQL servers in C++. |
| Filesystems | Local, S3, GCS, Azure, HDFS through `arrow::fs`. |

Most of the platform (columnar buffers, file format, scan, filter, transport) is a library
already maintained and tuned by the Apache Arrow project. The dp-service code becomes the
domain logic around it: the PV catalog, time-window semantics, the fragment clamp (#207), the
slice drain (#274), and annotations.

## 3. Sharing memory with the scientific Python ecosystem

SLAC users analyze data in Python: NumPy, pandas, PyArrow, Polars, Jupyter.

- **PyArrow is a binding over Arrow C++.** A C++ service, or a C++ client library exposed with
  pybind11 or nanobind, hands `arrow::Table` objects to Python **zero-copy** through the Arrow C
  Data Interface, in-process.
- The same C++ code can ship as both the server and a **Python extension** for local analysis
  of Parquet files: one implementation of time-window and PV semantics, used everywhere.
- Arrow Flight from the service lands directly in `pyarrow.flight` and pandas, with no
  protobuf decode on the client.

With Rust this path also exists (arrow-rs ↔ PyArrow through the C Data Interface, pyo3), but the
C++ route reuses the exact library PyArrow ships, with no format-version or feature skew
between server and client.

## 4. Fit with the accelerator controls ecosystem

- **EPICS** (base, PVAccess `pvxs`, Channel Access) is C/C++. An ingestion adapter that
  subscribes to PVs and writes Arrow buffers can link `pvxs` directly, in-process. There is no
  FFI layer or translation step.
- **HDF5** (export format) is a C library with an official C++ API. It drops the vendored
  `cisd:jhdf5` jar and avoids an FFI wrapper.
- The facility's controls engineers already write and maintain C++. Code review, on-call
  debugging, and long-term ownership stay inside the existing skill base. A Rust codebase would
  depend on a small group of Rust-capable maintainers.

## 5. Performance control at the data path

The data path is now CPU and memory-bandwidth bound (Parquet decode, merging hot and cold data,
time-range filtering, serialization), and C++ gives direct control over it:

- **Memory layout and allocation:** Arrow memory pools (jemalloc/mimalloc), arena allocation
  per request, and pre-sized builders for the hot cache.
- **SIMD:** Arrow kernels use runtime-dispatched AVX2/AVX-512. Custom kernels (for example a
  time-window merge) can use intrinsics or `xsimd` directly.
- **GPU path:** NVIDIA **libcudf**/RAPIDS is C++ and Arrow-compatible. Large-scale analytics
  (resampling, correlations across thousands of PVs) can be offloaded later without changing
  language.
- **Mature profiling and tooling:** perf, VTune, Valgrind/heaptrack, sanitizers (ASan, TSan,
  UBSan), and the same toolchain Arrow itself is tuned with.

The expected gain over the current Java/BSON/protobuf path is **5–20× on the data path**: no
per-sample boxing, no BSON document decode, vectorized scans, and zero-copy columnar handoff.
Memory footprint should also be several times smaller. These numbers must be confirmed by a
prototype benchmark (section 9).

## 6. Async model in C++

| Layer | Mechanism |
|---|---|
| gRPC | grpc++ callback API, or `asio-grpc` for coroutine integration |
| Request flow | C++20 coroutines on an `asio` executor (N threads ≈ cores) |
| Parquet / object-store I/O | Arrow async readers (`arrow::Future`), bridged into coroutines with a small awaitable adapter |
| MongoDB (metadata only) | Small bounded thread pool, `co_await db.run(...)`, one `mongocxx::client` per pool thread |
| WAL fsync | Dedicated I/O thread, or `io_uring` on Linux |
| CPU-heavy compute | Arrow's CPU thread pool, or a separate compute pool, so scans never block request handling |

## 7. What the C++ design must provide

These are the requirements that make the choice safe. Most are needed in any language.

1. **WAL before acknowledgment.** Ingest is appended and fsynced before the Arrow buffer is
   updated and the client is acked. On restart the WAL is replayed. This keeps today's guarantee
   that an acknowledged write is durable.
2. **Atomic flush commit.** Immutable files with unique names, a versioned manifest per
   partition with compare-and-swap commits in MongoDB, WAL sequence numbers for idempotent
   recovery, fencing tokens for multiple writers, and intent-based garbage collection. See
   `parquet-atomic-flush-design.md`. No mature Iceberg C++ exists, so this protocol is owned
   in-house.
3. **Compaction** of small Parquet files per partition, with a catalog swap in a single
   metadata transaction.
4. **Hot + cold merge** ordered by `(pvName, time)`. The #207 fragment clamp and the #274
   "accept or discard a slice whole" rule are preserved across the boundary.
5. **Error discipline.** Use `std::expected` / `arrow::Status` / `arrow::Result` at every
   boundary. No exception may escape a pool thread or coroutine into the executor. This is the
   C++ form of the rule that an escaped exception must never hang a stream (#235, #274).
6. **Sanitizer CI.** ASan, UBSan, and TSan builds run the full conformance suite. This replaces
   the compile-time guarantees a memory-safe language would give.
7. **Conformance suite.** The existing Java integration tests are pointed at the C++ servers as
   the black-box acceptance gate.

## 8. Honest trade-offs

| Area | Where Rust would be stronger | Mitigation in C++ |
|---|---|---|
| Memory and thread safety | Compile-time guarantees | Sanitizer CI, `std::expected`, clang-tidy, RAII-only ownership rules |
| Async MongoDB | Native driver | Irrelevant here: metadata-only traffic on a small pool |
| Embeddable SQL engine | DataFusion is more complete than Acero for SQL | The query shapes are fixed (time range × PV set), so Acero plans or hand-built Arrow scans are enough |
| Table format | delta-rs is mature, iceberg-rust is maturing | Own the commit protocol with the MongoDB catalog (section 7.2) |
| Build | cargo | CMake + vcpkg/conan with pinned versions, plus a prebuilt dependency container image for CI |

## 9. Validation before committing

Build a two-to-three-week prototype on production-like data:

1. Ingest a PV set into an Arrow buffer with a WAL, and flush it to partitioned Parquet.
2. Query time range × PV set through `arrow::dataset` and Acero, merging hot and cold data.
3. Compare against the current Java `querySamples` and `queryData` on the same data:
   - latency (p50 and p99)
   - throughput per core
   - memory use
   - bytes on disk compared with the `buckets` collection
4. Measure the Arrow → dp-grpc protobuf conversion cost separately. If it dominates, add an
   `arrow-ipc` column encoding or an Arrow Flight endpoint.

**Go/no-go:** C++ is justified if the prototype shows at least a 3× gain on the data path and
the commit and recovery protocol in section 7 holds under crash tests.

## Conclusion

Once bulk data moves to Arrow and Parquet, the platform is mostly columnar data processing, and
Arrow C++ is the reference implementation of that stack. MongoDB's lack of an async C++ driver
no longer matters, because only small metadata queries hit it, and a small pool handles those.

What tips the choice to C++ is:
- direct, zero-copy interoperability with PyArrow and the scientific Python stack
- in-process integration with EPICS (`pvxs`) and HDF5
- an upgrade path to GPU analytics (libcudf)
- alignment with the skills of the facility's controls engineers

The price is memory-safety discipline enforced by tooling rather than the compiler, and an
in-house file-commit protocol in place of a mature table format. Both are bounded, testable
engineering work.
