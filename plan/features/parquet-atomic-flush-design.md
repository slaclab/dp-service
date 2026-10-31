# Design: atomic flush of the Arrow hot cache to Parquet

## Problem

Flushing hot Arrow data to Parquet involves three stores that cannot share a transaction:

1. the **WAL** (local disk)
2. the **Parquet file** (local disk or object storage)
3. the **catalog** (MongoDB)

A crash can happen between any two of these writes. The design has to guarantee:

- **No loss:** acknowledged data is always either in the WAL or in a committed file.
- **No duplicates:** no sample is visible twice, whether a crash happens during a flush or during
  compaction.
- **Consistent reads:** a query sees each sample exactly once across hot and cold data, even
  while a flush or compaction is running.
- **No orphans:** files left behind by a crash are found and removed.

## Core principles

1. **The catalog is the only source of visibility.** A file exists for readers only when a
   committed catalog entry references it. Files are never "made visible" by a rename or a listing.
2. **Files are immutable** and get unique names (ULID). A file is never overwritten or appended
   to, and a retried flush writes a new file.
3. **Every sample carries a WAL sequence number.** Each file records the sequence range it
   covers, which makes recovery idempotent.
4. **Each partition has a manifest with a version number.** Every change is one compare-and-swap
   (CAS) on that one MongoDB document. A single-document update is atomic in MongoDB, so no
   multi-document transaction is needed.

## Data model (MongoDB)

```js
// one document per partition (time window × PV group)
partitionManifest {
  _id: "2026-10-29T00/grp-017",
  version: 42,              // incremented on every commit (CAS key)
  ownerEpoch: 7,            // fencing token of the ingester that owns the partition
  committedSeq: 918273,     // highest WAL sequence durably in committed files
  files: [
    { id: "01J...", path: "…/01J....parquet", seqLo: 900001, seqHi: 918273,
      tMin, tMax, rows, bytes, crc32c, pvs: [...] }
  ]
}

// intent / garbage-collection record per file write
fileIntent {
  _id: "01J...", partition: "...", state: "PENDING" | "COMMITTED" | "OBSOLETE",
  createdAt, obsoleteAt
}
```

## Flush protocol

```
1. Freeze      : seal the partition's current Arrow buffer
                 (seq range [lo, hi]); new writes go to a fresh buffer
2. Intent      : insert fileIntent{_id: ulid, state: PENDING}
3. Write       : write Parquet to its final unique path
                 POSIX : write tmp → fsync(file) → rename → fsync(dir)
                 S3/GCS: single PUT (object PUT is atomic); use multipart only above the size limit
4. Verify      : record size and crc32c (optional: re-read the footer)
5. Commit (CAS): findOneAndUpdate(
                   {_id: partition, version: v, ownerEpoch: e},
                   {$push: {files: entry}, $set: {committedSeq: hi}, $inc: {version: 1}})
                 → no match: lost ownership or a concurrent change; abort,
                   leaving the intent PENDING for GC
6. Mark        : fileIntent.state = COMMITTED (best effort; GC also checks the manifest)
7. Release     : drop the sealed Arrow buffer; truncate WAL segments ≤ min committedSeq
                 over all partitions in the segment
```

### What happens on a crash at each step

| Crash after step | State | Recovery |
|---|---|---|
| 1–2 | Intent only | The WAL still holds the data, so replay rebuilds the buffer. GC deletes the stale intent. |
| 3–4 | File written, not in the manifest | Not visible. GC deletes the file and its PENDING intent after a timeout. The WAL replays the data. |
| 5 | Committed | Replay skips WAL entries ≤ `committedSeq`, so there are no duplicates. |
| 6–7 | Committed, WAL not truncated | Same as above. Truncation is retried later. |

**Why it is idempotent:** recovery replays only sequence numbers above the manifest's
`committedSeq`. The commit is the single point that decides whether data lives in the WAL or in a
file.

## Reading hot and cold data without gaps or duplicates

```
1. Read the manifest once (snapshot): files + committedSeq = C
2. Read the hot buffers, taking only rows with seq > C
3. Merge-scan the files and the hot rows ordered by (pvName, time)
```

- A flush that commits during the query does not matter. The query still uses its own
  snapshot (version, C), and the in-memory buffer stays until step 7. That needs one rule:
  **a sealed buffer is released only after no reader holds a snapshot older than its commit**.
  Use a reference count or an epoch-based reclamation scheme.
- Every reader works on one snapshot, so the result is consistent.

## Compaction (many files become one, atomically)

```
1. Snapshot the manifest at version v; pick small files F = {f1..fn}
2. Write the merged file g (new ULID, intent PENDING); sort by (pvName, time); dedupe by seq
3. CAS: {version: v} → files = files − F + g, version + 1
4. Mark F as OBSOLETE with obsoleteAt = now
5. GC deletes OBSOLETE files after a grace period > the longest query time
   (e.g. 1 h), so readers on an old snapshot never hit a missing file
```

`g.seqLo/seqHi` cover the union of F. A CAS failure means a concurrent commit happened. Retry
from step 1, or abandon (g stays PENDING and GC removes it).

## Multiple ingesters: ownership and fencing

- Each partition is owned by one ingester through a **lease** in MongoDB
  (`partitionLease {owner, epoch, expiresAt}`), renewed periodically.
- Taking over a lease increments `epoch`. Every commit CAS includes `ownerEpoch: e`, so a paused
  ("zombie") old owner cannot commit after losing the lease. Its write fails the CAS.
- The new owner replays the old owner's WAL from shared storage, or clients retry their unacked
  writes. Choose one model up front: with a node-local WAL, unflushed data is only recoverable
  when that node comes back, so use shared or replicated WAL storage if ingesters can fail over.

## Garbage collection (periodic, idempotent)

1. `fileIntent` with state PENDING and older than the timeout (e.g. 1 h), whose id is in no
   manifest: delete the file (ignore not-found), then the intent.
2. OBSOLETE files past the grace period: delete the file, then the intent.
3. Optional full sweep, monthly: list the storage prefix and remove objects with no intent and
   no manifest reference. This covers the case where step 2 of the flush protocol was lost.

The GC never deletes a file referenced by any manifest. Check the manifest immediately before
deleting.

## Integrity

- Store `crc32c` and `bytes` per file in the manifest, and validate them on open. On a mismatch,
  return an **error** to the query, never silently skip the file: a skipped file is a silent wrong
  answer, the same failure class as #232.
- Parquet footer statistics (`tMin`/`tMax`, PV list) in the manifest allow pruning without
  opening files.
- Before trusting the manifest, a query checks that `files` is non-overlapping in seq ranges per
  partition. An overlap is a bug and must fail loudly.

## Why this is better than "temp file → catalog insert → make visible"

| Concern | Simple scheme | This design |
|---|---|---|
| Visibility | Two steps (catalog + rename) that can disagree | One CAS on one document |
| Duplicates after a crash | Possible (data replayed from the WAL plus already in a file) | Prevented by `committedSeq` |
| Compaction atomicity | Multiple catalog writes | One CAS swaps N files for 1 |
| Concurrent readers | Can see half-applied changes | Snapshot by manifest version |
| Zombie writers | Undetected | Fenced by `ownerEpoch` |
| Object storage (no rename) | Rename-based scheme breaks | Unique immutable PUT, so no rename is needed |
| Orphan cleanup | Requires a full listing | Precise through intents; full sweep only as a backstop |

This is the same model Apache Iceberg and Delta Lake use (immutable files, a versioned
manifest, an atomic pointer swap, snapshot reads), reduced to one MongoDB document per partition.
It needs no table-format library in C++.
