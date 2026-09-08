# Repack index publication outside the shared lock

## Hypothesis and profile

The bounded pack uploader previously held the shared indexer write lock while
serializing, compressing, encrypting, and uploading each completed index. Other
workers that had finished a pack upload waited for that lock, retaining their
I/O slots. A slow index PUT could therefore stall the whole repack pipeline.

The local baseline native sample captured workers in
`FileWriterHandle::index -> Indexer::add_with -> save_file` during a delayed
index PUT, with other upload workers blocked in the indexer lock acquisition.
The candidate sample shows index saves in `index_parallel` on multiple workers;
the indexer lock is released before serialization and backend I/O.

The implementation uses the existing upload workers. Each detaches a completed
index under the lock and saves it outside the lock while retaining its I/O
permit. No new queue, threads, or CLI knobs are needed. The existing worker join
and final partial-index flush still precede old-index and pack deletion.

## Reproducible workload

The fixture starts with 64 MiB of deterministic random input in 64 KiB files,
64-byte fixed chunks, compression level 1, and 1 MiB target data packs with no
growth. A second snapshot keeps alternate files, and the first snapshot is
forgotten. This forces mixed packs and repeated index publication during
repacking. Each trial clones the same retained seed and uses a fresh process.

Trials use fast repacking, a 128 MiB read buffer, and a 256 MiB pack upload
buffer. Local mode adds no delay. Latency mode adds 20 ms per pack GET, 60 ms per
pack PUT, and 500 ms per index PUT *during repacking*. Rebuild index PUTs are
not delayed. There is no shared network bandwidth model.

Each trial reads 104.98 MiB and writes 73.00 MiB of pack data, including the
substantial encryption/header overhead of very small chunks. Nine indexes are
saved during repacking, plus one during rebuild. A successful trial verifies
publication-after-pack-upload ordering and performs a full data check outside
the timed prune. Timing covers the whole prune, not just the repack phase.

This deliberately index-heavy fixture exposes lock contention. Its 64-byte
chunks publish indexes much more frequently per GiB than the fleet repository;
the measured speedup is not a prediction for Arc's long B2 repack. Larger blobs
and faster index PUTs reduce the benefit.

## Repeated results

Each row is the median of three fresh processes. Baseline/candidate order and
case order reverse on the middle repetition. All 18 trials passed the full
data check and publication-order assertions.

| Backend mode | Connections | Before seconds | After seconds | Before RSS MiB | After RSS MiB | Peak repack index PUTs before/after |
|---|---:|---:|---:|---:|---:|---|
| No delay | 5 | 3.002 | 2.649 | 575.4 | 556.4 | 1 / 1 |
| Delayed | 5 | 7.688 | 3.521 | 572.7 | 601.6 | 1 / 2 |
| Delayed | 10 | 7.821 | 3.623 | 573.8 | 584.9 | 1 / 2 |

The delayed-index workload improves by 2.18x at N=5 and 2.16x at N=10. The
no-delay case takes 11.8% less time. At N=5 the delayed run ranges are
7.593–7.768 seconds before and 3.482–3.559 seconds after. N=10 does not improve
on N=5 for this fixture; no concurrency default was changed.

RSS is the median sampled peak during prune and can miss brief peaks. The
N=5 delayed case adds approximately 29 MiB at the median; allocator and process
variation make RSS differences less stable than the elapsed-time result.
The optimization is retained. No losing code candidate required a revert.

For an individual run with one of the preserved benchmark executables:

```sh
python3 scripts/benchmark-prune.py --repack-index-heavy --size-mib 64 \
  --fast-repack --index-delay-ms 500 --cases local-5 latency-5 latency-10 \
  --repeats 3 --binary target/repack-index-comparison/candidate \
  --output target/repack-index-rerun
```

## Validation and resource bounds

The stalled-index test verifies that two workers can publish indexes
concurrently with only two total I/O slots, and that downloads backpressure
while both index uploads are stalled. It runs normal and fast repacking with a
3 MiB upload budget. An initial 2 MiB test budget admitted only one nominal
1 MiB pack plus overhead; that calibration was corrected before validation.

Index-failure tests verify worker error aggregation, preservation of old
indexes and packs, completion of all active backend calls before return, and
remaining-data integrity in normal and fast repacking. The complete validation
passed 201 core unit tests, 12 pipeline tests, and 24 existing prune cases.
The CLI compiles and Clippy passes with the pre-existing cache `manual_clamp`
lint excluded.

The connection and pack-body byte limits are unchanged. Detached indexes and
serialization/compression/encryption buffers can now exist on multiple workers
and remain outside the pack-body byte budget; this is not a total RSS limit.

## Artifacts

- Baseline harness commit: `c2cf27e` (production code still `628b7ee`).
- Optimization commit: `aa74e90`.
- Native samples: `target/repack-index-{baseline,candidate}-profile/`.
- Frozen binaries, source/build manifest, trial parameters, logs, and repeated
  measurements: `target/repack-index-comparison/`.
- Benchmark build: Cargo optimized test profile with debug information, not the
  release CLI. The repeated comparison excludes profiler overhead and does not
  overlap builds or other tests.

The retained seed repository and its ephemeral test key were removed after the
comparison. Native profiles, binaries, measurements, and validation logs remain.
No Arc process, installed binary, or fleet repository was changed by this work.
