# Furies S3 restore benchmark

## Method

Production core `999f42c`, harness `6c1602b`, built in an isolated checkout with
the optimized test profile (opt-level 1). Uncommitted index edits are excluded.
No production backup repository is accessed or changed. The test uses the
existing dedicated `rustic-prune-bench` bucket on
`http://s3.lynden.vitalsoft.com:7480`.

One snapshot contains 4 GiB of deterministic incompressible data in 256 files of
16 MiB, using the default chunker, compression level 1 and fixed 128 MiB data
pack targets. Fixture backup uses the ordinary serial path. A full repository
data check runs after setup. Four restores then read exactly this same snapshot
with backend connection limits 5, 10, 10, 5. Every destination and local cache
starts empty; the S3 backend instance is recreated for each trial.

This is a cold *local* cache test. Fixture creation/checking and earlier trials
can warm the Ceph/server cache, which is not flushed. All trials share a process,
so allocator retention and prior verification can affect later RSS and lifetime
CPU percentages. This incompressible fixture does not characterize workloads
with substantial decompression, many tiny files, or selective sparse restores.

The total timer covers repository opening, index loading, tree lookup, restore
planning, file contents and metadata restoration. Snapshot identity comes from
the setup result. Preparation and transfer times are recorded separately; the
transfer timer includes final metadata application. The timer ends when the
restore API returns. Local writes are buffered filesystem writes without an
additional fsync barrier, so these numbers do not measure durable-media flush
completion. Exact file counts and byte-for-byte comparisons happen after each
timer. Verification does not reuse destination files for later restores.

Five-second macOS `sample` captures begin five seconds into each restore;
recorded timing windows identify captures outside restore. Process descriptors
and RSS are sampled under a 1,024-descriptor soft limit. The core currently uses
20 restore worker threads. Instrumented logical range-read concurrency is
measured outside OpenDAL's limiter and includes callers waiting for admission;
it must not be equated with active HTTP requests. Similarly, the range-byte
counter covers requests awaiting data and returned buffers until released, not
just allocated response bodies. Full metadata GETs are not counted as range GETs.

## Validation and artifacts

The 64 MiB smoke fixture passed its full repository check and all four byte-for-
byte restores, followed by prefix cleanup. All 15 ordinary pipeline tests passed
(four explicit benchmarks ignored), strict Clippy passed, and formatting and
Python syntax checks passed.

`target/furies-restore-build/` holds the frozen binary, source provenance and test
log. `target/furies-restore-4g/` holds parameters, raw logs, results, process
samples, stack captures, their windows and scoped cleanup confirmation. The
smoke artifacts are in `target/furies-restore-smoke/`. Credentials remain in the
workspace `.env` and are not included in these artifacts.

## Reproduction

```sh
uv run --offline --no-project --with boto3 python scripts/benchmark-restore-s3.py \
  --binary target/furies-restore-build/prune_pipeline --profile \
  --output target/furies-restore-new-run
```

Use `--size-mib 64` for a smoke test. Output directories must be new. Each run
creates and deletes only its own `bench-restore-<uuid>/` prefix. The dedicated
S3 user and bucket remain available for future tests.

## Results — September 8, 2026

| Trial | Connections | Preparation | Transfer + metadata | Total | Total throughput |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 5 | 0.355 s | 31.789 s | 32.144 s | 127.43 MiB/s |
| 2 | 10 | 0.333 s | 32.949 s | 33.281 s | 123.07 MiB/s |
| 3 | 10 | 0.336 s | 32.122 s | 32.458 s | 126.19 MiB/s |
| 4 | 5 | 0.299 s | 30.775 s | 31.073 s | 131.82 MiB/s |

Median total time was **31.61 s at five connections** and **32.87 s at ten**.
Median per-run throughput was **129.62 versus 124.63 MiB/s**. Doubling the
connection limit showed no benefit in this repeated test; ten was approximately
4% slower by median time. These four small, profiled trials are not a statistical
estimate for all S3 restores.

Every trial issued exactly 127 instrumented range reads totaling 4,096.204 MiB,
consistent with fetching all file data anew. Coalescing limits these ranges to
40 MiB. Peak logical read calls were 20 in every trial, including requests waiting
inside OpenDAL. Peak tracked range bytes were 704–728 MiB. Process RSS increased
from 1,281 MiB in trial 1 to 2,409 MiB in trial 4; allocator retention in this
shared-process experiment prevents attributing that rise to connection count.
Peak sampled descriptors were 28, with zero retries or descriptor errors.

The setup full repository data check and all four exact file-count/byte
comparisons passed. Only the expected config NotFound warning appeared during
fresh repository initialization, outside restore timing. Cleanup confirmed the
unique S3 prefix empty.

## Profile interpretation

All four captures started 5.11–5.43 seconds into the restore and finished well
before restore completion. All 20 restore workers appeared in the backend range-
read path. Across 302,200 worker observations:

- 297,731 (98.52%) were inside `OpenDALBackend::read_partial`.
- 2,217 (0.73%) were inside local `write_at`.
- 2,234 (0.74%) were inside `read_encrypted_from_partial`.

These are sampled worker states, not CPU utilization percentages. The backend
read stacks predominantly terminate in Tokio blocking waits / `pthread_cond_wait`;
they include time queued behind the backend limiter. They identify waiting in
the read path but cannot separate network delay, client admission, RGW latency
and OSD service time. The incompressible fixture also gives little information
about decompression-heavy restores.

Use five connections for this Furies workload; increasing concurrency is not a
supported optimization here. No production restore change was made. The next
useful diagnostic is raw S3 GET throughput alongside network and Ceph metrics,
with a compressible/mixed-file fixture if those workloads matter. There is no
profile evidence here to justify a CPU or local-write optimization.

The artifact directory includes `profile-analysis.json`, `summary.json` and the
analysis script, in addition to the raw captures and individual results.
