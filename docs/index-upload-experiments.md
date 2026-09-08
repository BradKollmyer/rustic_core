# Parallel index rebuild experiments

The local fixture backs up 64 MiB of deterministic data in 1 MiB files with
64-byte fixed chunks and 1 MiB data packs, then replaces the source with a
1 KiB marker and forgets the original snapshot. Prune keeps deletion marks and
uses `--max-repack 0`, so it rebuilds indexes without repacking data. The tiny
chunks make the index workload large without requiring a large data set.
Every successful trial checks index publication and runs a full data check.

```sh
python3 scripts/benchmark-prune.py --index-heavy --size-mib 64 \
  --repeats 3 --cases local-baseline latency-baseline \
  --seed target/index-upload-fixture-marked --output target/index-upload-baseline
```

`--index-delay-ms` defaults to 500 in latency mode. This is an injected delay
per index upload, not a measurement of B2. `--profile` captures native macOS
`sample` stacks. Timing comparisons should run without profiling. Trials use
fresh processes and clone the same seed; `--seed` retains a disposable test
repository and its key across builds, and must be cleaned up after the study.
The runner samples RSS and reports index counts, bytes, and peak concurrency.
It uses Cargo's optimized test profile, not the release CLI profile.

## Serial baseline

The initial profiled trial rebuilt 18 indexes totaling 36.06 MiB in 10.229 s,
with peak index concurrency 1 and sampled peak RSS 382.7 MiB. The stack sample
shows the producer blocked inside `Indexer::add_with` through the backend's
injected upload delay. The 18 injected delays account for 9 s of wall time.
Artifacts: `target/index-upload-baseline-profile/`.

The fixture asserts that at least five output indexes were uploaded. Simply
retaining half of the original source was insufficient: prune could leave most
indexes unchanged and the first calibration only rewrote one index. Replacing
the source with the marker makes the obsolete packs require deletion marks.

## Experiment 1: asynchronous index uploads

Commit `f51c131` adds a prune-only worker pool, enabled by `--parallel-repack`
or `--repack-connections`. The existing upload byte budget bounds admitted
serialized indexes. All rebuild workers are joined before pack repacking or
old-index deletion; worker failures retain their original errors and aggregate
failure details. The default synchronous path remains the comparison baseline.

The candidate profile shows upload waits on five `prune-index` workers while
the producer serializes subsequent indexes or waits for worker capacity. Its
final join is visible separately. Artifacts:
`target/index-upload-candidate-profile/`.

After other builds and tests finished, the final comparison ran three fresh
processes per setting without profiling. Every run cloned the same fixture,
uploaded 18 indexes totaling 36.02 MiB, and performed no pack GETs or PUTs.
All 18 trials passed publication-order checks and full data checks. Order was
reversed on the middle repetition. Values below are medians; RSS is the median
sampled process peak during prune, including preparation allocations still
resident in the process.

| Injected delay per index | Pipeline | Seconds | Peak RSS MiB | Peak index PUTs |
|---|---|---:|---:|---:|
| None | Serial | 0.818 | 380 | 1 |
| None | N=5 | 0.378 | 394 | 2 |
| None | N=10 | 0.374 | 411 | 2 |
| 500 ms | Serial | 10.539 | 384 | 1 |
| 500 ms | N=5 | 2.273 | 446 | 5 |
| 500 ms | N=10 | 1.251 | 505 | 10 |

The latency case improved by **4.64x at N=5** and **8.42x at N=10**. The
no-delay case improved by about **2.2x**, with compression/encryption work
moving off the producer thread. N=5 used about 62 MiB more sampled RSS than
serial in the latency case; N=10 used about 121 MiB more. These are local
workload results, not forecasts for B2 or a total-memory guarantee.

**Decision: retain experiment 1.** It improved both measured workloads. No
optimization candidate without a win was retained, so no optimization revert
was needed. The earlier calibration and profiling runs are kept as artifacts;
only `target/index-upload-final-comparison/` supplies the table above. That
directory also contains raw measurements, native test-binary/source provenance,
validation logs, and cleanup evidence. The disposable fixture directories and
test keys were removed after the comparisons.

Validation passed 201 core unit tests, 10 pipeline tests, and 24 existing prune
cases. Tests cover concurrent index failures, preservation of old indexes and
packs, the deletion/repack barriers, backend connection caps, stalled upload
backpressure, byte limits, and oversized-index rejection. Clippy passed with
the preexisting cache `manual_clamp` lint excluded, and the CLI compiles.
