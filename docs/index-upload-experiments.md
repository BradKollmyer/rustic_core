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
