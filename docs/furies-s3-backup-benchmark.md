# Furies S3 backup benchmark

## Method

This Darwin-host diagnostic uses the dedicated Furies bucket at
`http://s3.lynden.vitalsoft.com:7480`. Production core is pinned to `5ef673a`,
excluding the unrelated local index edits. The harness is `cff0753`, built in
an isolated checkout with the optimized test profile (opt-level 1), not the
release CLI. No production repository is accessed.

The source is 4 GiB of deterministic incompressible data in 256 files of 16 MiB.
The repository uses its default chunker, compression level 1, fixed 128 MiB data
pack targets, and a backend limit of five connections. This exercises ordinary
backup chunking, compression and encryption. Prune's `--parallel-repack` worker
pool and its upload-buffer setting do not apply to backup.

Three sequential stages run against one newly initialized repository:

1. Initial backup of all source files.
2. Unchanged backup of the same source.
3. Backup after rewriting every fourth file with fresh data (1 GiB / 64 files).

Each timer includes loading the blob index and performing backup through snapshot
publication. Source creation/mutation, repository initialization/opening and all
verification are outside the timers. The three stages share a process and an
on-disk cache. Allocations retained from earlier verification can affect later
RSS; these are not independent fresh-process memory comparisons.

After each stage, snapshot summary assertions verify the expected file counts,
zero backup errors, zero new file data for the unchanged stage, and exactly
1 GiB of new file data for the changed stage. A full repository data check then
runs, followed by restoration of that snapshot into an empty local directory
and byte-for-byte comparison of every source file. All three snapshots are kept
until cleanup, so the final full check covers historical data too.

Data-pack writes are counted separately from cacheable tree-pack writes. The
instrumentation measures concurrent logical backend pack writes, not internal
HTTP multipart requests. Process RSS, lifetime CPU percentage and descriptors
are sampled, with a 1,024-descriptor soft limit. Five-second macOS stack profiles
start after five seconds in the initial and changed stages. Their windows are
recorded so verification activity can be excluded. Profiled elapsed times are
diagnostic single runs, not repeated performance comparisons.

## Reproduction

Build and retain the integration-test executable with:

```sh
cargo test --locked -p rustic_core --test prune_pipeline --no-run
```

Then run, supplying the executable path reported by Cargo:

```sh
uv run --offline --no-project --with boto3 python scripts/benchmark-backup-s3.py \
  --binary /path/to/prune_pipeline --profile \
  --output target/furies-backup-new-run
```

`--size-mib 64` provides a small functional smoke test. The output directory must
not already exist. Credentials are read from the workspace `.env`; only a
unique `bench-backup-<uuid>/` prefix inside `rustic-prune-bench` may be used.
Cleanup deletes only that prefix. The dedicated user and bucket remain available.

## Harness validation

The 64 MiB smoke run passed all three full data checks and restore comparisons.
The unchanged stage added zero file data and zero packs; the changed stage
correctly reported one changed file, three unchanged files and 16 MiB added.
All 13 ordinary pipeline tests passed, as did strict Clippy, formatting and
Python syntax checks. Initializing the fresh repository emitted an expected
NotFound warning while probing for a nonexistent config; that was outside the
timed backup and was not a retry or backup failure.

## Results

Measured on September 8, 2026:

| Stage | New file data | Backup time | New-data throughput | Pack writes | Peak concurrent data writes |
| --- | ---: | ---: | ---: | ---: | ---: |
| Initial | 4,096 MiB | 289.50 s | 14.15 MiB/s | 33 | 1 |
| Unchanged | 0 | 0.22 s | — | 0 | 0 |
| 25% changed | 1,024 MiB | 88.19 s | 11.61 MiB/s | 9 | 1 |

The changed stage reported 64 changed files and 192 unchanged files. Both stages
that added data wrote one index; the unchanged backup wrote neither packs nor an
index. Snapshot publication is still included in the unchanged backup timer.
All stages reported zero backup errors.

All three full repository checks and byte-for-byte restores passed. The run
finished successfully and its unique S3 prefix was deleted and confirmed empty.
There were no retries or descriptor-exhaustion warnings. Sampled peak descriptor
counts were 41, 13 and 25 for initial, unchanged and changed stages. Peak RSS was
1,233, 1,299 and 1,789 MiB respectively, subject to the shared-process caveat
above. This small repository does not reproduce Arc's large cache workload.

## Profile findings

Both five-second stack captures found exactly one thread in the logical S3 pack
write path. Its stack was `OpenDALBackend::write_bytes` → blocking
`Operator::write_options` → Tokio wait → `pthread_cond_wait`, for all 3,427
observations of that thread in the initial capture and all 3,747 in the changed
capture. These counts describe sampled thread states, not CPU percentages or
fractions of the entire backup duration.

The captures started 5.16 seconds into the initial stage and 5.44 seconds into
the changed stage; both ended well before backup completion and verification.

Source workers were waiting in `Packer::add` on the channel handoff. Compression
and hashing appeared only briefly in these captures. This supports backend write
waiting as the pipeline bottleneck during the sampled intervals. It does not
distinguish network latency from RGW/OSD latency or prove the same bottleneck
throughout every phase.

The measured peak of one data-pack write spans each whole backup, not just its
profile window. It agrees with the ordinary backup path in
`crates/core/src/blob/packer.rs`: without a shared upload sender, the packer
creates its own `FileWriterHandle` actor with one writer. Raising the backend
connection limit alone does not enable parallel data-pack uploads for backup.

The next experiment is an opt-in bounded backup upload pool, tested against this
baseline on identical input with the same correctness checks. Repeat and
alternate baseline/candidate runs before claiming a speedup. Prune's previous
throughput numbers are not a direct comparison: prune used fast repacking,
different reads and different upload concurrency.

## Artifacts

`target/furies-backup-4g/` contains the run log, parameters, measured results,
process samples, native stack profiles and their timing windows, and prefix
cleanup confirmation. `target/furies-backup-build/source.json` records the
pinned source and build profile; `pipeline-tests.log` records harness regression
tests. `target/furies-backup-smoke/` contains the small functional run.
