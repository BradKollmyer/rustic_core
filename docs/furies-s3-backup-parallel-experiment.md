# Furies S3 parallel backup experiment

## Change and validation

Candidate `3c5d8b1` adds opt-in `backup --parallel-uploads`. It reuses the
existing pack upload pool for data and tree packs, with at most five workers,
further capped by the backend's advertised connection limit. The default remains
the serial backup path. `--backup-upload-buffer` defaults to 1 GiB and bounds
completed pack bodies admitted to the upload pool; it is not a total process
memory limit. Source/compression buffers and the packs currently being built
also consume memory. Small budgets can reduce upload concurrency. Pack targets
are checked before reading source files; actual size, including final blobs,
headers and subsequent growth, is checked when reserving upload bytes.

The backup wrapper now forwards `connection_limit()`. Both packers are drained
and the upload pool joined before final index and snapshot publication. Worker
errors take precedence over secondary channel errors. A failed upload may leave
successfully written packs or indexes, but must not publish a snapshot.

All 15 pipeline tests passed, with three explicit benchmarks ignored. The new
backup tests exercise serial behavior, backend limits of one and three, an
8 MiB upload byte budget, full checks and restores, and injected pack/index
failures and an invalid buffer. Strict Clippy passed with CLI features enabled.
All four upload-pool unit tests also passed after `dbc5aec` updated the spawn
failure assertion for the shared worker name; this changes no production code.
The 64 MiB S3 smoke test passed initial, unchanged and changed backups, full
checks, byte-for-byte restores and scoped cleanup.

## Comparison method

Four isolated runs use the same frozen integration-test binary, built from
`3c5d8b1` in a detached checkout excluding unrelated local index changes.
Compilation uses the optimized test profile (opt-level 1), not the release CLI.
Only `parallel_uploads` changes between modes. Order: serial, parallel,
parallel, serial. Each run starts a new process, repository and local cache.

Each source is 2 GiB of the same deterministic incompressible bytes in 128 files
of 16 MiB. The default chunker, compression level 1, 128 MiB data-pack targets,
five backend connections and 1,024 descriptor soft limit match the earlier
backup diagnostic. Repositories have independently generated keys/configs.
Each run measures initial backup, unchanged backup, and a backup after replacing
32 files (512 MiB). Full repository checks and byte-for-byte restores run after
every stage, outside its timer. Earlier verification can affect later stages'
cache and resident memory. Native five-second stack samples run during initial
and changed backups in both modes; recorded windows identify any capture that
extends beyond a short backup stage.

Each timer includes index loading and backup through snapshot publication,
excluding source generation/mutation, repository opening and verification.
Rates describe new source bytes divided by elapsed backup time. This is a small
repeated diagnostic, not a large-repository or production B2 result.

Artifacts are under `target/furies-backup-ab-2g/`; build provenance and regression
results are under `target/furies-parallel-backup-build/`. Each trial removes only
its own unique prefix in the dedicated `rustic-prune-bench` bucket. Credentials
remain in the workspace `.env` and are not included in artifacts.

## Results — September 8, 2026

| Mode / repetition | Initial 2 GiB | Unchanged | Changed 512 MiB | Peak initial data PUTs |
| --- | ---: | ---: | ---: | ---: |
| Serial 1 | 78.33 s | 0.195 s | 24.72 s | 1 |
| Parallel 1 | 80.79 s | 0.186 s | 21.08 s | 5 |
| Parallel 2 | 78.61 s | 0.194 s | 19.76 s | 5 |
| Serial 2 | 79.43 s | 0.205 s | 22.02 s | 1 |

Initial median time was 78.88 s serial versus 79.70 s parallel: effectively no
improvement (parallel was 1.05% slower). Median per-run throughput was
25.97 versus 25.70 MiB/s. The earlier 4 GiB serial diagnostic was much slower;
it is not an appropriate baseline for this comparison.

Changed-file median time was 23.37 s serial versus 20.42 s parallel, a 12.62%
reduction. Both parallel observations were faster than both serial observations,
but there are only two measurements per mode. Median per-run changed-data
throughput was 21.98 versus 25.10 MiB/s. Initial-plus-changed median time improved
only 2.08%; the unchanged-stage difference is negligible.

Initial sampled peak RSS across repetitions was 1,191 MiB serial versus
1,371 MiB parallel. Changed-stage peaks were 1,571 versus 1,836 MiB, with the
shared-process/verification caveat above. Peak descriptor counts were 41 versus
45 across the full comparison. No retries or descriptor-exhaustion errors
occurred. Each fresh repository emitted one expected pre-timer config NotFound
warning. All 12 backup stages passed full checks and byte-for-byte restores;
all four trial prefixes were confirmed empty after cleanup.

## Profiles and decision

All eight five-second profile windows were fully inside timed backup stages.
Each serial capture showed one thread in `OpenDALBackend::write_bytes`. Each
parallel initial capture showed five, and each parallel changed capture showed
four. Full-run instrumentation independently confirmed one versus five initial
data PUTs, and one versus four changed-data PUTs. The workers wait beneath the
blocking OpenDAL write call; more concurrent writes did not increase sustained
initial-backup throughput here. Backend waiting alone therefore does not establish
that too few upload workers are the throughput bottleneck.

Retain the change as opt-in for the modest changed-file benefit; leave serial
backup as the default. This is not evidence of a general backup speedup, and it
does not establish what B2 or a different Ceph cluster would do. Further work
should isolate raw S3 PUT throughput and observe the network and RGW/OSD side
before increasing concurrency or optimizing CPU work. The raw data and profiles
are preserved in `analysis.json` and each trial directory for comparison.

## Reproduction

Using the candidate integration-test executable:

```sh
uv run --offline --no-project --with boto3 python scripts/benchmark-backup-s3.py \
  --binary target/furies-parallel-backup-build/prune_pipeline \
  --size-mib 2048 --profile --parallel-uploads \
  --output target/furies-backup-parallel-new-run
```

Omit `--parallel-uploads` for serial. Use a new output directory for every run.
The artifact root also preserves the four-run driver and analysis script.
