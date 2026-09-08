# Furies S3 prune benchmark

## Method

The client runs on the Darwin workstation against
`http://s3.lynden.vitalsoft.com:7480`, using the dedicated non-admin
`rustic-prune-bench` user and bucket. Credentials remain in the workspace
`.env`, outside the git repositories. No production repository is involved.

The frozen executable uses production core `5ef673a` plus the benchmark harness
in `4fd68db`. The unrelated local `binarysorted.rs` edits are excluded by building
in a detached worktree. This uses the optimized test profile (opt-level 1), not
the release CLI; compare connections within this experiment, not absolute rates
against Arc's release binary or native B2 backend.

The deterministic 8 GiB input has fixed 1 MiB chunks/files and target 128 MiB
packs. Two snapshots are created; alternate files are removed and the older
snapshot forgotten, leaving approximately half the data to retain. One seed is
uploaded, then server-side copied into a fresh prefix before each trial. Each
trial runs fast repack with unlimited repacking, zero unused allowance, immediate
deletion, a 128 MiB read budget, and a 1 GiB upload budget.

Three repetitions alternate the connection order: 5/10, 10/5, 5/10. No artificial
latency or rate limit is applied. Every trial uses a fresh process, an empty local
cache and a 1,024-descriptor soft limit. The fixture has few tree packs and does
not reproduce the production cache's 960 retained descriptors; the dedicated
subprocess cache regression covers that exhaustion separately.

Timing starts after prune planning and covers index rebuilding, repacking and
prune deletion. Source generation, seed upload, server-side copies, publication
ordering verification and a full surviving-data check are outside the timer.
Reported write throughput is retained pack MiB divided by total timed prune
seconds. Instrumentation records pack GET/PUT concurrency, warnings and retry
warnings; RSS and open descriptor counts are sampled roughly every half second
using ps/lsof and can miss short peaks. CPU samples from ps are process-lifetime
percentages, not per-interval utilization. The connection limit is the prune I/O
budget; HTTP keep-alive sockets may outnumber active requests.

The runner deletes only its generated UUID prefix, including seed and trial
objects. It retains results and logs, while removing the local fixture and its
key. The test user and empty bucket remain available for future runs.

## Repeating the experiment

Build the integration test with `cargo test --locked -p rustic_core --test
prune_pipeline --no-run`, retain the reported executable, then run:

```sh
uv run --no-project --with boto3 python scripts/benchmark-prune-s3.py \
  --binary /path/to/prune_pipeline \
  --output target/furies-s3-new-comparison
```

The output directory must not exist. Defaults are 8 GiB and three repetitions.
Use `--size-mib 64 --repeats 1` for a connectivity/integrity smoke test; that small
fixture cannot exercise multiple large pack uploads and is not a throughput test.

## Validation

The small S3 smoke test passed both connection counts, full checks and cleanup.
All 13 ordinary pipeline tests passed; strict Clippy and formatting checks passed.
The smoke test exposed a warning when cache removal finds an uncached data pack
absent locally (ENOENT). This is distinct from the earlier EMFILE exhaustion;
full integrity checks succeeded. No production code was changed for this run.

## Results (2026-09-08)

All six full checks passed. Each prune read 7,688.48 MiB of pack ranges and wrote
4,096.42 MiB of retained pack data. Both cases overlapped GETs and PUTs.

| Connections | Trial times (s) | Median time (s) | Median write MiB/s | Median sampled peak RSS (MiB) | Max sampled FDs | Peak concurrent PUTs |
|---|---|---:|---:|---:|---:|---:|
| 5 | 197.52, 177.48, 181.79 | 181.79 | 22.53 | 1,827.77 | 36 | 5 |
| 10 | 188.47, 187.60, 178.19 | 187.60 | 21.84 | 2,478.44 | 36 | 7 |

Ten connections did not demonstrate a throughput benefit: its median prune took
3.2% longer, while timing ranges overlap. The first pair alone would have
suggested a 4.8% throughput improvement for ten; subsequent repetitions show
why that was insufficient evidence. Five is sufficient for this particular
Mac-to-Furies workload. Arc's native B2 path is a different experiment.

Parallel uploads are working: peak active I/O was exactly 5 and 10, with up to
5 and 7 simultaneous PUTs respectively. The 1 GiB upload budget and 128 MiB
packs constrain how many complete packs can remain in flight. Increasing the
connection count alone does not guarantee greater throughput.

Sampled peak RSS ranged from 1,752–2,026 MiB at five connections and
2,280–2,763 MiB at ten. The upload buffer limits retained upload data, not total
process memory. No retry warnings or EMFILE errors occurred. Each trial emitted
one ENOENT cache-removal warning for an absent local pack; integrity checks
passed. Actual peak descriptor usage was low (at most 36 sampled), so this
workload does not replace the cache-pressure regression test.

All seed and trial objects under the generated benchmark prefix were deleted
and the prefix was verified empty. The local fixture and temporary master key
were removed by the runner. The dedicated user and bucket remain available.

Artifacts are in `target/furies-s3-8g-comparison/`: raw trial logs, per-process
samples, results, summary, parameters (including executable SHA-256), validation,
and cleanup confirmation. The frozen executable and source provenance are in
`target/furies-s3-build/`. Smoke results are in `target/furies-s3-smoke/`.
