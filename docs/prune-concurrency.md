# Bounded parallel repacking

Parallel repacking is opt-in. It downloads and uploads packs under one I/O
budget, while preserving the existing pack format, retention rules, and
upload-before-index ordering. It is currently enabled per prune invocation;
ordinary backups and the default prune pipeline keep their existing uploader.

```sh
rustic prune --parallel-repack
rustic prune --parallel-repack --repack-connections 5 \
  --repack-read-buffer 128MiB --repack-upload-buffer 256MiB
```

`--repack-connections` also enables parallel repacking on its own. When it is
omitted, rustic uses the configured backend connection limit, or 5 if none is
advertised. An explicit value cannot exceed the backend limit. For example,
OpenDAL `connections=5` caps `--repack-connections 10` at 5. Hot/cold repositories
use the smaller known limit of the two routes. At least two connections are
needed; invalid settings fail before repository mutation.

The limit is for this process's repack work. Other phases, other processes, and
other repository clients retain their own limits. OpenDAL's existing limiter
continues to apply to its backend operations.

## Scheduling and failure behavior

N-1 download workers share one upload pool of N workers across tree and data
packs. The completed-pack channel is unbuffered. A download worker keeps its
slot until it has handed the blobs to the packer, so producers cannot keep
fetching indefinitely while uploads are blocked.

A pack enters the index only after its upload succeeds. On failure, memory
waiters are cancelled, both packers are closed, and all upload workers are
joined. Backend errors retain pack IDs and aggregated failure details. Prune
returns before its subsequent old-index and pack deletion phase.

This does not override `--early-delete-index` or the existing cleanup of
already-unindexed files before repacking. Avoid early deletion if preserving
the old index after an interrupted repack is required.

## Byte budgets

- `--repack-read-buffer` defaults to **128 MiB**. It bounds the sum of retained
  range-download buffers. Ranges are coalesced only while they fit this budget;
  permits are held until blob handoff finishes.
- `--repack-upload-buffer` defaults to **256 MiB**. It bounds pack bodies detached
  from the builders and admitted to upload workers, including bodies being
  hashed, waiting for an I/O slot, uploading, or waiting for index publication.

The two budgets are independent so uploads can complete when the download
budget is full. Zero budgets are rejected. A single blob or completed pack
larger than its allowance fails with the option name, required bytes, and
configured bytes; the budget is never silently exceeded.

These are not total RSS limits. Two pack builders, decoded/compression buffers,
indexes, allocator overhead, cache, and backend-private buffers use additional
memory. A builder may hold a full pack while waiting for upload admission.

Allow enough upload bytes for the intended concurrency **and pack overhead**.
For example, two nominal 128 MiB packs may exceed 256 MiB after the final blob
and header are added, leaving only one data upload admitted. For large packs,
use an explicit larger budget if memory permits:

```sh
rustic prune --parallel-repack --repack-connections 5 \
  --repack-read-buffer 128MiB --repack-upload-buffer 1GiB
```

Debug logging reports peak admitted read and upload bytes at pool finalization.

## Local validation and benchmarks

`crates/core/tests/prune_pipeline.rs` exercises real backups, prunes, index
publication, and full data checks through a local backend that can delay or
fail requests. It checks connection limits, actual retained read bytes, upload
bytes, cache-wrapper forwarding, cancellation, stalled-upload backpressure,
and preservation of repository data on failure.

```sh
cargo test --offline --locked -p rustic_core --test prune_pipeline -- --test-threads=1
cargo test --offline --locked -p rustic_core --lib blob::byte_budget::tests
python3 scripts/benchmark-prune.py --size-mib 2048 --pack-mib 32 \
  --chunk-kib 1024 --read-mibps 256 --write-mibps 64 --repeats 2 \
  --cases latency-baseline latency-5 latency-10 \
  --output target/prune-large-p32-u256
```

The runner prepares one disposable local fixture, clones it for each trial,
runs each trial in a fresh process, and samples RSS with `ps` during prune.
Every successful trial verifies index publication ordering and reads all
remaining repository data. Fixtures and ephemeral test keys are removed on
normal completion or handled failure. Results and logs remain in `--output`.

Latency mode adds 20 ms per GET and 60 ms per pack PUT. Optional transfer rates
are **per request**, not a shared network bandwidth ceiling. Results establish
local scheduling and memory behavior; they do not establish a B2 speedup.
The benchmark uses Cargo's optimized test profile rather than the release CLI
profile. RSS sampling can miss brief peaks.

## September 2026 local results

On this Darwin host, a 2 GiB source fixture was reduced to 1 GiB of live data.
These runs used 1 MiB fixed chunks, compression level 1, normal repacking,
20/60 ms GET/PUT delays, and per-request read/write rates of 256/64 MiB/s.
Read buffers were capped at 128 MiB. Each row is the median of two fresh
processes; RSS is the median sampled peak during prune.

| Target pack MiB | Upload budget MiB | Connections | Seconds | Peak RSS MiB | Peak PUTs |
|---|---|---|---:|---:|---:|
| 32 | 256 | 10 | 3.83 | 846 | 7 |
| 32 | 256 | 5 | 6.23 | 768 | 5 |
| 32 | 256 | baseline | 20.38 | 1367 | 1 |
| 128 | 256 | 10 | 18.38 | 787 | 1 |
| 128 | 256 | 5 | 18.55 | 769 | 1 |
| 128 | 256 | baseline | 17.77 | 1898 | 1 |
| 128 | 1024 | 10 | 5.40 | 1410 | 7 |
| 128 | 1024 | 5 | 6.75 | 1134 | 4 |

At 32 MiB packs, N=5 and N=10 improved elapsed time by about 3.3x and 5.3x
respectively. At 128 MiB packs, the default upload budget serialized data PUTs
and provided a memory reduction without a speed improvement. Raising that
budget to 1 GiB restored overlapping uploads. This is why the byte budget must
be sized along with the connection count.

Each pack-size/upload-budget profile prepared its own fixture, then cloned it
identically for all connection counts and repetitions in that profile. The
128 MiB budget profiles read 1935.12 versus 1921.12 MiB and both wrote
1024.10 MiB, so their timing comparison is approximate. All 16 large-pack
trials completed full data checks and publication-order assertions.

The final local validation also passed 196 core unit tests (including CLI
parsing and weighted-budget tests), 8 pipeline integration tests, and 24
existing prune cases. The rustic CLI compiles. Clippy passed with the
preexisting unrelated cache `manual_clamp` lint excluded.
