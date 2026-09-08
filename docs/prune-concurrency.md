# Bounded parallel repacking

Parallel pruning is opt-in. It uploads rebuilt indexes concurrently, then
downloads and uploads packs under one I/O budget, while preserving the existing pack format, retention rules, and
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

The limit is for this process's index-rebuild and repack work. Other phases, other processes, and
other repository clients retain their own limits. OpenDAL's existing limiter
continues to apply to its backend operations.

## Scheduling and failure behavior

Index rebuilding uses N upload workers and an unbuffered handoff. The producer
serializes each completed index while workers compress, encrypt, verify, and
upload previous indexes. It drains all rebuilt indexes, including the final
partial index, before starting pack repacking. Any upload failure is returned
with aggregated index-upload errors before the later old-index deletion phase.
These workers are closed before the pack workers start, so their concurrency
and byte limits do not add together. Default pruning and ordinary backups keep
the existing synchronous index writer.

N-1 download workers share one upload pool of N workers across tree and data
packs. The completed-pack channel is unbuffered. A download worker keeps its
slot until it has handed the blobs to the packer, so producers cannot keep
fetching indefinitely while uploads are blocked.

A pack enters the index only after its upload succeeds. On failure, I/O and
memory waiters are cancelled, blocked channel senders and workers are woken,
both packers are closed, and all upload workers are joined. Backend errors
retain pack IDs and aggregated failure details. Prune returns before its
subsequent old-index and pack deletion phase.

A successful channel handoff schedules an upload; only pool finalization
confirms completion. Cancellation may discard accepted work that has not
started, and finalization returns the upload failure. Backend calls already
in progress must return before their workers can be joined.

This does not override `--early-delete-index` or the existing cleanup of
already-unindexed files before repacking. Avoid early deletion if preserving
the old index after an interrupted repack is required.

## Byte budgets

- `--repack-read-buffer` defaults to **128 MiB**. It bounds the sum of retained
  range-download buffers. Ranges are coalesced only while they fit this budget;
  permits are held until blob handoff finishes.
- `--repack-upload-buffer` defaults to **256 MiB**. During index rebuilding it
  bounds serialized index bodies admitted to workers. During pack repacking it
  bounds pack bodies detached from the builders, including bodies being hashed,
  waiting for an I/O slot, uploading, or waiting for index publication.

The two budgets are independent so uploads can complete when the download
budget is full. Zero budgets are rejected. A single blob or completed pack
larger than its allowance fails with the option name, required bytes, and
configured bytes; the budget is never silently exceeded.

These are not total RSS limits. Two pack builders, decoded/compression buffers,
indexes, allocator overhead, cache, and backend-private buffers use additional
memory. A builder may hold a full pack while waiting for upload admission.
During rebuilding, the producer can hold one serialized index waiting for byte
admission in addition to the index builder. Worker compression/encryption and
verification buffers are additional memory. An index larger than the upload
budget is rejected before it is handed to a worker.

Allow enough upload bytes for the intended concurrency **and pack overhead**.
For example, two nominal 128 MiB packs may exceed 256 MiB after the final blob
and header are added, leaving only one data upload admitted. For large packs,
use an explicit larger budget if memory permits:

```sh
rustic prune --parallel-repack --repack-connections 5 \
  --repack-read-buffer 128MiB --repack-upload-buffer 1GiB
```

Debug logging reports peak admitted serialized-index bytes after rebuild, and
peak admitted read and pack-upload bytes after repacking.

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

## Isolated B2 validation on Arc

The x86_64 Linux release CLI was built on Darwin with Apple Container and
Rosetta, targeting `x86_64-unknown-linux-musl`. The candidate includes core
commit `9ecd69e`; the frozen installed baseline identifies core `ca66cd1`.
Both include the same Storj revision `f4e5374`.

The fixture started with 1 GiB of random data in 1 MiB files, an 8 MiB target
data-pack size, grow factor 0, and compression level 1. After deleting alternate
files, a second backup and local forget left 512 MiB live. Three identical
copies were uploaded under a unique disposable B2 prefix before any remote
prune. Every invocation verified the test repository ID before mutation.

All three runs used OpenDAL's native B2 backend with `connections=10`, separate
caches, `--max-repack 1GiB --max-unused 0 --keep-delete 100y`, and normal
repacking. Candidate runs added `--repack-connections 5` or `10` and a 1 GiB
upload budget; the read budget remained 128 MiB. Metadata checks ran before
each prune and full `check --read-data` checks followed it. Process RSS was
sampled every 100 ms. These are single trials per setting, not repeated or
randomized measurements, and the small packs differ from the fleet repository.

| Pipeline | Whole prune seconds | Repack seconds | Sampled peak RSS MiB |
|---|---:|---:|---:|
| Installed baseline | 61.30 | 57.39 | 1026 |
| Parallel N=5 | 27.30 | 23.20 | 691 |
| Parallel N=10 | 16.00 | 12.17 | 848 |

Each run repacked 512 MiB of live blobs from the same 122 source packs and
passed the full data check. Whole-prune elapsed time improved by 2.25x at N=5
and 3.83x at N=10 in this trial. Integrity-check time is excluded from the
timings. RSS includes the CLI and backend, not just the explicit byte budgets.
The baseline comparison includes all pipeline changes, including bounded
reads; it does not isolate uploader parallelism alone. This demonstrates a
benefit on this B2 fixture, not a forecast for the fleet's larger packs.

After validation, all objects and old versions under the disposable B2 prefix
were removed and an empty version-inclusive listing was verified. The Arc
trial directory, including its ephemeral password and caches, was removed.
Arc's installed binary was unchanged, and its prune service remained inactive
with the timer disabled. The shared fleet repository received only a dry-run
preview; no retention or pack mutations were applied to it.

Build provenance, raw logs, measured results, and cleanup evidence are retained
locally under the sibling CLI repository's
`dist/arc-parallel-prune-20260907/` directory. The release artifact SHA-256 is
`d96e720d39658b8524c46e519a130bbf1a858944b29f990364b3c051933fb83f`.
