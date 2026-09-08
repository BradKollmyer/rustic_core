# Local packer performance experiments

## Platform and method

These experiments ran on the Darwin host on September 7, 2026. Linux `perf` was
not available; `/usr/bin/sample` provided native stack samples. No Arc process,
installed binary, or remote repository was changed.

Production baseline: `83e479f`. Benchmark executables use Cargo's optimized test
profile (opt-level 1, debug information), not the release CLI. The native CLI
also builds successfully in the dev profile at `../rustic/target/debug/rustic`.

Two disposable fixtures use deterministic random input, two snapshots, and
alternate files removed before forgetting the first snapshot:

| Fixture | Input | Fixed chunk / file size | Target pack | Pack bytes read / written per prune |
|---|---|---|---|---|
| Small blobs | 64 MiB | 64 B / 64 KiB | 1 MiB | 104.98 / 73.00 MiB |
| Large blobs | 2 GiB | 1 MiB / 1 MiB | 32 MiB | 1888.12 / 1024.10 MiB |

The small fixture's tiny chunks add substantial encryption and header overhead.
It stresses packs with many blobs; it is not representative of fleet blob sizes.

All comparisons use fast repack with five connections, a 128 MiB read budget,
and a 256 MiB upload budget. Local mode adds no artificial delays. Latency mode
adds 20 ms per pack GET and 60 ms per pack PUT, with no index delay or transfer
rate limit. These delays are not a model of shared B2 bandwidth.

Each stage compares frozen executables on copies of the same retained seed.
Three repetitions reverse version and fixture order on the middle repetition.
Timings cover the whole prune. Every trial checks publication ordering and
reads all remaining repository data after timing stops. Builds, other tests,
and profiling do not overlap the repeated timing runs. All 48 comparison
trials passed; raw results include sampled RSS and connection counts.

## Change 1: current-pack duplicate lookup

Commit: `2bbc5d5`.

The small-blob profile put `BasicPacker::add_raw` at the top of sampled runnable
work: its duplicate check scanned the growing `IndexPack.blobs` vector for
every blob. This creates quadratic work within a pack and extends the time
spent holding the raw packer lock. The normal packer caps a pack at 10,000 blobs.

A per-pack `HashSet<BlobId>` replaces that scan. It is updated only after the
blob was successfully appended and cleared when the pack is detached. The
ordered index remains unchanged; the set is never serialized or iterated to
produce pack data. A unit test verifies duplicate suppression, index order,
offsets, statistics, and membership reset across pack boundaries.

| Fixture / mode | Before median s | After median s | Before median peak RSS MiB | After median peak RSS MiB |
|---|---:|---:|---:|---:|
| Small / local | 2.667 | 0.795 | 568.2 | 567.0 |
| Small / latency | 2.677 | 1.677 | 568.2 | 564.6 |
| Large / local | 1.046 | 0.859 | 568.7 | 581.8 |
| Large / latency | 1.213 | 1.225 | 481.5 | 461.9 |

Small/local improves 3.35x, with non-overlapping ranges of 2.667–2.725 s before
and 0.767–0.800 s after. Small/latency improves 1.60x. Large-blob timings are
noisy and overlap; there is no demonstrated large/latency gain. Retained for
the clear many-blob improvement, without claiming a universal speedup.

The membership set adds bounded per-current-pack metadata and retains its
capacity across pack rotations. It also serves ordinary pack creation, but
backup throughput was not separately benchmarked.

## Change 2: buffered local-file writes

Commit: `79678ca`.

The large-blob profile showed upload workers in
`LocalBackend::write_bytes -> io::copy -> File::write`, copying through a small
reader buffer and making repeated writes. The initial large prune was shorter
than the fixed two-second sample; only stacks rooted in pack upload/local write
were used for this diagnosis, not later integrity-check work.

The local backend now writes `BytesList` slices through a 256 KiB `BufWriter`.
Small slices are combined, and sufficiently large slices bypass the buffer.
An explicit flush returns buffered write errors before `sync_all`; temporary
file cleanup, syncing, rename, and hooks keep their existing behavior. Tests
cover empty chunks, a buffer boundary, a large direct-write chunk, a partial
final buffer, truncation on replacement, an empty file, and existing hooks.

This stage's baseline already includes the membership optimization. It was
remeasured alongside the buffered candidate rather than reusing old timings.

| Fixture / mode | Before median s | After median s | Before median peak RSS MiB | After median peak RSS MiB |
|---|---:|---:|---:|---:|
| Small / local | 0.770 | 0.349 | 510.8 | 520.1 |
| Small / latency | 1.635 | 1.519 | 513.0 | 510.2 |
| Large / local | 1.144 | 0.719 | 563.8 | 623.9 |
| Large / latency | 1.577 | 1.174 | 513.1 | 431.3 |

Small/local improves 2.21x and large/local 1.59x. Large/local ranges are
0.813–1.378 s before and 0.683–0.720 s after. Latency reduces the advantage,
particularly with small packs. This is a local-backend gain, not a B2 PUT gain.

Each concurrent local write adds at most 256 KiB of buffering. RSS includes
all pipeline and backend state and can miss short peaks. The large/local
median rises by about 60 MiB, much more than those write buffers alone;
allocator variation and changed overlap make these sampled process peaks
noisy. Existing read/upload byte budgets and connection limits still apply.

## Remaining areas and validation

Pack hashing also appeared in the profiles, but the inspected stacks were
mostly SHA-256 compression, with much less copying overhead. No hashing change
was made. Reducing duplicate scans was better supported than introducing more
packer batching or changing lock scheduling. Neither implemented candidate
failed the benefit test, so no revert was needed.

Final checks passed 202 core unit tests, 12 pipeline tests, 24 existing prune
cases, and 2 local-backend tests. The pipeline tests include normal and fast
repack, cancellation, failed uploads, memory limits, and preservation of old
data on failure. Clippy passed with the pre-existing unrelated cache
`manual_clamp` lint excluded. Formatting and diff checks passed.

Native profiles are in `target/fast-pack-profile-{small,large}/` and
`target/fast-pack-profile-final-{small,large}/`. Final profiles deliberately
add index latency (small) or per-request transfer delay (large) to keep repack
active during sampling; their durations are not part of the comparison tables.
Frozen executables, manifests, repeated results, logs, and summaries are in
`target/fast-pack-experiments/` and `target/fast-pack-buffered-experiments/`.

For an individual small-blob comparison run:

```sh
python3 scripts/benchmark-prune.py --repack-index-heavy --size-mib 64 \
  --fast-repack --index-delay-ms 0 --cases local-5 latency-5 --repeats 3 \
  --binary target/fast-pack-buffered-experiments/buffered \
  --output target/fast-pack-rerun
```

Both retained fixture repositories and their ephemeral test keys were removed
after profiling and validation. Measurements, profiles, and build artifacts remain.
