#!/usr/bin/env python3
"""Benchmark local prune with per-request delays and fresh-process RSS sampling.

Only disposable local repositories are used. Run from any directory; requires
Cargo, Python 3, and ps. The backend rate is per request, not a shared link cap.
"""
import argparse
import csv
from contextlib import nullcontext
import json
import os
from pathlib import Path
import subprocess
import tempfile
import threading
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--size-mib', type=int, default=128)
    parser.add_argument('--pack-mib', type=int, default=1)
    parser.add_argument('--chunk-kib', type=int, default=64)
    parser.add_argument('--read-mibps', type=int, default=0)
    parser.add_argument('--write-mibps', type=int, default=0)
    parser.add_argument('--read-buffer-mib', type=int, default=128)
    parser.add_argument('--upload-buffer-mib', type=int, default=256)
    parser.add_argument('--seed', type=Path, help='Reuse a disposable fixture directory across builds; retains its test key until removed')
    parser.add_argument('--index-heavy', action='store_true', help='64-byte chunks in 1MiB files; skip pack repacking to measure index rebuilding')
    parser.add_argument('--index-delay-ms', type=int, default=500)
    parser.add_argument('--profile', action='store_true', help='Capture macOS sample profiles during prune')
    parser.add_argument('--repeats', type=int, default=3)
    parser.add_argument('--cases', nargs='+', choices=[f'{mode}-{n}' for mode in ('local', 'latency') for n in ('baseline', '5', '10')],
                        default=['local-baseline', 'local-5', 'local-10', 'latency-baseline', 'latency-5', 'latency-10'])
    parser.add_argument('--output', type=Path, default=Path('target/prune-upload-lab'))
    args = parser.parse_args()
    if min(args.size_mib, args.pack_mib, args.chunk_kib, args.read_buffer_mib, args.upload_buffer_mib, args.repeats) <= 0:
        parser.error('sizes, byte budgets, and repeats must be positive')
    if min(args.read_mibps, args.write_mibps, args.index_delay_ms) < 0:
        parser.error('transfer rates must be nonnegative')
    root = Path(__file__).resolve().parents[1]
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    build = subprocess.run(['cargo', 'test', '--offline', '--locked', '-p', 'rustic_core', '--test', 'prune_pipeline', '--no-run', '--message-format=json'],
                           cwd=root, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=True)
    (output / 'build.log').write_text(build.stderr)
    artifacts = [json.loads(line) for line in build.stdout.splitlines() if line.startswith('{')]
    binary = next(record['executable'] for record in artifacts if record.get('executable') and record.get('target', {}).get('name') == 'prune_pipeline')
    command = [binary, 'benchmark_prune_pipeline', '--ignored', '--nocapture', '--test-threads=1']
    results = []
    (output / 'parameters.json').write_text(json.dumps(vars(args), default=str, indent=2) + '\n')
    if args.seed is not None:
        args.seed.mkdir(parents=True, exist_ok=True)
    fixture_context = nullcontext(str(args.seed.resolve())) if args.seed else tempfile.TemporaryDirectory(prefix='prune-lab-seed-')
    with fixture_context as fixture:
        env = dict(os.environ, PRUNE_LAB_SEED=fixture, PRUNE_LAB_CASE='prepare-only',
                   PRUNE_LAB_MIB=str(args.size_mib), PRUNE_LAB_PACK_MIB=str(args.pack_mib),
                   PRUNE_LAB_CHUNK_KIB=str(args.chunk_kib), PRUNE_LAB_READ_MIBPS=str(args.read_mibps),
                   PRUNE_LAB_WRITE_MIBPS=str(args.write_mibps), PRUNE_LAB_READ_BUFFER_MIB=str(args.read_buffer_mib),
                   PRUNE_LAB_UPLOAD_BUFFER_MIB=str(args.upload_buffer_mib))
        if args.index_heavy:
            env['PRUNE_LAB_INDEX_HEAVY'] = '1'
            env['PRUNE_LAB_INDEX_DELAY_MS'] = str(args.index_delay_ms)
        print('Preparing local fixture...', flush=True)
        prep = subprocess.run(command, env=env, cwd=root, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=180)
        (output / 'prepare.log').write_text(prep.stdout)
        if prep.returncode:
            raise RuntimeError(prep.stdout)
        for repeat in range(args.repeats):
            cases = args.cases if repeat % 2 == 0 else list(reversed(args.cases))
            for case in cases:
                env['PRUNE_LAB_CASE'] = case
                proc = subprocess.Popen(command, env=env, cwd=root, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
                recording = threading.Event()
                finished = threading.Event()
                samples, lines = [], []
                profile = None

                def collect_output():
                    try:
                        for line in proc.stdout:
                            lines.append(line)
                            if line.startswith('BEGIN '):
                                recording.set()
                            if line.startswith(case + ','):
                                recording.clear()
                    finally:
                        finished.set()

                reader = threading.Thread(target=collect_output)
                reader.start()
                start = time.monotonic()
                try:
                    while not finished.is_set():
                        if time.monotonic() - start > 180:
                            raise TimeoutError(case)
                        if recording.is_set():
                            if args.profile and profile is None:
                                profile = subprocess.Popen(['/usr/bin/sample', str(proc.pid), '2', '1', '-file', str(output / f'{case}-{repeat}.sample.txt')], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                            rss = subprocess.run(['ps', '-o', 'rss=', '-p', str(proc.pid)], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                            if rss.returncode == 0 and rss.stdout.strip():
                                samples.append(int(rss.stdout.strip()))
                        time.sleep(.02)
                finally:
                    if proc.poll() is None and not finished.is_set():
                        proc.kill()
                    reader.join()
                    code = proc.wait()
                    if profile is not None:
                        profile.wait(timeout=15)
                    (output / f'{case}-{repeat}.log').write_text(''.join(lines))
                if code:
                    raise RuntimeError(''.join(lines))
                line = next(line for line in lines if line.startswith(case + ','))
                fields = [field.strip() for field in next(csv.reader([line]))]
                record = dict(case=case, repeat=repeat, seconds=float(fields[2]), read_mib=float(fields[3]), write_mib=float(fields[4]),
                              peak_get=int(fields[5]), peak_put=int(fields[6]), peak_io=int(fields[7]), overlap=fields[8],
                              peak_rss_mib=max(samples) / 1024 if samples else None, samples=len(samples))
                if len(fields) >= 12:
                    record.update(index_puts=int(fields[9]), peak_index_puts=int(fields[10]), index_mib=float(fields[11]))
                results.append(record)
                print(json.dumps(record), flush=True)
                (output / 'results.json').write_text(json.dumps(results, indent=2) + '\n')


if __name__ == '__main__':
    main()
