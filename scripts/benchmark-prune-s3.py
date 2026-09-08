#!/usr/bin/env python3
"""Run destructive prune trials only inside a unique prefix of rustic-prune-bench.

Requires boto3. Credentials are read from the workspace .env, never written to
results. Each trial copies an identical seed, runs in a fresh process with a
1024-FD soft limit and empty local cache, then checks all surviving data.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import os
from pathlib import Path
import resource
import shlex
import statistics
import subprocess
import tempfile
import threading
import time
import uuid

import boto3
from botocore.config import Config


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--env-file', type=Path, default=Path(__file__).resolve().parents[2] / '.env')
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--size-mib', type=int, default=8192)
    parser.add_argument('--repeats', type=int, default=3)
    args = parser.parse_args()
    if args.size_mib <= 0 or args.repeats <= 0:
        parser.error('size and repeats must be positive')
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    binary = args.binary.resolve()
    credentials = {}
    for line in args.env_file.read_text().splitlines():
        if line.startswith('CEPH_FURIES_S3_'):
            key, value = line.split('=', 1)
            credentials[key] = shlex.split(value)[0]
    bucket = credentials['CEPH_FURIES_S3_BUCKET']
    if bucket != 'rustic-prune-bench':
        raise RuntimeError('Refusing a bucket other than rustic-prune-bench')
    s3 = boto3.client('s3', endpoint_url=credentials['CEPH_FURIES_S3_ENDPOINT'],
        region_name=credentials['CEPH_FURIES_S3_REGION'],
        aws_access_key_id=credentials['CEPH_FURIES_S3_ACCESS_KEY_ID'],
        aws_secret_access_key=credentials['CEPH_FURIES_S3_SECRET_ACCESS_KEY'],
        config=Config(signature_version='s3v4', s3={'addressing_style': 'path'},
                      max_pool_connections=10, connect_timeout=10, read_timeout=120))
    prefix = 'bench-' + str(uuid.uuid4()) + '/'
    manifest = dict(endpoint=credentials['CEPH_FURIES_S3_ENDPOINT'], bucket=bucket,
        prefix=prefix, size_mib=args.size_mib, pack_mib=128, chunk_kib=1024,
        upload_buffer_mib=1024, read_buffer_mib=128, fd_limit=1024,
        repeats=args.repeats, binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest())
    (output / 'parameters.json').write_text(json.dumps(manifest, indent=2)+'\n')

    def objects(root):
        return [obj for page in s3.get_paginator('list_objects_v2').paginate(Bucket=bucket, Prefix=root)
                for obj in page.get('Contents', [])]

    def clean(root):
        assert root.startswith(prefix) and len(prefix) > 20
        items = objects(root)
        for i in range(0, len(items), 1000):
            response = s3.delete_objects(Bucket=bucket,
                Delete={'Objects': [{'Key': obj['Key']} for obj in items[i:i+1000]], 'Quiet': True})
            if response.get('Errors'):
                raise RuntimeError('S3 cleanup failed')
        if objects(root):
            raise RuntimeError('S3 prefix is not empty after cleanup')

    def restrict_fds():
        _, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        resource.setrlimit(resource.RLIMIT_NOFILE, (min(1024, hard), hard))

    results = []
    with tempfile.TemporaryDirectory(prefix='rustic-s3-seed-') as scratch:
        fixture = Path(scratch)
        env = dict(os.environ, PRUNE_LAB_SEED=str(fixture), PRUNE_LAB_CASE='prepare-only',
                   PRUNE_LAB_MIB=str(args.size_mib), PRUNE_LAB_PACK_MIB='128', PRUNE_LAB_CHUNK_KIB='1024')
        print('Preparing deterministic local fixture...', flush=True)
        with (output / 'prepare.log').open('w') as log:
            subprocess.run([str(binary), 'benchmark_prune_pipeline', '--exact', '--ignored', '--nocapture'],
                           env=env, stdout=log, stderr=subprocess.STDOUT, check=True, timeout=900)
        files = [p for p in (fixture/'repo').rglob('*') if p.is_file()]
        seed_prefix = prefix + 'seed/'
        try:
            print(f'Uploading {len(files)} seed objects...', flush=True)
            def upload(path):
                s3.put_object(Bucket=bucket, Key=seed_prefix+path.relative_to(fixture/'repo').as_posix(), Body=path.read_bytes())
            with ThreadPoolExecutor(max_workers=5) as workers:
                list(workers.map(upload, files))
            seed_objects = objects(seed_prefix)
            assert len(seed_objects) == len(files)
            for repeat in range(args.repeats):
                for n in ([5, 10] if repeat % 2 == 0 else [10, 5]):
                    trial_prefix = prefix + f'trial-{repeat}-{n}/'
                    print(f'Preparing repetition {repeat+1}, connections={n}...', flush=True)
                    def copy(obj):
                        s3.copy_object(Bucket=bucket, Key=trial_prefix+obj['Key'][len(seed_prefix):],
                                       CopySource={'Bucket': bucket, 'Key': obj['Key']})
                    with ThreadPoolExecutor(max_workers=5) as workers:
                        list(workers.map(copy, seed_objects))
                    env.update(credentials, PRUNE_S3_ROOT=trial_prefix,
                               PRUNE_S3_KEY_FILE=str(fixture/'key.json'), PRUNE_S3_CONNECTIONS=str(n))
                    proc = subprocess.Popen([str(binary), 'benchmark_prune_s3', '--exact', '--ignored', '--nocapture'],
                        env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
                        preexec_fn=restrict_fds)
                    recording = threading.Event()
                    lines = []
                    def read():
                        for line in proc.stdout:
                            lines.append(line)
                            if line.startswith('BEGIN '):
                                recording.set()
                                print(line.strip(), flush=True)
                            elif line.startswith('RESULT '):
                                recording.clear()
                                print(line.strip(), flush=True)
                    reader = threading.Thread(target=read)
                    reader.start()
                    samples = []
                    start = time.monotonic()
                    try:
                        while proc.poll() is None:
                            if time.monotonic()-start > 1200:
                                raise TimeoutError('S3 trial exceeded 20 minutes')
                            if recording.is_set():
                                rss = subprocess.run(['ps', '-o', 'rss=,pcpu=', '-p', str(proc.pid)], capture_output=True, text=True)
                                fd = subprocess.run(['/usr/sbin/lsof', '-a', '-p', str(proc.pid), '-d', '0-1048575', '-Ff'],
                                                    capture_output=True, text=True)
                                samples.append(dict(seconds=time.monotonic()-start,
                                    rss_mib=int(rss.stdout.split()[0])/1024 if rss.stdout.strip() else None,
                                    cpu_percent=float(rss.stdout.split()[1]) if rss.stdout.strip() else None,
                                    fds=sum(line.startswith('f') and line[1:].isdigit() for line in fd.stdout.splitlines()) if fd.returncode == 0 else None))
                            time.sleep(.5)
                    finally:
                        if proc.poll() is None:
                            proc.kill()
                        reader.join()
                        proc.wait()
                        (output/f'trial-{repeat}-{n}.log').write_text(''.join(lines))
                        (output/f'trial-{repeat}-{n}-samples.json').write_text(json.dumps(samples, indent=2)+'\n')
                    if proc.returncode or 'CHECK passed\n' not in lines:
                        raise RuntimeError(f'Trial failed; see trial-{repeat}-{n}.log')
                    record = json.loads(next(line.removeprefix('RESULT ') for line in lines if line.startswith('RESULT ')))
                    record.update(repeat=repeat+1, peak_rss_mib=max((x['rss_mib'] for x in samples if x['rss_mib'] is not None), default=None),
                                  peak_fds=max((x['fds'] for x in samples if x['fds'] is not None), default=None), check='passed')
                    record['write_mibps'] = record['write_mib']/record['seconds']
                    record['warning_lines'] = sum('[WARN]' in line for line in lines)
                    record['retry_warning_lines'] = sum('[WARN]' in line and 'retry' in line.lower() for line in lines)
                    results.append(record)
                    (output/'results.json').write_text(json.dumps(results, indent=2)+'\n')
                    print(json.dumps(record), flush=True)
                    clean(trial_prefix)
        finally:
            clean(prefix)
            (output/'cleanup.json').write_text(json.dumps({'prefix': prefix, 'empty': True})+'\n')
    summary = {n: {field: statistics.median(r[field] for r in results if r['connections'] == n)
                   for field in ['seconds', 'write_mibps', 'peak_rss_mib', 'peak_fds']
                   if all(r[field] is not None for r in results if r['connections'] == n)} for n in [5, 10]}
    (output/'summary.json').write_text(json.dumps(summary, indent=2)+'\n')
    print(json.dumps(summary), flush=True)


if __name__ == '__main__':
    main()
