#!/usr/bin/env python3
"""Measure initial, unchanged, and 25%-changed backups in a disposable Ceph prefix.

Only the dedicated rustic-prune-bench bucket is accepted. Each stage verifies all
repository data and restores and compares every source file outside the timer.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import resource
import shlex
import subprocess
import threading
import time
import uuid

import boto3
from botocore.config import Config


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--env-file', type=Path, default=Path(__file__).resolve().parents[2]/'.env')
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--size-mib', type=int, default=4096)
    parser.add_argument('--profile', action='store_true')
    args = parser.parse_args()
    if args.size_mib < 64 or args.size_mib % 64:
        parser.error('size must be a positive multiple of 64 MiB')
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
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
        config=Config(signature_version='s3v4', s3={'addressing_style': 'path'}))
    prefix = 'bench-backup-'+str(uuid.uuid4())+'/'
    binary = args.binary.resolve()
    manifest = dict(endpoint=credentials['CEPH_FURIES_S3_ENDPOINT'], bucket=bucket, prefix=prefix,
        source_mib=args.size_mib, file_mib=16, pack_mib=128, backend_connections=5,
        fd_limit=1024, profiled=args.profile, binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest())
    (output/'parameters.json').write_text(json.dumps(manifest, indent=2)+'\n')
    env = dict(os.environ, **credentials, PRUNE_S3_ROOT=prefix, BACKUP_S3_MIB=str(args.size_mib))
    def limits():
        _, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        resource.setrlimit(resource.RLIMIT_NOFILE, (min(1024, hard), hard))
    proc = None
    lines, results, samples, profiles = [], [], [], []
    phase = {'name': None}
    windows = {}
    reader = None
    try:
        print('Preparing source and repository...', flush=True)
        proc = subprocess.Popen([str(binary), 'benchmark_backup_s3', '--exact', '--ignored', '--nocapture'],
            env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, preexec_fn=limits)
        def read():
            with (output/'run.log').open('w') as log:
                for line in proc.stdout:
                    lines.append(line)
                    log.write(line)
                    log.flush()
                    if line.startswith('BEGIN backup-'):
                        name = line.strip().removeprefix('BEGIN backup-')
                        windows[name] = {'start': time.monotonic()}
                        phase['name'] = name
                        print(line.strip(), flush=True)
                    elif line.startswith('RESULT '):
                        result = json.loads(line.removeprefix('RESULT '))
                        results.append(result)
                        windows[result['stage']]['end'] = time.monotonic()
                        phase['name'] = None
                        print(line.strip(), flush=True)
                    elif line.startswith('CHECK '):
                        print(line.strip(), flush=True)
        reader = threading.Thread(target=read)
        reader.start()
        start = time.monotonic()
        profiled = set()
        while proc.poll() is None:
            if time.monotonic()-start > 1800:
                raise TimeoutError('Backup test exceeded 30 minutes')
            name = phase['name']
            if name:
                if args.profile and name in ('initial', 'changed') and name not in profiled and time.monotonic()-windows[name]['start'] >= 5:
                    path = output/f'{name}.sample.txt'
                    stream = path.with_suffix('.stderr').open('w')
                    sampler = subprocess.Popen(['/usr/bin/sample', str(proc.pid), '5', '1', '-file', str(path)], stdout=stream, stderr=stream)
                    profiles.append((name, sampler, stream, time.monotonic()))
                    profiled.add(name)
                ps = subprocess.run(['ps', '-o', 'rss=,pcpu=', '-p', str(proc.pid)], capture_output=True, text=True)
                fd = subprocess.run(['/usr/sbin/lsof', '-a', '-p', str(proc.pid), '-d', '0-1048575', '-Ff'], capture_output=True, text=True)
                samples.append(dict(stage=name, stage_seconds=time.monotonic()-windows[name]['start'],
                    rss_mib=int(ps.stdout.split()[0])/1024 if ps.stdout.strip() else None,
                    cpu_percent=float(ps.stdout.split()[1]) if ps.stdout.strip() else None,
                    fds=sum(line.startswith('f') and line[1:].isdigit() for line in fd.stdout.splitlines()) if fd.returncode==0 else None))
            time.sleep(.5)
        reader.join()
        if proc.returncode or len(results)!=3 or len([l for l in lines if l.startswith('CHECK ')])!=3:
            raise RuntimeError('Backup/verification failed; see run.log')
        for result in results:
            subset = [s for s in samples if s['stage']==result['stage']]
            result.update(check='passed', peak_rss_mib=max((s['rss_mib'] for s in subset if s['rss_mib'] is not None), default=None),
                          peak_fds=max((s['fds'] for s in subset if s['fds'] is not None), default=None))
            result['data_added_mibps'] = result['data_added_mib']/result['seconds']
        (output/'results.json').write_text(json.dumps(results, indent=2)+'\n')
        print(json.dumps(results), flush=True)
    finally:
        if proc is not None and proc.poll() is None:
            proc.kill()
            proc.wait()
        if reader is not None:
            reader.join()
        captures = []
        for name, sampler, stream, launched in profiles:
            try:
                code = sampler.wait(timeout=30)
            except subprocess.TimeoutExpired:
                sampler.kill(); sampler.wait(); code = -1
            stream.close()
            captures.append(dict(stage=name, exit_code=code,
                launch_seconds=launched-windows[name]['start'],
                stage_end_seconds=windows[name].get('end', time.monotonic())-windows[name]['start']))
        (output/'profiles.json').write_text(json.dumps(captures, indent=2)+'\n')
        (output/'samples.json').write_text(json.dumps(samples, indent=2)+'\n')
        objects = [obj for page in s3.get_paginator('list_objects_v2').paginate(Bucket=bucket, Prefix=prefix) for obj in page.get('Contents', [])]
        for i in range(0, len(objects), 1000):
            response = s3.delete_objects(Bucket=bucket, Delete={'Objects':[{'Key':obj['Key']} for obj in objects[i:i+1000]],'Quiet':True})
            if response.get('Errors'):
                raise RuntimeError('Cleanup failed')
        assert s3.list_objects_v2(Bucket=bucket, Prefix=prefix).get('KeyCount',0)==0
        (output/'cleanup.json').write_text(json.dumps({'prefix':prefix,'empty':True})+'\n')


if __name__ == '__main__':
    main()
