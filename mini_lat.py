#!/usr/bin/python3

import math
import json
import subprocess
import sys
import os.path

def measure_latency(directory, iops, request_size, rw, name):
    # Through experimentation, 1000 was the fewest number of IOs with a consistent average
    number_ios = 1000
    # Rate limit the fio command to non-burst IOPs to avoid inaccurate latency measurement due to throttling
    fio_command = [
        'fio', '--direct=1', '--ioengine=libaio', '--iodepth=1',
        '--time_based=0', f'--name={name}', f'--size=10MB',
        '--output-format=json', '--overwrite=1', f'--rw={rw}',
        f'--bs={request_size}k', f'--number_ios={number_ios}',
        f'--rate_iops={iops}', f'--directory={directory}'
    ]
    # For usability, print an approximate expected runtime
    expected_runtime = 2

    return json.loads(subprocess.check_output(fio_command))

def get_latency(directory, iops, request_size, random):
    write = ('rand' if random else '') + 'write'
    read = ('rand' if random else '') + 'read'

    jobname = 'test'

    parsed = measure_latency(directory, iops, request_size, write, jobname)
    write_clat_mean = parsed['jobs'][0]['write']['clat_ns']['mean']

    parsed = measure_latency(directory, iops, request_size, read, jobname)
    read_clat_mean = parsed['jobs'][0]['read']['clat_ns']['mean']

    winner = max(write_clat_mean, read_clat_mean)

    # The fio filename_format is, by default, $jobname.$jobnum.$filenum (when
    # no other format specifier is given). This script only runs a single job
    # with a single file, so the suffix will always be 0.0
    job_filename = jobname + '.0.0'
    # Remove the file--both to avoid leaving files around and also to ensure a
    # consistent experimental environment for latency tests.
    try:
        os.remove(os.path.join(directory, job_filename))
    except FileNotFoundError:
        pass

    # convert to seconds
    return winner / 1_000_000_000

directory="/mnt/sabrent"

max_read_iops = 650000
max_write_iops = 700000
max_iops = min(max_read_iops, max_write_iops)

max_read_bw_mbps = 7100
max_write_bw_mbps = 6600
max_bw_mbps = min(max_read_bw_mbps, max_write_bw_mbps)
# Convert to kbps
max_bw = max_bw_mbps * 1024

# cat /sys/block/nvme1n1/queue/max_hw_sectors_kb
max_hw_sectors_kb = 2048

IOPS_headroom = 0.87

IOPS_seq_IO = max_iops * (1 - IOPS_headroom)
RS_min_seq_IO_1 = math.ceil(min(max_hw_sectors_kb, (max_bw / IOPS_seq_IO)))
RS_min_seq_IO_2 = math.ceil(min(max_hw_sectors_kb, (max_bw / max_iops)))
RS_min_seq_IO = max(RS_min_seq_IO_1, RS_min_seq_IO_2)

latency_max_BW = get_latency(directory, max_iops, RS_min_seq_IO, False)
latency_base_rand = get_latency(directory, max_iops, 8, True)

QD_max_seq_BW = math.ceil(max_bw * latency_max_BW / RS_min_seq_IO)
read_ahead = math.ceil(QD_max_seq_BW * RS_min_seq_IO)
print(f"read_ahead_kb is {read_ahead}")


print("Sequential write latency")
print(f"""
 fio --direct=1 --ioengine=libaio --iodepth=1 --time_based=0 --name=test --size=10MB \
 --overwrite=1 --bs={RS_min_seq_IO}k --number_ios=1000 \
 --rate={max_write_bw_mbps * 1024} \
 --directory={directory} \
 --rw=write
""")

print("Sequential read latency")
print(f"""
 fio --direct=1 --ioengine=libaio --iodepth=1 --time_based=0 --name=test \
 --size=10MB \
 --overwrite=1 --bs={RS_min_seq_IO}k --number_ios=1000 \
 --rate={max_read_bw_mbps * 1024} \
 --directory={directory} \
 --rw=read
""")

print("Random read latency")
print(f"""
 fio --direct=1 --ioengine=libaio --iodepth=1 --time_based=0 \
 --name=test --size=10MB \
 --overwrite=1 --bs=8k --number_ios=1000 \
 --rate_iops={max_read_iops} \
 --directory={directory} \
 --rw=randread
""")

# Random write latency
print("Random write latency")
print(f"""
 fio --direct=1 --ioengine=libaio --iodepth=1 --time_based=0 \
 --name=test --size=10MB \
 --overwrite=1 --bs=8k --number_ios=1000 --rate_iops={max_write_iops} \
 --directory={directory} \
 --rw=randwrite
""")
