#!/usr/bin/env python3

import re
import json
import sys

# duration: 100 s
re_duration = re.compile(r"^duration: (\d+.) s")

# latency average = 2.591 ms
re_latency_average = re.compile(r"^latency average = (\d+.\d+) ms")

# latency stddev = 1.300 ms
re_latency_stddev = re.compile(r"^latency stddev = (\d+.\d+) ms")

# tps = 537.881487 (without initial connection time)
re_tps = re.compile(r"^tps = (\d+.\d+) \(without initial connection time\)")

# initial connection time = 3.020 ms
re_connection = re.compile(r"^initial connection time = (\d+.\d+) ms")

f = open(sys.argv[1])
data = {}

for line in f:
    match = re_duration.match(line)
    if match is not None:
        data['duration'] = int(match.group(1))
        continue

    match = re_latency_average.match(line)
    if match is not None:
        data['lat_avg'] = float(match.group(1))
        continue

    match = re_latency_stddev.match(line)
    if match is not None:
        data['lat_stddev'] = float(match.group(1))
        continue

    match = re_tps.match(line)
    if match is not None:
        data['tps_excl'] = float(match.group(1))
        continue

    match = re_connection.match(line)
    if match is not None:
        data['connection_time'] = float(match.group(1))
        continue

    data['duration'] = data.get('duration', None)

json.dump(data, sys.stdout)
