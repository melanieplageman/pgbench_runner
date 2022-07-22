#!/usr/bin/env python3

import re
import json
import sys

# latency average = 2.591 ms
# latency stddev = 1.300 ms
re_latency = re.compile(r"^latency (average|stddev) = (\d+.\d+) ms")

# tps = 537.881487 (without initial connection time)
re_tps = re.compile(r"^tps = (\d+.\d+) \(without initial connection time\)")

# initial connection time = 3.020 ms
re_connection = re.compile(r"^initial connection time = (\d+.\d+) ms")

def parse_summary(filename):
    data = {}
    with open(filename, 'r') as f:
        for line in f:
            if line.startswith('latency'):
                match = re_latency.match(line)
                if match.group(1) == 'average':
                    data['lat_avg'] = match.group(2)
                if match.group(1) == 'stddev':
                    data['lat_stddev'] = match.group(2)

            elif line.startswith('tps'):
                match = re_tps.match(line)
                data['tps_excl'] = match.group(1)

            elif line.startswith('initial'):
                match = re_connection.match(line)
                data['connection_time'] = match.group(1)

    return json.dumps(data)

print(parse_summary(sys.argv[1]))
