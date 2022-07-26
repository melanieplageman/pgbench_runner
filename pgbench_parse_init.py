#!/usr/bin/env python3

import re
import json
import sys

f = open(sys.argv[1])
output = {}

# done in 1127.08 s (drop tables 0.00 s, create tables 0.01 s, client-side generate 435.15 s, vacuum 332.26 s, primary keys 359.65 s).
re_init_total = re.compile(
    r"^done in (\d+.\d+) s \("
    r"drop tables (\d+.\d+) s, "
    r"create tables (\d+.\d+) s, "
    r"client-side generate (\d+.\d+) s, "
    r"vacuum (\d+.\d+) s, "
    r"primary keys (\d+.\d+) s\)"
)
output = {}

for line in f:
    match = re_init_total.match(line)
    if match is None:
        continue
    data = match.groups(0)
    output['total'] = float(data[0])
    output['drop_tables'] = float(data[1])
    output['create_tables'] = float(data[2])
    output['client-side_generate'] = float(data[3])
    output['vacuum'] = float(data[4])
    output['primary_keys'] = float(data[5])

json.dump(output, sys.stdout)
