#!/usr/bin/env python3

import re
import json
import sys
# done in 1127.08 s (drop tables 0.00 s, create tables 0.01 s, client-side generate 435.15 s, vacuum 332.26 s, primary keys 359.65 s).
def parse_init(filename):
    re_init_total = re.compile(r"^done in (\d+.\d+) s \(drop tables (\d+.\d+) s, create tables (\d+.\d+) s, client-side generate (\d+.\d+) s, vacuum (\d+.\d+) s, primary keys (\d+.\d+) s\)")
    output = {}
    with open(filename, 'r') as fh:
        for line in fh:
            match = re_init_total.match(line)
            if match:
                data = match.groups(0)
                output['total'] = data[0]
                output['drop_tables'] = data[1]
                output['create_tables'] = data[2]
                output['client-side_generate'] = data[3]
                output['vacuum'] = data[4]
                output['primary_keys'] = data[5]

    return output

init_dict = parse_init(sys.argv[1])
print(json.dumps(init_dict))
