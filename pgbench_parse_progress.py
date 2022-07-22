#!/usr/bin/env python3

import re
import json
import sys
import datetime

# progress: 560.0 s, 55376.5 tps, lat 0.866 ms stddev 0.268
re_progress = re.compile(r"^progress: (\d+.\d+) s, (\d+.\d+) tps, lat (\d+.\d+) ms stddev (\d+.\d+|NaN)")

def parse_progress(filename):
    fieldnames = ('ts', 'tps', 'lat', 'stddev')
    output = []
    with open(filename, 'r') as f:
        for line in f:
            if not line.startswith('progress'):
                continue
            match = re_progress.match(line)
            data = list(match.groups())
            if (float(data[0]) > 1000000000):
                from datetime import timezone
                ts = datetime.datetime.fromtimestamp(float(match[1]), tz=timezone.utc)
                data[0] = ts.isoformat(timespec='milliseconds')
            output.append(dict(zip(fieldnames, data)))

    return json.dumps(output)


print(parse_progress(sys.argv[1]))
