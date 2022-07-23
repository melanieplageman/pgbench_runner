#!/usr/bin/env python3

import re
import json
import sys
from datetime import datetime, timezone

# progress: 560.0 s, 55376.5 tps, lat 0.866 ms stddev 0.268
re_progress = re.compile(
    r"^progress: (?P<ts>\d+.\d+) s, "
    r"(?P<tps>\d+.\d+) tps, "
    r"lat (?P<lat>\d+.\d+) ms "
    r"stddev (?P<stddev>\d+.\d+|NaN)"
)

f = open(sys.argv[1])
output = []

for line in f:
    match = re_progress.match(line)
    if match is None:
        continue
    data = match.groupdict()

    data['ts'] = datetime.fromtimestamp(
        float(data['ts']), tz=timezone.utc
    ).isoformat(timespec='milliseconds')
    data['tps'] = float(data['tps'])
    data['lat'] = float(data['lat'])
    data['stddev'] = float(data['stddev'])

    output.append(data)

json.dump(output, sys.stdout)
