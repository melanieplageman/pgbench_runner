#!/usr/bin/env python3

import json
import sys
import csv

output = []
with open(sys.argv[1]) as csvfile:
    reader = csv.DictReader(csvfile, delimiter='|')
    for row in reader:
        output.append(row)

json.dump(output, sys.stdout)
