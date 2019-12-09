#!/usr/bin/env python

import sys

result = {}

for line in sys.stdin:
    line = line.strip().split(" ")
    key = line[0]
    value = int(line[1])

    if key in result:
        result[key] += value
    else:
        result[key] = value

for key in result:
    print'%s$$$$%s' % (key, result[key]) 