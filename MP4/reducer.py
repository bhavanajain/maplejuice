#!/usr/bin/env python

import sys

result = {}

for line in sys.stdin:
	words = line.strip().split(" ")
	key = words[0]
	value = int(words[1])

	if key in result:
		result[key] += value
	else:
		result[key] = value

for key in result:
	print'%s-%s' % (key, result[key]) 