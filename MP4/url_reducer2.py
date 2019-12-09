#!/usr/bin/env python

import sys

result = {}

total = 0

url_dict = {}

for line in sys.stdin:
    line = line.strip().split(" ")
    key = line[0]	# this is just 1
    url, urlcount = tuple(line[1].split("$$$$"))

    urlcount = int(urlcount)
    url_dict[url] = urlcount
    total += urlcount

for url in url_dict:
    print'%s=%s' % (url, float(url_dict[url]) * 100/total) 