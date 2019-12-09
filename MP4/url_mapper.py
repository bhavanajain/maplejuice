#!/usr/bin/env python

import sys
import re

urlcount = {}

for line in sys.stdin:
    line = line.strip()

    urls = re.findall('https?://(?:[-\w.]|(?:%[\da-fA-F]{2}))+', line)

    for url in urls:
        url = url.replace(":", "").replace("/", "").replace(".", "").replace("-", "")
        if url  in urlcount:
            urlcount[url] += 1
        else:
            urlcount[url] = 1
            

for key in urlcount.keys():
    print '%s %s'%(key, urlcount[key])

