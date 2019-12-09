#!/usr/bin/python

import sys
import re

wordcount = {}

for line in sys.stdin:
    words = line.strip().split(" ")
    for word in words:
    	if len(word) > 0:
    		if word.isalpha():
	    		word = word.lower()
	    		if word in wordcount:
	    			wordcount[word] += 1
	    		else:
	    			wordcount[word] = 1
    	
for key in wordcount:
	print'%s %s' % (key, wordcount[key]) 