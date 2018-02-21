#!/usr/bin/env python

import sys

prev_ip = None
counter = 0

for line in sys.stdin:
    words = line.split('\t')
    counter += 1
print(counter)
