#!/usr/bin/env python

import sys
from collections import defaultdict


cur_ip = None
res = defaultdict(int)


for line in sys.stdin:
    ip, country = line.strip().split('\t', 1)
    if cur_ip != ip:
        cur_ip = ip
        res[country] += 1

for country, count in res.items()[::-1]:
    print country, count


