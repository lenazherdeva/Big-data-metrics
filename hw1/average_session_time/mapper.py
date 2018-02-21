#!/usr/bin/env python

import sys
import re


for line in sys.stdin:
    try:
        words = list(map(''.join, re.findall(r'\"(.*?)\"|\[(.*?)\]|(\S+)', line)))
        query_time = words[3]
        ip = words[0]
        code = words[5]
    except:
        continue
    if code == "200":
        date, sep, other = query_time.partition(':')
        time = other.split(' ')[0]
        print "%s\t%s" % (ip, time)

