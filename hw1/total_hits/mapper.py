#!/usr/bin/env python

import sys
import re


for line in sys.stdin:
    try:
        words = list(map(''.join, re.findall(r'\"(.*?)\"|\[(.*?)\]|(\S+)', line)))
        code = words[5]
        ip = words[0]
    except:
        continue
    if code == "200":
        print "%s\t%d" % (ip, 1)
