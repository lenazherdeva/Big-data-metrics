#!/usr/bin/env python

import sys
import re
from datetime import datetime


for line in sys.stdin:
    try:
        words = list(map(''.join, re.findall(r'\"(.*?)\"|\[(.*?)\]|(\S+)', line)))
        ip = words[0]
        date = words[3]
        code = words[5]
    except:
        continue
    if code == '200':
        date = datetime.strptime(date[:-6], "%d/%b/%Y:%H:%M:%S")
        cur_day = date.timetuple().tm_yday
        print "%s\t%s" % (ip, cur_day)
