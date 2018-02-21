#!/usr/bin/env python


import sys
from collections import defaultdict
from bisect import bisect
import re


def int_ip(ip):
    byte_0, byte_1, byte_2, byte_3 = map(int, ip.split("."))
    dec = byte_0 << 24 | byte_1 << 16 | byte_2 << 8 | byte_3 << 0
    return dec


bottom_index = []
countries = []
countries_dict = defaultdict(lambda : len(countries_dict))
for line in open("IP2LOCATION-LITE-DB1.CSV"):
    try:
        values = re.match(r'"(\d+)","(\d+)","(.*)","(.*)"', line)
        bottom, top, code, name = values.groups()
    except:
        continue
    countries.append(countries_dict[name])
    bottom_index.append(int(bottom))
countries_dict = {value: key for key, value in countries_dict.items()}

for line in sys.stdin:
    try:
        words = list(map(''.join, re.findall(r'\"(.*?)\"|\[(.*?)\]|(\S+)', line)))
        code = words[5]
        ip = words[0]
    except:
        continue
    if code == "200":
        index = bisect(bottom_index, int_ip(ip))
        print "%s\t%s" % (ip, countries_dict[countries[index - 1]])

