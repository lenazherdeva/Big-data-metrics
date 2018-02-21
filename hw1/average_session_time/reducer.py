#!/usr/bin/env python

import sys

SESSION_TIME = 30*60

total_sessions = -1
total_time = 0
cur_time = 0
prev_ip = None
prev_moment = -3000

for line in sys.stdin:
    words = line.split('\t')
    ip = words[0]
    hours, minutes, sec = list(map(int, words[1].split(':')))
    moment = sec + 60 * (minutes + 60 * hours)
    if ip != prev_ip or moment - prev_moment > SESSION_TIME:
        prev_ip = ip
        total_time += cur_time
        total_sessions += 1
        cur_time = 0
    else:
        cur_time += moment - prev_moment
    prev_moment = moment

total_time += cur_time
total_sessions += 1


print float(total_time) / total_sessions
