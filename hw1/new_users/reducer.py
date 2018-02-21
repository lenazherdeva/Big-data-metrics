#!/usr/bin/env python


import sys
from datetime import datetime


def main():
    new_users = 0
    prev_ip = None
    dates_list = set()
    cur_day_inp = sys.argv[1]
    cur_day = datetime.strptime(cur_day_inp, "%Y-%m-%d").timetuple().tm_yday
    corr = set([cur_day])
    for line in sys.stdin:
        ip, day = line.split('\t')
        day = int(day)
        if ip != prev_ip:
            if prev_ip is not None:
                if len(dates_list) == 1 and dates_list == corr:
                    new_users += 1
                dates_list = set()

            dates_list.add(day)
            prev_ip = ip
        else:
            dates_list.add(day)

    if prev_ip is not None:
        if len(dates_list) == 1 and dates_list == corr:
            new_users += 1

    print new_users


if __name__ == "__main__":
    main()
