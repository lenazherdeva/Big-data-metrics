#!/usr/bin/env python

import os
import datetime
import re
import argparse
import json

from pyspark import SparkConf, SparkContext


LOG_LINE_RE = re.compile('([\d\.:]+) - - \[(\S+ [^"]+)\] "(\w+) ([^"]+) (HTTP/[\d\.]+)" (\d+) \d+ "([^"]+)" "([^"]+)"')


def extract_fields(line):
    match = LOG_LINE_RE.match(line)
    if not match:
        return None
    if match.group(6) != "200":
        return None
    ip = match.group(1)
    try:
        date = datetime.datetime.strptime(match.group(2)[:-6], "%d/%b/%Y:%H:%M:%S")
        timestamp = int(date.strftime("%s"))
    except:
        return None
    url = match.group(7)
    return ((ip, timestamp), (timestamp, url))


def tuple_partitioner(pair):
    return hash(pair[0])


def get_date():
    previous_day = datetime.datetime.now() - datetime.timedelta(days=1)
    parser = argparse.ArgumentParser(description="Profile Liked Three Days")
    parser.add_argument("--date", type=str, default=previous_day.strftime("%Y-%m-%d"))
    args, _ = parser.parse_known_args()
    date = datetime.datetime.strptime(args.date, "%Y-%m-%d")
    return date


def count_user_sessions(events):
    events = events[1]
    events = iter(events)
    start_timestamp = None
    last_timestamp = None
    max_ref = None
    referers = []
    for timestamp, refer in events:
        if start_timestamp is None:
            start_timestamp = timestamp
            max_ref = refer
        elif timestamp == start_timestamp:
            if refer > max_ref:
                max_ref = refer
        elif timestamp - last_timestamp > 30 * 60:
            referers.append(max_ref)
            start_timestamp = timestamp
            max_ref = refer
        last_timestamp = timestamp
    if max_ref is not None:
        referers.append(max_ref)
    return referers


def main():
    conf = SparkConf().setAppName("User sessions")
    sc = SparkContext(conf=conf)
    date = get_date()
    date -= datetime.timedelta(days=2)
    log = sc.textFile("/user/bigdatashad/logs/" + date.strftime("%Y-%m-%d"))
    fields = log.map(extract_fields).filter(lambda x: x is not None)
    ips_sorted = fields.repartitionAndSortWithinPartitions(8, tuple_partitioner) \
        .map(lambda x: (x[0][0], x[1]))
    ips_grouped = ips_sorted.groupByKey()
    sessions = ips_grouped.flatMap(count_user_sessions).countByValue()
    return sessions


if __name__ == '__main__':
    data = main()
    date = get_date()
    date -= datetime.timedelta(days=2)
    directory = '/home/ezherdeva/hw/hw1/results/' + date.strftime("%Y-%m-%d")
    if not os.path.exists(directory):
        os.makedirs(directory)
    with open(directory + '/session_referers', 'w') as f:
        json.dump(data, f)

