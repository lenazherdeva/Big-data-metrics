#!/usr/bin/env python

import re
import datetime
import sys
import happybase
import random
import getpass
from collections import Counter

from pyspark import SparkConf, SparkContext


LOG_LINE_RE = r'([(\d\.)]+) - - \[(.*?)\] "(.*?)" (\d+) (\d+) "(.*?)" "(.*?)"'
BATCH_SIZE = 2 ** 15

"""def extract_fields(line):
    match = LOG_LINE_RE.match(line)
    if not match:
        return None
    if match.group(6) != "200":
        return None
    ip = match.group(1)
    profile = match.group(4)
    if 'like=1' in profile:
        return None
    try:
        date = match.group(2)[:-6]
        hour = datetime.datetime.strptime(date, '%d/%b/%Y:%H:%M:%S').hour
    except:
        return None
    return ip, profile"""


def extract_fields(log_line):
    try:
        matches = re.match(LOG_LINE_RE, log_line).groups()
        result = {
            'ip': matches[0],
            'date': matches[1],
            'request': matches[2],
            'response': matches[3],
            'response_size': matches[4],
            'referer': matches[5],
            'user_agent': matches[6]
        }
        if result['response'] != '200':
            return None
        profile = re.match(r'^/(id\d+)$', result['request'].split(' ')[1]).groups()[0]
        ip = result['ip']
        return ip, profile
    except:
        return None


def create_htable(table_name):
    HOSTS = ["hadoop2-%02d.yandex.ru" % i for i in xrange(11, 14)]
    host = random.choice(HOSTS)
    conn = happybase.Connection(host)
    conn.open()
    if table_name not in conn.tables():
        conn.create_table(table_name, {'cf': dict()})
    return happybase.Table(table_name, conn)


def put_to_hbase(data, date):
    TABLE_NAME = 'bigdatashad_{}_user_most_visited_profiles'.format(getpass.getuser())
    table = create_htable(table_name=TABLE_NAME)
    b = table.batch()
    cur_size = 0
    for ip, top_profile_ids in data.items():
        b.put('{}_{}'.format(ip, date), {'cf:top_profile_ids': ' '.join(map(str, top_profile_ids))})
        cur_size += 1
        if cur_size >= BATCH_SIZE:
            b.send()
            b = table.batch()
            cur_size = 0


def count_and_sort(profiles):
    profiles = Counter(profiles).items()
    profiles = sorted(profiles, key=lambda x: x[0])
    profiles = sorted(profiles, key=lambda x: x[1], reverse=True)  # lexicographically
    profile_ids = [x[0] for x in profiles]
    return profile_ids


def main(date):
    conf = SparkConf().setAppName("User most visited profiles")
    sc = SparkContext(conf=conf)
    log = sc.textFile('/user/bigdatashad/logs/{}/access.log.{}'.format(date, date))

    fields = log.map(extract_fields).filter(lambda x: x is not None)
    users = fields.groupByKey().mapValues(count_and_sort)
    profiles = dict(users.collect())

    put_to_hbase(profiles, date=date)


if __name__ == '__main__':
    date = sys.argv[1]
    main(date)
