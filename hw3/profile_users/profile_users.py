#!/usr/bin/env python

import re
import datetime
import sys
import happybase
import random
import getpass


from pyspark import SparkConf, SparkContext


LOG_LINE_RE = re.compile('([\d\.:]+) - - \[(\S+ [^"]+)\] "(\w+) ([^"]+) (HTTP/[\d\.]+)" (\d+) \d+ "([^"]+)" "([^"]+)"')
BATCH_SIZE = 2 ** 15


def extract_fields(line):
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
    return profile, hour, ip


def get_hits_for_hour(hours):
    count_dict = dict((hour, 0) for hour in range(0, 24))
    for hour in hours:
        count_dict[hour] += 1
    counts = [count_dict[key] for key in sorted(count_dict.keys())]
    return counts


def create_htable(table_name):
    HOSTS = ["hadoop2-%02d.yandex.ru" % i for i in xrange(11, 14)]
    host = random.choice(HOSTS)
    conn = happybase.Connection(host)
    conn.open()
    if table_name not in conn.tables():
        conn.create_table(table_name, {'cf': dict()})
    return happybase.Table(table_name, conn)


def put_to_hbase(profiles_with_hits_dict, date):
    TABLE_NAME = 'bigdatashad_{}_profile_users'.format(getpass.getuser())
    table = create_htable(table_name=TABLE_NAME)
    b = table.batch()
    cur_size = 0
    for ip, top_profile_ids in profiles_with_hits_dict.items():
        b.put('{}_{}'.format(ip, date), {'cf:hour_counts': ' '.join(map(str, top_profile_ids))})
        cur_size += 1
        if cur_size >= BATCH_SIZE:
            b.send()
            b = table.batch()
            cur_size = 0


def main(date):
    conf = SparkConf().setAppName("Profile users")
    sc = SparkContext(conf=conf)
    log = sc.textFile('/user/bigdatashad/logs/{}/access.log.{}'.format(date, date))

    fields = log.map(extract_fields).filter(lambda x: x is not None).distinct()
    fields_profile_hour = fields.map(lambda x: (x[0], x[1]))
    users = fields_profile_hour.groupByKey().mapValues(get_hits_for_hour)
    profiles_with_hits_dict = dict(users.collect())

    put_to_hbase(profiles_with_hits_dict, date=date)


if __name__ == '__main__':
    date = sys.argv[1]
    main(date)
