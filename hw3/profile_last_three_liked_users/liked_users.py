#!/usr/bin/env python

import re
import datetime
import sys
import happybase
import random
import getpass
import copy
import heapq
import os


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


LOG_LINE_RE = re.compile('([\d\.:]+) - - \[(\S+) [^"]+\] "(\w+) ([^"]+) (HTTP/[\d\.]+)" (\d+) \d+ "([^"]+)" "([^"]+)"')


def extract_fields(line):
    try:
        match = LOG_LINE_RE.search(line.strip())
        if not match:
            return None
        if match.group(6) != "200":
            return None
        ip = match.group(1)
        date = datetime.datetime.strptime(match.group(2), "%d/%b/%Y:%H:%M:%S")
        url = match.group(4)
        referer = match.group(7)
        timestamp = int(date.strftime("%s"))
        person = url[1:].split('?')[0]
        return person, (ip, timestamp)
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
    TABLE_NAME = 'bigdatashad_{}_profile_last_three_liked_users'.format(getpass.getuser())
    table = create_htable(table_name=TABLE_NAME)
    b = table.batch()
    cur_size = 0
    for ip, top_profile_ids in data.items():
        b.put('{}_{}'.format(ip, date), {'cf:liked_users': ' '.join(map(str, top_profile_ids))})
        cur_size += 1
        if cur_size >= BATCH_SIZE:
            b.send()
            b = table.batch()
            cur_size = 0


def get_workdirs(date):
    log_path = '/user/bigdatashad/logs/{}/access.log.{}'.format(date, date)
    return log_path


def subtract_from_date(date, n_days):
    dt = datetime.datetime.strptime(date, '%Y-%m-%d') - datetime.timedelta(days=n_days)
    return dt.strftime('%Y-%m-%d')


def count_and_sort(ip_and_timestamp):
    ans = [x[0] for x in heapq.nlargest(3, ip_and_timestamp, key=lambda x: x[1])]
    return ans


def main(date):
    conf = SparkConf().setAppName("Profile last three liked users")
    sc = SparkContext(conf=conf)
    """for _ in range(5):
        files.append('/user/bigdatashad/logs/{}/access.log.{}'.format(date, date))
        print(files)
        date1 = datetime.datetime.strptime(date, "%Y-%m-%d")
        date1 -= datetime.timedelta(days=1)
        date = date1.strftime("%Y-%m-%d")"""
    all_log = [get_workdirs(subtract_from_date(date, i)) for i in range(5)]
    log = sc.textFile(','.join(all_log))
    profiles = log.map(extract_fields) \
        .filter(lambda x: x is not None) \
        .distinct() \
        .groupByKey() \
        .mapValues(count_and_sort)
    profiles_dict = dict(profiles.collect())
    put_to_hbase(profiles_dict, date=date)


if __name__ == '__main__':
    date = sys.argv[1]
    main(date)
