#!/usr/bin/env python


import re
import datetime
from pyspark import SparkConf, SparkContext
import argparse
import os


LOG_LINE_RE = r'\"(.*?)\"|\[(.*?)\]|(\S+)'


def parse_log_line(log_line):
    fields = list(map(''.join, re.findall(LOG_LINE_RE, log_line)))
    return fields


def extract_fields(log_line):
    try:
        fields = parse_log_line(log_line)
        response = fields[5]
        if response != '200':
            return None
        url = fields[4]
        if 'like=1' in url:
            id = re.match(r'.*/id(\d+)', url).groups()[0]
            return id
    except:
        return None


def get_date():
    previous_day = datetime.datetime.now() - datetime.timedelta(days=1)
    parser = argparse.ArgumentParser(description="Profile Liked Three Days")
    parser.add_argument("--date", type=str, default=previous_day.strftime("%Y-%m-%d"))
    args, _ = parser.parse_known_args()
    date = datetime.datetime.strptime(args.date, "%Y-%m-%d")
    return date


def main():
    conf = SparkConf().setAppName("profile_liked")
    sc = SparkContext(conf=conf)
    date = get_date()
    date -= datetime.timedelta(days=2)
    print date
    liked_users = None
    for _ in range(3):
        log = sc.textFile("/user/bigdatashad/logs/" + date.strftime("%Y-%m-%d"))
        users = log.map(extract_fields).filter(lambda x: x is not None)
        if liked_users is None:
            liked_users = users
        else:
            liked_users = liked_users.intersection(users)
        date -= datetime.timedelta(days=1)
    count_liked_users = liked_users.count()
    return count_liked_users


if __name__ == '__main__':
    count = main()
    date = get_date()
    date -= datetime.timedelta(days=2)
    directory = '/home/ezherdeva/hw/hw1/results/' + date.strftime("%Y-%m-%d")
    if not os.path.exists(directory):
        os.makedirs(directory)
    with open(directory + '/profile_liked_three_days', 'w') as f:
        f.write(str(count))
