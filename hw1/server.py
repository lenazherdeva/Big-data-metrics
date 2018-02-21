#!/usr/bin/env python

import argparse
import datetime
import getpass
import hashlib
import os.path
import struct
import re
import json
import random
import happybase


from flask import Flask, request, abort, jsonify

app = Flask(__name__)
app.secret_key = "secret_key"


def iterate_between_dates(start_date, end_date):
    span = end_date - start_date
    for i in xrange(span.days + 1):
        yield start_date + datetime.timedelta(days=i)


@app.route("/")
def index():
    return "OK!"


def convert_to_dict_view(content):
    res = {}
    for line in content.splitlines():
        val, count = re.match(r"(.*)\s(\d*)", line.strip()).groups()
        res[val] = int(count)
    return res


def connect_to_hbase(hosts, table_name):
    host = random.choice(hosts)
    connection = happybase.Connection(host)
    connection.open()
    if table_name not in connection.tables():
        connection.create_table(table_name, {'cf': dict()})
    return happybase.Table(table_name, connection)


def results_for_profile_hits(date, profile_id):
    try:
        hosts = ['hadoop2-%02d.yandex.ru' % i for i in xrange(11, 14)]
        TABLE_NAME = 'bigdatashad_{}_profile_hits'.format(getpass.getuser())
        table = connect_to_hbase(hosts, table_name=TABLE_NAME)
        row = '{}_{}'.format(profile_id, date)
        result_str = list(table.scan(row_start='/'+row, row_stop='/'+row))[0][1]['cf:hour_counts']
        result = map(int, result_str.split(' '))
        return result
    except Exception as e:
        print e
        return None


def results_for_profile_users(date, profile_id):
    try:
        hosts = ['hadoop2-%02d.yandex.ru' % i for i in xrange(11, 14)]
        TABLE_NAME = 'bigdatashad_{}_profile_users'.format(getpass.getuser())
        table = connect_to_hbase(hosts, table_name=TABLE_NAME)
        row = '{}_{}'.format(profile_id, date)
        result_str = list(table.scan(row_start='/'+row, row_stop='/'+row))[0][1]['cf:hour_counts']
        result = map(int, result_str.split(' '))
        return result
    except Exception as e:
        print e
        return None


def results_for_user_most_visited_profiles(date, ip):
    try:
        hosts = ['hadoop2-%02d.yandex.ru' % i for i in xrange(11, 14)]
        TABLE_NAME = 'bigdatashad_{}_user_most_visited_profiles'.format(getpass.getuser())
        table = connect_to_hbase(hosts, TABLE_NAME)
        row = '{}_{}'.format(ip, date)
        result_str = list(table.scan(row_start=row, row_stop=row))[0][1]['cf:top_profile_ids']
        result = result_str.split(' ')
        return result
    except Exception as e:
        print e
        return []


def results_for_profile_last_three_liked_users(date, profile_id):
    try:
        hosts = ['hadoop2-%02d.yandex.ru' % i for i in xrange(11, 14)]
        TABLE_NAME = 'bigdatashad_{}_profile_last_three_liked_users'.format(getpass.getuser())
        table = connect_to_hbase(hosts, TABLE_NAME)
        row = '{}_{}'.format(profile_id, date)
        result_str = list(table.scan(row_start=row, row_stop=row))[0][1]['cf:liked_users']
        result = result_str.split(' ')
        return result
    except Exception as e:
        print e
        return []


def results_for_date(date, profile_id, ip):
    results = {}
    result_hits = results_for_profile_hits(date, profile_id)
    if result_hits is not None:
        results['profile_hits'] = result_hits
    results_users = results_for_profile_users(date, profile_id)
    if results_users is not None:
        results['profile_users'] = results_users
    results_most_visited = results_for_user_most_visited_profiles(date, ip)
    if results_most_visited is not None:
        results['user_most_visited_profiles'] = results_most_visited
    result_liked_users = results_for_profile_last_three_liked_users(date, profile_id)
    #if result_liked_users is not None:
    #    results['profile_last_three_liked_users'] = result_liked_users
    return results


"""metrics = [('total_hits', int), ('average_session_time', float),
           ("users_by_country", convert_to_dict_view), ("new_users", int),
           ('profile_liked_three_days', int)]"""


"""@app.route("/api/hw1")
def api_hw1():
    start_date = request.args.get("start_date", None)
    end_date = request.args.get("end_date", None)
    if start_date is None or end_date is None:
        abort(400)
    start_date = datetime.datetime(*map(int, start_date.split("-")))
    end_date = datetime.datetime(*map(int, end_date.split("-")))

    result = {}

    for date in iterate_between_dates(start_date, end_date):
        date_str = date.strftime("%Y-%m-%d")
        result[date_str] = {}
        try:
            result[date_str]['session_referers']= json.load(open('/home/ezherdeva/hw/hw1/results/' + date_str + '/' + 'session_referers'))
        except Exception:
            pass
        for metric, date_type in metrics:
            filename = '/home/ezherdeva/hw/hw1/results/' + date_str + '/' + metric
            #filename = '/home/ezherdeva/hw/hw1/results/2017-10-18/total_hits'
            if not os.path.isfile(filename):
                continue
            with open(filename) as f:
                content = f.read()
                result[date_str][metric] = date_type(content)
    return jsonify(result)"""


@app.route('/api/hw1')
def api_hw1():
    return jsonify({})


@app.route('/api/hw3')
def api_hw3():
    start_date = request.args.get('start_date', None)
    end_date = request.args.get('end_date', None)
    profile_id = request.args.get('profile_id', None)
    ip = request.args.get('user_ip', None)
    print('start')

    if start_date is None or end_date is None or profile_id is None:
        abort(400)

    start_date = datetime.datetime(*map(int, start_date.split('-')))
    end_date = datetime.datetime(*map(int, end_date.split('-')))

    result = {}
    for date in iterate_between_dates(start_date, end_date):
        result[date.strftime('%Y-%m-%d')] = results_for_date(date.strftime('%Y-%m-%d'), profile_id, ip)

    print(result)
    return jsonify(result)


def login_to_port(login):
    hasher = hashlib.new("sha1")
    hasher.update(login)
    values = struct.unpack("IIIII", hasher.digest())
    folder = lambda a, x: a ^ x + 0x9e3779b9 + (a << 6) + (a >> 2)
    return 10000 + reduce(folder, values) % 20000


def main():
    parser = argparse.ArgumentParser(description="HW 1 Elena Zherdeva")
    parser.add_argument("--host", type=str, default="127.0.0.1")
    parser.add_argument("--port", type=int, default=login_to_port(getpass.getuser()))
    parser.add_argument("--debug", action="store_true", dest="debug")
    parser.add_argument("--no-debug", action="store_false", dest="debug")
    parser.set_defaults(debug=False)

    args = parser.parse_args()
    app.run(host=args.host, port=args.port, debug=args.debug)

if __name__ == "__main__":
    main()
