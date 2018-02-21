#!/usr/bin/env python

import sys
import happybase
import random


TABLE_NAME = 'hbaseshad_ezherdeva'


def create_table(connection, table_name):
    if table_name in connection.tables():
        return connection.table(table_name)
    #if table_name.endswith('profile_last_three_liked_users'):
    #    connection.create_table(table_name, {"cf": dict(max_versions=3)})
    else:
        connection.create_table(table_name, {"cf": dict()})
    return connection.table(table_name)


def put_key_value(table, key, value):
    table.put(unicode(key).encode('utf-8'), {"cf:value": unicode(value).encode('utf-8')})


def main(date):

    HOSTS = ["hadoop2-%02d.yandex.ru" % i for i in xrange(11, 14)]
    host = random.choice(HOSTS)
    conn = happybase.Connection(host)

    hits_table = create_table(conn, TABLE_NAME + 'profile_hits')
    users_table = create_table(conn, TABLE_NAME + 'profile_users')
    hits_batch = hits_table.batch(batch_size=10000)
    users_batch = users_table.batch(batch_size=10000)

    hours_dict = {}
    old_key = None

    for line in sys.stdin:
        profile, hour, ip = line.split('\t')
        if profile != old_key:
            if old_key is not None:
                for old_hour, ips in hours_dict.items():
                    key = '_'.join([date, old_key, old_hour])
                    put_key_value(hits_batch, key, str(len(ips)))
                    put_key_value(users_batch, key, str(len(set(ips))))
            old_key = profile
            hours_dict = {str(i).zfill(2): [] for i in range(24)}
        hours_dict[hour].append(ip)

    for old_hour, ips in hours_dict.items():
        key = '_'.join([date, old_key, old_hour])
        put_key_value(hits_batch, key, str(len(ips)))
        put_key_value(users_batch, key, str(len(set(ips))))
    hits_batch.send()
    users_batch.send()


if __name__ == '__main__':
    date = sys.argv[1]
    main(date)
