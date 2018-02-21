import datetime
import argparse
import subprocess
import os


METRICS = {"total_hits", "average_session_time", "users_by_country", "new_users"}
CWD = os.path.dirname(os.path.realpath(__file__))


def run(date):
    processes = {}
    for metric in METRICS:
        print(metric)
        print(os.path.join(CWD, metric, "run.sh"))

        subprocess.Popen([os.path.join(CWD, metric) + '/run.sh %s' % date], shell=True)


def main():
    previous_day = datetime.datetime.now() - datetime.timedelta(days=1)

    parser = argparse.ArgumentParser(description="All metric run script")
    parser.add_argument("--date", type=str, default=previous_day.strftime("%Y-%m-%d"))

    args = parser.parse_args()
    print(args.date)
    run(date=args.date)


if __name__ == "__main__":
    main()

"""
import argparse
import datetime
import subprocess
import shutil
import os
import os.path
import time


CWD = os.path.dirname(os.path.realpath(__file__))
#METRICS = {"total_hits", "average_session_time", "users_by_country", "new_users"}
METRICS = {"total_hits"}
SLEEPING_TIME = 3 * 60


def run(date):
    processes = {}
    for metric in METRICS:
        processes[metric] = subprocess.Popen([os.path.join(CWD, metric, "run.sh"), "%s" % date], shell=True)
        print(processes[metric])


        #print "%s\t%d" % (words[0], 1)
        #processes[metric] = subprocess.Popen([os.path.join(CWD, metric, "run.py"),
        #                                      "--date={}".format(date)])

        #processes[metric] = subprocess.Popen([os.path.join(CWD, metric, "run.sh"),
                                              #"%s%s" %("--date=", date)])

    run_metrics = METRICS
    while True:  # waiting
        new_run_metrics = []
        for metric in run_metrics:
            if processes[metric].poll() is not None:
                print(metric, processes[metric].poll())
            else:
                new_run_metrics.append(metric)
        if new_run_metrics:
            run_metrics = new_run_metrics
            time.sleep(SLEEPING_TIME)
        else:
            break


def main():
    previous_day = datetime.datetime.now() - datetime.timedelta(days=1)

    parser = argparse.ArgumentParser(description="All metric run script")
    parser.add_argument("--date", type=str, default=previous_day.strftime("%Y-%m-%d"))

    args = parser.parse_args()
    print(args.date)
    run(date=args.date)


if __name__ == "__main__":
    main()
"""