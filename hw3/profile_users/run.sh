#!/usr/bin/env bash

DATE=$1
CURRENT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

spark-submit --master yarn --num-executors 32 $CURRENT_DIR/profile_users.py $DATE
