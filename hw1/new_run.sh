#!/bin/sh
date=$(date +%Y-%m-%d)
date=$(date -I -d "$date - 1 day")

chmod +x /home/ezherdeva/hw/hw3/profile_users/run.sh


/home/ezherdeva/hw/hw3/profile_users/run.sh $date

