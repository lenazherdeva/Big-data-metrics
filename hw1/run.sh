#!/bin/sh
date=$(date +%Y-%m-%d)
date=$(date -I -d "$date - 1 day")

chmod +x /home/ezherdeva/hw/hw1/total_hits/run.sh
chmod +x /home/ezherdeva/hw/hw1/average_session_time/run.sh
chmod +x /home/ezherdeva/hw/hw1/users_by_country/run.sh
chmod +x /home/ezherdeva/hw/hw1/new_users/run.sh
chmod +x /home/ezherdeva/hw/hw1/profile_liked_three_days/run.sh

/home/ezherdeva/hw/hw1/total_hits/run.sh $date
/home/ezherdeva/hw/hw1/average_session_time/run.sh $date
/home/ezherdeva/hw/hw1/users_by_country/run.sh $date
/home/ezherdeva/hw/hw1/new_users/run.sh $date
/home/ezherdeva/hw/hw1/profile_liked_three_days/run.sh
