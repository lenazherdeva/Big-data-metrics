DATE=$1
ROOT_DIR="/home/ezherdeva/hw/hw1"
METRIC_NAME="new_users"
DATE_DIR="$ROOT_DIR/results/$DATE"
OUT_DIR="$DATE_DIR/$METRIC_NAME"
OUT_HDFS_DIR="results/$DATE/$METRIC_NAME"
CURRENT_DIR="/home/ezherdeva/hw/hw1/$METRIC_NAME"
NUM_REDUCERS=4

hdfs dfs -rm -r -skipTrash ${OUT_HDFS_DIR}

FOLDER="/user/bigdatashad/logs/"

DAY_0=$FOLDER$DATE/
DAY_1=$FOLDER$(date -d "$DATE -1 days" +%Y-%m-%d)/
DAY_2=$FOLDER$(date -d "$DATE -2 days" +%Y-%m-%d)/
DAY_3=$FOLDER$(date -d "$DATE -3 days" +%Y-%m-%d)/
DAY_4=$FOLDER$(date -d "$DATE -4 days" +%Y-%m-%d)/
DAY_5=$FOLDER$(date -d "$DATE -5 days" +%Y-%m-%d)/
DAY_6=$FOLDER$(date -d "$DATE -6 days" +%Y-%m-%d)/
DAY_7=$FOLDER$(date -d "$DATE -7 days" +%Y-%m-%d)/
DAY_8=$FOLDER$(date -d "$DATE -8 days" +%Y-%m-%d)/
DAY_9=$FOLDER$(date -d "$DATE -9 days" +%Y-%m-%d)/
DAY_10=$FOLDER$(date -d "$DATE -10 days" +%Y-%m-%d)/
DAY_11=$FOLDER$(date -d "$DATE -11 days" +%Y-%m-%d)/
DAY_12=$FOLDER$(date -d "$DATE -12 days" +%Y-%m-%d)/
DAY_13=$FOLDER$(date -d "$DATE -13 days" +%Y-%m-%d)/

echo "$DAY_13"

yarn jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
    -D mapreduce.job.name="$METRIC_NAME($DATE)" \
    -D mapreduce.job.reduces=$NUM_REDUCERS \
    -files mapper.py,reducer.py \
    -mapper mapper.py \
    -reducer "reducer.py $DATE" \
    -input $DAY_0 \
    -input $DAY_1 \
    -input $DAY_2 \
    -input $DAY_3 \
    -input $DAY_4 \
    -input $DAY_5 \
    -input $DAY_6 \
    -input $DAY_7 \
    -input $DAY_8 \
    -input $DAY_9 \
    -input $DAY_10 \
    -input $DAY_11 \
    -input $DAY_12 \
    -input $DAY_13 \
    -output ${OUT_HDFS_DIR}


for num in `seq 0 $[$NUM_REDUCERS - 1]`
do
    hdfs dfs -cat ${OUT_HDFS_DIR}/part-0000$num > ${OUT_DIR}
    hdfs dfs -cat ${OUT_HDFS_DIR}/part-0000$num | head
done