DATE=$1
ROOT_DIR="/home/ezherdeva/hw/hw1"
METRIC_NAME="average_session_time"
DATE_DIR="$ROOT_DIR/results/$DATE"
OUT_DIR="$DATE_DIR/$METRIC_NAME"
OUT_HDFS_DIR="results/$DATE/$METRIC_NAME"
CURRENT_DIR="/home/ezherdeva/hw/hw1/$METRIC_NAME"
NUM_REDUCERS=1

hdfs dfs -rm -r -skipTrash ${OUT_HDFS_DIR}

yarn jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
    -D mapreduce.job.name="$METRIC_NAME($DATE)" \
    -D mapreduce.job.reduces=$NUM_REDUCERS \
    -D mapred.output.key.comparator.class=org.apache.hadoop.mapred.lib.KeyFieldBasedComparator \
    -D mapred.text.key.comparator.options=-k1,2 \
    -D stream.num.map.output.key.fields=2 \
    -D mapred.text.key.partitioner.options=-k1,1 \
    -files "$CURRENT_DIR/mapper.py","$CURRENT_DIR/reducer.py" \
    -mapper mapper.py \
    -reducer reducer.py \
    -input /user/bigdatashad/logs/$DATE/access.log.$DATE \
    -output ${OUT_HDFS_DIR}

mkdir -p $DATE_DIR

if [ -e "$OUT_DIR" ] ; then
    rm -r "$OUT_DIR"
fi


hdfs dfs -get \
    $OUT_HDFS_DIR/part-00000 \
    $OUT_DIR