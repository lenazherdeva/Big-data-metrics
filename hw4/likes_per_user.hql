USE ezherdeva;

ADD JAR /opt/cloudera/parcels/CDH-5.9.0-1.cdh5.9.0.p0.23/lib/hive/lib/hive-contrib.jar;

DROP TABLE access_log;
CREATE EXTERNAL TABLE access_log (
    ip STRING,
    date STRING,
    url STRING,
    status STRING,
    referer STRING,
    user_agent STRING
)
PARTITIONED BY (day string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.RegexSerDe'
WITH SERDEPROPERTIES (
    "input.regex" = "([\\d\\.:]+) - - \\[(\\S+) [^\"]+\\] \"\\w+ ([^\"]+) HTTP/[\\d\\.]+\" (\\d+) \\d+ \"([^\"]+)\" \"(.*?)\""
)
STORED AS TEXTFILE;

ALTER TABLE access_log ADD PARTITION(day='${DATE}')
LOCATION '/user/bigdatashad/logs/${DATE}';

SELECT COUNT(CASE WHEN url LIKE '%like%' THEN 1 END) / COUNT(DISTINCT ip) FROM access_log
WHERE day='${DATE}' AND status='200';