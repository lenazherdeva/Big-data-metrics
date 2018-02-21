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

DROP TABLE parsed_log;

CREATE TABLE IF NOT EXISTS parsed_log (
    ip STRING,
    date TIMESTAMP,
    status SMALLINT,
    url STRING,
    profile STRING,
    referer STRING
)
PARTITIONED BY (day STRING, hour STRING)
STORED AS RCFILE;

INSERT OVERWRITE TABLE parsed_log
PARTITION(day='${DATE}', hour)

SELECT
    ip,
    from_unixtime(unix_timestamp(date ,'dd/MMM/yyyy:HH:mm:ss')),
    CAST(status AS smallint),
    url,
    regexp_extract(url, '/(id\\d+)$', 1),
    referer,
    regexp_extract(date, '(\\d+/.+/\\d+):(\\d+):(\\d+):(\\d+)', 2) as hour
FROM access_log
WHERE day='${DATE}' AND status='200';

SELECT hour, rank_r, profile, count_v FROM
(SELECT hour, profile, COUNT(*) as count_v, RANK() OVER (PARTITION BY hour ORDER BY COUNT(*) DESC, profile ASC) as rank_r
FROM parsed_log WHERE profile != '' GROUP BY hour, profile ORDER BY hour, rank_r) table WHERE rank_r < 4;