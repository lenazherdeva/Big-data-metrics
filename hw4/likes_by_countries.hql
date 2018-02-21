USE ezherdeva;

ADD JAR /opt/cloudera/parcels/CDH-5.9.0-1.cdh5.9.0.p0.23/lib/hive/lib/hive-contrib.jar;


ADD FILE /home/ezherdeva/hw/hw4/IP2LOCATION-LITE-DB1.CSV;
ADD FILE /home/ezherdeva/hw/hw4/path.py;

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


SELECT country, COUNT(CASE WHEN url LIKE '%like%' THEN 1 END), COUNT(DISTINCT ip),
COUNT(CASE WHEN url LIKE '%like%' THEN 1 END) / COUNT(DISTINCT ip) FROM
(SELECT TRANSFORM (ip, url) USING 'path.py' AS country, ip, url FROM parsed_log WHERE day='${DATE}' AND status='200')
TABLE
GROUP BY country ORDER BY country;