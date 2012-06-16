
DROP TABLE IF EXISTS weblog_entries;
CREATE EXTERNAL TABLE weblog_entries (
      md5 STRING,
      url STRING,
      request_date STRING,
      request_time STRING,
      ip STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'
LOCATION '/input/weblog/';
