DROP TABLE IF EXISTS nigeria_vips;
CREATE EXTERNAL TABLE nigeria_vips (
    name STRING,
    birthday STRING,
    description STRING
) ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'
LOCATION '/input/nigeria_vips/';

DROP TABLE IF EXISTS nigeria_holidays;
CREATE EXTERNAL TABLE nigeria_holidays (
    yearly_date STRING,
    description STRING
) ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'
LOCATION '/input/nigeria_holidays/';