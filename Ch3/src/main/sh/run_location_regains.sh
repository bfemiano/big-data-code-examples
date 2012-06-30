#!/bin/bash
$HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/contrib/streaming/hadoop-streaming-0.20.2-cdh3u1.jar \
    -input /input/acled_cleaned/Nigeria_ACLED_cleaned.tsv \
    -output /output/acled_analytic_out \
    -mapper location_regains_mapper.py \
    -reducer location_regains_by_time.py \
    -file ../python/location_regains_by_time.py \
    -file ../python/location_regains_mapper.py \
    -jobconf stream.num.map.output.key.fields=2 \
    -jobconf map.output.key.field.separator=\t \
    -jobconf num.key.fields.for.partition=1 \
    -jobconf mapred.reduce.tasks=1