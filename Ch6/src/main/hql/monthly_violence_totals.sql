
SELECT from_unixtime(unix_timestamp(event_date, 'yyyy-MM-dd'),'yyyy-MMM'),
       COALESCE(CAST(sum(fatalities) as STRING), 'Unknown')
       FROM acled_nigeria_cleaned
       GROUP BY from_unixtime(unix_timestamp(event_date, 'yyyy-MM-dd'),'yyyy-MMM');