SELECT event_type,event_date,days_sense FROM (
    SELECT event_type,event_date,
            datediff(to_date(from_unixtime(unix_timestamp())),
                     to_date(from_unixtime(unix_timestamp(event_date,'yyyy-MM-dd')))
                     ) AS days_sense
     FROM acled_nigeria_cleaned) date_differences
     ORDER BY event_date DESC LIMIT 20;