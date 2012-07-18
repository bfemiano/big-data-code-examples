SET mapred.child.java.opts=-Xmx512M;

SELECT /*+ MAPJOIN(nh)*/ acled.event_date, acled.event_type, nh.description
    FROM acled_nigeria_cleaned acled
    JOIN nigeria_holidays nh
        ON (substr(acled.event_date,6) = nh.yearly_date);
