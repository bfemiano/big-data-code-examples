SET mapred.child.java.opts=-Xmx512M;

DROP TABLE IF EXISTS longest_event_delta_per_loc;
CREATE TABLE longest_event_delta_per_loc (
    loc STRING,
    start_date STRING,
    end_date STRING,
    days INT
);

ADD FILE ../python/calc_longest_nonviolent_period.py;
FROM (
        SELECT loc,event_date,event_type
        FROM acled_nigeria_cleaned
        DISTRIBUTE BY loc SORT BY loc, event_date
     ) mapout
INSERT OVERWRITE TABLE longest_event_delta_per_loc
REDUCE mapout.loc, mapout.event_date, mapout.event_type
USING 'python calc_longest_nonviolent_period.py'
AS loc, start_date, end_date, days;
