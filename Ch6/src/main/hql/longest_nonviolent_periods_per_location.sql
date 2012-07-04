SET mapred.child.java.opts=-Xmx512M;

DROP TABLE IF EXISTS max_nonviolent_periods_per_loc;
CREATE TABLE max_nonviolent_periods_per_loc (
    loc STRING,
    start_date STRING,
    end_date STRING
)

ADD FILE ../python/calc_longest_nonviolent_period.py;
FROM (
    FROM acled_nigeria_cleaned
        MAP (loc,event_date)
        DISTRIBUTE BY loc
        SORT BY loc,event_date
    ) mapout
    INSERT OVERWRITE TABLE max_nonviolent_periods_per_loc
    REDUCE (mapout.loc,mapout.event_date)
    USING ('python calc_longest_nonviolent_period.py')
    AS (loc, start_date, end_date);
)
