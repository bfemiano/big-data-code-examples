SET mapred.child.java.opts=-Xmx512M;

NOT DONE YET: Need to stream join, figure out the correct output fields and aliases, persist to a table.

SELECT acled.event_date, acled.event_type, vips.name, vips.description, vips.birthday
    FROM acled_nigeria_cleaned acled
    FULL OUTER JOIN nigeria_vips vips
        ON (substr(acled.event_date,6) = substr(vips.birthday,6));