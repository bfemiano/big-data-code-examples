SET mapred.child.java.opts=-Xmx512M;

DROP TABLE IF EXISTS acled_nigeria_event_people_links;
CREATE TABLE acled_nigeria_event_people_links AS
SELECT acled.event_date, acled.event_type, vips.name, vips.description as pers_desc, vips.birthday
    FROM nigeria_vips vips
    FULL OUTER JOIN acled_nigeria_cleaned acled
        ON (substr(acled.event_date,6) = substr(vips.birthday,6));