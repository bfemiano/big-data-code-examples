
-- Same as above asumes tab delimited
ip_countries = LOAD '/input/weblog_ip/ip_to_country.txt' AS (ip: chararray, country:chararray);

-- group by country
country_grpd = GROUP ip_countries BY country;

-- get counts by country
country_counts = FOREACH country_grpd GENERATE FLATTEN(group), COUNT(ip_countries) as counts;

-- STORE tab delimited format
STORE country_counts INTO '/output/geo_weblog_entries';


