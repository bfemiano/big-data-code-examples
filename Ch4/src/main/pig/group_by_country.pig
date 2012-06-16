ip_countries = LOAD '/input/weblog_ip/ip_to_country.txt' AS (ip: chararray, country:chararray);
country_grpd = GROUP ip_countries BY country;
country_counts = FOREACH country_grpd GENERATE FLATTEN(group), COUNT(ip_countries) as counts;
STORE country_counts INTO '/output/geo_weblog_entries';


