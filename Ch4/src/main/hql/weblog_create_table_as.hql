
CREATE TABLE weblog_entries_with_url_length AS
    SELECT url, request_date, request_time, length(url) as url_length FROM weblog_entries;