SELECT wle.*, itc.country FROM weblog_entries wle
    JOIN ip_to_country itc ON wle.ip = itc.ip;