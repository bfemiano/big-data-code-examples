#!/usr/bin/python
import sys

for line in sys.stdin:
    (loc,event_date,event_type,actor,lat,lon,src,fatalities) = line.strip().split('\t');
    (day,month,year) = event_date.split('/')
    if len(day) == 1:
        day = '0' + day
    if len(month) == 1:
        month = '0' + month;
    if len(year) == 2:
        if int(year) > 30 and int(year) < 99:
            year = '19' + year
        else:
            year = '20' + year
    event_date = year + '-' + month + '-' + day
    print '\t'.join([loc,event_date,event_type]);
