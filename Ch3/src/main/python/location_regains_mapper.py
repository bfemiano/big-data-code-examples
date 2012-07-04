#!/usr/bin/python
import sys

for line in sys.stdin:
    (loc,event_date,event_type,actor,lat,lon,src,fatalities) = line.strip().split('\t');
    print '\t'.join([loc,event_date,event_type]);
