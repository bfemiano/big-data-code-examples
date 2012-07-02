#!/usr/bin/python
import sys

for line in sys.stdin:
	(loc,event_date,year,event_type,actor,lat,lon,src,fatalities) = line.strip().split('\t')
	if loc != 'LOCATION': #remove header row
		if fatalities == 'ZERO_FLAG':
		    fatalities = 'FLAG'
		print '\t'.join([loc,event_date,event_type,actor,lat,lon,src,fatalities]) #strip out year


