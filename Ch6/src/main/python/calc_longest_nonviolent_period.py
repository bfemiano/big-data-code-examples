#!/usr/bin/python
import sys
from datetime import datetime,timedelta

current_loc = "START_OF_APP"
(prev_date,start_date,end_date,start_time_obj,end_time_obj,current_diff)=('','','',None,None,timedelta.min)
for line in sys.stdin:
	(loc,event_date,event_type) = line.strip('\n').split('\t')
	if loc != current_loc and current_loc != "START_OF_APP":
		if end_date != '': # must have found > 2 events for the loc
			print '\t'.join([current_loc,start_date,event_date,str(current_diff.days)])
		(prev_date,start_date,end_date,start_time_obj,end_time_obj,current_diff)=('','','',None,None,timedelta.min)
	end_time_obj = datetime.strptime(event_date,'%Y-%m-%d') 
	current_loc = loc
	if start_time_obj is not None: # implies > 2 events, now we can diff
		diff = end_time_obj - start_time_obj
		if diff > current_diff:
			current_diff = diff #set the current max time delta
			start_date = prev_date 
			end_date = event_date
	prev_date = event_date
	start_time_obj = end_time_obj