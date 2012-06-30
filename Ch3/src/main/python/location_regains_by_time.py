#!/usr/bin/python
import sys

current_loc = "START_OF_APP"
govt_regains=[]
for line in sys.stdin:	
	(loc,event_date,event_type) = line.strip('\n').split('\t')
	if loc != current_loc:
		if current_loc != "START_OF_APP":
			print current_loc + '\t' + '\t'.join(govt_regains)
		current_loc = loc
		govt_regains = []
	if event_type.find('regains') != -1:
	    govt_regains.append(event_date)