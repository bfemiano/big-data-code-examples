import sys
import csv

def search_for_boss(items, employee):
 	found = False
	for current in items:
		subs = current[2]
		if subs != 'none':
			sub_list = subs[subs.find(':')+1:].split(',')
			for sub in sub_list:
				if sub == employee:					
					print 'found boss ' + current[0] + ' for employee ' + employee
					found = True
					search_for_boss(items, current[0])
	if not found:
		print 'no boss found for employee ' + employee				

def main():
	smartReader = csv.reader(open('gooftech_rdf_triples.txt', 'rb'), delimiter='\t')
	smartWriter = csv.writer(open('sorted_gooftech.csv', 'wb'), delimiter='\t', quoting=csv.QUOTE_MINIMAL)
	sorted_gooftech = sorted(smartReader, key=lambda employee: employee[0])
	for row in sorted_gooftech:
		smartWriter.writerow(row)
	if len(sys.argv) > 1:
		employee = sys.argv[1]
		print 'back searching for: ' + employee
		search_for_boss(sorted_gooftech,employee)

if __name__ == "__main__":
	main()

