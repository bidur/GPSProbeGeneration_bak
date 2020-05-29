
import   csv
from faker import Faker
from collections import defaultdict
import pandas as pd

def anonymize_column_values(column_to_anonymize, in_file, op_file):
	# 'Anonymizes the given original data in 'ID' field of CSV file '
	
	faker  = Faker()
 
	# Create mappings of ids_mapped  to faked ids.
	ids_mapped  = defaultdict(faker.msisdn) 
	# Other mappings can be done as well e.g. last_name, ssn, msisdn
    # msisdn=Mobile Station International Subscriber Directory Number
 
	#with open(in_file, 'en_US') as f:  # python 2.7
	with open(in_file, 'r' ) as f:       
	    with open(op_file, 'w', newline='') as o :
	    # DictReader to extract fields
	        reader = csv.DictReader(f)
	        writer = csv.DictWriter(o, reader.fieldnames)
	        writer.writeheader()
	        
	        for row in reader:
	            row[column_to_anonymize]  = ids_mapped[row[column_to_anonymize]]
	            writer.writerow(row)
 
"""
Reference: https://qxf2.com/blog/anonymize-data-using-faker/
"""
