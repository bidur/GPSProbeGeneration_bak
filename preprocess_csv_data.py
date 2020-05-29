
import random 
import pandas as pd
import os


def preprocess_data(sampling_percent, input_file, output_file):
	
	df = pd.read_csv(input_file)# 4900 rows
	print ('preprocess_data input:  ',len(df));

	#remove duplicate rows
	df.drop_duplicates(inplace=True) # 48782rows

	# get duplicate rows in terms of 'ap_id' and 'timestamp'
	df_dup = df[df.duplicated(['ap_id','timestamp'],keep=False)] # 294 rows # 60 ap_id
	df_dup = df_dup.sort_values(by=['ap_id'], ascending=True)

	# remove all duplicates from df -> new values will be calculated and kept for the duplicates
	df_clean = df.drop_duplicates(['ap_id','timestamp'], keep=False)
	
	

	'''
	# save intermediate data
	df_dup.to_csv('same_ts_diff_loc.csv',index=False)
	df_clean.to_csv('df_clean.csv',index=False)
	'''
	# Handle case: same timestamp and differnt location (e.g. L1 and L2 have same timestamp t1) -> remove these values and keep a single location ( avg(l1,l3), t1)
	arr_clean_rows = []
	arr_ap_id = df_dup.ap_id.unique() # duplicate ap_id

	# process each ap_id
	for ap_id in arr_ap_id:
		df_ap_id = df_dup.query('ap_id == "'+ap_id+'"')
		# get duplicate ts for the ap_id
		arr_ts =  df_ap_id.timestamp.unique()
		
		# process each ts
		for ts in arr_ts:
			df_ts = df_ap_id[df_ap_id['timestamp'].isin([ts])]
			new_lat = df_ts['lat'].mean()
			new_lon = df_ts['lon'].mean()
			arr_clean_rows.append({'ap_id': ap_id,  'timestamp':ts, 'lat': new_lat, 'lon':new_lon})

		
	# add new row to clean df
	if len(arr_clean_rows):
		df_clean = df_clean.append(arr_clean_rows)
	
	df_clean.insert(0,'id',range(len(df_clean)))# add a new id column
	
	# APPLY sampling_percent
	df_sample = apply_sampling(sampling_percent, df_clean)
	df_sample.to_csv(output_file,index=False)
	df_clean.to_csv(output_file.replace('.csv','_ALL.csv'),index=False)
	print (output_file)


def apply_sampling(sampling_percent, df_all):
		
	arr_ap_id = df_all.ap_id.unique() # duplicate ap_id
		
	id_count_to_keep = int(len(arr_ap_id) * sampling_percent * 0.01 ) 
	if  id_count_to_keep == 0:
		id_count_to_keep = 1
		
	arr_ap_id_sampled = random.sample(list(arr_ap_id), id_count_to_keep)#  to randomly select samples
	df_sample = df_all[df_all.ap_id.isin(arr_ap_id_sampled)]
	return df_sample
	
	
