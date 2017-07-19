import pandas as pd
import numpy as np
import os
from pandas import read_csv
# file_path = '/mnt/volume/ggcluster/spark-2.1.1-bin-hadoop2.7/thangbk2209/results/out.csv'
folder_path ='/mnt/volume/ggcluster/clusterdata-2011-2/task_usage/'
# minTime = data[0][0]
list_file_name = []
list_max_time = []
for file_name in os.listdir(folder_path):
	list_file_name.append(file_name)
	print len(list_file_name)
	df = read_csv('%s%s'%(folder_path,file_name), header=None,index_col=False)
	data = df.values
	maxTime = data[0][1]
	for i in range(len(data)):
		if(data[i][1] > maxTime):
			maxTime = data[i][1]
	list_max_time.append(maxTime)
	if(len(list_file_name) == 50):
		break

print "List max time: "
print list_max_time
print "List file name: "
print list_file_name
	# if(data[i][0] < minTime):
		# minTime = data[i][0]
# for time_stamp in range(int(minTime),int(maxTime),extraTime):
# 	timeStampData=[]
# 	for i in range(len(data)):
# 		if(data[i][0]<=time_stamp and data[i][1]>time_stamp):
# 			timeStampData.append(data[i])		
# 	newDf = pd.DataFrame(timeStampData)
# 	newDf.to_csv('Data/pythonData/%s.csv'%(time_stamp))