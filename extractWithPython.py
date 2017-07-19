import pandas as pd
import numpy as np
import os
from pandas import read_csv
# file_path = '/mnt/volume/ggcluster/spark-2.1.1-bin-hadoop2.7/thangbk2209/results/out.csv'
folder_path = '/mnt/volume/ggcluster/spark-2.1.1-bin-hadoop2.7/thangbk2209/TopJobId/'
# minTime = data[0][0]
list_file_name = []
list_max_time = []
for num in range(175,271):
    file_name = "JobMaxTaskpart-00"+str(num)+"-of-00500.csv"
	list_file_name.append(file_name)
	print len(list_file_name)
	df = read_csv('%s%s'%(folder_path,file_name), header=None,index_col=False)
	data = df.values
	maxTime = data[0][1]
	for i in range(len(data)):
		if(data[i][1] > maxTime):
			maxTime = data[i][1]
	list_max_time.append(maxTime)
print "List file name: "
print list_file_name
print "List max time: "
print list_max_time
	# if(data[i][0] < minTime):
		# minTime = data[i][0]
# for time_stamp in range(int(minTime),int(maxTime),extraTime):
# 	timeStampData=[]
# 	for i in range(len(data)):
# 		if(data[i][0]<=time_stamp and data[i][1]>time_stamp):
# 			timeStampData.append(data[i])		
# 	newDf = pd.DataFrame(timeStampData)
# 	newDf.to_csv('Data/pythonData/%s.csv'%(time_stamp))