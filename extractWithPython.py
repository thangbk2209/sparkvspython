import pandas as pd
import numpy as np
import os
from pandas import read_csv
file_path = '/mnt/volume/ggcluster/spark-2.1.1-bin-hadoop2.7/thangbk2209/results/out.csv'



# timeStamp = 2503200000000
minTime = 2503200000000
# maxTime = 2506200000000
extraTime = 10000000

df = read_csv('%s'%(file_path), header=None,index_col=False)
data = df.values
maxTime = data[0][1]
# minTime = data[0][0]
for i in range(len(data)):
	if(data[i][1] > maxTime):
		maxTime = data[i][1]
	# if(data[i][0] < minTime):
		# minTime = data[i][0]
for time_stamp in range(int(minTime),int(maxTime),extraTime):
	timeStampData=[]
	for i in range(len(data)):
		if(data[i][0]<=time_stamp and data[i][1]>time_stamp):
			timeStampData.append(data[i])		
	newDf = pd.DataFrame(timeStampData)
	newDf.to_csv('Data/pythonData/%s.csv'%(time_stamp))