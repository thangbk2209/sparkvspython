# Lay ra resource usage tai cac diem thoi gian- chi lay ra cac ban ghi thoa man rang start time < time stamp
# va end time > time stamp ma chua cong lai cac gia tri resource cua no
# y tuong: Tim ra min start time vaf max end time trong tung part. Sau do truy van du lieu trong tung part 
#voi gia tri tang dan tu gia tri min start time cua part do- tuy nhien chua xu li van de chong lan thoi gian
# giua cac part -- max end time cua part truoc > min start cuar part sau
from pyspark.sql.session import SparkSession as spark
import pandas as pd
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from matplotlib import pyplot
import os
sc = SparkContext(appName="Task_usage")
sql_context = SQLContext(sc)

# folder_path ='/mnt/volume/ggcluster/clusterdata-2011-2/task_usage/'
folder_path = '/mnt/volume/ggcluster/spark-2.1.1-bin-hadoop2.7/thangbk2209/TopJobId/'

dataSchema = StructType([StructField('startTime', StringType(), True),
                         StructField('endTime', StringType(), True),
                         StructField('JobId', LongType(), True),
                         StructField('meanCPUUsage', FloatType(), True),
                         # canonical memory usage
                         StructField('CMU', FloatType(), True),
                         # assigned memory usage
                         StructField('AssignMem', FloatType(), True),
                         # unmapped page cache memory usage
                         StructField('unmapped_cache_usage', FloatType(), True),
                         StructField('page_cache_usage', FloatType(), True),
                         StructField('mean_diskIO_time', FloatType(), True),
                         StructField('mean_local_disk_space', FloatType(), True)])
FileNameARR=[]

for num in range(175,271):
    file_name = "JobMaxTaskpart-00"+str(num).zfill(3)+"-of-00500.csv"
    FileNameARR.append(file_name)
    df = (
        sql_context.read
        .format('com.databricks.spark.csv')
        .schema(dataSchema)
        .load("%s%s"%(folder_path,file_name))
    )
    df.createOrReplaceTempView("dataFrame")
   
    TimeDf = sql_context.sql("SELECT min(startTime),max(endTime) from dataFrame")
   
    TimeDf.toPandas().to_csv('thangbk2209/sparkvspython/Data/sparkData/Time%s'%(file_name), index=False, header=None)
print FileNameARR
sc.stop()