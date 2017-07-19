# Truy xuat ra min start time va max end time cuar tat ca cac part trong bo du lieu google cluster trace
from pyspark.sql.session import SparkSession as spark
import pandas as pd
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from matplotlib import pyplot
import os
sc = SparkContext(appName="Task_usage")
sql_context = SQLContext(sc)

folder_path ='/mnt/volume/ggcluster/clusterdata-2011-2/task_usage/'
# file_path = '/mnt/volume/ggcluster/spark-2.1.1-bin-hadoop2.7/thangbk2209/results/out.csv'

dataSchema = StructType([StructField('startTime', LongType(), True),
                         StructField('endTime', LongType(), True),
                         StructField('JobId', LongType(), True),
                         StructField('taskIndex', LongType(), True),
                         StructField('machineId', LongType(), True),
                         StructField('meanCPUUsage', FloatType(), True),
                         # canonical memory usage
                         StructField('CMU', FloatType(), True),
                         # assigned memory usage
                         StructField('AssignMem', FloatType(), True),
                         # unmapped page cache memory usage
                         StructField('unmapped_cache_usage', FloatType(), True),
                         StructField('page_cache_usage', FloatType(), True),
                         StructField('max_mem_usage', FloatType(), True),
                         StructField('mean_diskIO_time', FloatType(), True),
                         StructField('mean_local_disk_space', FloatType(), True),
                         StructField('max_cpu_usage', FloatType(), True),
                         StructField('max_disk_io_time', FloatType(), True),
                         StructField('cpi', FloatType(), True),
                         StructField('mai', FloatType(), True),
                         StructField('sampling_portion', FloatType(), True),
                         StructField('agg_type', FloatType(), True),
                         StructField('sampled_cpu_usage', FloatType(), True)])

list_file_name=[]
list_max_time=[]
# 2505600000000
for file_name in os.listdir(folder_path):
    list_file_name.append(file_name)
    print len(list_file_name)
    df = (
        sql_context.read
        .format('com.databricks.spark.csv')
        .schema(dataSchema)
        .load("%s%s"%(folder_path,file_name))
    )
    df.createOrReplaceTempView("dataFrame")
    maxEnd = sql_context.sql("SELECT max(endTime) as maxEndTime from dataFrame").rdd.map(lambda r: r.maxEndTime).collect()
    # minStart = sql_context.sql("SELECT min(startTime) as minStartTime from dataFrame").rdd.map(lambda r: r.minStartTime).collect()
    maxTime = int(maxEnd[0])
    list_max_time.append(maxTime)
    if(len(list_file_name)==100):
        break
    # minTime = int(minStart[0])
print "List file name: "
print list_file_name
print "List max time: "
print list_max_time
sc.stop()