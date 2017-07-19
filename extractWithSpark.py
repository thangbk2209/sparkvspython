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

# folder_path ='/mnt/volume/ggcluster/clusterdata-2011-2/task_usage/'
file_path = '/mnt/volume/ggcluster/spark-2.1.1-bin-hadoop2.7/thangbk2209/results/out.csv'

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

minTime = 2503200000000

extraTime = 10000000
# 2505600000000


df = (
    sql_context.read
    .format('com.databricks.spark.csv')
    .schema(dataSchema)
    .load("%s"%(file_path))
)
df.createOrReplaceTempView("dataFrame")
maxEnd = sql_context.sql("SELECT max(endTime) as maxEndTime from dataFrame").rdd.map(lambda r: r.maxEndTime).collect()
# minStart = sql_context.sql("SELECT min(startTime) as minStartTime from dataFrame").rdd.map(lambda r: r.minStartTime).collect()
maxTime = int(maxEnd[0])
# minTime = int(minStart[0])
print maxTime

# print a
# # df.printSchema()
for time_stamp in range(minTime,maxTime,extraTime ):
    sumCPUUsage = sql_context.sql("SELECT * from dataFrame where %s >= startTime and %s < endTime"%(time_stamp,time_stamp))
    # sumCPUUsage = sql_context.sql("SELECT startTime/1000000, endTime/1000000, JobId, taskIndex, machineId, meanCPUUsage, CMU, AssignMem, unmapped_cache_usage, page_cache_usage, max_mem_usage, mean_diskIO_time,  mean_local_disk_space, max_cpu_usage, max_disk_io_time, cpi ,mai, sampling_portion, agg_type, sampled_cpu_usage from dataFrame")
    schema_df = ["startTime","numberOfJob"]
    sumCPUUsage.toPandas().to_csv('thangbk2209/sparkvspython/Data/sparkData/%s.csv'%(time_stamp), index=False, header=None)
# sumCPUUsage.write.save("results/test.csv", format="csv", columns=schema_df)
sc.stop()