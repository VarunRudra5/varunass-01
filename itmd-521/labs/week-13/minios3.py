from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import to_date,year, month, avg,to_timestamp ,stddev_pop



# Removing hard coded password - using os module to import them
import os
import sys

conf = SparkConf()
conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.0')
conf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')

conf.set('spark.hadoop.fs.s3a.access.key', os.getenv('ACCESSKEY'))
conf.set('spark.hadoop.fs.s3a.secret.key', os.getenv('SECRETKEY'))
# Configure these settings
# https://medium.com/@dineshvarma.guduru/reading-and-writing-data-from-to-minio-using-spark-8371aefa96d2
conf.set("spark.hadoop.fs.s3a.path.style.access", "true")
conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
# https://github.com/minio/training/blob/main/spark/taxi-data-writes.py
# https://spot.io/blog/improve-apache-spark-performance-with-the-s3-magic-committer/
conf.set('spark.hadoop.fs.s3a.committer.magic.enabled','true')
conf.set('spark.hadoop.fs.s3a.committer.name','magic')
# Internal IP for S3 cluster proxy
conf.set("spark.hadoop.fs.s3a.endpoint", "http://infra-minio-proxy-vm0.service.consul")

spark = SparkSession.builder.appName("vrudra part-3").config('spark.driver.host','spark-edge.service.consul').config(conf=conf).getOrCreate()

df = spark.read.csv('s3a://itmd521/80.txt')

splitDF = df.withColumn('WeatherStation', df['_c0'].substr(5, 6)) \
.withColumn('WBAN', df['_c0'].substr(11, 5)) \
.withColumn('ObservationDate',to_date(df['_c0'].substr(16,8), 'yyyyMMdd')) \
.withColumn('ObservationHour', df['_c0'].substr(24, 4).cast(IntegerType())) \
.withColumn('Latitude', df['_c0'].substr(29, 6).cast('float') / 1000) \
.withColumn('Longitude', df['_c0'].substr(35, 7).cast('float') / 1000) \
.withColumn('Elevation', df['_c0'].substr(47, 5).cast(IntegerType())) \
.withColumn('WindDirection', df['_c0'].substr(61, 3).cast(IntegerType())) \
.withColumn('WDQualityCode', df['_c0'].substr(64, 1).cast(IntegerType())) \
.withColumn('SkyCeilingHeight', df['_c0'].substr(71, 5).cast(IntegerType())) \
.withColumn('SCQualityCode', df['_c0'].substr(76, 1).cast(IntegerType())) \
.withColumn('VisibilityDistance', df['_c0'].substr(79, 6).cast(IntegerType())) \
.withColumn('VDQualityCode', df['_c0'].substr(86, 1).cast(IntegerType())) \
.withColumn('AirTemperature', df['_c0'].substr(88, 5).cast('float') /10) \
.withColumn('ATQualityCode', df['_c0'].substr(93, 1).cast(IntegerType())) \
.withColumn('DewPoint', df['_c0'].substr(94, 5).cast('float')) \
.withColumn('DPQualityCode', df['_c0'].substr(99, 1).cast(IntegerType())) \
.withColumn('AtmosphericPressure', df['_c0'].substr(100, 5).cast('float')/ 10) \
.withColumn('APQualityCode', df['_c0'].substr(105, 1).cast(IntegerType())).drop('_c0')

# Writing the CSV data to 60-uncompressed folder
splitDF.write.format("csv").mode("overwrite").option("header","true").save("s3a://vrudra/80-uncompressed.csv")

# Writing the CSV data to 60-compressed folder with LZ4 compression
splitDF.write.format("csv").mode("overwrite").option("header","true").option("compression","lz4").save("s3a://vrudra/80-compressed.csv")

# Writing the DataFrame to Parquet format
splitDF.write.format("parquet").mode("overwrite").option("header","true").save("s3a://vrudra/80.parquet")

# # Reading the uncompressed CSV data
writeDF=spark.read.csv('s3a://vrudra/80-uncompressed.csv')

# Coalescing DataFrame to a single partition for efficient writing
colesce_df = writeDF.coalesce(1)

# Writing the coalesced DataFrame to a single CSV file
colesce_df.write.format("csv").mode("overwrite").option("header","true").save("s3a://vrudra/80.csv")


#part-2

# Convert ObservationDate to timestamp
writeDF = writeDF.withColumn("ObservationDate", to_timestamp(writeDF["ObservationDate"], "dd/MM/yyyy"))

# Calculating the average temperature per month per year
avg_temp_per_month_per_year = writeDF.withColumn("Year", writeDF["ObservationDate"].substr(1, 4)) \
    .withColumn("Month", writeDF["ObservationDate"].substr(6, 2)) \
    .groupBy("Year", "Month") \
    .agg(avg("AirTemperature").alias("AvgTemperature"))

# Calculating the standard deviation of temperature over the decade per month
std_dev_per_month = writeDF.withColumn("Month", writeDF["ObservationDate"].substr(6, 2)) \
    .groupBy("Month") \
    .agg(stddev_pop("AirTemperature").alias("StdDevTemperature"))

# Join the two dataframes to get average temperature and standard deviation in one dataframe
result = avg_temp_per_month_per_year.join(std_dev_per_month, "Month")

# Write the result as a Parquet file
result.write.parquet("s3a://vrudra/part-three.parquet")

# Take only 12 records (the month and standard deviations)
top_12 = result.select("Month", "StdDevTemperature").limit(12)

# Write the top 12 records to a CSV file
top_12.write.csv("s3a://vrudra/part-three.csv", header=True)

spark.stop()

