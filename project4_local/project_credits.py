import findspark
findspark.init("/opt/spark/")

import argparse

from pyspark.sql import * 
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time

# Minio
ap = argparse.ArgumentParser()

ap.add_argument("-aki", "--accessKeyId", required=True, type=str)
ap.add_argument("-sak", "--secretAccessKey", required=True, type=str)

args = vars(ap.parse_args())

accessKeyId = args["accessKeyId"]
secretAccessKey = args["secretAccessKey"]


spark = SparkSession.builder \
.appName("Project") \
.master("local[2]") \
.config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.0,io.delta:delta-core_2.12:2.4.0") \
.config("fs.s3a.access.key", accessKeyId) \
.config("fs.s3a.secret.key", secretAccessKey) \
.config("fs.s3a.path.style.access", True) \
.config("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
.config("fs.s3a.endpoint", "http://minio:9000") \
.getOrCreate()
# s3.amazonaws.com
# http://minio:9000

## EXTRACT

df_credits = spark.read.parquet("s3a://tmdb-bronze/credits/")

df_credits.show(10)
## TRANSFORM

cast_schema = ArrayType(StructType([
        StructField('cast_id', IntegerType(), True),
        StructField('character', StringType(), True),
        StructField('credit_id', StringType(), True),
        StructField('gender', IntegerType(), True),
        StructField('id', IntegerType(), True),
        StructField('name', StringType(), True),
]))

df_credits_cast = df_credits.withColumn("cast", from_json(col("cast"), cast_schema))

df_credits_cast = df_credits_cast.select("movie_id", "title", explode(col("cast")).alias("cast"), "crew")

df_credits_cast = df_credits_cast.withColumn("cast_id", col("cast.cast_id"))
df_credits_cast = df_credits_cast.withColumn("character", col("cast.character"))
df_credits_cast = df_credits_cast.withColumn("credit_id", col("cast.credit_id"))
df_credits_cast = df_credits_cast.withColumn("gender", col("cast.gender"))
df_credits_cast = df_credits_cast.withColumn("id", col("cast.id"))
df_credits_cast = df_credits_cast.withColumn("name", col("cast.name"))

df_credits_cast = df_credits_cast.select("movie_id", "title", col("cast.*"))

df_credits_cast = df_credits_cast.withColumn("credit_id", when(col("credit_id").isNull(), 0000000000)
                                             .otherwise(col("credit_id")))

df_credits_cast.show(10)

crew_schema = ArrayType(StructType([
        StructField("credit_id", StringType(), True),
        StructField("department", StringType(), True),
        StructField("gender", IntegerType(), True),
        StructField("id", IntegerType(), True),
        StructField("job", StringType(), True),
        StructField("name", StringType(), True),
]))

df_credits_crew = df_credits.withColumn("crew", from_json(col("crew"), crew_schema))

df_credits_crew = df_credits_crew.select("movie_id", "title", explode(col("crew")).alias("crew"))

df_credits_crew = df_credits_crew.withColumn("credit_id", col("crew.credit_id"))
df_credits_crew = df_credits_crew.withColumn("department", col("crew.department"))
df_credits_crew = df_credits_crew.withColumn("gender", col("crew.gender"))
df_credits_crew = df_credits_crew.withColumn("id", col("crew.id"))
df_credits_crew = df_credits_crew.withColumn("job", col("crew.job"))
df_credits_crew = df_credits_crew.withColumn("name", col("crew.name"))

df_credits_crew = df_credits_crew.select("movie_id", "title", col("crew.*"))

df_credits_crew = df_credits_crew.withColumn("credit_id", when(col("credit_id").isNull(), 0000000000)
                                             .otherwise(col("credit_id")))

df_credits_crew.show(10)
## LOAD


dataframes = [df_credits_cast, df_credits_crew]

table_folders = ["cast", "crew"]

for i, dataframe in enumerate(dataframes):
    time.sleep(5)
    table_folder_name = table_folders[i]
    dataframe.write.format("delta").mode("overwrite").save(f"s3a://tmdb-silver/{table_folder_name}")

spark.stop()