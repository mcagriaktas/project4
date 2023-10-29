import findspark
findspark.init("/opt/spark/")

from pyspark.sql import * 
from pyspark.sql.functions import *
from pyspark.sql.types import *

accessKeyId='cagri'
secretAccessKey='35413541'

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

# Checking

try:
    df_credits = spark.read.parquet("s3a://tmdb-bronze/credits/")
    df_movies = spark.read.parquet("s3a://tmdb-bronze/movies/")

    df_credits_count = df_credits.count()
    df_movies_count = df_movies.count()

    if df_credits_count == 4803:
        print("df_credits tables is ready.")
    else:
        print("credits tables has a problem.")

    if df_movies_count == 4803:
        print("movies tables is ready.")
    else:
        print("movies tables has a problem.")

except Exception as e:
    print(f"An error occurred: {str(e)}")
    raise e
finally:
    spark.stop()





