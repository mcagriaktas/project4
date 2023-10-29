import findspark
findspark.init("/opt/spark/")

from pyspark.sql import SparkSession

accessKeyId = 'cagri'
secretAccessKey = '35413541'

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


# List objects in the bucket
objects = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration()).listStatus(
    spark._jvm.org.apache.hadoop.fs.Path("s3a://tmdb-silver/"))

# Filter and count the directories
num_directories = len([obj for obj in objects if obj.isDir()])

# Check if the number of directories is greater than or equal to 8
if num_directories == 8:
    print("Silver bucket is ready.")
else:
    print("Silver bucket is not ready.")

# Stop the Spark session
spark.stop()
