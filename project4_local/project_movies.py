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


# EXTRACT

df_movies = spark.read.parquet("s3a://tmdb-bronze/movies/")

df_movies.show(5)

df_movies = df_movies.withColumn("movie_id", col("id").cast(StringType()))

df_movies_movies = df_movies.select(
        col("movie_id"),
        col("title"),
        col("budget").cast(DoubleType()),
        col("homepage"),
        col("original_language"),
        col("original_title"),
        col("overview"),
        col("popularity").cast(FloatType()),
        col("release_date").cast(DateType()),
        col("revenue").cast(DoubleType()),
        col("runtime").cast(IntegerType()),
        col("status"),
        col("tagline"),
        col("vote_average").cast(FloatType()),
        col("vote_count").cast(IntegerType())
    )


movies_genres_schema = ArrayType(StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True)
]))

df_movies_genres = df_movies.withColumn("genres", from_json(col("genres"), movies_genres_schema))

df_movies_genres = df_movies_genres.select("movie_id", explode(col("genres")).alias("genres"))

df_movies_genres = df_movies_genres.select("movie_id", col("genres.*"))

df_movies_genres = df_movies_genres.withColumn("id", when(col("id").isNull(), -9999)
                                             .otherwise(col("id")))


movies_keywords_schema = ArrayType(StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True)
]))

df_movies_keywords = df_movies.withColumn("keywords", from_json(col("keywords"), movies_keywords_schema))

df_movies_keywords = df_movies_keywords.select("movie_id", explode(col("keywords")).alias("keywords"))

df_movies_keywords = df_movies_keywords.select("movie_id", col("keywords.*"))

df_movies_keywords = df_movies_keywords.withColumn("id", when(col("id").isNull(), -9999)
                                             .otherwise(col("id")))

df_movies_keywords.show(5)

movies_production_companies_schema = ArrayType(StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True)  
]))

df_movies_production_companies = df_movies.withColumn("production_companies", from_json(col("production_companies"), movies_production_companies_schema))

df_movies_production_companies = df_movies_production_companies.select("movie_id", explode(col("production_companies")).alias("production_companies"))

df_movies_production_companies = df_movies_production_companies.select("movie_id", col("production_companies.*"))

df_movies_production_companies = df_movies_production_companies.withColumn("id", when(col("id").isNull(), -9999)
                                             .otherwise(col("id")))


movies_production_countries_schema = ArrayType(StructType([
        StructField("iso_3166_1", StringType(), True),
        StructField("name", StringType(), True)
]))

df_movies_production_countries = df_movies.withColumn("production_countries", from_json(col("production_countries"), movies_production_countries_schema))

df_movies_production_countries = df_movies_production_countries.select("movie_id", explode(col("production_countries")).alias("production_countries"))

df_movies_production_countries = df_movies_production_countries.select("movie_id", col("production_countries.*"))

df_movies_production_countries = df_movies_production_countries.withColumn("iso_3166_1", when(col("iso_3166_1").isNull(), "XX")
                                             .otherwise(col("iso_3166_1")))

df_movies_production_countries.show(5)

movies_spoken_languages_schema = ArrayType(StructType([
        StructField("iso_639_1", StringType(), True),
        StructField("name", StringType(), True)
]))

df_movie_spoken_languages = df_movies.withColumn("spoken_languages", from_json(col("spoken_languages"), movies_spoken_languages_schema))

df_movie_spoken_languages = df_movie_spoken_languages.select("movie_id", explode(col("spoken_languages")).alias("spoken_languages"))

df_movie_spoken_languages = df_movie_spoken_languages.select("movie_id", col("spoken_languages.*"))

df_movie_spoken_languages = df_movie_spoken_languages.withColumn("iso_639_1", when(col("iso_639_1").isNull(), "XX")
                                             .otherwise(col("iso_639_1")))

df_movie_spoken_languages.show(1)
## LOAD

dataframes = [df_movies_movies, df_movies_genres, df_movies_keywords, 
              df_movies_production_companies, df_movies_production_countries, 
              df_movie_spoken_languages]

table_folders = ["movies", "genres", "keywords", 
                  "production_companies", "production_countries",
                  "spoken_languages"]

for i, dataframe in enumerate(dataframes):
    time.sleep(5)
    table_folder_name = table_folders[i]
    dataframe.write.format("delta").mode("overwrite").save(f"s3a://tmdb-silver/{table_folder_name}")

spark.stop()