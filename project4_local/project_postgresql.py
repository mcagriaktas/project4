from datetime import date
from typing import Optional
from sqlmodel import Field, SQLModel
from sqlmodel import create_engine
import pandas as pd
import time 
import argparse

import findspark
findspark.init("/opt/spark/")
from pyspark.sql import * 
from pyspark.sql.functions import *
from pyspark.sql.types import *


# Minio
ap = argparse.ArgumentParser()

ap.add_argument("-aki", "--accessKeyId", required=True, type=str)
ap.add_argument("-sak", "--secretAccessKey", required=True, type=str)

args = vars(ap.parse_args())

accessKeyId = args['accessKeyId']
secretAccessKey = args['secretAccessKey']


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


# DB table Schema
class cast(SQLModel, table=True):

    id: Optional[int] = Field(default=None, primary_key=True)
    movie_id: str
    title: str
    cast_id: int
    character: str
    credit_id: str
    gender: int
    name: str
          
class crew(SQLModel, table=True):

    id: Optional[int] = Field(default=None, primary_key=True)
    movie_id = str
    title = str
    credit_id = str
    department = str
    gender = int
    job = str
    name = str

class movies(SQLModel, table=True):

    movie_id: str = Field(default=None, primary_key=True)
    title: str
    budget: float
    homepage: str
    original_language: str
    original_title: str
    overview: str
    popularity: float
    release_date: date
    revenue: float
    runtime: int
    status: str
    tagline: str
    vote_average: float
    vote_count: int

class genres(SQLModel, table=True):

    id: Optional[int] = Field(default=None, primary_key=True)
    movie_id: str
    name: str

class keywords(SQLModel, table=True):

    id: Optional[int] = Field(default=None, primary_key=True)
    movie_id: str
    name: str

class production_companies(SQLModel, table=True):

    id: Optional[int] = Field(default=None, primary_key=True)
    movie_id: str
    name: str

class production_countries(SQLModel, table=True):

    movie_id: Optional[str] = Field(default=None, primary_key=True)
    iso_3166_1: str
    name: str

class spoken_languages(SQLModel, table=True):

    movie_id: Optional[str] = Field(default=None, primary_key=True)
    iso_639_1: str
    name: str

SQLALCHEMY_DATABASE_URL="postgresql://cagri:35413541@postgres:5432/mydb"
engine = create_engine(SQLALCHEMY_DATABASE_URL, echo=True)

def create_db_and_tables():
    SQLModel.metadata.create_all(engine)

create_db_and_tables()

table_folders_credits = ["cast", "crew"]


for table in table_folders_credits:
    dataframe = spark.read.format("delta").load(f"s3a://tmdb-silver/{table}/")
    dataframe = dataframe.toPandas()
    dataframe.to_sql(f"{table}", con=engine, if_exists='replace', index=False)

time.sleep(5)

table_folders_movies = ["movies", "genres", "keywords", 
                  "production_companies", "production_countries",
                  "spoken_languages"]

for table in table_folders_movies:
    dataframe = spark.read.format("delta").load(f"s3a://tmdb-silver/{table}/")
    dataframe = dataframe.toPandas()
    dataframe.to_sql(f"{table}", con=engine, if_exists='replace', index=False)

spark.stop()



    










