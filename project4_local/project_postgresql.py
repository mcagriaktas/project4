from typing import Optional
from sqlmodel import Field, SQLModel
from sqlmodel import create_engine
import pandas as pd

import time 
import findspark
findspark.init("/opt/spark/")

import argparse

from pyspark.sql import * 
from pyspark.sql.functions import *
from pyspark.sql.types import *

"""
# Minio
ap = argparse.ArgumentParser()

ap.add_argument("-aki", "--accessKeyId", required=True, type=str)
ap.add_argument("-sak", "--secretAccessKey", required=True, type=str)

args = vars(ap.parse_args())

accessKeyId = args["accessKeyId"]
secretAccessKey = args["secretAccessKey"]
"""
accessKeyId = "cagri"
secretAccessKey = "35413541"

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

class creditscast(SQLModel, table=True):

    id: Optional[int] = Field(default=None, primary_key=True)
    movie_id = int
    title = str
    cast_id = int
    character = str
    credit_id = str
    gender = int
    name = str
          
class creditscrew(SQLModel, table=True):

    id: Optional[int] = Field(default=None, primary_key=True)
    movie_id = int
    title = str
    credit_id = str
    department = str
    gender = int
    job = str
    name = str


# database : mydb
# user: cagri
# password: 35413511
# ip: 127.0.0.1
# port: 5432

SQLALCHEMY_DATABASE_URL="postgresql://cagri:35413541@postgres:5432/mydb"
engine = create_engine(SQLALCHEMY_DATABASE_URL, echo=True)

def create_db_and_tables():
    SQLModel.metadata.create_all(engine)

create_db_and_tables()

df_CreditsCast = spark.read.format("delta").load("s3a://tmdb-silver/CreditsCast/")
time.sleep(5)
df_CreditsCrew = spark.read.format("delta").load("s3a://tmdb-silver/CreditsCrew/")

pandas_df_credits_cast = df_CreditsCast.toPandas()
pandas_df_credits_crew = df_CreditsCrew.toPandas()

pandas_df_credits_cast.to_sql('creditscast', con=engine, if_exists='replace', index=False)
time.sleep(5)
pandas_df_credits_crew.to_sql('creditscrew', con=engine, if_exists='replace', index=False)

spark.stop()



    










