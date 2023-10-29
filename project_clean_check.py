import minio
import os 
import argparse

# Minio
ap = argparse.ArgumentParser()

ap.add_argument("-aki", "--accessKeyId", required=True, type=str)
ap.add_argument("-sak", "--secretAccessKey", required=True, type=str)

args = vars(ap.parse_args())

accessKeyId = args['accessKeyId']
secretAccessKey = args['secretAccessKey']

client = minio.Minio(endpoint="minio:9000", access_key=accessKeyId, secret_key=secretAccessKey, secure=False)

def count_minio_bucket_objects(bucket_name):
  objects = client.list_objects(bucket_name)
  object_count = 0
  for object in objects:
    object_count += 1
  return object_count

bucket_name = "tmdb-silver"

object_count = count_minio_bucket_objects(bucket_name)

try:
  if object_count == 8:
    print("Your tables are ready.")
except Exception as e:
  print("Your tables are not ready. An error occurred:", e)

