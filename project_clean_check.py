import minio
import os 

accessKeyId = "cagri"
secretAccessKey = "35413541"

if accessKeyId is None or secretAccessKey is None:
    raise ValueError("AWS access key or secret key not set in environment variables.")

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

