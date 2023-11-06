import minio 
import argparse
from minio.error import S3Error


# Minio
ap = argparse.ArgumentParser()

ap.add_argument("-aki", "--accessKeyId", required=True, type=str)
ap.add_argument("-sak", "--secretAccessKey", required=True, type=str)

args = vars(ap.parse_args())

accessKeyId = args['accessKeyId']
secretAccessKey = args['secretAccessKey']

client = minio.Minio(endpoint="minio:9000", access_key=accessKeyId, secret_key=secretAccessKey, secure=False)


def check_folder_exists(bucket_name, folder_name):
    try:
        objects = client.list_objects(bucket_name, prefix=folder_name, recursive=True)

        for obj in objects:
            return True

    except S3Error as e:
        print("Error: ", e)
        pass

    return False

bucket_name = "tmdb-silver"
folder_silver = ["cast", "crew", "movies", "genres", "keywords", 
                  "production_companies", "production_countries",
                  "spoken_languages"]

if all(check_folder_exists(bucket_name, folder_name) for folder_name in folder_silver):
    print("Eight folders exist in the bucket.")
else:
    print("One or more folders do not exist in the bucket.")





