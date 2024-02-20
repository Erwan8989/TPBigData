import json
import random

from minio import Minio, S3Error

bucket_name = "transactions"

client = Minio(
    "127.0.0.1:9000",
    secure=False,
    access_key="minio",
    secret_key="minio123",
)

# Make 'transactions' bucket if not exist.
found = client.bucket_exists(bucket_name)
if not found:
    client.make_bucket(bucket_name)
else:
    print("Bucket 'transactions' already exists")


def write_minio(data):
    with open('data.json', 'w', encoding='utf-8') as f:
        json.dump(data, f)

    try:
        client.fput_object(bucket_name, f"data-{idx}.json", "data.json")
        print(f"Message {idx} envoyé")

    except S3Error as err:
        print(f"Erreur lors de l'upload: {err}")
