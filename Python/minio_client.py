from minio import Minio

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
