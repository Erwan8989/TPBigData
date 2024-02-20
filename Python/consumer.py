import os

from kafka import TopicPartition, KafkaConsumer
from minio import S3Error

from minio_client import client, bucket_name
import json
from pyspark.sql import SparkSession
import pyspark.sql.functions as func

spark = SparkSession.builder \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

tp = TopicPartition('transactions', 0)
consumer = KafkaConsumer(bootstrap_servers=['127.0.0.1:9092'], consumer_timeout_ms=10000)
consumer.assign([tp])

last_offset = consumer.end_offsets([tp])[tp]
print("Consumer started")

for idx, message in enumerate(consumer):
    data = json.loads(message.value.decode('utf-8'))

    data['date'] = data['date'] + "+00:00"
    data['date'] = func.to_timestamp(data['date'], "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")

    if data['devise'] == "USD":
        to_eur = round(data['montant'] * 0.84, 2)
    else:
        to_eur = data['montant']

    if "None" in data['utilisateur']['adresse']:
        data['utilisateur']['adresse'] = None

    writepath = f"./data/transac_{idx + 1}.json"
    mode = 'a' if os.path.exists(writepath) else 'w'
    with open(writepath, mode) as f:
        f.write(message.value.decode('utf-8'))

    print(f"{idx + 1}: Un {data['type_transaction']} a eu lieu à {data['lieu']} "
          f"par {data['utilisateur']['nom']} "
          f"pour un montant de {data['montant']} {data['devise']} ({to_eur} EUR) ")
    # time.sleep(1)

consumer.close()
print("Consumer stopped")
spark.read.json("data").write.parquet("parquet", mode="overwrite")

parquet_files = [f for f in os.listdir("parquet") if f.endswith(".parquet")]
print(f"Uploading {len(parquet_files)} files to Minio")
for file in parquet_files:
    with open(f"parquet/{file}", 'r') as infile:
        try:
            client.fput_object(bucket_name, file, infile.name)
        except S3Error as err:
            print(f"Erreur lors de l'upload de {file}: {err}")

