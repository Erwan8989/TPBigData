from kafka import *
from minio import S3Error

from minio_client import client, bucket_name
import json
import time
from pyspark.sql import SparkSession
import pyspark.sql.functions as func

spark = SparkSession.builder.getOrCreate()

consumer = KafkaConsumer('transactions', bootstrap_servers=['127.0.0.1:9092'])
transactions = list()

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

    with open("data.json", "w") as f:
        f.write(message.value.decode('utf-8'))

    try:
        client.fput_object(bucket_name, f"{idx + 1}.json", "data.json")
    except S3Error as err:
        print(f"Erreur lors de l'upload de {idx}: {err}")

    print(f"{idx + 1}: Un {data['type_transaction']} a eu lieu à {data['lieu']} "
          f"par {data['utilisateur']['nom']} "
          f"pour un montant de {data['montant']} {data['devise']} ({to_eur} EUR) ")
    time.sleep(1)
