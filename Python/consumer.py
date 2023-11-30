from kafka import *
import json
import time

consumer = KafkaConsumer('transactions', bootstrap_servers=['127.0.0.1:9092'])

for idx, message in enumerate(consumer):
    data = json.loads(message.value.decode('utf-8'))
    print(f"{idx + 1}: Un {data['type_transaction']} de {data['montant']} {data['devise']} a eu lieu à {data['lieu']}")
    time.sleep(1)
