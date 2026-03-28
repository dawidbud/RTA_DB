from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    group_id='filter-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Nasłuchuję na duże transakcje (> 1000)...")
for msg in consumer:
    tx = msg.value
    if tx['amount'] > 1000:
        print(f"ALERT: {tx['tx_id']} | {tx['amount']} PLN | {tx['store']}")