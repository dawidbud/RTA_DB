from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    group_id='enrich-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

for msg in consumer:
    tx = msg.value
    amt = tx['amount']
    tx['risk_level'] = "HIGH" if amt > 3000 else ("MEDIUM" if amt > 1000 else "LOW")
    print(f"ID: {tx['tx_id']} | Risk: {tx['risk_level']}")