from kafka import KafkaConsumer, KafkaProducer
import json

consumer = KafkaConsumer('transactions', bootstrap_servers='broker:9092',
    auto_offset_reset='earliest', group_id='scoring-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')))

alert_producer = KafkaProducer(bootstrap_servers='broker:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'))

for msg in consumer:
    tx = msg.value
    score = 0
    if tx['amount'] > 3000: score += 3
    if tx['category'] == 'elektronika' and tx['amount'] > 1500: score += 2
    if tx['hour'] < 6: score += 2
    
    if score >= 3:
        tx['fraud_score'] = score
        alert_producer.send('alerts', value=tx)
        print(f"!!! FRAUD DETECTED !!! {tx['tx_id']} | Score: {score}")