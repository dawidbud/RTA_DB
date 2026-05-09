from kafka import KafkaConsumer, KafkaProducer
import json
import requests

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    group_id='ml-scoring',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

alert_producer = KafkaProducer(
    bootstrap_servers='broker:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

API_URL = "http://localhost:8001/score"

for message in consumer:
    tx = message.value
    tx_id = tx.get('tx_id')
    amount = float(tx.get('amount', 0.0))
    is_electronics = int(tx.get('is_electronics', 0))

    payload = {
        'amount': amount,
        'is_electronics': is_electronics,
        'tx_per_minute': 5
    }

    response = requests.post(API_URL, json=payload)
    result = response.json()

    if result.get('is_fraud'):
        alert = {
            'tx_id': tx_id,
            'amount': amount,
            'fraud_probability': result.get('fraud_probability'),
            'timestamp': tx.get('timestamp'),
            'alert_time': __import__('datetime').datetime.utcnow().isoformat() + 'Z'
        }
        alert_producer.send('alerts', value=alert)
        alert_producer.flush()
        print(f"ALERT: {tx_id} -> {alert['fraud_probability']:.2f}")
    else:
        print(f"OK: {tx_id} -> {result.get('fraud_probability'):.2f}")
