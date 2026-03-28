from kafka import KafkaProducer
import json, random, time
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers='broker:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def generate_transaction():
    is_suspicious = random.random() < 0.05
    if is_suspicious:
        amount, category, hour = round(random.uniform(3001.0, 5000.0), 2), 'elektronika', random.randint(0, 5)
    else:
        amount, category, hour = round(random.uniform(5.0, 3000.0), 2), random.choice(['odzież', 'żywność', 'książki']), random.randint(6, 23)
    
    return {
        'tx_id': f'TX{random.randint(1000,9999)}',
        'amount': amount,
        'store': random.choice(['Warszawa', 'Kraków', 'Gdańsk', 'Wrocław']),
        'category': category,
        'hour': hour,
        'timestamp': datetime.now().isoformat()
    }

print("Producent wysyła dane...")
for i in range(100):
    tx = generate_transaction()
    producer.send('transactions', value=tx)
    print(f"[{i+1}] Wysłano {tx['tx_id']} | {tx['amount']} PLN")
    time.sleep(0.5)
producer.close()