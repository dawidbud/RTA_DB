from kafka import KafkaConsumer
from collections import Counter, defaultdict
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    group_id='count-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

store_counts = Counter()
total_amount = defaultdict(float)
msg_count = 0

for msg in consumer:
    tx = msg.value
    store = tx['store']
    store_counts[store] += 1
    total_amount[store] += tx['amount']
    msg_count += 1

    if msg_count % 10 == 0:
        print(f"\n--- RAPORT PO {msg_count} WIADOMOŚCIACH ---")
        print(f"{'Sklep':<12} | {'Sztuk':<6} | {'Suma':>10}")
        for s in sorted(store_counts.keys()):
            print(f"{s:<12} | {store_counts[s]:<6} | {total_amount[s]:>10.2f}")