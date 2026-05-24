from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

customers = ['Alice', 'Bob', 'Charlie', 'David', 'Eva']
products = ['Laptop', 'Phone', 'Tablet', 'Watch', 'Headphone']

print('Producer is running...')
while True:
    order = {
        'customer': random.choice(customers),
        'product': random.choice(products),
        'amount': random.randint(10, 500),
    }
    producer.send('sales_topic', value=order)
    print(f"Sent: {order}")
    time.sleep(2) # 1 giay gui 1 lan