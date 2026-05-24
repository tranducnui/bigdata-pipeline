from kafka import KafkaProducer, KafkaConsumer
import json
import pyodbc

consumer = KafkaConsumer(
    'sales_topic',
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    group_id='sales_group'
)

conn = pyodbc.connect(
    'DRIVER={SQL Server};'
    'SERVER=localhost,1435;'
    'DATABASE=BigDataDB;'
    'UID=sa;'
    'PWD=BigData123!;'
)

cursor = conn.cursor()

print('Consumer is running...')
for message in consumer:
    order = message.value
    print(f'received: {order}')
    cursor.execute(
        "INSERT INTO sales_realtime (customer, product, amount) VALUES (?,?,?)",
        order['customer'], order['product'], order['amount']
    )
    conn.commit()