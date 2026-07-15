import pandas as pd
import json
import time
from kafka import KafkaProducer

def start_stream():
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
    
    df = pd.read_csv('ecommerce_transactions.csv')
    print("Kafka Stream started. Simulating live transactions...")
    
    try:
        # Use records from index 15001 onwards to simulate 'new' data
        for i in range(15001, len(df)):
            row = df.iloc[i]
            data = {
                'category': str(row['Product_Category']),
                'amount': float(row['Purchase_Amount']),
                'timestamp': str(pd.Timestamp.now()) # Use current time for Spark Windows
            }
            producer.send('sales_topic', value=data)
            if i % 10 == 0:
                print(f"Sent record {i} to Kafka...")
            time.sleep(1) # Send 1 record per second
    except KeyboardInterrupt:
        print("Streaming stopped.")

if __name__ == "__main__":
    start_stream()