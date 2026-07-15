import json
import time
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

producer.send("numbers", {"group": 1, "number": 50})
producer.send("numbers", {"group": 1, "number": 55})

print("Sent group 1: 50, 55")

time.sleep(10)

producer.send("numbers", {"group": 2, "number": 60})
producer.send("numbers", {"group": 2, "number": 65})

print("Sent group 2: 60, 65")

producer.flush()
producer.close()