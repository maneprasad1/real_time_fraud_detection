import boto3
import pandas as pd
import io
import time
from kafka import KafkaProducer
import json

#CONFIG
bucket = "online-fraud-detection-bucket"
file_key = "fraud_data.csv"
topic_name = "fraud-topic"
kafka_bootstrap = "localhost:9092"

#S3 READ (as stream)
s3 = boto3.client('s3')
csv_obj = s3.get_object(Bucket=bucket, Key=file_key)
body = csv_obj['Body']

#KAFKA PRODUCER
producer = KafkaProducer(
    bootstrap_servers=[kafka_bootstrap],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

batch_size = 10000  #rows per second
print("Starting streaming from S3 to Kafka...", flush=True)

#Read file in chunks
for chunk in pd.read_csv(io.BytesIO(body.read()), chunksize=batch_size):
    for _, row in chunk.iterrows():
        producer.send(topic_name, row.to_dict())
    producer.flush()
    print(f"Sent {len(chunk)} rows", flush=True)
    time.sleep(1)

print("Done sending all rows.")

