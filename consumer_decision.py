from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType
import pandas as pd
import joblib, boto3, io, gc, os
import json

# Spark Session
spark = SparkSession.builder.appName("FraudStream").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Schema 
schema = StructType() \
    .add("step", IntegerType()) \
    .add("type", StringType()) \
    .add("amount", DoubleType()) \
    .add("nameOrig", StringType()) \
    .add("oldbalanceOrg", DoubleType()) \
    .add("newbalanceOrig", DoubleType()) \
    .add("nameDest", StringType()) \
    .add("oldbalanceDest", DoubleType()) \
    .add("newbalanceDest", DoubleType()) \
    .add("isFraud", IntegerType()) \
    .add("isFlaggedFraud", IntegerType()) \
    .add("transaction_id", StringType())

# Load model from S3 
bucket = "online-fraud-detection-bucket"
model_key = "decision_tree.pkl"
s3 = boto3.client("s3")
model = joblib.load(io.BytesIO(s3.get_object(Bucket=bucket, Key=model_key)["Body"].read()))
model_bc = spark.sparkContext.broadcast(model)

#  UDF 
def predict(amount, o1, n1, o2, n2):
    X = pd.DataFrame([[amount, o1, n1, o2, n2]],
                     columns=["amount", "oldbalanceOrg", "newbalanceOrig", "oldbalanceDest", "newbalanceDest"])
    return int(model_bc.value.predict(X)[0])

predict_udf = udf(predict, IntegerType())

# Read from Kafka
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "172.31.5.14:9092") \
    .option("subscribe", "fraud-topic") \
    .option("startingOffsets", "latest") \
    .load()

data_df = df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*")

# Setup SNS client 
sns_client = boto3.client("sns", region_name="ap-south-1")
sns_topic_arn = os.getenv("SNS_TOPIC_ARN") 

def send_fraud_alerts(fraud_df):

    alerts = fraud_df.limit(2).toJSON().collect()
    for alert_json in alerts:
        alert = json.loads(alert_json)

        msg = (
            f"Fraudulent Transaction Detected!\n\n"
            f"Transaction ID: {alert.get('transaction_id')}\n"
            f"Type: {alert.get('type')}\n"
            f"Amount: ${alert.get('amount')}\n"
            f"Old Balance (Origin): ${alert.get('oldbalanceOrg')}\n"
            f"New Balance (Origin): ${alert.get('newbalanceOrig')}\n"
            f"Old Balance (Dest): ${alert.get('oldbalanceDest')}\n"
            f"New Balance (Dest): ${alert.get('newbalanceDest')}\n"
        )

        sns_client.publish(
            TopicArn=sns_topic_arn,
            Subject="Fraud Alert",
            Message=msg
        )

# Batch Processor 
def process_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    # Limit to 10K rows
    limited_df = batch_df.limit(10000)

    # Predict and add prediction column
    pred_df = limited_df.withColumn("prediction", predict_udf(
        col("amount"), col("oldbalanceOrg"), col("newbalanceOrig"),
        col("oldbalanceDest"), col("newbalanceDest")
    ))
    fraud_df = pred_df.filter(col("prediction") == 1)
    send_fraud_alerts(fraud_df)
    # Write to S3
    for label, filt_df, s3_key in [
        ("fraud", pred_df.filter(col("prediction") == 1), "fraud_predictions/frauds.csv"),
        ("nonfraud", pred_df.filter(col("prediction") == 0), "nonfraud_predictions/nonfrauds.csv")
    ]:
        pdf = filt_df.select(
            "amount", "oldbalanceOrg", "newbalanceOrig", "oldbalanceDest", "newbalanceDest",
            "type", "transaction_id", "prediction"
        ).coalesce(1).toPandas()

        if pdf.empty:
            continue

        buf = io.StringIO()
        pdf.to_csv(buf, index=False, header=False)

        try:
            existing = s3.get_object(Bucket=bucket, Key=s3_key)["Body"].read().decode()
        except s3.exceptions.NoSuchKey:
            existing = "amount,oldbalanceOrg,newbalanceOrig,oldbalanceDest,newbalanceDest,type,transaction_id,prediction\n"

        s3.put_object(Bucket=bucket, Key=s3_key, Body=existing + buf.getvalue())
    del batch_df, limited_df, pred_df, pdf, buf, existing
    gc.collect()

# Start Stream 
data_df.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("append") \
    .trigger(processingTime="1 second") \
    .start() \
    .awaitTermination()
