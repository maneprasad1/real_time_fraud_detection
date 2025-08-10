# Real-Time Fraud Detection Pipeline with AWS, Kafka, and PySpark  

## Overview  
This project is a **Real-time fraud detection system** that processes transaction data streamed from **AWS S3** through **Apache Kafka** and analyzes it using **PySpark** on an EC2 instance. The pipeline predicts fraudulent transactions using a **pretrained Decision Tree model** and sends alerts via **AWS SNS**. The results are stored back in **S3** under separate folders for fraud and non-fraud transactions.  

---

## Project Flow  

1. **Data Storage (AWS S3)**  
   - The dataset `fraud_data.csv` is stored in an S3 bucket.  

2. **Data Streaming (EC2-1 with Kafka Producer)**  
   - Kafka Producer reads `fraud_data.csv` from S3.  
   - Streams **10,000 rows per second** to a Kafka topic (`fraud-topic`).  

3. **Data Processing (EC2-2 with Kafka Consumer + PySpark)**  
   - Kafka Consumer reads messages from `fraud-topic`.  
   - PySpark processes **10k rows/sec** in micro-batches.  
   - Pretrained **Decision Tree model** (trained in Google Colab) predicts whether a transaction is fraudulent.  

4. **Fraud Alerts (AWS SNS)**  
   - If a transaction is predicted as fraud → An **email alert** is sent to the bank using AWS SNS.  

5. **Data Storage (AWS S3)**  
   - **Fraudulent transactions** → Saved in `/fraud_data/` folder in S3.  
   - **Non-fraudulent transactions** → Saved in `/nonfraud_data/` folder in S3.  

---

## Tech Stack  

- **Cloud**: AWS EC2, S3, SNS  
- **Streaming**: Apache Kafka  
- **Processing**: Apache Spark (PySpark)  
- **Machine Learning**: Scikit-learn (Decision Tree)  
- **Development**: Python, Google Colab  

---

## Project Architecture  

<img width="827" height="465" alt="project_architecture drawio" src="https://github.com/user-attachments/assets/e9b862a8-1ef4-4ca3-b810-028bcb4f68cd" />

