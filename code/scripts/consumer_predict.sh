#!/bin/bash

spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 consumer_decision.py
