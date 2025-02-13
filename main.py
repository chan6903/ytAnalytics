import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import mysql.connector

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("YouTubeTrendingETL") \
    .config("spark.sql.catalogImplementation", "hive") \
    .enableHiveSupport() \
    .getOrCreate()

# Define Schema for YouTube Data
youtube_schema = StructType([
    StructField("video_id", StringType(), True),
    StructField("title", StringType(), True),
    StructField("channel_title", StringType(), True),
    StructField("publish_time", StringType(), True),
    StructField("tags", StringType(), True),
    StructField("views", IntegerType(), True),
    StructField("likes", IntegerType(), True),
    StructField("dislikes", IntegerType(), True),
    StructField("comment_count", IntegerType(), True),
    StructField("category_id", IntegerType(), True)
])

# Function to Extract Data from HDFS
def extract_data():
    df = spark.read.option("header", "true").csv("hdfs:///youtube_trending_data/*.csv", schema=youtube_schema)
    return df

# Function to Transform Data
def transform_data(df):
    df_transformed = df.withColumn("publish_time", col("publish_time").cast(StringType()))
    return df_transformed

# Function to Load Data into Hive
def load_data(df):
    df.write.mode("overwrite").saveAsTable("youtube_trending_data")

# Function to Load Metadata into MySQL
def load_metadata():
    conn = mysql.connector.connect(host="localhost", user="root", password="password", database="metadata_db")
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS etl_metadata (run_id INT AUTO_INCREMENT PRIMARY KEY, run_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
    cursor.execute("INSERT INTO etl_metadata (run_time) VALUES (NOW())")
    conn.commit()
    conn.close()

# Define Airflow DAG
def etl_pipeline():
    df = extract_data()
    df_transformed = transform_data(df)
    load_data(df_transformed)
    load_metadata()

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 2, 13),
    "retries": 1
}

dag = DAG(
    "youtube_trending_etl",
    default_args=default_args,
    schedule_interval="@daily"
)

run_etl = PythonOperator(
    task_id="run_etl_pipeline",
    python_callable=etl_pipeline,
    dag=dag
)

run_etl
