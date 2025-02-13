# Open-Source ETL Pipeline for YouTube Trending Videos

## Project Overview

This project processes YouTube trending video data from multiple regions using an ETL pipeline. The pipeline includes:

- **Extract**: Load raw CSV data from HDFS.
- **Transform**: Clean and process data using PySpark.
- **Load**: Store structured data in Hive Metastore.
- **Metadata Storage**: Uses MySQL for tracking pipeline execution.
- **Visualization**: Uses Apache Superset or Metabase for dashboards.
- **Orchestration**: Automates ETL using Apache Airflow.

## Tech Stack

- **Storage**: HDFS
- **Processing**: PySpark
- **Data Catalog**: Hive Metastore
- **Database**: MySQL (for metadata)
- **Deployment**: Local machine
- **Visualization**: Apache Superset / Metabase
- **Orchestration**: Apache Airflow

## Folder Structure

```
├── data/                   # Sample dataset (for testing)
├── dags/                   # Airflow DAGs
├── scripts/                # PySpark ETL scripts
├── sql/                    # SQL queries for Hive & MySQL
├── dashboards/             # Superset/Metabase setup files
├── docker-compose.yml      # Optional Docker setup
├── README.md               # Project documentation
```

## Setup Instructions

### Install Dependencies

Ensure you have the following installed:

- **Hadoop (HDFS)**
- **Apache Hive**
- **MySQL**
- **PySpark**
- **Apache Airflow**
- **Apache Superset** (or **Metabase** for visualization)

Install Python dependencies:

```bash
pip install pyspark mysql-connector-python apache-airflow
```

### Start HDFS & Hive Metastore

```bash
start-dfs.sh
start-hive.sh
```

### Create MySQL Database for Metadata

```sql
CREATE DATABASE youtube_metadata;
```

### Run the ETL Pipeline

Execute the PySpark script to process data:

```bash
spark-submit scripts/etl_pipeline.py
```

### Schedule with Airflow

Start Airflow services:

```bash
airflow webserver &
airflow scheduler &
```

Trigger the DAG in the Airflow UI.

### Visualization with Superset

Start Superset:

```bash
superset db upgrade
superset init
superset run -p 8088 --with-threads --reload --debugger
```

Access **[http://localhost:8088](http://localhost:8088)** and connect to Hive Metastore.

### Dashboard Setup

1. **Connect Apache Superset** to Hive.
2. **Create a dataset** from the processed YouTube data.
3. **Build visualizations** for trending videos.

