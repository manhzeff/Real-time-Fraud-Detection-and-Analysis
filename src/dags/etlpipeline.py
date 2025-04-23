# /opt/airflow/dags/aws_s3_etl_training_dag_clean.py

from __future__ import annotations

import pendulum
import os
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

# --- Constants ---
DAGS_FOLDER = os.path.dirname(os.path.realpath(__file__))
SCRIPTS_PATH = os.path.join(DAGS_FOLDER, "scripts")

CONSOLIDATE_SCRIPT = "consolidate_json_to_parquet_s3.py"
PROCESS_SCRIPT_SPARK = "process_raw_to_processed_single_bucket_s3.py"
LOAD_SANDBOX_SCRIPT_SPARK = "load_processed_to_sandbox_spark_s3.py"
TRAIN_SCRIPT = "train_fraud_model_s3_source_minio_artifacts.py" # Ensure correct script

# --- Environment Variables Definition ---
# Uses Jinja templating to pull from Airflow Variables. Assumes default AWS auth (IAM roles).
common_env_vars = {
    # S3 Config
    "AWS_REGION": '{{ var.value.get("aws_region", "us-east-1") }}',
    "S3_BUCKET_NAME": '{{ var.value.get("s3_bucket_name", "default-s3-bucket") }}',

    # S3 Prefixes/Keys
    "SOURCE_JSON_PREFIX": '{{ var.value.get("source_json_prefix", "transactions/") }}',
    "SOURCE_PARQUET_KEY": '{{ var.value.get("source_parquet_key", "raw/consolidated_transactions.parquet") }}',
    "PROCESSED_OUTPUT_PREFIX": '{{ var.value.get("processed_output_prefix", "processed/transactions_processed_parquet/") }}',
    "SANDBOX_DEST_PREFIX": '{{ var.value.get("sandbox_dest_prefix", "sandbox/transactions_processed_parquet/") }}',

    # Spark Config (Versions & Master)
    "HADOOP_AWS_VERSION": '{{ var.value.get("hadoop_aws_version", "3.3.4") }}',
    "AWS_JAVA_SDK_VERSION": '{{ var.value.get("aws_java_sdk_version", "1.12.262") }}',
    "SPARK_MASTER": '{{ var.value.get("spark_master", "local[*]") }}', # Configurable Spark master

    # MLflow Config
    "MLFLOW_TRACKING_URI": '{{ var.value.get("mlflow_tracking_uri", "http://mlflow-server:5500") }}',
    "MLFLOW_EXPERIMENT_NAME": '{{ var.value.get("mlflow_experiment_name", "Airflow Fraud Training S3") }}',

    # Misc
    'PYTHONUNBUFFERED': '1',
}

# --- DAG Definition ---
with DAG(
    dag_id="aws_s3_etl_training_pipeline_clean",
    schedule=None,
    start_date=pendulum.datetime(2024, 4, 22, tz="UTC"),
    catchup=False,
    tags=["aws", "s3", "etl", "pyspark", "training", "mlflow"],
    doc_md="ETL pipeline using S3, Spark, and MLflow for fraud detection model training.",
) as dag:

    # --- Task Definitions ---
    task_1_consolidate = BashOperator(
        task_id="consolidate_json_to_parquet",
        bash_command=f"python {os.path.join(SCRIPTS_PATH, CONSOLIDATE_SCRIPT)}",
        env=common_env_vars,
    )

    task_2_process_spark = BashOperator(
        task_id="process_raw_data_spark",
        bash_command=f"spark-submit --master {{{{ var.value.get('spark_master', 'local[*]') }}}} {os.path.join(SCRIPTS_PATH, PROCESS_SCRIPT_SPARK)}",
        env=common_env_vars,
    )

    task_3_load_sandbox_spark = BashOperator(
        task_id="load_processed_to_sandbox_spark",
        bash_command=f"spark-submit --master {{{{ var.value.get('spark_master', 'local[*]') }}}} {os.path.join(SCRIPTS_PATH, LOAD_SANDBOX_SCRIPT_SPARK)}",
        env=common_env_vars,
    )

    task_4_train_model = BashOperator(
        task_id="train_fraud_model",
        bash_command=f"python {os.path.join(SCRIPTS_PATH, TRAIN_SCRIPT)}",
        env={
             **common_env_vars,
             # Training script specific inputs (reading from sandbox)
             "AWS_SOURCE_BUCKET": '{{ var.value.get("s3_bucket_name") }}',
             "AWS_SOURCE_OBJECT": '{{ var.value.get("sandbox_dest_prefix") }}', # Training script reads this prefix
             # Example training parameters (can also be in script's config)
             "MODEL_TYPE": "RandomForest",
             "RESAMPLING_METHOD": "smote",
            },
    )

    # --- Task Dependencies ---
    task_1_consolidate >> task_2_process_spark >> task_3_load_sandbox_spark >> task_4_train_model