# /opt/airflow/dags/scripts/process_raw_to_processed_single_bucket_s3.py

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, TimestampType, BooleanType, StringType
from dotenv import load_dotenv
import sys

# Tải biến môi trường
load_dotenv()

# --- Cấu hình AWS S3 ---
AWS_REGION = os.getenv('AWS_REGION')
# Bucket S3 CHUNG chứa cả dữ liệu nguồn và đích
S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME') # Ví dụ: my-fraud-detection-bucket
# Key (đường dẫn) tới file Parquet nguồn trong bucket
SOURCE_PARQUET_KEY = os.getenv('SOURCE_PARQUET_KEY', 'raw/consolidated_transactions.parquet')
# Prefix (thư mục) output trong cùng bucket S3
PROCESSED_OUTPUT_PREFIX = os.getenv('PROCESSED_OUTPUT_PREFIX', "processed/transactions_processed_parquet") # Đổi tên biến và giá trị mặc định

# --- Phiên bản JARs - !! QUAN TRỌNG: Điều chỉnh cho phù hợp với môi trường Spark/Hadoop của bạn !! ---
HADOOP_AWS_VERSION = os.getenv('HADOOP_AWS_VERSION', "3.3.4")
AWS_JAVA_SDK_VERSION = os.getenv('AWS_JAVA_SDK_VERSION', "1.12.262")

def main():
    print("Starting script: process_raw_to_processed_single_bucket_s3.py (using PySpark with AWS S3)")

    # --- Kiểm tra cấu hình ---
    required_configs = {
        "AWS_REGION": AWS_REGION,
        "S3_BUCKET_NAME": S3_BUCKET_NAME
        # SOURCE_PARQUET_KEY và PROCESSED_OUTPUT_PREFIX có giá trị mặc định
    }
    missing_configs = [k for k, v in required_configs.items() if not v]
    if missing_configs:
        print(f"Error: Missing AWS S3 configuration in environment variables: {', '.join(missing_configs)}")
        sys.exit(1)

    # Đảm bảo PROCESSED_OUTPUT_PREFIX kết thúc bằng '/' nếu nó không rỗng
    processed_output_dir = PROCESSED_OUTPUT_PREFIX
    if processed_output_dir and not processed_output_dir.endswith('/'):
        processed_output_dir += '/'
        print(f"Auto-appending '/' to PROCESSED_OUTPUT_PREFIX: {processed_output_dir}")
    elif not processed_output_dir: # Xử lý trường hợp rỗng
         print("Error: PROCESSED_OUTPUT_PREFIX cannot be empty.")
         sys.exit(1)


    print(f"AWS Region: {AWS_REGION}")
    print(f"S3 Bucket (Source & Processed): {S3_BUCKET_NAME}")
    print(f"Source Parquet Key: {SOURCE_PARQUET_KEY}")
    print(f"Processed Output Prefix: {processed_output_dir}")


    spark = None
    try:
        spark_builder = SparkSession.builder.appName("AWS S3 Raw to Processed ETL (Single Bucket)")

        # --- Cấu hình Spark cho AWS S3 (Giữ nguyên như phiên bản trước) ---
        spark_builder.config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        # Xác thực qua IAM Roles hoặc Env Vars (mặc định)
        print("Configuring Spark to use default AWS credential providers (IAM roles, Env Vars, etc.).")
        # Cấu hình Output Committer
        spark_builder.config("spark.sql.parquet.output.committer.class", "org.apache.hadoop.mapreduce.lib.output.DirectFileOutputCommitter")
        spark_builder.config("spark.sql.sources.commitProtocolClass", "org.apache.spark.internal.io.cloud.PathOutputCommitProtocol")
        spark_builder.config("spark.hadoop.mapreduce.outputcommitter.factory.scheme.s3a", "org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory")
        # Cấu hình JAR Packages
        jar_packages = [
            f"org.apache.hadoop:hadoop-aws:{HADOOP_AWS_VERSION}",
            f"com.amazonaws:aws-java-sdk-bundle:{AWS_JAVA_SDK_VERSION}"
        ]
        spark_builder.config("spark.jars.packages", ",".join(jar_packages))

        spark = spark_builder.getOrCreate()
        print("SparkSession created successfully.")
        print(f"Using JAR packages: {','.join(jar_packages)}")

        # ---- Xử lý dữ liệu ----
        # Đường dẫn input và output giờ cùng trỏ đến S3_BUCKET_NAME
        input_path = f"s3a://{S3_BUCKET_NAME}/{SOURCE_PARQUET_KEY}"
        output_path = f"s3a://{S3_BUCKET_NAME}/{processed_output_dir}" # Sử dụng biến đã chuẩn hóa

        print(f"Reading Parquet data from: {input_path}")
        df = spark.read.parquet(input_path)

        print("Schema read from Parquet file:")
        df.printSchema()

        if df.rdd.isEmpty():
            print(f"No data found in {input_path}. Exiting.")
            sys.exit(0)

        initial_count = df.count()
        print(f"Initial record count: {initial_count}")

        # ---- Logic xử lý ví dụ (Giữ nguyên) ----
        cols_to_drop = ["type", "partition", "offset", "key"]
        existing_cols_to_drop = [c for c in cols_to_drop if c in df.columns]
        if existing_cols_to_drop:
             print(f"Dropping columns: {existing_cols_to_drop}")
             df_cleaned = df.drop(*existing_cols_to_drop)
        else:
             df_cleaned = df

        print("Casting data types...")
        df_typed = df_cleaned \
            .withColumn("amount", F.col("amount").cast(DoubleType())) \
            .withColumn("timestamp", F.col("timestamp").cast(TimestampType())) \
            .withColumn("is_fraud", F.col("is_fraud").cast(BooleanType())) \
            .withColumn("transaction_id", F.col("transaction_id").cast(StringType())) \
            .withColumn("user_id", F.col("user_id").cast(StringType())) \
            .withColumn("merchant_id", F.col("merchant_id").cast(StringType()))

        df_processed = df_typed.withColumn("is_large_transaction", F.when(F.col("amount") > 1000, True).otherwise(False))
        df_processed = df_processed.withColumn("processed_at", F.current_timestamp())
        df_final = df_processed

        processed_count = df_final.count()
        print(f"Record count after processing: {processed_count}")
        print("Schema after processing:")
        df_final.printSchema()
        print("Sample data after processing:")
        df_final.show(5, truncate=False)

        if processed_count != initial_count:
            print(f"Warning: Record count changed during processing ({initial_count} -> {processed_count}). Check transformation logic.")

        # ---- Ghi dữ liệu đã xử lý ----
        print(f"Writing processed data to Parquet format at: {output_path}")
        # Ghi ra định dạng Parquet vào cùng bucket S3 nhưng prefix khác
        df_final.write.mode("overwrite").parquet(output_path)

        print(f"Successfully processed data and wrote to {output_path}")

    except Exception as e:
        print(f"An error occurred during PySpark processing: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    finally:
        if spark:
            print("Stopping SparkSession.")
            spark.stop()

if __name__ == "__main__":
    main()