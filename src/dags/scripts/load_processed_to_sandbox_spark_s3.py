# /opt/airflow/dags/scripts/load_processed_to_sandbox_spark_s3.py

import os
from pyspark.sql import SparkSession
# Không cần functions as F vì không có biến đổi dữ liệu
from dotenv import load_dotenv
import sys

# Tải biến môi trường
load_dotenv()

# --- Cấu hình AWS S3 ---
AWS_REGION = os.getenv('AWS_REGION')
# Bucket S3 CHUNG chứa dữ liệu nguồn và đích
S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME') # Ví dụ: my-fraud-detection-bucket
# Prefix (thư mục) chứa dữ liệu Parquet đã xử lý (nguồn)
SOURCE_PROCESSED_PREFIX = os.getenv('PROCESSED_OUTPUT_PREFIX', "processed/transactions_processed_parquet/")
# Prefix (thư mục) đích cho sandbox
SANDBOX_DEST_PREFIX = os.getenv('SANDBOX_DEST_PREFIX', "sandbox/transactions_processed_parquet/")

# --- Phiên bản JARs - !! QUAN TRỌNG: Điều chỉnh cho phù hợp !! ---
HADOOP_AWS_VERSION = os.getenv('HADOOP_AWS_VERSION', "3.3.4")
AWS_JAVA_SDK_VERSION = os.getenv('AWS_JAVA_SDK_VERSION', "1.12.262")

def ensure_trailing_slash(prefix):
    """Đảm bảo prefix kết thúc bằng dấu '/' """
    if prefix and not prefix.endswith('/'):
        return prefix + '/'
    if not prefix:
        print("Lỗi: Prefix không được để trống.")
        sys.exit(1) # Thoát nếu prefix rỗng
    return prefix

def main():
    print("--- Bắt đầu Script Load Processed to Sandbox (PySpark AWS S3) ---")

    # --- Kiểm tra cấu hình ---
    required_configs = {
        "AWS_REGION": AWS_REGION,
        "S3_BUCKET_NAME": S3_BUCKET_NAME
    }
    missing_configs = [k for k, v in required_configs.items() if not v]
    if missing_configs:
        print(f"Error: Missing AWS S3 configuration: {', '.join(missing_configs)}")
        sys.exit(1)

    # Chuẩn hóa và kiểm tra prefix
    try:
        source_prefix_norm = ensure_trailing_slash(SOURCE_PROCESSED_PREFIX)
        dest_prefix_norm = ensure_trailing_slash(SANDBOX_DEST_PREFIX)
    except SystemExit: # Bắt lỗi từ ensure_trailing_slash
        sys.exit(1)

    if source_prefix_norm == dest_prefix_norm:
        print(f"Lỗi: Prefix nguồn và đích giống hệt nhau: '{source_prefix_norm}'.")
        sys.exit(1)

    print(f"AWS Region: {AWS_REGION}")
    print(f"S3 Bucket: {S3_BUCKET_NAME}")
    print(f"Source Prefix (Processed): {source_prefix_norm}")
    print(f"Destination Prefix (Sandbox): {dest_prefix_norm}")

    spark = None
    try:
        spark_builder = SparkSession.builder.appName("S3 Processed to Sandbox Copy (Spark)")

        # --- Cấu hình Spark cho AWS S3 (Tương tự script Spark trước) ---
        spark_builder.config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        print("Configuring Spark for default AWS credential providers (IAM roles, Env Vars...).")
        # Output Committer
        spark_builder.config("spark.sql.parquet.output.committer.class", "org.apache.hadoop.mapreduce.lib.output.DirectFileOutputCommitter")
        spark_builder.config("spark.sql.sources.commitProtocolClass", "org.apache.spark.internal.io.cloud.PathOutputCommitProtocol")
        spark_builder.config("spark.hadoop.mapreduce.outputcommitter.factory.scheme.s3a", "org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory")
        # JAR Packages
        jar_packages = [
            f"org.apache.hadoop:hadoop-aws:{HADOOP_AWS_VERSION}",
            f"com.amazonaws:aws-java-sdk-bundle:{AWS_JAVA_SDK_VERSION}"
        ]
        spark_builder.config("spark.jars.packages", ",".join(jar_packages))

        spark = spark_builder.getOrCreate()
        print("SparkSession created successfully.")
        print(f"Using JAR packages: {','.join(jar_packages)}")

        # --- Đọc và Ghi dữ liệu ---
        input_path = f"s3a://{S3_BUCKET_NAME}/{source_prefix_norm}"
        output_path = f"s3a://{S3_BUCKET_NAME}/{dest_prefix_norm}"

        print(f"Reading Parquet data from: {input_path}")
        # Đọc dữ liệu từ thư mục nguồn
        # Spark có thể xử lý nếu input_path là thư mục chứa nhiều file Parquet (partitioned)
        df = spark.read.parquet(input_path)

        # (Tùy chọn) In thông tin để kiểm tra
        print("Schema of data being copied:")
        df.printSchema()
        record_count = df.count()
        print(f"Number of records to copy: {record_count}")

        if record_count == 0:
             print(f"No data found in source path '{input_path}'. Nothing to copy.")
             # Thoát thành công vì không có gì để làm
             sys.exit(0)

        print(f"Writing data to destination: {output_path}")
        # Ghi DataFrame vào thư mục đích với chế độ overwrite
        # Chế độ "overwrite" sẽ xóa thư mục đích nếu nó tồn tại trước khi ghi dữ liệu mới
        df.write.mode("overwrite").parquet(output_path)

        print(f"Successfully copied data from {source_prefix_norm} to {dest_prefix_norm} in bucket {S3_BUCKET_NAME}.")
        print("--- Script Load Processed to Sandbox (PySpark AWS S3) finished successfully ---")

    except Exception as e:
        print(f"An error occurred during PySpark execution: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1) # Thoát với lỗi

    finally:
        if spark:
            print("Stopping SparkSession.")
            spark.stop()

if __name__ == "__main__":
    main()