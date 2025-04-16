# /opt/airflow/dags/scripts/process_raw_to_processed.py

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, TimestampType, BooleanType, StringType # Thêm StringType
from dotenv import load_dotenv

# Tải biến môi trường
load_dotenv()

# --- Cấu hình MinIO ---
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY')
RAW_BUCKET = os.getenv('MINIO_RAW_BUCKET', 'raw')
PROCESSED_BUCKET = os.getenv('MINIO_PROCESSED_BUCKET', 'processed')

# --- Phiên bản JARs - !! QUAN TRỌNG: Điều chỉnh cho phù hợp với môi trường Spark/Hadoop của bạn !! ---
# Ví dụ: Nếu Spark dùng Hadoop 3.3.4
HADOOP_AWS_VERSION = "3.3.4"
AWS_JAVA_SDK_VERSION = "1.12.262" # Thường đi kèm hoặc tương thích

# --- Định nghĩa tên thư mục output trong bucket processed ---
PROCESSED_OUTPUT_DIR = "transactions_processed_parquet"

def main():
    print("Starting script: process_raw_to_processed.py (using PySpark)")

    if not all([MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, RAW_BUCKET, PROCESSED_BUCKET]):
        print("Error: Missing MinIO configuration in environment variables.")
        return

    # Đảm bảo endpoint có scheme (http/https)
    minio_endpoint_url = MINIO_ENDPOINT
    if not minio_endpoint_url.startswith("http"):
        print(f"Warning: Assuming http scheme for MinIO endpoint '{MINIO_ENDPOINT}'")
        minio_endpoint_url = f"http://{MINIO_ENDPOINT}"

    print(f"Configuring Spark for MinIO endpoint: {minio_endpoint_url}")
    print(f"Using Raw Bucket: {RAW_BUCKET}, Processed Bucket: {PROCESSED_BUCKET}")

    spark = None # Khởi tạo spark là None để kiểm tra trong finally
    try:
        spark_builder = SparkSession.builder.appName("MinIO Raw to Processed ETL")

        # Cấu hình S3A Connector cho MinIO
        spark_builder.config("spark.hadoop.fs.s3a.endpoint", minio_endpoint_url)
        spark_builder.config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        spark_builder.config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        spark_builder.config("spark.hadoop.fs.s3a.path.style.access", "true")
        spark_builder.config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

        # Cấu hình Output Committer
        spark_builder.config("spark.sql.parquet.output.committer.class", "org.apache.hadoop.mapreduce.lib.output.DirectFileOutputCommitter")
        spark_builder.config("spark.sql.sources.commitProtocolClass", "org.apache.spark.internal.io.cloud.PathOutputCommitProtocol")
        spark_builder.config("spark.hadoop.mapreduce.outputcommitter.factory.scheme.s3a", "org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory")

        # Cấu hình JAR Packages (!! Quan trọng - phiên bản phải khớp !!)
        jar_packages = [
            f"org.apache.hadoop:hadoop-aws:{HADOOP_AWS_VERSION}",
            f"com.amazonaws:aws-java-sdk-bundle:{AWS_JAVA_SDK_VERSION}"
        ]
        spark_builder.config("spark.jars.packages", ",".join(jar_packages))

        # Khởi tạo SparkSession
        spark = spark_builder.getOrCreate()
        print("SparkSession created successfully.")
        print(f"Using JAR packages: {','.join(jar_packages)}")

        # ---- Xử lý dữ liệu ----
        input_path = f"s3a://{RAW_BUCKET}/"
        output_path = f"s3a://{PROCESSED_BUCKET}/{PROCESSED_OUTPUT_DIR}/" # Sử dụng biến thư mục output

        print(f"Reading JSON data from: {input_path}")

        # Đọc dữ liệu JSON từ Raw bucket
        df = spark.read.json(input_path)

        print("Schema inferred by Spark:")
        df.printSchema()

        # Kiểm tra xem DataFrame có dữ liệu không
        if df.rdd.isEmpty():
             print(f"No data found in {input_path}. Exiting.")
             return # Thoát sớm nếu không có dữ liệu

        print(f"Initial record count: {df.count()}")

        # ---- Logic xử lý ví dụ ----
        # Ép kiểu dữ liệu để đảm bảo tính nhất quán
        df_typed = df \
            .withColumn("amount", F.col("amount").cast(DoubleType())) \
            .withColumn("timestamp", F.to_timestamp(F.col("timestamp"))) \
            .withColumn("is_fraud", F.col("is_fraud").cast(BooleanType())) \
            .withColumn("transaction_id", F.col("transaction_id").cast(StringType())) # Đảm bảo ID là string
            # Thêm các cột khác nếu cần ép kiểu

        # Thêm cột mới
        df_processed = df_typed.withColumn("amount_doubled", F.col("amount") * 2)

        # Thêm cột thời gian xử lý
        df_processed = df_processed.withColumn("processed_at", F.current_timestamp())

        # Bỏ lọc để giữ lại tất cả cho ví dụ này
        df_final = df_processed

        print(f"Record count after processing: {df_final.count()}")
        print("Schema after processing:")
        df_final.printSchema()
        print("Sample data after processing:")
        df_final.show(5, truncate=False)

        # ---- Ghi dữ liệu đã xử lý ----
        print(f"Writing processed data to Parquet format at: {output_path}")

        # Ghi ra định dạng Parquet vào Processed Bucket
        df_final.write.mode("overwrite").parquet(output_path)

        print("Successfully processed data and wrote to processed bucket.")

    except Exception as e:
        print(f"An error occurred during PySpark processing: {e}")
        import traceback
        traceback.print_exc()

    finally:
        if spark: # Chỉ gọi stop nếu spark đã được khởi tạo thành công
            print("Stopping SparkSession.")
            spark.stop()

if __name__ == "__main__":
    main()