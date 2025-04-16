# /opt/airflow/dags/scripts/load_processed_to_sandbox.py

import os
from datetime import datetime
from minio import Minio
from minio.error import S3Error
from minio.commonconfig import CopySource
from dotenv import load_dotenv

# Tải biến môi trường
load_dotenv()

# --- Cấu hình MinIO ---
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY')
PROCESSED_BUCKET = os.getenv('MINIO_PROCESSED_BUCKET', 'processed')
SANDBOX_BUCKET = os.getenv('MINIO_SANDBOX_BUCKET', 'sandbox')
USE_SSL = MINIO_ENDPOINT.startswith('https') if MINIO_ENDPOINT else False

# --- Thư mục chứa dữ liệu Parquet đã xử lý (phải khớp với script PySpark) ---
PROCESSED_DATA_DIR = "transactions_processed_parquet" # Thư mục chứa Parquet trong bucket processed
# --- Thư mục đích trong sandbox (có thể giống hoặc khác) ---
SANDBOX_DEST_DIR = "transactions_processed_parquet"

def ensure_bucket_exists(client, bucket_name):
    """Kiểm tra và tạo bucket nếu chưa tồn tại"""
    if not bucket_name:
        print("Error: Bucket name is not configured.")
        return False
    try:
        found = client.bucket_exists(bucket_name)
        if not found:
            client.make_bucket(bucket_name)
            print(f"Bucket '{bucket_name}' created.")
        else:
            print(f"Bucket '{bucket_name}' already exists.")
        return True
    except S3Error as e:
        print(f"Error checking or creating bucket {bucket_name}: {e}")
        return False
    except Exception as e:
        print(f"An unexpected error occurred with bucket {bucket_name}: {e}")
        return False


def copy_directory_minio(client, source_bucket, source_dir, dest_bucket, dest_dir):
    """Sao chép tất cả object từ thư mục nguồn sang thư mục đích giữa các bucket"""
    copied_count = 0
    error_count = 0
    source_dir_prefix = source_dir if source_dir.endswith('/') else source_dir + '/'
    dest_dir_prefix = dest_dir if dest_dir.endswith('/') else dest_dir + '/'

    try:
        print(f"Listing objects in source '{source_bucket}/{source_dir_prefix}'")
        objects_to_copy = client.list_objects(source_bucket, prefix=source_dir_prefix, recursive=True)

        for obj in objects_to_copy:
            if obj.object_name: # Chỉ xử lý các object thực sự, bỏ qua "thư mục" trống
                source_object_path = obj.object_name
                # Tạo đường dẫn đích bằng cách thay thế prefix nguồn bằng prefix đích
                relative_path = source_object_path[len(source_dir_prefix):]
                dest_object_path = dest_dir_prefix + relative_path

                print(f"Copying '{source_bucket}/{source_object_path}' to '{dest_bucket}/{dest_object_path}'")
                try:
                    copy_source = CopySource(source_bucket, source_object_path)
                    client.copy_object(
                        dest_bucket,
                        dest_object_path,
                        copy_source
                    )
                    copied_count += 1
                except S3Error as e:
                    print(f"ERROR copying '{source_object_path}': {e}")
                    error_count += 1
                except Exception as e:
                    print(f"UNEXPECTED ERROR copying '{source_object_path}': {e}")
                    error_count += 1

        print(f"Directory copy finished. Copied: {copied_count}, Errors: {error_count}")
        return error_count == 0 # Trả về True nếu không có lỗi
    except S3Error as e:
        print(f"Error listing objects in source directory '{source_bucket}/{source_dir_prefix}': {e}")
        return False
    except Exception as e:
        print(f"An unexpected error occurred during directory copy: {e}")
        return False


def main():
    print("Starting script: load_processed_to_sandbox.py (copying Parquet directory)")
    if not all([MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, PROCESSED_BUCKET, SANDBOX_BUCKET]):
        print("Error: Missing MinIO configuration in environment variables.")
        return

    minio_client = None
    try:
        # Khởi tạo MinIO client
        minio_client = Minio(
            MINIO_ENDPOINT.replace('https://','').replace('http://',''),
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=USE_SSL
        )
        print(f"MinIO client initialized for endpoint: {MINIO_ENDPOINT}")

        # Đảm bảo các bucket tồn tại
        if not ensure_bucket_exists(minio_client, PROCESSED_BUCKET):
             print(f"Processed bucket '{PROCESSED_BUCKET}' check/creation failed. Exiting.")
             return
        if not ensure_bucket_exists(minio_client, SANDBOX_BUCKET):
             print(f"Sandbox bucket '{SANDBOX_BUCKET}' check/creation failed. Exiting.")
             return

        # Thực hiện sao chép thư mục
        print(f"Attempting to copy directory '{PROCESSED_DATA_DIR}' from '{PROCESSED_BUCKET}' to '{SANDBOX_DEST_DIR}' in '{SANDBOX_BUCKET}'")
        success = copy_directory_minio(
            minio_client,
            PROCESSED_BUCKET,
            PROCESSED_DATA_DIR,
            SANDBOX_BUCKET,
            SANDBOX_DEST_DIR
        )

        if success:
            print("Successfully copied processed data directory to sandbox bucket.")
        else:
            print("Copy operation encountered errors.")
            # Bạn có thể quyết định dừng pipeline ở đây nếu cần
            # raise Exception("Failed to copy processed data to sandbox")

        # (Optional) Logic xóa thư mục nguồn sau khi copy thành công
        # Cần cẩn thận khi xóa tự động!
        # if success:
        #     try:
        #         print(f"Attempting to remove source directory '{PROCESSED_BUCKET}/{PROCESSED_DATA_DIR}'")
        #         objects_to_delete = minio_client.list_objects(PROCESSED_BUCKET, prefix=PROCESSED_DATA_DIR + '/', recursive=True)
        #         delete_object_list = [obj.object_name for obj in objects_to_delete if obj.object_name]
        #         if delete_object_list:
        #             for del_err in minio_client.remove_objects(PROCESSED_BUCKET, delete_object_list):
        #                 print(f"Deletion Error: {del_err}")
        #         print(f"Finished attempting to remove source directory.")
        #     except S3Error as e:
        #         print(f"Error during source directory removal: {e}")


    except S3Error as e:
        print(f"MinIO S3 Error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()