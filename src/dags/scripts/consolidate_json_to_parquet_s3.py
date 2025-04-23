# /opt/airflow/dags/scripts/consolidate_json_to_parquet_s3.py

import os
import sys
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv
import traceback
import pandas as pd
import io
import json # Để xử lý lỗi JSON nếu cần

# Tải biến môi trường
load_dotenv()

# --- Cấu hình AWS S3 ---
AWS_REGION = os.getenv('AWS_REGION')
# Bucket chứa cả file JSON nguồn và là nơi lưu file Parquet đích
BUCKET_NAME = os.getenv('S3_BUCKET_NAME', 'rawfrauddectect') # Đổi tên biến cho rõ ràng
# Tiền tố (thư mục) chứa các file JSON nguồn
SOURCE_PREFIX = os.getenv('SOURCE_JSON_PREFIX', 'transactions/')
# Key (đường dẫn + tên file) đầy đủ cho file Parquet đích
TARGET_PARQUET_KEY = os.getenv('TARGET_PARQUET_KEY', 'raw/consolidated_transactions.parquet')

# Kiểm tra cấu hình thiết yếu
if not all([AWS_REGION, BUCKET_NAME]):
    print("Lỗi: Thiếu cấu hình AWS S3 cần thiết trong biến môi trường.")
    print("Yêu cầu: AWS_REGION, S3_BUCKET_NAME")
    print("Lưu ý: Thông tin xác thực AWS (keys) nên được quản lý thông qua IAM Roles, biến môi trường AWS chuẩn, hoặc tệp credentials.")
    sys.exit(1) # Thoát với mã lỗi

# Đảm bảo SOURCE_PREFIX kết thúc bằng '/' nếu nó không rỗng
if SOURCE_PREFIX and not SOURCE_PREFIX.endswith('/'):
    SOURCE_PREFIX += '/'
    print(f"Tự động thêm '/' vào SOURCE_PREFIX: {SOURCE_PREFIX}")

def ensure_bucket_exists(s3_client, bucket_name, region):
    """
    Kiểm tra xem bucket S3 có tồn tại không.
    Thoát khỏi script với mã lỗi nếu có vấn đề xảy ra.
    Không tạo bucket trong script này vì mục đích là xử lý bucket đã có.
    """
    if not bucket_name:
        print(f"Lỗi: Tên bucket không được cung cấp hoặc rỗng.")
        sys.exit(1)
    try:
        print(f"Kiểm tra sự tồn tại của bucket '{bucket_name}'...")
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"Bucket '{bucket_name}' đã tồn tại và có thể truy cập.")
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code")
        if error_code == '404' or error_code == 'NoSuchBucket':
            print(f"Lỗi: Bucket '{bucket_name}' không tồn tại.")
        elif error_code == '403':
            print(f"Lỗi: Không có quyền truy cập bucket '{bucket_name}'. Kiểm tra lại quyền IAM.")
        else:
            print(f"Lỗi ClientError không xác định khi kiểm tra bucket '{bucket_name}': {e}")
        sys.exit(1) # Thoát nếu không truy cập được bucket
    except Exception as e:
        print(f"Lỗi không mong muốn khi kiểm tra bucket '{bucket_name}': {e}")
        traceback.print_exc()
        sys.exit(1)

def main():
    """
    Liệt kê các tệp JSON trong SOURCE_PREFIX của BUCKET_NAME,
    đọc và gộp chúng thành một DataFrame pandas,
    sau đó ghi DataFrame đó thành file Parquet tại TARGET_PARQUET_KEY trong cùng bucket.
    """
    print("--- Bắt đầu Script Gộp JSON thành Parquet ---")
    print(f"Bucket S3: {BUCKET_NAME}")
    print(f"Region: {AWS_REGION}")
    print(f"Thư mục JSON nguồn (Prefix): {SOURCE_PREFIX}")
    print(f"File Parquet đích (Key): {TARGET_PARQUET_KEY}")

    s3_client = None
    all_dataframes = [] # List để chứa các DataFrame từ mỗi file JSON
    processed_files_count = 0
    error_files_count = 0

    try:
        # Khởi tạo S3 client
        s3_client = boto3.client('s3', region_name=AWS_REGION)
        print("S3 client đã được khởi tạo.")

        # Đảm bảo bucket tồn tại và có thể truy cập
        ensure_bucket_exists(s3_client, BUCKET_NAME, AWS_REGION)

        # --- Bước 1: Liệt kê và đọc các file JSON ---
        print(f"\nĐang liệt kê và đọc các tệp .json từ s3://{BUCKET_NAME}/{SOURCE_PREFIX}...")
        paginator = s3_client.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(Bucket=BUCKET_NAME, Prefix=SOURCE_PREFIX)

        for page in page_iterator:
            if "Contents" in page:
                for obj in page['Contents']:
                    source_object_key = obj['Key']

                    # Chỉ xử lý các file .json, bỏ qua thư mục và các file khác
                    if not source_object_key.lower().endswith(".json") or source_object_key.endswith('/'):
                        continue

                    print(f"  Đang xử lý file: {source_object_key}")
                    try:
                        # Tải nội dung file JSON từ S3
                        response = s3_client.get_object(Bucket=BUCKET_NAME, Key=source_object_key)
                        json_data = response['Body'].read()

                        # Đọc JSON vào DataFrame
                        # Giả định: Mỗi dòng trong file JSON là một bản ghi JSON hợp lệ (JSON Lines format)
                        # Nếu file JSON chứa một danh sách các object [{}, {}], dùng pd.read_json(io.BytesIO(json_data))
                        try:
                            # Thử đọc dạng JSON Lines trước
                            df_single = pd.read_json(io.BytesIO(json_data), lines=True)
                        except ValueError:
                            # Nếu lỗi, thử đọc như một JSON array/object chuẩn
                            print(f"    Cảnh báo: Không đọc được {source_object_key} dạng JSON Lines, thử đọc dạng chuẩn.")
                            try:
                                # Cần đưa buffer về đầu trước khi đọc lại
                                json_io = io.BytesIO(json_data)
                                json_io.seek(0)
                                df_single = pd.read_json(json_io)
                                # Nếu JSON gốc là một object đơn lẻ, cần chuyển thành list chứa object đó
                                if isinstance(df_single, pd.Series):
                                    df_single = pd.DataFrame([df_single])
                            except Exception as e_std:
                                print(f"    Lỗi khi đọc {source_object_key} dạng chuẩn: {e_std}. Bỏ qua file.")
                                raise # Ném lại lỗi để bắt ở khối ngoài và tăng error_count

                        if not df_single.empty:
                            all_dataframes.append(df_single)
                            processed_files_count += 1
                        else:
                            print(f"    Cảnh báo: File {source_object_key} đọc ra DataFrame rỗng.")

                    except ClientError as s3_err:
                        print(f"  Lỗi S3 khi đọc file '{source_object_key}': {s3_err}")
                        error_files_count += 1
                    except Exception as read_err: # Bắt lỗi khi đọc/parse JSON
                        print(f"  Lỗi khi đọc hoặc parse JSON từ file '{source_object_key}': {read_err}")
                        error_files_count += 1
            # else:
                # print("Trang không chứa đối tượng nào.")

        print(f"\nĐã xử lý {processed_files_count} file JSON.")
        if error_files_count > 0:
            print(f"Gặp lỗi khi đọc {error_files_count} file.")

        # --- Bước 2: Gộp các DataFrame ---
        if not all_dataframes:
            print("\nKhông có dữ liệu JSON nào được đọc thành công. Không tạo file Parquet.")
            # Quyết định xem đây là lỗi hay thành công khi không có file nguồn
            if error_files_count > 0:
                 print("Script kết thúc với lỗi do không đọc được file.")
                 sys.exit(1)
            else:
                 print("Script hoàn thành nhưng không có file JSON nguồn để xử lý.")
                 sys.exit(0) # Kết thúc thành công vì không có gì để làm


        print("\nĐang gộp dữ liệu từ các file JSON...")
        try:
            # Cảnh báo về bộ nhớ nếu quá nhiều dataframes
            if len(all_dataframes) > 1000: # Ngưỡng tùy chỉnh
                 print(f"Cảnh báo: Gộp {len(all_dataframes)} DataFrames, có thể tốn nhiều bộ nhớ.")

            combined_df = pd.concat(all_dataframes, ignore_index=True)
            print(f"Gộp dữ liệu thành công. Tổng số dòng: {len(combined_df)}")
            if combined_df.empty:
                print("DataFrame sau khi gộp bị rỗng. Không tạo file Parquet.")
                sys.exit(0) # Kết thúc thành công vì không có dữ liệu
        except Exception as concat_err:
             print(f"Lỗi nghiêm trọng khi gộp DataFrames: {concat_err}")
             traceback.print_exc()
             sys.exit(1)


        # --- Bước 3: Chuyển đổi sang Parquet và Upload ---
        print(f"\nĐang chuyển đổi sang Parquet và tải lên s3://{BUCKET_NAME}/{TARGET_PARQUET_KEY}...")
        try:
            parquet_buffer = io.BytesIO()
            # Ghi DataFrame vào buffer dưới dạng Parquet, sử dụng engine pyarrow
            combined_df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
            parquet_buffer.seek(0) # Đưa con trỏ về đầu buffer để S3 đọc

            # Tải buffer lên S3
            s3_client.put_object(Bucket=BUCKET_NAME, Key=TARGET_PARQUET_KEY, Body=parquet_buffer)
            print("Tải file Parquet lên S3 thành công!")

        except ImportError:
             print("Lỗi: Thư viện 'pyarrow' chưa được cài đặt. Chạy 'pip install pyarrow'")
             sys.exit(1)
        except ClientError as upload_err:
            print(f"Lỗi S3 khi tải file Parquet lên '{TARGET_PARQUET_KEY}': {upload_err}")
            sys.exit(1)
        except Exception as parquet_err:
            print(f"Lỗi khi tạo hoặc tải file Parquet: {parquet_err}")
            traceback.print_exc()
            sys.exit(1)

        print("\n--- Script Gộp JSON thành Parquet hoàn tất thành công ---")

    except ClientError as e:
        # Lỗi chung từ Boto3 (ví dụ: không kết nối được, sai credentials)
        print(f"\nLỗi ClientError trong quá trình thực thi chính: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\nLỗi không mong muốn trong quá trình thực thi chính: {e}")
        traceback.print_exc()
        sys.exit(1)

# Chỉ chạy hàm main nếu script được thực thi trực tiếp
if __name__ == "__main__":
    main()