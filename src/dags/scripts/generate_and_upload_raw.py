# /opt/airflow/dags/scripts/generate_and_upload_raw.py

import os
import sys
from minio import Minio
from minio.error import S3Error
from minio.commonconfig import CopySource
from dotenv import load_dotenv
import traceback # Import traceback để sử dụng khi cần

# Tải biến môi trường
load_dotenv()

# --- Cấu hình MinIO ---
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY')
# Bucket nguồn chứa JSON từ Kafka Consumer
SOURCE_BUCKET = os.getenv('MINIO_SOURCE_BUCKET', 'raw') # Đặt tên bucket nguồn ở đây
# Bucket đích cho bước đầu tiên của pipeline Airflow
RAW_BUCKET = os.getenv('MINIO_RAW_BUCKET', 'raw-data')
USE_SSL = MINIO_ENDPOINT.startswith('https') if MINIO_ENDPOINT else False

# Kiểm tra cấu hình thiết yếu
if not all([MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, SOURCE_BUCKET, RAW_BUCKET]):
    print("Lỗi: Thiếu cấu hình MinIO cần thiết trong biến môi trường.")
    print("Yêu cầu: MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_SOURCE_BUCKET, MINIO_RAW_BUCKET")
    sys.exit(1) # Thoát với mã lỗi để Airflow biết tác vụ thất bại

# Xác định có sử dụng SSL dựa trên endpoint không
USE_SSL = MINIO_ENDPOINT.startswith('https')

def ensure_bucket_exists(client, bucket_name):
    """
    Kiểm tra xem bucket có tồn tại không, nếu không thì tạo mới.
    Thoát khỏi script với mã lỗi nếu có vấn đề xảy ra.
    """
    if not bucket_name:
        print(f"Lỗi: Tên bucket không được cung cấp hoặc rỗng.")
        sys.exit(1) # Thoát với lỗi
    try:
        found = client.bucket_exists(bucket_name)
        if not found:
            client.make_bucket(bucket_name)
            print(f"Bucket '{bucket_name}' đã được tạo.")
        else:
            print(f"Bucket '{bucket_name}' đã tồn tại.")
    except S3Error as e:
        print(f"Lỗi S3 khi kiểm tra/tạo bucket '{bucket_name}': Mã lỗi {e.code}, Thông điệp: {e.message}")
        sys.exit(1) # Thoát với lỗi
    except Exception as e:
        print(f"Lỗi không mong muốn khi kiểm tra/tạo bucket '{bucket_name}': {e}")
        traceback.print_exc()
        sys.exit(1) # Thoát với lỗi

def main():
    """
    Liệt kê các tệp JSON trong SOURCE_BUCKET dưới tiền tố 'transactions/year=2025/'
    cho các tháng 01, 02, 03, 04 và sao chép CHỈ CÁC TỆP JSON đó
    vào thư mục gốc của RAW_BUCKET.
    """
    print(f"Bắt đầu script: Sao chép các tệp JSON liên quan từ '{SOURCE_BUCKET}' sang '{RAW_BUCKET}' (chỉ lấy tệp)")

    minio_client = None
    try:
        # Khởi tạo MinIO client
        endpoint_cleaned = MINIO_ENDPOINT.replace('https://','').replace('http://','')
        minio_client = Minio(
            endpoint_cleaned,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=USE_SSL
        )
        print(f"MinIO client đã được khởi tạo cho endpoint: {MINIO_ENDPOINT}")

        # Đảm bảo các bucket tồn tại
        ensure_bucket_exists(minio_client, SOURCE_BUCKET)
        ensure_bucket_exists(minio_client, RAW_BUCKET)

        copied_count = 0
        error_count = 0
        target_year = "2025" # Có thể lấy từ biến môi trường hoặc tham số Airflow nếu cần
        source_base_prefix = f"transactions/year={target_year}/"
        target_months = {"01", "02", "03", "04"} # Tháng 1 đến tháng 4

        print(f"Liệt kê các đối tượng trong '{SOURCE_BUCKET}' với tiền tố '{source_base_prefix}'...")
        objects_generator = minio_client.list_objects(SOURCE_BUCKET, prefix=source_base_prefix, recursive=True)

        # (Tùy chọn) Sử dụng set để kiểm tra tên tệp trùng lặp nếu cần
        # copied_filenames = set()

        for obj in objects_generator:
            # Bỏ qua các thư mục ảo và các tệp không phải JSON
            if obj.is_dir or not obj.object_name or not obj.object_name.lower().endswith(".json"):
                continue

            source_object_name = obj.object_name # Giữ lại tên đầy đủ của nguồn để copy
            dest_object_name = None # Khởi tạo để dùng trong log lỗi

            try:
                # Phân tích đường dẫn để lấy thông tin tháng
                parts = source_object_name.split('/')
                month_part = next((part for part in parts if part.startswith("month=")), None)

                if month_part:
                    month = month_part.split('=')[1]
                    # Kiểm tra xem tháng này có nằm trong danh sách mục tiêu không
                    if month in target_months:
                        print(f"  Tìm thấy tệp hợp lệ: {source_object_name}")

                        # --- THAY ĐỔI CHÍNH ---
                        # Lấy tên tệp (phần cuối cùng của đường dẫn) làm tên đích
                        # Ví dụ: 'transactions/year=2025/month=01/day=01/file.json' -> 'file.json'
                        dest_object_name = source_object_name.split('/')[-1]

                        # CẢNH BÁO: Nếu có các tệp cùng tên trong các thư mục nguồn khác nhau
                        # (ví dụ: .../month=01/day=01/data.json và .../month=01/day=02/data.json)
                        # thì việc sao chép vào thư mục gốc của RAW_BUCKET sẽ khiến tệp sau ghi đè tệp trước.
                        # Nếu cần xử lý trường hợp này, bạn có thể thêm logic kiểm tra hoặc đổi tên tệp đích.
                        # Ví dụ kiểm tra trùng lặp (bỏ comment nếu muốn sử dụng):
                        # if dest_object_name in copied_filenames:
                        #     print(f"    CẢNH BÁO: Tên tệp '{dest_object_name}' đã tồn tại trong bucket đích từ đường dẫn khác. Bỏ qua sao chép.")
                        #     continue # Bỏ qua tệp này
                        # else:
                        #     copied_filenames.add(dest_object_name)

                        print(f"    Tên tệp đích sẽ là: {dest_object_name}")

                        # Thực hiện sao chép hiệu quả bằng copy_object
                        copy_source = CopySource(SOURCE_BUCKET, source_object_name)
                        minio_client.copy_object(
                            RAW_BUCKET,          # Bucket đích
                            dest_object_name,    # Tên đối tượng đích (CHỈ TÊN TỆP)
                            copy_source          # Nguồn sao chép
                        )
                        print(f"    Đã sao chép tới: {RAW_BUCKET}/{dest_object_name}")
                        copied_count += 1
                else:
                    # Cảnh báo nếu không thể xác định tháng từ đường dẫn
                    print(f"  CẢNH BÁO: Không thể xác định tháng cho đối tượng '{source_object_name}'. Bỏ qua.")

            except StopIteration:
                 print(f"  CẢNH BÁO: Không thể phân tích đường dẫn cho '{source_object_name}'. Bỏ qua.")
            except S3Error as s3_err:
                dest_info = f"thành '{dest_object_name}'" if dest_object_name else ""
                print(f"  LỖI S3 khi sao chép '{source_object_name}' {dest_info}: {s3_err}")
                error_count += 1
            except Exception as proc_err:
                print(f"  LỖI KHÔNG MONG MUỐN khi xử lý '{source_object_name}': {proc_err}")
                traceback.print_exc() # In chi tiết lỗi để gỡ lỗi
                error_count += 1

        # --- Kết thúc vòng lặp xử lý ---

        print(f"\nQuá trình xử lý hoàn tất.")
        print(f"Số tệp JSON đã sao chép thành công vào gốc bucket '{RAW_BUCKET}': {copied_count}")
        print(f"Số lỗi gặp phải khi sao chép: {error_count}")

        if error_count > 0:
             print("Script kết thúc với lỗi do không thể sao chép một số tệp.")
             sys.exit(1) # Báo lỗi cho Airflow

        print("Script hoàn thành thành công.")

    except S3Error as e:
        print(f"Lỗi S3 trong quá trình thực thi chính: {e}")
        sys.exit(1) # Thoát với lỗi
    except Exception as e:
        print(f"Lỗi không mong muốn trong quá trình thực thi chính: {e}")
        traceback.print_exc()
        sys.exit(1) # Thoát với lỗi

# Chỉ chạy hàm main nếu script được thực thi trực tiếp
if __name__ == "__main__":
    main()