import json
import io
import os # Thêm thư viện os
from dotenv import load_dotenv # Thêm thư viện dotenv
from minio import Minio
from minio.error import S3Error
import sys # Thêm thư viện sys để thoát nếu thiếu config

load_dotenv() # Tải biến môi trường từ file .env


def merge_json_files_in_minio(
    minio_endpoint,
    access_key,
    secret_key,
    source_bucket_name,
    dest_bucket_name,
    merged_file_name,
    secure=True
):
    """
    Kết nối tới MinIO, đọc các file JSON từ source_bucket,
    gộp nội dung và lưu vào một file lớn trong dest_bucket.
    (Hàm giữ nguyên như trước)
    """

    # --- 1. Khởi tạo client MinIO ---
    try:
        client = Minio(
            minio_endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure
        )
        print(f"Kết nối tới MinIO endpoint '{minio_endpoint}' thành công.")
    except Exception as e:
        print(f"Lỗi: Không thể kết nối tới MinIO. Chi tiết: {e}")
        return

    # --- 2. Kiểm tra sự tồn tại của các bucket ---
    try:
        found_source = client.bucket_exists(source_bucket_name)
        found_dest = client.bucket_exists(dest_bucket_name)
        if not found_source:
            print(f"Lỗi: Bucket nguồn '{source_bucket_name}' không tồn tại.")
            return
        if not found_dest:
            print(f"Lỗi: Bucket đích '{dest_bucket_name}' không tồn tại.")
            return
        print(f"Bucket nguồn '{source_bucket_name}' và đích '{dest_bucket_name}' hợp lệ.")
    except S3Error as e:
        print(f"Lỗi khi kiểm tra bucket: {e}")
        return

    # --- 3. Lấy danh sách các file JSON trong bucket nguồn ---
 # --- 3. Lấy danh sách các file JSON trong thư mục 'transactions' của bucket nguồn ---
    json_files = []
    target_prefix = "transactions/" # Đảm bảo có dấu / ở cuối nếu đó là thư mục
    try:
        # Cập nhật thông báo cho rõ ràng hơn
        print(f"Đang tìm các file .json trong thư mục '{target_prefix}' của bucket '{source_bucket_name}'...")
        # Thêm tham số prefix vào hàm list_objects
        objects = client.list_objects(
            source_bucket_name,
            prefix=target_prefix,  # Chỉ lấy các đối tượng bắt đầu bằng prefix này
            recursive=True       # Vẫn lấy các file trong thư mục con (nếu có) của 'transactions'
        )
        for obj in objects:
            # Kiểm tra xem đối tượng có kết thúc bằng .json không
            # Không cần kiểm tra startswith nữa vì đã lọc bằng prefix
            if obj.object_name.lower().endswith('.json'):
                json_files.append(obj.object_name)

        # Cập nhật thông báo kết quả
        print(f"Tìm thấy {len(json_files)} file JSON trong thư mục '{target_prefix}'.")
        if not json_files:
            print(f"Không tìm thấy file JSON nào trong thư mục '{target_prefix}' để gộp.")
            return
    except S3Error as e:
        print(f"Lỗi khi liệt kê các đối tượng trong thư mục '{target_prefix}': {e}")
        return

    # --- 4. Đọc, phân tích cú pháp và gộp dữ liệu JSON ---
    merged_data = []
    print("Bắt đầu đọc và gộp dữ liệu...")
    for file_name in json_files:
        response = None # Khởi tạo response để tránh lỗi ở finally
        try:
            print(f"  Đang xử lý file: {file_name}")
            response = client.get_object(source_bucket_name, file_name)
            file_content_bytes = response.read()
            file_content_str = file_content_bytes.decode('utf-8')
            data = json.loads(file_content_str)
            merged_data.append(data) # Hoặc merged_data.extend(data) tùy logic gộp
        except json.JSONDecodeError:
            print(f"  Cảnh báo: File '{file_name}' không phải là JSON hợp lệ hoặc rỗng. Bỏ qua.")
        except S3Error as e:
            print(f"  Lỗi khi tải file '{file_name}': {e}. Bỏ qua.")
        except Exception as e:
            print(f"  Lỗi không xác định khi xử lý file '{file_name}': {e}. Bỏ qua.")
        finally:
            if response:
                response.close()
                response.release_conn()

    if not merged_data:
        print("Không có dữ liệu hợp lệ nào được gộp.")
        return

    # --- 5. Chuyển đổi dữ liệu đã gộp thành JSON string và bytes ---
    try:
        merged_json_str = json.dumps(merged_data, indent=4, ensure_ascii=False)
        merged_json_bytes = merged_json_str.encode('utf-8')
        merged_json_stream = io.BytesIO(merged_json_bytes)
        merged_data_size = len(merged_json_bytes)
        print(f"Đã gộp dữ liệu thành công. Kích thước: {merged_data_size} bytes.")
    except Exception as e:
        print(f"Lỗi khi chuyển đổi dữ liệu gộp thành JSON: {e}")
        return

    # --- 6. Tải file JSON đã gộp lên bucket đích ---
    try:
        print(f"Đang tải file '{merged_file_name}' lên bucket '{dest_bucket_name}'...")
        client.put_object(
            dest_bucket_name,
            merged_file_name,
            merged_json_stream,
            merged_data_size,
            content_type='application/json'
        )
        print(f"Hoàn thành! File '{merged_file_name}' đã được lưu vào bucket '{dest_bucket_name}'.")
    except S3Error as e:
        print(f"Lỗi khi tải file đã gộp lên MinIO: {e}")
    except Exception as e:
        print(f"Lỗi không xác định khi tải file: {e}")


# --- Hàm chính để chạy script ---
if __name__ == "__main__":
    # --- Tải biến môi trường từ file .env ---
    print("Đang tải cấu hình từ file .env...")
    load_dotenv()

    # --- Lấy cấu hình từ biến môi trường ---
    MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "127.0.0.1:9000")
    MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
    MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
    SOURCE_BUCKET = os.getenv("SOURCE_BUCKET", "test")
    DEST_BUCKET = os.getenv("DEST_BUCKET", "processed")
    MERGED_FILENAME = os.getenv("MERGED_FILENAME", "data.json") # Mặc định có thể đặt trong .env
    MINIO_SECURE_STR = os.getenv("MINIO_SECURE", "False") # Mặc định là "True" nếu không có trong .env

    # --- Kiểm tra xem các biến môi trường cần thiết đã được đặt chưa ---
    required_vars = {
        "MINIO_ENDPOINT": MINIO_ENDPOINT,
        "MINIO_ACCESS_KEY": MINIO_ACCESS_KEY,
        "MINIO_SECRET_KEY": MINIO_SECRET_KEY,
        "SOURCE_BUCKET": SOURCE_BUCKET,
        "DEST_BUCKET": DEST_BUCKET,
        "MERGED_FILENAME": MERGED_FILENAME,
    }
    missing_vars = [k for k, v in required_vars.items() if not v] # Kiểm tra giá trị không rỗng

    if missing_vars:
        print("\nLỗi: Các biến môi trường sau chưa được đặt hoặc rỗng trong file .env:")
        for var in missing_vars:
            print(f" - {var}")
        print("\nVui lòng kiểm tra lại file .env của bạn.")
        sys.exit(1) # Thoát script nếu thiếu cấu hình

    # Chuyển đổi giá trị MINIO_SECURE từ string sang boolean
    # Các giá trị như 'true', '1', 't', 'yes' (không phân biệt hoa thường) sẽ là True
    USE_SECURE = MINIO_SECURE_STR.lower() in ('true', '1', 't', 'yes', 'y')

    print("Cấu hình đã được tải thành công.")

    # --- Gọi hàm thực thi với cấu hình đã tải ---
    merge_json_files_in_minio(
        MINIO_ENDPOINT,
        MINIO_ACCESS_KEY,
        MINIO_SECRET_KEY,
        SOURCE_BUCKET,
        DEST_BUCKET,
        MERGED_FILENAME,
        secure=USE_SECURE
    )