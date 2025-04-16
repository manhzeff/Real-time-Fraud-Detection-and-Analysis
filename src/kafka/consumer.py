import logging
# Thay đổi import: Sử dụng Consumer từ confluent_kafka
from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition, OFFSET_INVALID # Thêm Consumer, KafkaError, KafkaException, OFFSET_INVALID
import json
import sys
import os
import time
import certifi
from dotenv import load_dotenv
from datetime import datetime, timezone
import io

# --- Import clients (Không đổi) ---
from elasticsearch import Elasticsearch
from minio import Minio
from minio.error import S3Error

# --- Tải biến môi trường (Không đổi) ---
load_dotenv()

# --- Cấu hình Kafka / Confluent Cloud (Không đổi tên biến, chỉ dùng trong cấu hình Consumer) ---
KAFKA_BROKER = os.getenv('CONFLUENT_BOOTSTRAP_SERVERS')
KAFKA_API_KEY = os.getenv('CONFLUENT_API_KEY')
KAFKA_API_SECRET = os.getenv('CONFLUENT_API_SECRET')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'transactions')
KAFKA_CONSUMER_GROUP = os.getenv('KAFKA_CONSUMER_GROUP', 'transaction-processor-group-final')

# --- Cấu hình Elasticsearch (Không đổi) ---
ES_CLOUD_ID = os.getenv('ELASTICSEARCH_CLOUD_ID')
ES_API_KEY = os.getenv('ELASTICSEARCH_API_KEY')
ES_USER = os.getenv('ELASTICSEARCH_USER')
ES_PASSWORD = os.getenv('ELASTICSEARCH_PASSWORD')
ES_INDEX = os.getenv('ELASTICSEARCH_INDEX', 'kafka-invalid-transactions-log-v1')

# --- Cấu hình MinIO (Không đổi) ---
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'localhost:9000')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY')
MINIO_BUCKET = os.getenv('MINIO_BUCKET', 'kafka-valid-transactions')
MINIO_USE_SSL = os.getenv('MINIO_USE_SSL', 'False').lower() == 'true'

# --- Kiểm tra cấu hình cần thiết (Không đổi) ---
missing_configs = []
if not all([KAFKA_BROKER, KAFKA_API_KEY, KAFKA_API_SECRET]): missing_configs.append("Kafka/Confluent Cloud")
if not ES_CLOUD_ID or not (ES_API_KEY or (ES_USER and ES_PASSWORD)): missing_configs.append("Elasticsearch Cloud (Cloud ID và API Key/User+Pass)")
if not all([MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_BUCKET]): missing_configs.append("MinIO")

if missing_configs:
    print(f"Lỗi: Thiếu cấu hình cho: {', '.join(missing_configs)}.")
    print("Vui lòng kiểm tra file .env hoặc các biến môi trường.")
    sys.exit(1)

# --- Thiết lập Logging (Không đổi) ---
LOG_FILE = 'consumer_processor_confluent.log' # Đổi tên file log
logging.basicConfig(level=logging.INFO,
                    filename=LOG_FILE,
                    filemode='w',
                    format='%(asctime)s %(levelname)-8s [%(name)-12s] %(message)s')
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s %(levelname)-8s [%(name)-12s] %(message)s')
console_handler.setFormatter(formatter)

main_logger = logging.getLogger('main')
es_logger = logging.getLogger('elasticsearch')
minio_logger = logging.getLogger('minio')
kafka_logger = logging.getLogger('kafka.consumer') # Đổi tên logger kafka
validation_logger = logging.getLogger('validation')

main_logger.addHandler(console_handler)
main_logger.addHandler(logging.FileHandler(LOG_FILE, mode='w'))
main_logger.setLevel(logging.INFO)

# --- Khởi tạo Clients (Không đổi) ---
es_client = None
minio_client = None

# Elasticsearch Client (Không đổi)
try:
    es_args = {"cloud_id": ES_CLOUD_ID, "request_timeout": 60}
    if ES_API_KEY:
        es_args["api_key"] = ES_API_KEY
        main_logger.info(f"ES: Connecting to Cloud ID {ES_CLOUD_ID} using API Key.")
    elif ES_USER and ES_PASSWORD:
        es_args["basic_auth"] = (ES_USER, ES_PASSWORD)
        main_logger.info(f"ES: Connecting to Cloud ID {ES_CLOUD_ID} using Basic Auth.")
    else:
        raise ValueError("Missing Elasticsearch credentials (API Key or User/Password).")

    es_client = Elasticsearch(**es_args)
    if not es_client.ping(): raise ConnectionError("ES: Ping failed.")
    main_logger.info(f"ES: Connection successful.")
except ValueError as ve: main_logger.error(f"ES: Configuration error: {ve}")
except Exception as e: main_logger.error(f"ES: Connection failed: {e}")

# MinIO Client (Không đổi)
try:
    minio_client = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=MINIO_USE_SSL)
    main_logger.info(f"MinIO: Client initialized for endpoint: {MINIO_ENDPOINT}")
except Exception as e: main_logger.error(f"MinIO: Client initialization failed: {e}")

# --- Hàm tiện ích (Không đổi) ---
def ensure_minio_bucket_exists(client, bucket_name):
    if not client: main_logger.error("MinIO: Client not initialized."); return False
    try:
        if not client.bucket_exists(bucket_name):
            main_logger.warning(f"MinIO: Bucket '{bucket_name}' not found. Creating...")
            client.make_bucket(bucket_name)
            main_logger.info(f"MinIO: Bucket '{bucket_name}' created.")
        else: main_logger.info(f"MinIO: Bucket '{bucket_name}' exists.")
        return True
    except Exception as e: main_logger.error(f"MinIO: Error checking/creating bucket '{bucket_name}': {e}"); return False

def parse_iso_datetime(timestamp_str):
    """Cố gắng parse ISO timestamp, trả về datetime object hoặc None."""
    if not isinstance(timestamp_str, str): return None
    try:
        if timestamp_str.endswith('Z'):
            timestamp_str = timestamp_str[:-1] + '+00:00'
        return datetime.fromisoformat(timestamp_str)
    except ValueError:
        return None

def validate_transaction(data):
    """Validate transaction data more thoroughly."""
    errors = []
    if not isinstance(data, dict): return False, "Payload is not a dictionary."

    # ID checks
    if not data.get("transaction_id") or not isinstance(data["transaction_id"], str): errors.append("Missing/invalid transaction_id (string).")
    if not data.get("customer_id") or not isinstance(data["customer_id"], str): errors.append("Missing/invalid customer_id (string).")

    # Amount/Currency checks
    if "amount" not in data or not isinstance(data["amount"], (int, float)): errors.append("Missing/invalid amount (number).")
    if not data.get("currency") or not isinstance(data["currency"], str) or len(data["currency"]) != 3: errors.append("Missing/invalid currency (3-letter string).")

    # Timestamp check
    ts_obj = parse_iso_datetime(data.get("timestamp"))
    if not ts_obj: errors.append("Missing/invalid timestamp (ISO 8601 format).")

    # Enum checks
    if data.get("is_fraud") not in [0, 1]: errors.append("Invalid is_fraud value (must be 0 or 1).")
    valid_types = ["purchase", "transfer", "withdrawal", "payment", "refund", "reversal", "fee"]
    if data.get("transaction_type") not in valid_types: errors.append(f"Invalid transaction_type (must be one of {valid_types}).")
    valid_statuses = ["completed", "pending", "failed", "reversed", "flagged", "authorized"]
    if data.get("transaction_status") not in valid_statuses: errors.append(f"Invalid transaction_status (must be one of {valid_statuses}).")

    # Basic presence check for other important fields
    for f in ["location", "payment_method", "merchant_category", "device_os"]:
        if not data.get(f): errors.append(f"Missing field: {f}.")

    validation_logger.debug(f"Validation for {data.get('transaction_id')}: Errors - {errors}")
    return (False, "; ".join(errors)) if errors else (True, None)

# --- Thay đổi cách lấy metadata Kafka ---
def send_warning_to_elasticsearch(es, index_name, invalid_data, reason, msg): # Nhận message object thay vì dict
    """Send validation warning to Elasticsearch."""
    if not es: main_logger.error("ES: Client not available. Cannot send warning."); return
    # Lấy metadata từ message object của confluent-kafka
    topic = msg.topic()
    partition = msg.partition()
    offset = msg.offset()
    doc_id = f"{topic}-{partition}-{offset}" # Tạo unique ID

    doc = {
        "@timestamp": datetime.now(timezone.utc),
        "log.level": "warning",
        "kafka.topic": topic,
        "kafka.partition": partition,
        "kafka.offset": offset,
        "error.message": "Invalid transaction data",
        "error.reason": reason,
        "invalid_message": invalid_data,
        "service.name": "kafka_consumer_validator",
        "event.kind": "alert",
        "event.category": "database",
        "event.action": "data_validation_failed"
    }
    try:
        response = es.index(index=index_name, document=doc, id=doc_id)
        es_logger.debug(f"ES: Sent warning for offset {offset}, doc_id: {response.get('_id')}")
    except Exception as e:
        es_logger.error(f"ES: Failed to send warning for offset {offset}: {e}")
        # main_logger.debug(f"ES Failed Doc Data: {doc}")

def upload_to_minio(minio, bucket_name, tx_data):
    """Upload valid transaction JSON to MinIO."""
    if not minio: main_logger.error("MinIO: Client not available. Cannot upload."); return

    tx_id = tx_data.get("transaction_id", f"unknown_{int(datetime.now(timezone.utc).timestamp())}")
    dt_obj = parse_iso_datetime(tx_data.get("timestamp")) or datetime.now(timezone.utc)
    object_name = f"transactions/year={dt_obj.strftime('%Y')}/month={dt_obj.strftime('%m')}/day={dt_obj.strftime('%d')}/{tx_id}.json"

    try:
        json_bytes = json.dumps(tx_data, indent=2, ensure_ascii=False).encode('utf-8')
        json_stream = io.BytesIO(json_bytes)
        result = minio.put_object(bucket_name, object_name, json_stream, len(json_bytes), content_type='application/json')
        minio_logger.debug(f"MinIO: Uploaded {object_name} to {bucket_name}, ETag: {result.etag}")
        return True # Trả về True nếu thành công
    except Exception as e:
        minio_logger.error(f"MinIO: Failed to upload {object_name} to {bucket_name}: {e}")
        return False # Trả về False nếu thất bại

# --- Callback cho confluent-kafka (Optional nhưng hữu ích) ---
def error_cb(err):
    """Callback cho lỗi chung của Consumer."""
    kafka_logger.error(f"Kafka Consumer error: {err}")

def stats_cb(stats_json_str):
    """Callback cho thống kê định kỳ từ librdkafka (JSON string)."""
    # Bạn có thể parse JSON và log hoặc gửi đi đâu đó
    kafka_logger.debug(f"Kafka Stats: {stats_json_str}") # Log ở DEBUG để tránh spam

# --- Hàm Consumer chính (Đã chuyển đổi) ---
def consume_and_process():
    main_logger.info("Starting consumer process with confluent-kafka...")
    if not ensure_minio_bucket_exists(minio_client, MINIO_BUCKET):
        main_logger.critical("MinIO: Bucket check/creation failed. Exiting.")
        sys.exit(1)

    # --- Cấu hình Consumer cho confluent-kafka ---
    conf = {
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': KAFKA_CONSUMER_GROUP,
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': KAFKA_API_KEY,
        'sasl.password': KAFKA_API_SECRET,
        'ssl.ca.location': certifi.where(),
        'auto.offset.reset': 'earliest', # Or 'latest'
        'enable.auto.commit': False, # Disable auto-commit for manual control
        # Không có value_deserializer ở đây
        # Các tham số tương tự khác:
        'fetch.max.bytes': 52428800, # Tăng kích thước fetch (default 50MB)
        'max.poll.interval.ms': 300000, # Thời gian tối đa giữa các lần poll (default 5 mins)
        'session.timeout.ms': 30000, # Nên cao hơn heartbeat.interval.ms (default 10s, tăng lên 30s)
        'heartbeat.interval.ms': 10000, # Tăng heartbeat interval (default 3s, tăng lên 10s)
        # Thêm callback
        'error_cb': error_cb,
        # 'stats_cb': stats_cb, # Bật nếu muốn xem stats
        # 'statistics.interval.ms': 30000 # Tần suất callback stats (ms)
    }
    # --------------------------------------------

    consumer = None
    try:
        consumer = Consumer(conf)
        kafka_logger.info(f"Kafka: Consumer created with config: { {k:v for k,v in conf.items() if 'password' not in k} }") # Log config trừ password
        consumer.subscribe([KAFKA_TOPIC]) # Subscribe sau khi tạo consumer
        kafka_logger.info(f"Kafka: Subscribed to topic '{KAFKA_TOPIC}'")
    except KafkaException as e:
        kafka_logger.critical(f"Kafka: Consumer initialization failed (KafkaException): {e}")
        sys.exit(1)
    except Exception as e:
        kafka_logger.critical(f"Kafka: Consumer initialization failed (Other Exception): {e}")
        sys.exit(1)

    msg_count = valid_count = invalid_json_count = invalid_data_count = 0
    last_log_time = time.time()
    last_commit_time = time.time()
    commit_interval_secs = 10 # Commit mỗi 10 giây
    processed_offsets = {} # Dict để lưu offset cao nhất đã xử lý cho từng partition: {(topic, partition): offset}

    try:
        while True:
            msg = consumer.poll(timeout=1.0) # Poll tối đa 1 giây

            if msg is None:
                # Timeout, không có message mới
                kafka_logger.debug("Kafka: Poll timeout, no new messages.")
                 # Kiểm tra xem có cần commit định kỳ không ngay cả khi không có message mới
                if time.time() - last_commit_time >= commit_interval_secs and processed_offsets:
                    try:
                        offsets_to_commit = [TopicPartition(tp[0], tp[1], off + 1) for tp, off in processed_offsets.items() if off != OFFSET_INVALID]
                        if offsets_to_commit:
                            consumer.commit(offsets=offsets_to_commit, asynchronous=False) # Commit đồng bộ
                            kafka_logger.info(f"Kafka: Committed offsets (periodic): { {f'{tp.topic}-{tp.partition}': tp.offset for tp in offsets_to_commit} }")
                            processed_offsets.clear() # Reset sau khi commit thành công
                            last_commit_time = time.time()
                    except KafkaException as e:
                         kafka_logger.error(f"Kafka: Failed to commit offsets periodically (KafkaException): {e}")
                         # Có thể cần logic retry hoặc xử lý khác
                    except Exception as e:
                        kafka_logger.error(f"Kafka: Failed to commit offsets periodically (Exception): {e}")
                continue # Quay lại vòng lặp poll

            if msg.error():
                # Xử lý lỗi từ Kafka
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # Kết thúc partition, không phải lỗi thực sự
                    kafka_logger.info(f"Kafka: Reached end of partition: {msg.topic()}-{msg.partition()} at offset {msg.offset()}")
                elif msg.error():
                    # Lỗi thực sự
                    kafka_logger.error(f"Kafka: Consumer error: {msg.error()}")
                    # Có thể thêm logic để dừng hoặc xử lý lỗi nghiêm trọng ở đây
                    # raise KafkaException(msg.error()) # Ném lỗi ra ngoài nếu muốn dừng hẳn
                continue # Bỏ qua message lỗi và tiếp tục poll

            # ---- Xử lý message hợp lệ ----
            msg_count += 1
            current_offset = msg.offset()
            tp_key = (msg.topic(), msg.partition()) # Key cho dictionary offset

            main_logger.debug(f"Processing message from {tp_key[0]}-{tp_key[1]} at offset {current_offset}")
            transaction_data = None
            is_valid = False
            error_reason = None
            processing_success = False # Cờ đánh dấu xử lý thành công (validate + ghi/gửi)

            try:
                # 1. Deserialization
                raw_value = msg.value()
                if raw_value is None:
                    raise ValueError("Message value is None")
                transaction_data = json.loads(raw_value.decode('utf-8'))

                # 2. Validation
                is_valid, error_reason = validate_transaction(transaction_data)

                if is_valid:
                    # 3a. Upload to MinIO (if valid)
                    if upload_to_minio(minio_client, MINIO_BUCKET, transaction_data):
                        valid_count += 1
                        validation_logger.debug(f"Offset {current_offset}: Data VALID and uploaded.")
                        processing_success = True
                    else:
                        # Lỗi upload MinIO, không nên commit offset này
                        main_logger.error(f"Offset {current_offset}: Data VALID but MinIO upload FAILED. Offset will not be committed.")
                        processing_success = False # Đánh dấu xử lý thất bại
                        # Cân nhắc: Gửi vào dead-letter queue hoặc retry?

                else:
                    # 3b. Send to Elasticsearch (if invalid)
                    invalid_data_count += 1
                    validation_logger.warning(f"Offset {current_offset}: Data INVALID: {error_reason}")
                    send_warning_to_elasticsearch(es_client, ES_INDEX, transaction_data, error_reason, msg)
                    processing_success = True # Coi như xử lý thành công (đã ghi log lỗi)

            except json.JSONDecodeError as e:
                invalid_json_count += 1
                error_reason = f"JSON Decode Error: {e}"
                main_logger.error(f"Offset {current_offset}: {error_reason}")
                raw_data = "Could not decode raw data"
                try: raw_data = msg.value().decode('utf-8', errors='ignore')
                except: pass
                send_warning_to_elasticsearch(es_client, ES_INDEX, {"raw_message_on_error": raw_data}, error_reason, msg)
                processing_success = True # Coi như xử lý thành công (đã ghi log lỗi)
            except Exception as e:
                # Lỗi không mong muốn khác trong quá trình xử lý
                invalid_data_count += 1 # Hoặc một bộ đếm lỗi xử lý riêng
                error_reason = f"Unexpected processing error: {e}"
                main_logger.error(f"Offset {current_offset}: {error_reason}", exc_info=True)
                send_warning_to_elasticsearch(es_client, ES_INDEX, transaction_data if transaction_data else {"error_context": "Processing failed before data decode"}, error_reason, msg)
                processing_success = True # Coi như xử lý thành công (đã ghi log lỗi)

            # ---- Cập nhật offset đã xử lý và kiểm tra commit ----
            if processing_success:
                # Chỉ cập nhật offset nếu xử lý thành công (bao gồm cả việc ghi log lỗi)
                processed_offsets[tp_key] = current_offset
            else:
                # Nếu xử lý thất bại (ví dụ: lỗi MinIO), không cập nhật offset
                # Consumer sẽ đọc lại message này ở lần poll sau (nếu chưa commit offset cũ hơn)
                # Cần có cơ chế retry hoặc dead-letter queue để tránh bị kẹt vĩnh viễn
                main_logger.warning(f"Offset {current_offset}: Processing failed. Not updating offset for {tp_key}.")
                # Cân nhắc thêm delay nhỏ ở đây nếu lỗi liên tục
                # time.sleep(1)


            # Kiểm tra commit định kỳ
            current_time = time.time()
            if current_time - last_commit_time >= commit_interval_secs and processed_offsets:
                try:
                    # Tạo list offset cần commit (offset_last_processed + 1)
                    offsets_to_commit = [TopicPartition(tp[0], tp[1], off + 1) for tp, off in processed_offsets.items() if off != OFFSET_INVALID]
                    if offsets_to_commit:
                         # Commit đồng bộ
                        consumer.commit(offsets=offsets_to_commit, asynchronous=False)
                        kafka_logger.info(f"Kafka: Committed offsets (periodic): { {f'{tp.topic}-{tp.partition}': tp.offset for tp in offsets_to_commit} }")
                        processed_offsets.clear() # Reset sau khi commit thành công
                        last_commit_time = current_time
                except KafkaException as e:
                    kafka_logger.error(f"Kafka: Failed to commit offsets periodically (KafkaException): {e}")
                     # Xử lý lỗi commit: retry, ghi log nghiêm trọng, dừng consumer?
                    # Nếu commit thất bại, offset cũ vẫn được giữ trong processed_offsets
                    # và sẽ được thử commit lại ở lần sau.
                except Exception as e:
                    kafka_logger.error(f"Kafka: Failed to commit offsets periodically (Exception): {e}")


            # Log progress định kỳ
            if current_time - last_log_time >= 60: # Log mỗi 60 giây
                main_logger.info(f"Progress: Total Msgs Processed Loop: {msg_count}, Valid (Uploaded): {valid_count}, Invalid JSON: {invalid_json_count}, Invalid Data (Logged): {invalid_data_count}")
                last_log_time = current_time


    except KeyboardInterrupt:
        main_logger.info("KeyboardInterrupt received. Shutting down gracefully...")
    except KafkaException as e:
         main_logger.critical(f"CRITICAL: Unhandled KafkaException in main loop: {e}", exc_info=True)
    except Exception as e:
        main_logger.critical(f"CRITICAL: Unhandled exception in main loop: {e}", exc_info=True)
    finally:
        if consumer:
            main_logger.info("Closing Kafka consumer...")
            try:
                # Commit lần cuối trước khi đóng (nếu có offset chưa commit)
                if processed_offsets:
                     offsets_to_commit = [TopicPartition(tp[0], tp[1], off + 1) for tp, off in processed_offsets.items() if off != OFFSET_INVALID]
                     if offsets_to_commit:
                         main_logger.info(f"Performing final commit: { {f'{tp.topic}-{tp.partition}': tp.offset for tp in offsets_to_commit} }")
                         consumer.commit(offsets=offsets_to_commit, asynchronous=False)
            except Exception as e:
                 main_logger.error(f"Kafka: Failed to perform final commit: {e}")
            finally:
                 consumer.close() # Luôn đóng consumer
                 kafka_logger.info("Kafka: Consumer closed.")

        main_logger.info(f"Final Stats: Total Msgs Processed Loop: {msg_count}, Valid (Uploaded): {valid_count}, Invalid JSON: {invalid_json_count}, Invalid Data (Logged): {invalid_data_count}")
        main_logger.info("Consumer process finished.")


# --- Chạy Consumer ---
if __name__ == "__main__":
    # Critical dependency check before starting (Không đổi)
    if minio_client is None:
        main_logger.critical("MinIO: Client unavailable. Cannot upload valid data. Exiting.")
        sys.exit(1)
    if es_client is None:
        main_logger.warning("ES: Client unavailable. Validation warnings will not be sent.")
        # If ES warnings are critical, uncomment the following lines:
        # main_logger.critical("ES: Client unavailable. Cannot send warnings. Exiting.")
        # sys.exit(1)


    consume_and_process()