import logging
from kafka import KafkaConsumer, TopicPartition # Thêm TopicPartition để seek nếu cần debug
import json
import sys
import os
import time
import certifi
from dotenv import load_dotenv
from datetime import datetime, timezone # Thêm timezone
import io

# --- Import clients ---
from elasticsearch import Elasticsearch
from minio import Minio
from minio.error import S3Error

# --- Tải biến môi trường ---
load_dotenv()

# --- Cấu hình Kafka / Confluent Cloud ---
KAFKA_BROKER = os.getenv('CONFLUENT_BOOTSTRAP_SERVERS')
KAFKA_API_KEY = os.getenv('CONFLUENT_API_KEY') # Đổi tên để rõ ràng hơn
KAFKA_API_SECRET = os.getenv('CONFLUENT_API_SECRET') # Đổi tên để rõ ràng hơn
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'transactions')
KAFKA_CONSUMER_GROUP = os.getenv('KAFKA_CONSUMER_GROUP', 'transaction-processor-group-final')

# --- Cấu hình Elasticsearch (Elastic Cloud) ---
ES_CLOUD_ID = os.getenv('ELASTICSEARCH_CLOUD_ID')
ES_API_KEY = os.getenv('ELASTICSEARCH_API_KEY')
ES_USER = os.getenv('ELASTICSEARCH_USER')
ES_PASSWORD = os.getenv('ELASTICSEARCH_PASSWORD')
ES_INDEX = os.getenv('ELASTICSEARCH_INDEX', 'kafka-invalid-transactions-log-v1')

# --- Cấu hình MinIO (Docker) ---
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'localhost:9000')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY')
MINIO_BUCKET = os.getenv('MINIO_BUCKET', 'kafka-valid-transactions')
MINIO_USE_SSL = os.getenv('MINIO_USE_SSL', 'False').lower() == 'true'

# --- Kiểm tra cấu hình cần thiết ---
missing_configs = []
if not all([KAFKA_BROKER, KAFKA_API_KEY, KAFKA_API_SECRET]): missing_configs.append("Kafka/Confluent Cloud")
if not ES_CLOUD_ID or not (ES_API_KEY or (ES_USER and ES_PASSWORD)): missing_configs.append("Elasticsearch Cloud (Cloud ID và API Key/User+Pass)")
if not all([MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_BUCKET]): missing_configs.append("MinIO")

if missing_configs:
    print(f"Lỗi: Thiếu cấu hình cho: {', '.join(missing_configs)}.")
    print("Vui lòng kiểm tra file .env hoặc các biến môi trường.")
    sys.exit(1)

# --- Thiết lập Logging ---
LOG_FILE = 'consumer_processor.log'
logging.basicConfig(level=logging.INFO,
                    filename=LOG_FILE,
                    filemode='w',
                    format='%(asctime)s %(levelname)-8s [%(name)-12s] %(message)s') # Format log chi tiết hơn
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s %(levelname)-8s [%(name)-12s] %(message)s')
console_handler.setFormatter(formatter)

# Tạo logger riêng
main_logger = logging.getLogger('main')
es_logger = logging.getLogger('elasticsearch')
minio_logger = logging.getLogger('minio')
kafka_logger = logging.getLogger('kafka')
validation_logger = logging.getLogger('validation')

# Gắn handler (chỉ gắn vào logger chính nếu không muốn log của thư viện)
main_logger.addHandler(console_handler)
main_logger.addHandler(logging.FileHandler(LOG_FILE, mode='w')) # Ghi log chính vào file
main_logger.setLevel(logging.INFO)
# Đặt level cho các logger khác nếu muốn xem log debug của chúng
# es_logger.setLevel(logging.DEBUG)
# minio_logger.setLevel(logging.DEBUG)
# kafka_logger.setLevel(logging.DEBUG)
# validation_logger.setLevel(logging.DEBUG)

# --- Khởi tạo Clients ---
es_client = None
minio_client = None

# Elasticsearch Client
try:
    es_args = {"cloud_id": ES_CLOUD_ID, "request_timeout": 60} # Tăng timeout
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

# MinIO Client
try:
    minio_client = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=MINIO_USE_SSL)
    main_logger.info(f"MinIO: Client initialized for endpoint: {MINIO_ENDPOINT}")
except Exception as e: main_logger.error(f"MinIO: Client initialization failed: {e}")

# --- Hàm tiện ích ---
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
        # Xử lý 'Z' và các offset khác nhau
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
    # Allow 0 amount for certain types, negative only for reversals? (adjust logic as needed)
    # if data.get("amount", 0) <= 0 and data.get("transaction_type") not in ["refund", "reversal"]: errors.append("Amount must be positive for this transaction type.")
    if not data.get("currency") or not isinstance(data["currency"], str) or len(data["currency"]) != 3: errors.append("Missing/invalid currency (3-letter string).")

    # Timestamp check
    ts_obj = parse_iso_datetime(data.get("timestamp"))
    if not ts_obj: errors.append("Missing/invalid timestamp (ISO 8601 format).")
    # Optional: Check if timestamp is within a reasonable range (e.g., not too far in past/future)
    # if ts_obj and (ts_obj < datetime.now(timezone.utc) - timedelta(days=30) or ts_obj > datetime.now(timezone.utc) + timedelta(hours=1)):
    #     errors.append("Timestamp is outside expected range.")

    # Enum checks
    if data.get("is_fraud") not in [0, 1]: errors.append("Invalid is_fraud value (must be 0 or 1).")
    valid_types = ["purchase", "transfer", "withdrawal", "payment", "refund", "reversal", "fee"] # Add more if needed
    if data.get("transaction_type") not in valid_types: errors.append(f"Invalid transaction_type (must be one of {valid_types}).")
    valid_statuses = ["completed", "pending", "failed", "reversed", "flagged", "authorized"]
    if data.get("transaction_status") not in valid_statuses: errors.append(f"Invalid transaction_status (must be one of {valid_statuses}).")

    # Basic presence check for other important fields
    for f in ["location", "payment_method", "merchant_category", "device_os"]:
        if not data.get(f): errors.append(f"Missing field: {f}.")

    validation_logger.debug(f"Validation for {data.get('transaction_id')}: Errors - {errors}")
    return (False, "; ".join(errors)) if errors else (True, None)

def send_warning_to_elasticsearch(es, index_name, invalid_data, reason, kafka_meta):
    """Send validation warning to Elasticsearch."""
    if not es: main_logger.error("ES: Client not available. Cannot send warning."); return
    doc = {
        "@timestamp": datetime.now(timezone.utc), # Use timezone-aware UTC timestamp
        "log.level": "warning",
        "kafka.topic": kafka_meta.get('topic'),
        "kafka.partition": kafka_meta.get('partition'),
        "kafka.offset": kafka_meta.get('offset'),
        "error.message": "Invalid transaction data",
        "error.reason": reason,
        "invalid_message": invalid_data, # Include the problematic message
        "service.name": "kafka_consumer_validator",
        "event.kind": "alert",
        "event.category": "database", # Or appropriate category
        "event.action": "data_validation_failed"
    }
    try:
        response = es.index(index=index_name, document=doc, id=f"{kafka_meta.get('topic')}-{kafka_meta.get('partition')}-{kafka_meta.get('offset')}") # Use unique ID
        es_logger.debug(f"ES: Sent warning for offset {kafka_meta.get('offset')}, doc_id: {response.get('_id')}")
    except Exception as e:
        es_logger.error(f"ES: Failed to send warning for offset {kafka_meta.get('offset')}: {e}")
        # Consider logging the failed doc to file or another system as backup
        # main_logger.debug(f"ES Failed Doc Data: {doc}")

def upload_to_minio(minio, bucket_name, tx_data):
    """Upload valid transaction JSON to MinIO."""
    if not minio: main_logger.error("MinIO: Client not available. Cannot upload."); return

    tx_id = tx_data.get("transaction_id", f"unknown_{int(datetime.now(timezone.utc).timestamp())}")
    # Use date partitioning for better organization
    dt_obj = parse_iso_datetime(tx_data.get("timestamp")) or datetime.now(timezone.utc)
    object_name = f"transactions/year={dt_obj.strftime('%Y')}/month={dt_obj.strftime('%m')}/day={dt_obj.strftime('%d')}/{tx_id}.json"

    try:
        json_bytes = json.dumps(tx_data, indent=2, ensure_ascii=False).encode('utf-8') # ensure_ascii=False for non-latin chars
        json_stream = io.BytesIO(json_bytes)
        result = minio.put_object(bucket_name, object_name, json_stream, len(json_bytes), content_type='application/json')
        minio_logger.debug(f"MinIO: Uploaded {object_name} to {bucket_name}, ETag: {result.etag}")
    except Exception as e:
        minio_logger.error(f"MinIO: Failed to upload {object_name} to {bucket_name}: {e}")
        # Consider adding retry logic or dead-letter queue mechanism here

# --- Hàm Consumer chính ---
def consume_and_process():
    main_logger.info("Starting consumer process...")
    if not ensure_minio_bucket_exists(minio_client, MINIO_BUCKET):
         main_logger.critical("MinIO: Bucket check/creation failed. Exiting.")
         sys.exit(1)

    consumer = None # Initialize consumer to None
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BROKER,
            group_id=KAFKA_CONSUMER_GROUP,
            security_protocol='SASL_SSL',
            sasl_mechanism='PLAIN',
            sasl_plain_username=KAFKA_API_KEY,
            sasl_plain_password=KAFKA_API_SECRET,
            ssl_cafile=certifi.where(),
            auto_offset_reset='earliest', # Or 'latest' depending on requirement
            enable_auto_commit=False, # Disable auto-commit for manual control
            value_deserializer=lambda v: json.loads(v.decode('utf-8')), # Potential errors handled below
            consumer_timeout_ms=-1, # Block indefinitely
            fetch_max_wait_ms=500, # Poll timeout
            heartbeat_interval_ms=3000, # Standard intervals for Confluent Cloud
            session_timeout_ms=10000,
            # request_timeout_ms=30000 # Default is higher
        )
        kafka_logger.info(f"Kafka: Consumer connected. Subscribed to '{KAFKA_TOPIC}', Group ID '{KAFKA_CONSUMER_GROUP}'")
    except Exception as e:
        kafka_logger.critical(f"Kafka: Consumer initialization failed: {e}")
        sys.exit(1)

    msg_count = valid_count = invalid_json_count = invalid_data_count = 0
    last_log_time = time.time()

    try:
        while True: # Keep running indefinitely
            try:
                 # Poll for messages with a timeout
                msg_pack = consumer.poll(timeout_ms=1000, max_records=100) # Poll for 1 second, get up to 100 records
                if not msg_pack:
                    # No messages received, continue polling
                    kafka_logger.debug("Kafka: No messages received in poll.")
                    continue

                # Process messages received in the poll
                for tp, messages in msg_pack.items(): # tp = TopicPartition
                    kafka_logger.debug(f"Processing {len(messages)} messages from {tp.topic}-{tp.partition}")
                    offsets_to_commit = {} # Track offsets per partition

                    for message in messages:
                        msg_count += 1
                        current_offset = message.offset
                        kafka_meta = {'topic': message.topic, 'partition': message.partition, 'offset': current_offset}
                        main_logger.debug(f"Processing message at offset {current_offset}")
                        transaction_data = None
                        is_valid = False
                        error_reason = None

                        try:
                            # Deserialization already happened, if it failed, it would raise exception here or earlier
                            transaction_data = message.value
                            is_valid, error_reason = validate_transaction(transaction_data)

                            if is_valid:
                                valid_count += 1
                                validation_logger.debug(f"Offset {current_offset}: Data VALID.")
                                upload_to_minio(minio_client, MINIO_BUCKET, transaction_data)
                            else:
                                invalid_data_count += 1
                                validation_logger.warning(f"Offset {current_offset}: Data INVALID: {error_reason}")
                                send_warning_to_elasticsearch(es_client, ES_INDEX, transaction_data, error_reason, kafka_meta)

                        except json.JSONDecodeError as e:
                            # This might not be caught here if deserializer fails earlier, but good as a fallback
                            invalid_json_count += 1
                            error_reason = f"JSON Decode Error: {e}"
                            main_logger.error(f"Offset {current_offset}: {error_reason}")
                            raw_data = "Could not decode raw data"
                            try: raw_data = message.value.decode('utf-8', errors='ignore')
                            except: pass
                            send_warning_to_elasticsearch(es_client, ES_INDEX, {"raw_message_on_error": raw_data}, error_reason, kafka_meta)
                        except Exception as e:
                            # Catch unexpected errors during validation or upload/send
                            invalid_data_count += 1 # Count as invalid data processing error
                            error_reason = f"Unexpected processing error: {e}"
                            main_logger.error(f"Offset {current_offset}: {error_reason}", exc_info=True)
                            send_warning_to_elasticsearch(es_client, ES_INDEX, transaction_data if transaction_data else {"error_context": "Processing failed"}, error_reason, kafka_meta)

                        # Mark the offset to be committed for this partition (+1 because commit is for the *next* offset)
                        offsets_to_commit[tp] = current_offset + 1

                    # Commit offsets for the processed partition batch
                    if offsets_to_commit:
                        commit_map = {tp: OffsetAndMetadata(offset, '') for tp, offset in offsets_to_commit.items()}
                        try:
                            consumer.commit(offsets=commit_map)
                            kafka_logger.debug(f"Kafka: Committed offsets: {commit_map}")
                        except Exception as e:
                            kafka_logger.error(f"Kafka: Failed to commit offsets {commit_map}: {e}")
                            # Handle commit failure (e.g., retry, log critical error) - potentially seek back?

                # Log progress periodically
                current_time = time.time()
                if current_time - last_log_time >= 60: # Log every 60 seconds
                    main_logger.info(f"Progress: Total Msgs: {msg_count}, Valid: {valid_count}, Invalid JSON: {invalid_json_count}, Invalid Data: {invalid_data_count}")
                    last_log_time = current_time

            except Exception as poll_err:
                # Handle errors during the poll() call itself
                kafka_logger.error(f"Kafka: Error during consumer poll: {poll_err}", exc_info=True)
                # Implement backoff or reconnection logic if necessary
                time.sleep(5) # Wait before retrying poll

    except KeyboardInterrupt:
        main_logger.info("KeyboardInterrupt received. Shutting down gracefully...")
    except Exception as e:
        main_logger.critical(f"CRITICAL: Unhandled exception in main loop: {e}", exc_info=True)
    finally:
        if consumer:
            main_logger.info("Closing Kafka consumer...")
            consumer.close()
            kafka_logger.info("Kafka: Consumer closed.")
        main_logger.info(f"Final Stats: Total Msgs: {msg_count}, Valid: {valid_count}, Invalid JSON: {invalid_json_count}, Invalid Data: {invalid_data_count}")
        main_logger.info("Consumer process finished.")


# --- Chạy Consumer ---
if __name__ == "__main__":
    # Critical dependency check before starting
    if minio_client is None:
        main_logger.critical("MinIO: Client unavailable. Cannot upload valid data. Exiting.")
        sys.exit(1)
    # ES is for warnings, maybe allow running without it? Or make it critical too?
    if es_client is None:
        main_logger.warning("ES: Client unavailable. Validation warnings will not be sent.")
        # If ES warnings are critical, uncomment the following lines:
        # main_logger.critical("ES: Client unavailable. Cannot send warnings. Exiting.")
        # sys.exit(1)

    # Thêm import này ở đầu file consumer.py nếu chưa có
    from kafka.structs import OffsetAndMetadata

    consume_and_process()