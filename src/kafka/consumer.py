# -*- coding: utf-8 -*-
from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition, OFFSET_INVALID
import json
import sys
import os
import time
import certifi
from dotenv import load_dotenv
from datetime import datetime, timezone
import io
import logging

# --- Import clients ---
from elasticsearch import Elasticsearch
import boto3  
from botocore.exceptions import ClientError, NoCredentialsError # 

# --- Tải biến môi trường ---
load_dotenv()

# --- Cấu hình Kafka / Confluent Cloud  ---
KAFKA_BROKER = os.getenv('CONFLUENT_BOOTSTRAP_SERVERS')
KAFKA_API_KEY = os.getenv('CONFLUENT_API_KEY')
KAFKA_API_SECRET = os.getenv('CONFLUENT_API_SECRET')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'transactions')
KAFKA_CONSUMER_GROUP = os.getenv('KAFKA_CONSUMER_GROUP', 'transaction-processor-group-final')

# --- Cấu hình Elasticsearch  ---
ES_CLOUD_ID = os.getenv('ELASTICSEARCH_CLOUD_ID')
ES_API_KEY = os.getenv('ELASTICSEARCH_API_KEY')
ES_USER = os.getenv('ELASTICSEARCH_USER')
ES_PASSWORD = os.getenv('ELASTICSEARCH_PASSWORD')
ES_INDEX = os.getenv('ELASTICSEARCH_INDEX', 'kafka-invalid-transactions-log-v1')


AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION') # Cần thiết cho boto3, đặc biệt khi tạo bucket
AWS_S3_BUCKET = os.getenv('AWS_S3_BUCKET') # Đổi tên biến bucket

# --- Kiểm tra cấu hình cần thiết ---
missing_configs = []
if not all([KAFKA_BROKER, KAFKA_API_KEY, KAFKA_API_SECRET]): missing_configs.append("Kafka/Confluent Cloud")
if not ES_CLOUD_ID or not (ES_API_KEY or (ES_USER and ES_PASSWORD)): missing_configs.append("Elasticsearch Cloud (Cloud ID và API Key/User+Pass)")
if not all([AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, AWS_S3_BUCKET]):
    missing_configs.append("AWS S3 (Access Key, Secret Key, Region, Bucket)")

if missing_configs:
    print(f"Lỗi: Thiếu cấu hình cho: {', '.join(missing_configs)}.")
    print("Vui lòng kiểm tra file .env hoặc các biến môi trường.")
    sys.exit(1)

# --- Thiết lập Logging (Giữ nguyên logic, đổi tên logger) ---
LOG_FILE = 'consumer_processor_confluent.log'
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
s3_logger = logging.getLogger('s3') 
kafka_logger = logging.getLogger('kafka.consumer')
validation_logger = logging.getLogger('validation')

main_logger.addHandler(console_handler)
main_logger.addHandler(logging.FileHandler(LOG_FILE, mode='w'))
main_logger.setLevel(logging.INFO)

# --- Khởi tạo Clients ---
es_client = None
s3_client = None # <<< 

# Elasticsearch Client 
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

# AWS S3 Client --- # <<< : Khởi tạo S3 client thay vì MinIO client
try:
    s3_client = boto3.client(
        's3',
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )
    main_logger.info(f"S3: Client initialized for region: {AWS_REGION}")
    # Có thể thêm kiểm tra kết nối bằng head_bucket nếu muốn, nhưng bỏ qua để giữ logic tối giản
except NoCredentialsError:
    main_logger.error("S3: Credentials not found. Check environment variables or AWS configuration.")
    # Giữ luồng xử lý lỗi tương tự MinIO gốc: ghi log lỗi nhưng không exit ngay lập tức
except ClientError as e:
    main_logger.error(f"S3: Client initialization failed (ClientError): {e}")
except Exception as e:
    main_logger.error(f"S3: Client initialization failed (Unexpected Error): {e}")


# --- Hàm tiện ích ---

def ensure_s3_bucket_exists(client, bucket_name, region):
    """Kiểm tra và tạo bucket nếu cần thiết trên AWS S3."""
    if not client:
        main_logger.error("S3: Client not initialized.") # <<< : Log message
        return False
    try:
        client.head_bucket(Bucket=bucket_name)
        main_logger.info(f"S3: Bucket '{bucket_name}' exists.") # <<< : Log message
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code')
        if error_code == '404' or error_code == 'NoSuchBucket':
            main_logger.warning(f"S3: Bucket '{bucket_name}' not found. Creating...") # <<< : Log message
            try:
                if region == 'us-east-1':
                     client.create_bucket(Bucket=bucket_name)
                else:
                     client.create_bucket(
                         Bucket=bucket_name,
                         CreateBucketConfiguration={'LocationConstraint': region}
                     )
                main_logger.info(f"S3: Bucket '{bucket_name}' created in region {region}.") # <<< : Log message
                # Không cần chờ như trong một số ví dụ, giữ logic đơn giản
            except ClientError as create_err:
                main_logger.error(f"S3: Error creating bucket '{bucket_name}': {create_err}") # <<< : Log message
                return False
            except Exception as create_gen_err:
                 main_logger.error(f"S3: Unexpected error creating bucket '{bucket_name}': {create_gen_err}") # <
                 return False
        else:
            # Bao gồm cả lỗi 403 (Permission denied) và các lỗi khác
            main_logger.error(f"S3: Error checking bucket '{bucket_name}': {e}") # <<< : Log message
            return False
    except Exception as e:
        # Lỗi không mong muốn khác khi gọi head_bucket
        main_logger.error(f"S3: Unexpected error checking bucket '{bucket_name}': {e}") # <<< : Log message
        return False
    return True

# --- Các hàm parse_iso_datetime, validate_transaction, send_warning_to_elasticsearch  ---
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
    # --- LOGIC VALIDATION GIỮ NGUYÊN ---
    errors = []
    if not isinstance(data, dict): return False, "Payload is not a dictionary."
    if not data.get("transaction_id") or not isinstance(data["transaction_id"], str): errors.append("Missing/invalid transaction_id (string).")
    if not data.get("customer_id") or not isinstance(data["customer_id"], str): errors.append("Missing/invalid customer_id (string).")
    if "amount" not in data or not isinstance(data["amount"], (int, float)): errors.append("Missing/invalid amount (number).")
    if not data.get("currency") or not isinstance(data["currency"], str) or len(data["currency"]) != 3: errors.append("Missing/invalid currency (3-letter string).")
    ts_obj = parse_iso_datetime(data.get("timestamp"))
    if not ts_obj: errors.append("Missing/invalid timestamp (ISO 8601 format).")
    if data.get("is_fraud") not in [0, 1]: errors.append("Invalid is_fraud value (must be 0 or 1).")
    valid_types = ["purchase", "transfer", "withdrawal", "payment", "refund", "reversal", "fee"]
    if data.get("transaction_type") not in valid_types: errors.append(f"Invalid transaction_type (must be one of {valid_types}).")
    valid_statuses = ["completed", "pending", "failed", "reversed", "flagged", "authorized"]
    if data.get("transaction_status") not in valid_statuses: errors.append(f"Invalid transaction_status (must be one of {valid_statuses}).")
    for f in ["location", "payment_method", "merchant_category", "device_os"]:
        if not data.get(f): errors.append(f"Missing field: {f}.")
    validation_logger.debug(f"Validation for {data.get('transaction_id')}: Errors - {errors}")
    return (False, "; ".join(errors)) if errors else (True, None)


def send_warning_to_elasticsearch(es, index_name, invalid_data, reason, msg): # Nhận message object thay vì dict
    """Send validation warning to Elasticsearch."""
    # --- LOGIC GỬI ES GIỮ NGUYÊN ---
    if not es: main_logger.error("ES: Client not available. Cannot send warning."); return
    topic = msg.topic()
    partition = msg.partition()
    offset = msg.offset()
    doc_id = f"{topic}-{partition}-{offset}" # Tạo unique ID
    doc = {
        "@timestamp": datetime.now(timezone.utc), "log.level": "warning",
        "kafka.topic": topic, "kafka.partition": partition, "kafka.offset": offset,
        "error.message": "Invalid transaction data", "error.reason": reason,
        "invalid_message": invalid_data, "service.name": "kafka_consumer_validator",
        "event.kind": "alert", "event.category": "database", "event.action": "data_validation_failed"
    }
    try:
        response = es.index(index=index_name, document=doc, id=doc_id)
        response_id = None
        if isinstance(response, dict):
            response_id = response.get('_id')
        elif hasattr(response, '_id'):
            response_id = response._id
        if response_id:
              es_logger.debug(f"ES: Sent warning for offset {offset}, doc_id: {response_id}")
        else:
              es_logger.debug(f"ES: Sent warning for offset {offset}, doc_id: {doc_id} (could not get _id from response: {response})")
    except Exception as e:
        es_logger.error(f"ES: Failed to send warning for offset {offset}: {e}")



def upload_to_s3(s3_client_param, bucket_name, tx_data): # Đổi tên param để rõ hơn
    """
    Upload valid transaction JSON to AWS S3 using the same object naming logic.
    """
    if not s3_client_param:
        main_logger.error("S3: Client not available. Cannot upload.") # <<< 
        return False

    # --- Logic tạo object_name giữ nguyên như yêu cầu của code gốc ---
    tx_id = tx_data.get("transaction_id")
    if not tx_id:
        timestamp_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        data_hash = hash(json.dumps(tx_data, sort_keys=True))
        tx_id = f"missingID_{timestamp_ms}_{data_hash}"
        # <<< Log message và logger
        s3_logger.warning(f"S3: Transaction data missing 'transaction_id'. Using generated ID: {tx_id}. Original Data: {tx_data}")

    # Tạo object name trực tiếp trong thư mục 'transactions/' sử dụng tx_id
    object_name = f"transactions/{tx_id}.json"
    # --------------------------------------------------------------

    try:
        # --- Logic chuẩn bị dữ liệu giữ nguyên ---
        json_bytes = json.dumps(tx_data, indent=2, ensure_ascii=False).encode('utf-8')
        # Không cần tạo stream cho put_object của boto3 khi dùng bytes

        # --- Upload lên S3 bằng boto3 ---
        response = s3_client_param.put_object(
            Bucket=bucket_name,
            Key=object_name,
            Body=json_bytes, # Truyền thẳng bytes
            ContentType='application/json'
        )

        # Kiểm tra thành công dựa trên response (khác với minio client)
        status_code = response.get('ResponseMetadata', {}).get('HTTPStatusCode')
        if status_code == 200:
            # <<< : Log message và logger
            s3_logger.debug(f"S3: Uploaded {object_name} to {bucket_name}, ETag: {response.get('ETag')}")
            return True # Trả về True nếu thành công
        else:
             s3_logger.error(f"S3: Failed to upload {object_name} to {bucket_name}. Status Code: {status_code}. Response: {response}")
             return False # Trả về False nếu thất bại (dựa trên status code)

    # <<< : Bắt lỗi ClientError thay vì S3Error >>>
    except ClientError as client_err:
        s3_logger.error(f"S3: ClientError uploading {object_name} to {bucket_name}: {client_err}") # <<< : Log message
        return False # Trả về False nếu thất bại
    except Exception as e:
        # <<< : Log message và logger
        s3_logger.error(f"S3: Unexpected error uploading {object_name} to {bucket_name}: {e}", exc_info=True)
        return False # Trả về False nếu thất bại

# ===============================================================
# ===============================================================

# --- Callback cho confluent-kafka  ---
def error_cb(err):
    """Callback cho lỗi chung của Consumer."""
    kafka_logger.error(f"Kafka Consumer error: {err}")

def stats_cb(stats_json_str):
    """Callback cho thống kê định kỳ từ librdkafka (JSON string)."""
    kafka_logger.debug(f"Kafka Stats: {stats_json_str}")

def consume_and_process():
    main_logger.info("Starting consumer process with confluent-kafka and AWS S3...")

   
    if not ensure_s3_bucket_exists(s3_client, AWS_S3_BUCKET, AWS_REGION):
        main_logger.critical("S3: Bucket check/creation failed. Exiting.") #
        sys.exit(1)

    # --- Cấu hình Consumer cho confluent-kafka  ---
    conf = {
        'bootstrap.servers': KAFKA_BROKER, 'group.id': KAFKA_CONSUMER_GROUP,
        'security.protocol': 'SASL_SSL', 'sasl.mechanisms': 'PLAIN',
        'sasl.username': KAFKA_API_KEY, 'sasl.password': KAFKA_API_SECRET,
        'ssl.ca.location': certifi.where(), 'auto.offset.reset': 'latest',
        'enable.auto.commit': False, 'fetch.max.bytes': 52428800,
        'max.poll.interval.ms': 300000, 'session.timeout.ms': 30000,
        'heartbeat.interval.ms': 10000, 'error_cb': error_cb,
        # 'stats_cb': stats_cb, 'statistics.interval.ms': 30000
    }
    # --------------------------------------------

    consumer = None
    try:
        # --- Khởi tạo Consumer  ---
        consumer = Consumer(conf)
        kafka_logger.info(f"Kafka: Consumer created with config: { {k:v for k,v in conf.items() if 'password' not in k and 'secret' not in k} }")
        consumer.subscribe([KAFKA_TOPIC])
        kafka_logger.info(f"Kafka: Subscribed to topic '{KAFKA_TOPIC}'")
    except KafkaException as e:
        kafka_logger.critical(f"Kafka: Consumer initialization failed (KafkaException): {e}")
        sys.exit(1)
    except Exception as e:
        kafka_logger.critical(f"Kafka: Consumer initialization failed (Other Exception): {e}")
        sys.exit(1)

    # --- Khởi tạo biến đếm và thời gian  ---
    msg_count = valid_count = invalid_json_count = invalid_data_count = 0
    last_log_time = time.time()
    last_commit_time = time.time()
    commit_interval_secs = 10
    processed_offsets = {}

    try:
         # --- Vòng lặp xử lý chính (Giữ nguyên logic tổng thể) ---
        while True:
            msg = consumer.poll(timeout=1.0)

            # --- Xử lý timeout và commit định kỳ ---
            if msg is None:
                kafka_logger.debug("Kafka: Poll timeout, no new messages.")
                if time.time() - last_commit_time >= commit_interval_secs and processed_offsets:
                    try:
                        offsets_to_commit = [TopicPartition(tp[0], tp[1], off + 1) for tp, off in processed_offsets.items() if off != OFFSET_INVALID]
                        if offsets_to_commit:
                            consumer.commit(offsets=offsets_to_commit, asynchronous=False)
                            kafka_logger.info(f"Kafka: Committed offsets (periodic): { {f'{tp.topic}-{tp.partition}': tp.offset for tp in offsets_to_commit} }")
                            processed_offsets.clear()
                            last_commit_time = time.time()
                    except KafkaException as e:
                        kafka_logger.error(f"Kafka: Failed to commit offsets periodically (KafkaException): {e}")
                    except Exception as e:
                        kafka_logger.error(f"Kafka: Failed to commit offsets periodically (Exception): {e}")
                continue

             # --- Xử lý lỗi Kafka  ---
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    kafka_logger.info(f"Kafka: Reached end of partition: {msg.topic()}-{msg.partition()} at offset {msg.offset()}")
                elif msg.error():
                    kafka_logger.error(f"Kafka: Consumer error: {msg.error()}")
                    if msg.error().code() == KafkaError.OFFSET_OUT_OF_RANGE:
                           kafka_logger.critical(f"Kafka: Offset out of range for {msg.topic()}-{msg.partition()}. Manual intervention likely required.")
                continue

            msg_count += 1
            current_offset = msg.offset()
            tp_key = (msg.topic(), msg.partition())

            main_logger.debug(f"Processing message from {tp_key[0]}-{tp_key[1]} at offset {current_offset}")
            transaction_data = None
            is_valid = False
            error_reason = None
            processing_success = False # Cờ này rất quan trọng cho logic commit

            try:
                # 1. Deserialization 
                raw_value = msg.value()
                if raw_value is None:
                    error_reason = "Message value is None (Tombstone)"
                    main_logger.warning(f"Offset {current_offset}: {error_reason}. Skipping.")
                    processing_success = True # Coi như xử lý thành công
                else:
                    transaction_data = json.loads(raw_value.decode('utf-8'))

                    # 2. Validation
                    is_valid, error_reason = validate_transaction(transaction_data)

                    if is_valid:
                       
                        if upload_to_s3(s3_client, AWS_S3_BUCKET, transaction_data):
                            valid_count += 1
                           
                            validation_logger.debug(f"Offset {current_offset}: Data VALID and uploaded to S3.")
                            processing_success = True
                        else:
                            # Lỗi upload S3, không nên commit offset này 
                            main_logger.error(f"Offset {current_offset}: Data VALID but S3 upload FAILED. Offset will not be committed.")
                            processing_success = False # Đánh dấu xử lý thất bại
                    else:
                        # 3b. Send to Elasticsearch (if invalid data) 
                        invalid_data_count += 1
                        validation_logger.warning(f"Offset {current_offset}: Data INVALID: {error_reason}")
                        send_warning_to_elasticsearch(es_client, ES_INDEX, transaction_data, error_reason, msg)
                        processing_success = True # Coi như xử lý thành công (đã ghi log lỗi)

             # --- Xử lý lỗi JSONDecodeError và Exception khác 
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
                invalid_data_count += 1
                error_reason = f"Unexpected processing error: {e}"
                main_logger.error(f"Offset {current_offset}: {error_reason}", exc_info=True)
                error_context = {"error_context": "Processing failed unexpectedly"}
                if transaction_data: error_context["partially_processed_data"] = transaction_data
                elif 'raw_value' in locals() and raw_value is not None:
                    try: error_context["raw_message_on_error"] = raw_value.decode('utf-8', errors='ignore')
                    except: error_context["raw_message_on_error"] = "Could not decode raw data"
                send_warning_to_elasticsearch(es_client, ES_INDEX, error_context, error_reason, msg)
                processing_success = True # Coi như xử lý thành công (đã ghi log lỗi)

            # ---- Cập nhật offset đã xử lý và kiểm tra commit (Giữ nguyên logic) ----
            if processing_success:
                processed_offsets[tp_key] = current_offset
            else:
                # Xử lý thất bại (ví dụ: lỗi S3 upload), không cập nhật offset
                main_logger.warning(f"Offset {current_offset}: Processing failed for {tp_key}. Offset not updated. Message will be re-polled.")
                time.sleep(1) # Giữ nguyên delay

            # Kiểm tra commit định kỳ 
            current_time = time.time()
            if current_time - last_commit_time >= commit_interval_secs and processed_offsets:
                try:
                    offsets_to_commit = [TopicPartition(tp[0], tp[1], off + 1) for tp, off in processed_offsets.items() if off != OFFSET_INVALID]
                    if offsets_to_commit:
                        consumer.commit(offsets=offsets_to_commit, asynchronous=False)
                        kafka_logger.info(f"Kafka: Committed offsets (periodic): { {f'{tp.topic}-{tp.partition}': tp.offset for tp in offsets_to_commit} }")
                        processed_offsets.clear()
                        last_commit_time = current_time
                except KafkaException as e:
                    kafka_logger.error(f"Kafka: Failed to commit offsets periodically (KafkaException): {e}")
                except Exception as e:
                    kafka_logger.error(f"Kafka: Failed to commit offsets periodically (Exception): {e}")


            # Log progress định kỳ 
            if current_time - last_log_time >= 60: # Log mỗi 60 giây
                main_logger.info(f"Progress: Total Msgs: {msg_count}, Valid (S3 Uploaded): {valid_count}, Invalid JSON: {invalid_json_count}, Invalid Data (ES Logged): {invalid_data_count}")
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
                if processed_offsets:
                    offsets_to_commit = [TopicPartition(tp[0], tp[1], off + 1) for tp, off in processed_offsets.items() if off != OFFSET_INVALID]
                    if offsets_to_commit:
                        main_logger.info(f"Performing final commit: { {f'{tp.topic}-{tp.partition}': tp.offset for tp in offsets_to_commit} }")
                        consumer.commit(offsets=offsets_to_commit, asynchronous=False) # Commit đồng bộ lần cuối
            except Exception as e:
                main_logger.error(f"Kafka: Failed to perform final commit: {e}")
            finally:
                consumer.close()
                kafka_logger.info("Kafka: Consumer closed.")

        main_logger.info(f"Final Stats: Total Msgs: {msg_count}, Valid (S3 Uploaded): {valid_count}, Invalid JSON: {invalid_json_count}, Invalid Data (ES Logged): {invalid_data_count}")
        main_logger.info("Consumer process finished.")


# --- Chạy Consumer ---
if __name__ == "__main__":
    if s3_client is None:
        main_logger.critical("S3: Client unavailable. Cannot upload valid data. Exiting.") # 
        sys.exit(1)
    # --- Kiểm tra es_client ---
    if es_client is None:
        main_logger.warning("ES: Client unavailable. Validation warnings will not be sent.")
      

    consume_and_process() 