import logging
from faker import Faker
from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime
import calendar
import os
import certifi
from dotenv import load_dotenv
import cProfile # Thêm để profiling (nếu cần)
import pstats   # Thêm để profiling (nếu cần)

# --- Tải biến môi trường ---
load_dotenv()

# --- Cấu hình Kafka từ .env ---
KAFKA_BROKER = os.getenv('CONFLUENT_BOOTSTRAP_SERVERS')
API_KEY = os.getenv('CONFLUENT_API_KEY')
API_SECRET = os.getenv('CONFLUENT_API_SECRET')
TOPIC_NAME = os.getenv('KAFKA_TOPIC', 'transactions')

# --- Cấu hình Producer ---
NUM_TRANSACTIONS = 2000000  # Tăng số lượng để thấy rõ sự khác biệt
# >>> THAY ĐỔI 1: Loại bỏ hoặc giảm mạnh delay <<<
DELAY_SECONDS = 0       # Đặt về 0 để producer tự quản lý batching
LOG_FILE = 'producer.log'

# --- Kiểm tra cấu hình Kafka ---
# (Giữ nguyên phần kiểm tra)
if not all([KAFKA_BROKER, API_KEY, API_SECRET, TOPIC_NAME]):
    print("Lỗi: Thiếu cấu hình Kafka/Confluent Cloud trong file .env hoặc biến môi trường.")
    print("Cần: CONFLUENT_BOOTSTRAP_SERVERS, CONFLUENT_API_KEY, CONFLUENT_API_SECRET, KAFKA_TOPIC")
    exit(1)

# --- Thiết lập Logging ---
# (Giữ nguyên phần logging)
logging.basicConfig(level=logging.INFO,
                    filename=LOG_FILE,
                    filemode='w',
                    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - %(message)s')
console_handler.setFormatter(formatter)
producer_logger = logging.getLogger('KafkaProducer')
producer_logger.addHandler(console_handler)
producer_logger.setLevel(logging.INFO)


# --- Khởi tạo Faker và Kafka Producer ---
fake = Faker()
try:
    producer_logger.info("Đang khởi tạo Kafka Producer...")
    start_time_init = time.time()
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        security_protocol='SASL_SSL',
        sasl_mechanism='PLAIN',
        sasl_plain_username=API_KEY,
        sasl_plain_password=API_SECRET,
        ssl_cafile=certifi.where(),
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks='1', # Giữ nguyên hoặc đổi thành 1 nếu chấp nhận rủi ro
        retries=5,
        # >>> THAY ĐỔI 2: Tối ưu batching và nén <<<
        linger_ms=5,      # Tăng thời gian gom batch (thử nghiệm các giá trị 50, 100, 200)
        batch_size=16384,   # Tăng kích thước batch (thử nghiệm 32768, 65536, 131072)
        compression_type='gzip' # Thêm nén (chọn 'gzip', 'snappy', 'lz4')
    )
    init_duration = time.time() - start_time_init
    producer_logger.info(f"Kết nối Producer thành công tới Confluent Cloud: {KAFKA_BROKER} (Khởi tạo mất {init_duration:.2f}s)")
except Exception as e:
    producer_logger.error(f"Không thể kết nối Producer tới Confluent Cloud. Lỗi: {e}")
    exit(1)

# --- Hàm Áp dụng Quy tắc Gian lận (Giữ nguyên) ---
def apply_fraud_rules(transaction):
    # (Giữ nguyên logic)
    rules_triggered = []
    if transaction['amount'] > 900: rules_triggered.append("high_amount")
    if transaction['transaction_type'] == "transfer" and transaction['payment_method'] == "digital_wallet": rules_triggered.append("suspicious_method")
    if transaction.get('fraud_score', 0) > 0.8: rules_triggered.append("high_fraud_score")
    if transaction.get('risk_score', 0) > 0.8: rules_triggered.append("high_risk_score")
    if transaction.get('customer_segment') == "vip" and transaction['amount'] > 500: rules_triggered.append("vip_large_transaction")
    if transaction.get('transaction_hour', 12) < 6 or transaction.get('transaction_hour', 12) > 22: rules_triggered.append("off_hours")
    return rules_triggered

# --- Hàm Tạo Giao dịch (Giữ nguyên) ---
# Nếu hàm này chạy chậm, nó cũng sẽ là một điểm nghẽn.
# Có thể profiling hàm này nếu cần.
def generate_transaction():
    # (Giữ nguyên logic tạo dữ liệu)
    # ... (toàn bộ code generate_transaction của bạn) ...
    is_fraud = random.choices([0, 1], weights=[0.97, 0.03])[0]
    amount = round(random.uniform(1, 1000), 2)
    if is_fraud: amount = round(amount * random.uniform(1.5, 3.5), 2)

    location = random.choice(["VN", "US", "UK", "JP", "SG", "AU", "DE", "FR"])
    lat_long = {"VN": (21.0285, 105.8542),"US": (37.7749, -122.4194),"UK": (51.5074, -0.1278), "JP": (35.6762, 139.6503),"SG": (1.3521, 103.8198),"AU": (-33.8688, 151.2093), "DE": (52.5200, 13.4050), "FR": (48.8566, 2.3522) }
    txn_time = fake.date_time_this_year(tzinfo=datetime.now().astimezone().tzinfo)
    customer_age = random.randint(18, 80)
    customer_segment = random.choice(["regular", "premium", "vip", "new"])
    loyalty_points = random.randint(0, 15000)
    risk_score = round(random.uniform(0, 1), 3)
    fraud_score = round(random.uniform(0.6, 1), 3) if is_fraud else round(random.uniform(0, 0.4), 3)
    device_os = random.choice(["Android", "iOS", "Windows", "macOS", "Linux"])
    device_model = fake.bothify(text='Model-##??', letters='ABCXYZ')
    transaction_hour = txn_time.hour
    day_of_week = txn_time.weekday()
    day_of_week_name = calendar.day_name[day_of_week]
    merchant_category = random.choice(["electronics", "clothing", "groceries", "food_delivery", "entertainment", "travel", "utilities", "services", "financial", "gaming"])

    transaction = {
        "transaction_id": fake.uuid4(),
        "amount": amount,
        "currency": "USD",
        "timestamp": txn_time.isoformat(),
        "customer_id": f"CUST{fake.random_int(min=1, max=2000):05d}",
        "customer_name": fake.name(),
        "customer_email": fake.email(),
        "customer_age": customer_age,
        "customer_segment": customer_segment,
        "location": location,
        "is_fraud": is_fraud,
        "transaction_type": random.choice(["purchase", "transfer", "withdrawal", "payment", "refund"]),
        "device_id": fake.uuid4(),
        "device_os": device_os,
        "device_model": device_model,
        "ip_address": fake.ipv4_public(),
        "merchant_id": f"MERCH{fake.random_int(min=1, max=1500):04d}",
        "merchant_name": fake.company(),
        "merchant_category": merchant_category,
        "user_agent": fake.user_agent(),
        "payment_method": random.choice(["credit_card", "debit_card", "digital_wallet", "bank_transfer", "crypto"]),
        "transaction_status": random.choice(["completed", "pending", "failed", "reversed", "flagged"]),
        "latitude": round(lat_long[location][0] + random.uniform(-0.05, 0.05), 6),
        "longitude": round(lat_long[location][1] + random.uniform(-0.05, 0.05), 6),
        "account_balance": round(random.uniform(-500, 10000), 2),
        "transaction_hour": transaction_hour,
        "day_of_week": day_of_week_name,
        "risk_score": risk_score,
        "fraud_score": fraud_score,
        "loyalty_points": loyalty_points,
    }
    transaction["fraud_rules"] = apply_fraud_rules(transaction)

    # (Giữ nguyên kiểm tra chất lượng)
    if amount <= 0 and transaction["transaction_type"] not in ["refund", "reversal"]:
        producer_logger.debug(f"Data quality issue (amount<=0): {transaction['transaction_id']}")
    if is_fraud not in [0, 1]:
        producer_logger.error(f"Critical data quality issue (is_fraud invalid): {transaction['transaction_id']}")
        return None

    return transaction

# --- Hàm Gửi Giao dịch (Cập nhật logging để đo tốc độ) ---
def produce_transactions(topic, n_transactions, delay):
    producer_logger.info(f"Bắt đầu tạo và gửi {n_transactions} giao dịch tới topic '{topic}' trên Confluent Cloud với cấu hình tối ưu")
    start_time = time.time()
    sent_count = 0
    error_count = 0
    generation_time_total = 0
    send_call_time_total = 0

    # Callback để xử lý kết quả gửi (bất đồng bộ)
    def on_send_success(record_metadata):
        nonlocal sent_count
        sent_count += 1
        # Log ít thường xuyên hơn để không ảnh hưởng hiệu năng
        if sent_count % (n_transactions // 20 or 1) == 0: # Log khoảng 20 lần
             producer_logger.debug(f"Gửi thành công tin nhắn {sent_count}/{n_transactions} tới partition {record_metadata.partition} @ offset {record_metadata.offset}")

    def on_send_error(excp):
        nonlocal error_count
        error_count += 1
        producer_logger.error(f'Gửi tin nhắn thất bại!', exc_info=excp)

    for i in range(n_transactions):
        gen_start = time.perf_counter()
        transaction = generate_transaction()
        gen_end = time.perf_counter()
        generation_time_total += (gen_end - gen_start)

        if transaction:
            try:
                send_call_start = time.perf_counter()
                # Gửi message không đồng bộ và gắn callback
                producer.send(topic, value=transaction).add_callback(on_send_success).add_errback(on_send_error)
                send_call_end = time.perf_counter()
                send_call_time_total += (send_call_end - send_call_start)

                # >>> Loại bỏ time.sleep(delay) ở đây <<<
            except Exception as e:
                # Lỗi này thường xảy ra nếu hàng đợi nội bộ của producer đầy
                error_count += 1
                producer_logger.error(f"Lỗi khi gọi producer.send(): {e}")
                # Có thể thêm sleep ngắn ở đây nếu producer bị quá tải cục bộ
                # time.sleep(0.1)
        else:
            producer_logger.warning(f"Bỏ qua việc gửi giao dịch thứ {i+1} do vấn đề chất lượng/tạo dữ liệu.")

        # Log tiến trình ít hơn
        if (i + 1) % (n_transactions // 10 or 1) == 0:
             current_time = time.time()
             elapsed = current_time - start_time
             rate = (i + 1) / elapsed if elapsed > 0 else 0
             producer_logger.info(f"Đã xử lý {i + 1}/{n_transactions} tin nhắn... ({rate:.2f} msg/s)")


    producer_logger.info("Hoàn tất vòng lặp tạo tin nhắn. Đang chờ gửi hết tin nhắn còn lại (flushing)...")
    flush_start_time = time.time()
    try:
        producer.flush(timeout=120) # Giữ timeout đủ lớn cho cloud
        flush_duration = time.time() - flush_start_time
        producer_logger.info(f"Producer flushed sau {flush_duration:.2f}s.")
    except Exception as e:
        producer_logger.error(f"Lỗi trong quá trình flush producer: {e}")
    finally:
        producer.close()
        producer_logger.info("Producer đã đóng kết nối.")

    end_time = time.time()
    total_duration = end_time - start_time
    actual_sent = sent_count # Số lượng thực sự được callback xác nhận thành công
    throughput = actual_sent / total_duration if total_duration > 0 else 0

    producer_logger.info("--- THỐNG KÊ ---")
    producer_logger.info(f"Tổng thời gian chạy: {total_duration:.2f} giây")
    producer_logger.info(f"Số tin nhắn yêu cầu gửi: {n_transactions}")
    producer_logger.info(f"Số tin nhắn gửi thành công (đã xác nhận): {actual_sent}")
    producer_logger.info(f"Số lỗi khi gửi (producer.send hoặc callback): {error_count}")
    producer_logger.info(f"Throughput trung bình: {throughput:.2f} tin nhắn/giây")
    producer_logger.info(f"Thời gian trung bình tạo 1 tin nhắn: {(generation_time_total / n_transactions * 1000):.4f} ms")
    producer_logger.info(f"Thời gian trung bình gọi producer.send(): {(send_call_time_total / n_transactions * 1000):.4f} ms")


# --- Chạy Producer ---
if __name__ == "__main__":
    # --- Tùy chọn: Profiling để tìm điểm nghẽn trong code Python ---
    # profiler = cProfile.Profile()
    # profiler.enable()

    produce_transactions(TOPIC_NAME, n_transactions=NUM_TRANSACTIONS, delay=DELAY_SECONDS)

    # profiler.disable()
    # producer_logger.info("--- PROFILING RESULTS ---")
    # stats = pstats.Stats(profiler).sort_stats('cumulative') # hoặc 'tottime'
    # stats.print_stats(20) # In 20 dòng tốn thời gian nhất

    producer_logger.info("Hoàn thành quá trình producer.")