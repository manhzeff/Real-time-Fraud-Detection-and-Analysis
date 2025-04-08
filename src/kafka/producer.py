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

# --- Tải biến môi trường ---
load_dotenv()

# --- Cấu hình Kafka từ .env ---
KAFKA_BROKER = os.getenv('CONFLUENT_BOOTSTRAP_SERVERS')
API_KEY = os.getenv('CONFLUENT_API_KEY')
API_SECRET = os.getenv('CONFLUENT_API_SECRET')
TOPIC_NAME = os.getenv('KAFKA_TOPIC', 'transactions')

# --- Cấu hình Producer ---
NUM_TRANSACTIONS = 1000  # Số lượng giao dịch muốn tạo
DELAY_SECONDS = 0.1     # Giảm độ trễ để gửi nhanh hơn
LOG_FILE = 'producer.log'

# --- Kiểm tra cấu hình Kafka ---
if not all([KAFKA_BROKER, API_KEY, API_SECRET, TOPIC_NAME]):
    print("Lỗi: Thiếu cấu hình Kafka/Confluent Cloud trong file .env hoặc biến môi trường.")
    print("Cần: CONFLUENT_BOOTSTRAP_SERVERS, CONFLUENT_API_KEY, CONFLUENT_API_SECRET, KAFKA_TOPIC")
    exit(1)

# --- Thiết lập Logging ---
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
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        security_protocol='SASL_SSL',
        sasl_mechanism='PLAIN',
        sasl_plain_username=API_KEY,
        sasl_plain_password=API_SECRET,
        ssl_cafile=certifi.where(),
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks='all', # Đảm bảo độ bền cao hơn trên Cloud
        retries=5,
        linger_ms=20 # Tăng thời gian gom batch
    )
    producer_logger.info(f"Kết nối Producer thành công tới Confluent Cloud: {KAFKA_BROKER}")
except Exception as e:
    producer_logger.error(f"Không thể kết nối Producer tới Confluent Cloud. Lỗi: {e}")
    exit(1)

# --- Hàm Áp dụng Quy tắc Gian lận (Giữ nguyên) ---
def apply_fraud_rules(transaction):
    rules_triggered = []
    if transaction['amount'] > 900: rules_triggered.append("high_amount")
    if transaction['transaction_type'] == "transfer" and transaction['payment_method'] == "digital_wallet": rules_triggered.append("suspicious_method")
    if transaction.get('fraud_score', 0) > 0.8: rules_triggered.append("high_fraud_score") # Thêm .get để an toàn hơn
    if transaction.get('risk_score', 0) > 0.8: rules_triggered.append("high_risk_score")
    if transaction.get('customer_segment') == "vip" and transaction['amount'] > 500: rules_triggered.append("vip_large_transaction")
    if transaction.get('transaction_hour', 12) < 6 or transaction.get('transaction_hour', 12) > 22: rules_triggered.append("off_hours")
    return rules_triggered

# --- Hàm Tạo Giao dịch (Cập nhật timestamp format) ---
def generate_transaction():
    is_fraud = random.choices([0, 1], weights=[0.97, 0.03])[0] # Giảm tỷ lệ fraud một chút
    amount = round(random.uniform(1, 1000), 2)
    if is_fraud: amount = round(amount * random.uniform(1.5, 3.5), 2) # Tăng biên độ

    location = random.choice(["VN", "US", "UK", "JP", "SG", "AU", "DE", "FR"]) # Thêm địa điểm
    lat_long = {"VN": (21.0285, 105.8542),"US": (37.7749, -122.4194),"UK": (51.5074, -0.1278), "JP": (35.6762, 139.6503),"SG": (1.3521, 103.8198),"AU": (-33.8688, 151.2093), "DE": (52.5200, 13.4050), "FR": (48.8566, 2.3522) }

    # Sử dụng timezone-aware datetime và chuẩn ISO 8601 bao gồm timezone
    txn_time = fake.date_time_this_year(tzinfo=datetime.now().astimezone().tzinfo)

    customer_age = random.randint(18, 80)
    customer_segment = random.choice(["regular", "premium", "vip", "new"])
    loyalty_points = random.randint(0, 15000)
    risk_score = round(random.uniform(0, 1), 3) # Thêm độ chính xác
    fraud_score = round(random.uniform(0.6, 1), 3) if is_fraud else round(random.uniform(0, 0.4), 3) # Điều chỉnh fraud score range

    device_os = random.choice(["Android", "iOS", "Windows", "macOS", "Linux"])
    device_model = fake.bothify(text='Model-##??', letters='ABCXYZ') # Tạo model ngẫu nhiên hơn

    transaction_hour = txn_time.hour
    day_of_week = txn_time.weekday()
    day_of_week_name = calendar.day_name[day_of_week]
    merchant_category = random.choice(["electronics", "clothing", "groceries", "food_delivery", "entertainment", "travel", "utilities", "services", "financial", "gaming"]) # Thêm category

    transaction = {
        "transaction_id": fake.uuid4(),
        "amount": amount,
        "currency": "USD", # Thêm tiền tệ
        "timestamp": txn_time.isoformat(), # Chuẩn ISO 8601 với timezone
        "customer_id": f"CUST{fake.random_int(min=1, max=2000):05d}", # Tăng số lượng KH
        "customer_name": fake.name(),
        "customer_email": fake.email(), # Thêm email
        "customer_age": customer_age,
        "customer_segment": customer_segment,
        "location": location,
        "is_fraud": is_fraud,
        "transaction_type": random.choice(["purchase", "transfer", "withdrawal", "payment", "refund"]), # Thêm refund
        "device_id": fake.uuid4(), 
        "device_os": device_os,
        "device_model": device_model,
        "ip_address": fake.ipv4_public(), # Dùng IP public
        "merchant_id": f"MERCH{fake.random_int(min=1, max=1500):04d}",
        "merchant_name": fake.company(), # Thêm tên merchant
        "merchant_category": merchant_category,
        "user_agent": fake.user_agent(),
        "payment_method": random.choice(["credit_card", "debit_card", "digital_wallet", "bank_transfer", "crypto"]), # Thêm crypto
        "transaction_status": random.choice(["completed", "pending", "failed", "reversed", "flagged"]), # Thêm flagged
        "latitude": round(lat_long[location][0] + random.uniform(-0.05, 0.05), 6),
        "longitude": round(lat_long[location][1] + random.uniform(-0.05, 0.05), 6),
        "account_balance": round(random.uniform(-500, 10000), 2), # Cho phép số dư âm
        "transaction_hour": transaction_hour,
        "day_of_week": day_of_week_name,
        "risk_score": risk_score,
        "fraud_score": fraud_score,
        "loyalty_points": loyalty_points,
        "metadata": {
            "source": "Faker-KafkaGen-v2.0-CC",
            "generated_at": datetime.now().astimezone().isoformat(), # Dùng timezone-aware
            "producer_version": "2.0-CC"
        }
    }
    transaction["fraud_rules"] = apply_fraud_rules(transaction)

    # Kiểm tra chất lượng dữ liệu cơ bản
    if amount <= 0 and transaction["transaction_type"] not in ["refund", "reversal"]: # Cho phép amount <= 0 nếu là refund/reversal
        producer_logger.debug(f"Data quality issue (amount<=0): {transaction['transaction_id']}")
        # return None # Có thể bỏ qua hoặc vẫn gửi tùy yêu cầu
    if is_fraud not in [0, 1]:
        producer_logger.error(f"Critical data quality issue (is_fraud invalid): {transaction['transaction_id']}")
        return None # Nên bỏ qua lỗi nghiêm trọng này

    return transaction

# --- Hàm Gửi Giao dịch ---
def produce_transactions(topic, n_transactions, delay):
    producer_logger.info(f"Bắt đầu tạo và gửi {n_transactions} giao dịch tới topic '{topic}' trên Confluent Cloud")
    sent_count = 0
    error_count = 0
    for i in range(n_transactions):
        transaction = generate_transaction()
        if transaction:
            try:
                # Gửi message không đồng bộ
                producer.send(topic, value=transaction)
                sent_count += 1
                # Log tiến trình ít thường xuyên hơn
                if sent_count % (n_transactions // 10 or 1) == 0: # Log khoảng 10 lần
                    producer_logger.info(f"Đã gửi {sent_count}/{n_transactions} tin nhắn...")
            except Exception as e:
                error_count += 1
                producer_logger.error(f"Gửi tin nhắn thất bại. Lỗi: {e}")
                # Cân nhắc xử lý lỗi ở đây (vd: thử lại sau, ghi vào file lỗi...)
            time.sleep(delay) # Giữ độ trễ nhỏ
        else:
             producer_logger.warning(f"Bỏ qua việc gửi giao dịch thứ {i+1} do vấn đề chất lượng/tạo dữ liệu.")

    producer_logger.info("Hoàn tất việc tạo. Đang chờ gửi hết tin nhắn còn lại (flushing)...")
    try:
        producer.flush(timeout=120) # Tăng timeout cho flush trên cloud
        producer_logger.info(f"Producer flushed. Tổng cộng đã gửi: {sent_count}, Lỗi gửi: {error_count}.")
    except Exception as e:
        producer_logger.error(f"Lỗi trong quá trình flush producer: {e}")
    finally:
        producer.close()
        producer_logger.info("Producer đã đóng kết nối.")

# --- Chạy Producer ---
if __name__ == "__main__":
    produce_transactions(TOPIC_NAME, n_transactions=NUM_TRANSACTIONS, delay=DELAY_SECONDS)
    producer_logger.info("Hoàn thành quá trình producer.")