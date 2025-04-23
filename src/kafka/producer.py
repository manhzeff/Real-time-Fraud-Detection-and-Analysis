import logging
import json
import time
import random
import calendar
import os
import certifi
from datetime import datetime
from faker import Faker
# Thay đổi import: Sử dụng Producer từ confluent_kafka
from confluent_kafka import Producer, KafkaError, KafkaException 
from dotenv import load_dotenv

# --- Tải biến môi trường ---
load_dotenv()

# --- Cấu hình và Hằng số ---
KAFKA_BROKER = os.getenv('CONFLUENT_BOOTSTRAP_SERVERS')
API_KEY = os.getenv('CONFLUENT_API_KEY')
API_SECRET = os.getenv('CONFLUENT_API_SECRET')
TOPIC_NAME = os.getenv('KAFKA_TOPIC', 'transactions')
NUM_TRANSACTIONS = 100
LOG_FILE = 'producer_confluent.log'

# --- Thiết lập Logging (Không thay đổi) ---
def setup_logging(log_file):
    """Cấu hình logging cho ứng dụng."""
    logging.basicConfig(level=logging.INFO,
                        filename=log_file,
                        filemode='w',
                        format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - %(message)s')
    console_handler.setFormatter(formatter)

    logger = logging.getLogger('ConfluentKafkaProducer')
    if logger.hasHandlers():
        logger.handlers.clear()
    logger.addHandler(console_handler)
    logger.setLevel(logging.INFO)
    return logger

# --- Class Chính: TransactionProducer ---
class TransactionProducer:
    """
    Quản lý việc tạo và gửi giao dịch giả lập tới Kafka sử dụng confluent-kafka.
    """
    def __init__(self, bootstrap_servers, api_key, api_secret, topic, logger):
        """Khởi tạo Producer."""
        self.bootstrap_servers = bootstrap_servers
        self.api_key = api_key
        self.api_secret = api_secret
        self.topic = topic
        self.logger = logger
        self.fake = Faker()
        self.producer = self._initialize_producer()

        # Biến theo dõi thống kê
        self.delivered_count = 0 # Đổi tên để rõ hơn là đã được giao thành công
        self.error_count = 0
        self.generation_time_total = 0
        self.produce_call_time_total = 0 # Đổi tên để khớp với tên phương thức

    def _initialize_producer(self):
        """Khởi tạo đối tượng confluent_kafka.Producer với cấu hình."""
        if not all([self.bootstrap_servers, self.api_key, self.api_secret, self.topic]):
            self.logger.error("Lỗi: Thiếu cấu hình Kafka/Confluent Cloud.")
            self.logger.error("Cần: CONFLUENT_BOOTSTRAP_SERVERS, CONFLUENT_API_KEY, CONFLUENT_API_SECRET, KAFKA_TOPIC")
            raise ValueError("Thiếu cấu hình Kafka")

        # --- Cấu hình cho confluent-kafka ---
    
        conf = {
            'bootstrap.servers': self.bootstrap_servers,
            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms': 'PLAIN', # Chú ý 'mechanisms' số nhiều
            'sasl.username': self.api_key,
            'sasl.password': self.api_secret,
            'ssl.ca.location': certifi.where(), # Đường dẫn tới CA certs
            # Tối ưu hóa hiệu năng (tên tham số cũng khác)
            'acks': 'all',          # Đảm bảo tin nhắn được ghi nhận bởi tất cả ISR
            'retries': 5,           # Số lần thử lại nếu gửi thất bại
            'linger.ms': 5,         # Thời gian chờ (ms) để gom tin nhắn thành batch
            'batch.size': 16384 * 4,    # Kích thước tối đa batch (bytes) (có thể điều chỉnh)
            'compression.type': 'gzip', # Loại nén
            'enable.idempotence': 'true', # Bật producer idempotent để tránh trùng lặp (yêu cầu acks='all', retries > 0, max.in.flight.requests.per.connection <= 5)
            'max.in.flight.requests.per.connection': 5, # Giới hạn số request đang bay trên mỗi connection
             # Buffer memory cho producer
            'queue.buffering.max.messages': 100000, # Số message tối đa trong hàng đợi nội bộ
            'queue.buffering.max.ms': 1000,       # Thời gian tối đa buffer message
            # Callback cho lỗi chung của producer (ví dụ: disconnect)
            'error_cb': self._producer_error_callback,
          
        }
        # ---------------------------------------

        try:
            self.logger.info("Đang khởi tạo Confluent Kafka Producer với cấu hình:")
            # Log cấu hình (trừ password) để dễ debug
            for key, value in conf.items():
                if 'password' in key.lower(): # Kiểm tra key chứa 'password' (không phân biệt hoa thường)
                    self.logger.info(f"  {key}: ******")
                elif callable(value): # Nếu giá trị là một hàm/phương thức
                    self.logger.info(f"  {key}: <function {value.__name__}>") # Log tên của hàm/phương thức
                else:
                    self.logger.info(f"  {key}: {value}") # Log các giá trị khác bình thường


            start_time_init = time.time()
            # Khởi tạo Producer với dictionary config
            producer = Producer(conf)
            init_duration = time.time() - start_time_init
            self.logger.info(f"Khởi tạo Producer thành công tới {self.bootstrap_servers} (mất {init_duration:.2f}s)")
            return producer
        # Bắt lỗi cụ thể của confluent-kafka
        except KafkaException as e:
            self.logger.error(f"Lỗi KafkaException khi khởi tạo Producer: {e}", exc_info=True)
            raise ConnectionError(f"Không thể khởi tạo Kafka Producer: {e}") from e
        except Exception as e:
            self.logger.error(f"Lỗi không mong muốn khi khởi tạo Producer: {e}", exc_info=True)
            raise ConnectionError(f"Lỗi không mong muốn khi khởi tạo Kafka: {e}") from e

    def _producer_error_callback(self, kafka_error):
        """Callback xử lý lỗi chung của producer (không phải lỗi gửi từng message)."""
        self.logger.error(f"Lỗi Producer toàn cục: {kafka_error.str()}")
        # Có thể thêm logic xử lý tại đây, ví dụ: thử kết nối lại hoặc dừng hẳn

    def _apply_fraud_rules(self, transaction):
        """Áp dụng các quy tắc phát hiện gian lận đơn giản (Không thay đổi)."""
        rules_triggered = []
        if transaction['amount'] > 900: rules_triggered.append("high_amount")
        if transaction['transaction_type'] == "transfer" and transaction['payment_method'] == "digital_wallet": rules_triggered.append("suspicious_method")
        if transaction.get('fraud_score', 0) > 0.8: rules_triggered.append("high_fraud_score")
        if transaction.get('risk_score', 0) > 0.8: rules_triggered.append("high_risk_score")
        if transaction.get('customer_segment') == "vip" and transaction['amount'] > 500: rules_triggered.append("vip_large_transaction")
        if transaction.get('transaction_hour', 12) < 6 or transaction.get('transaction_hour', 12) > 22: rules_triggered.append("off_hours")
        return rules_triggered

    def _generate_transaction(self):
        """Tạo một bản ghi giao dịch giả lập (Không thay đổi logic tạo dữ liệu)."""
        is_fraud = random.choices([0, 1], weights=[0.97, 0.03])[0]
        amount = round(random.uniform(1, 1000), 2)
        if is_fraud: amount = round(amount * random.uniform(1.5, 3.5), 2)

        location = random.choice(["VN", "US", "UK", "JP", "SG", "AU", "DE", "FR"])
        lat_long = {"VN": (21.0285, 105.8542),"US": (37.7749, -122.4194),"UK": (51.5074, -0.1278), "JP": (35.6762, 139.6503),"SG": (1.3521, 103.8198),"AU": (-33.8688, 151.2093), "DE": (52.5200, 13.4050), "FR": (48.8566, 2.3522) }
        txn_time = self.fake.date_time_this_year(tzinfo=datetime.now().astimezone().tzinfo)
        customer_age = random.randint(18, 80)
        customer_segment = random.choice(["regular", "premium", "vip", "new"])
        loyalty_points = random.randint(0, 15000)
        risk_score = round(random.uniform(0, 1), 3)
        fraud_score = round(random.uniform(0.6, 1), 3) if is_fraud else round(random.uniform(0, 0.4), 3)
        device_os = random.choice(["Android", "iOS", "Windows", "macOS", "Linux"])
        transaction_hour = txn_time.hour
        day_of_week = txn_time.weekday()
        day_of_week_name = calendar.day_name[day_of_week]
        merchant_category = random.choice(["electronics", "clothing", "groceries", "food_delivery", "entertainment", "travel", "utilities", "services", "financial", "gaming"])

        transaction = {
            "transaction_id": self.fake.uuid4(),
            "amount": amount,
            "currency": "USD",
            "timestamp": txn_time.isoformat(),
            "customer_id": f"CUST{self.fake.random_int(min=1, max=2000):05d}",
            "customer_name": self.fake.name(),
            "customer_email": self.fake.email(),
            "customer_age": customer_age,
            "customer_segment": customer_segment,
            "location": location,
            "is_fraud": is_fraud,
            "transaction_type": random.choice(["purchase", "transfer", "withdrawal", "payment", "refund"]),
            "device_id": self.fake.uuid4(),
            "device_os": device_os,
            "device_model": self.fake.bothify(text='Model-##??', letters='ABCXYZ'),
            "ip_address": self.fake.ipv4_public(),
            "merchant_id": f"MERCH{self.fake.random_int(min=1, max=1500):04d}",
            "merchant_name": self.fake.company(),
            "merchant_category": merchant_category,
            "user_agent": self.fake.user_agent(),
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
        transaction["fraud_rules"] = self._apply_fraud_rules(transaction)

        # Kiểm tra chất lượng dữ liệu cơ bản
        if amount <= 0 and transaction["transaction_type"] not in ["refund", "reversal"]:
            self.logger.debug(f"Data quality issue (amount<=0): {transaction['transaction_id']}")
        if is_fraud not in [0, 1]:
            self.logger.error(f"Critical data quality issue (is_fraud invalid): {transaction['transaction_id']}")
            return None

        return transaction

    # --- Callback cho confluent-kafka ---
    def delivery_report(self, err, msg, total_transactions=0):
        """
        Callback được gọi bởi confluent-kafka khi tin nhắn được gửi thành công hoặc thất bại.
        err (KafkaError): Lỗi (None nếu thành công).
        msg (Message): Thông tin về tin nhắn.
        total_transactions: Tổng số tin nhắn dự kiến gửi (chỉ dùng để log ít hơn).
        """
        if err is not None:
            self.error_count += 1
            # Log chi tiết lỗi từ KafkaError
            self.logger.error(f"Gửi tin nhắn thất bại: {err.str()}") # err.str() cung cấp mô tả lỗi
        
        else:
            self.delivered_count += 1
          
            log_frequency = max(1, total_transactions // 50)
            if self.delivered_count % log_frequency == 0:
                self.logger.debug(f"Gửi thành công tin nhắn {self.delivered_count}/{total_transactions} "
                                  f"tới topic {msg.topic()} partition {msg.partition()} @ offset {msg.offset()}")

    # ------------------------------------

    def produce(self, n_transactions):
        """Tạo và gửi số lượng giao dịch được chỉ định."""
        if not self.producer:
            self.logger.error("Producer chưa được khởi tạo. Không thể gửi tin nhắn.")
            return

        self.logger.info(f"Bắt đầu tạo và gửi {n_transactions} giao dịch tới topic '{self.topic}'")
        start_time = time.time()
        self.delivered_count = 0 # Reset counters for this run
        self.error_count = 0
        self.generation_time_total = 0
        self.produce_call_time_total = 0
        produced_count = 0 # Đếm số lần gọi produce() thành công (chưa chắc đã delivered)

        for i in range(n_transactions):
            gen_start = time.perf_counter()
            transaction = self._generate_transaction()
            gen_end = time.perf_counter()
            self.generation_time_total += (gen_end - gen_start)

            if transaction:
                try:
                    # --- Thay đổi cách gửi ---
                    # 1. Serialize dữ liệu TRƯỚC KHI gọi produce()
                    serialized_value = json.dumps(transaction).encode('utf-8')


                    produce_call_start = time.perf_counter()
                    self.producer.produce(
                        self.topic,
                        value=serialized_value,
                        key=transaction['transaction_id'].encode('utf-8'), # Thêm key để phân phối tốt hơn (optional)
                        # Sử dụng lambda để truyền tham số bổ sung vào callback
                        callback=lambda err, msg, count=n_transactions: self.delivery_report(err, msg, total_transactions=count)
                        # hoặc chỉ cần callback=self.delivery_report nếu không cần total_transactions
                        # callback=self.delivery_report
                    )
                    produce_call_end = time.perf_counter()
                    self.produce_call_time_total += (produce_call_end - produce_call_start)
                    produced_count += 1 # Tăng bộ đếm đã gọi produce

                    self.producer.poll(0)
                    # -------------------------

                # Bắt lỗi BufferError nếu hàng đợi nội bộ của producer đầy
                except BufferError:
                    self.error_count += 1
                    self.logger.warning(f"Hàng đợi producer đầy (BufferError). Tạm dừng và chờ poll/flush. "
                                        f"Đã thử gửi {produced_count}/{n_transactions}.")
                    # Khi buffer đầy, cần gọi poll() hoặc flush() để giải phóng
                    # poll(1) chờ tối đa 1s để xử lý event/callback
                    self.producer.poll(1)
                    # Thử gửi lại message hiện tại (hoặc bỏ qua nếu logic yêu cầu)
                    # Ở đây ta bỏ qua và đi tiếp vòng lặp, lỗi đã được đếm
                    continue # Bỏ qua message này và đi tiếp

                except KafkaException as e:
                    self.error_count += 1
                    self.logger.error(f"Lỗi KafkaException khi gọi producer.produce(): {e}")
                    # Có thể thêm logic dừng hoặc thử lại phức tạp hơn ở đây
                except Exception as e:
                    self.error_count += 1
                    self.logger.error(f"Lỗi không mong muốn khi gọi producer.produce(): {e}", exc_info=True)

            else:
                self.logger.warning(f"Bỏ qua việc gửi giao dịch thứ {i+1} do vấn đề chất lượng/tạo dữ liệu.")

            # Log tiến trình ít hơn
            log_frequency_progress = max(1, n_transactions // 20) # Log khoảng 20 lần
            if (i + 1) % log_frequency_progress == 0:
                current_time = time.time()
                elapsed = current_time - start_time
                # Tính rate dựa trên số message đã gọi produce() hoặc delivered_count
                rate_produced = produced_count / elapsed if elapsed > 0 else 0
                rate_delivered = self.delivered_count / elapsed if elapsed > 0 else 0
                pending_messages = len(self.producer) # Số message đang chờ trong hàng đợi
                self.logger.info(f"Đã xử lý {i + 1}/{n_transactions}. Đã gọi produce(): {produced_count}. Delivered: {self.delivered_count}. Lỗi: {self.error_count}. "
                                 f"Rate (produced): {rate_produced:.2f} msg/s. Rate (delivered): {rate_delivered:.2f} msg/s. Pending: {pending_messages}")


        # Hoàn tất quá trình gửi sau vòng lặp
        self._finalize_sending(start_time, n_transactions, produced_count)

    def _finalize_sending(self, start_time, n_transactions, produced_count):
        """Hoàn tất quá trình gửi: flush producer và in thống kê."""
        self.logger.info(f"Hoàn tất vòng lặp tạo {n_transactions} tin nhắn (đã gọi produce() {produced_count} lần).")
        self.logger.info(f"Đang chờ gửi hết tin nhắn còn lại trong hàng đợi (flushing)... Số tin nhắn chờ: {len(self.producer)}")

        flush_start_time = time.time()
        try:

            if self.producer:
                remaining = self.producer.flush(timeout=120) # Timeout 120 giây
                flush_duration = time.time() - flush_start_time
                if remaining > 0:
                    self.logger.warning(f"Producer flushed sau {flush_duration:.2f}s, nhưng vẫn còn {remaining} tin nhắn chưa được gửi/xác nhận (có thể do timeout hoặc lỗi trước đó).")
                    # Những tin nhắn này có thể đã bị lỗi và callback lỗi đã được gọi
                    # Hoặc flush timeout trước khi xác nhận cuối cùng đến
                else:
                    self.logger.info(f"Producer flushed thành công sau {flush_duration:.2f}s. Tất cả {produced_count} tin nhắn đã được xử lý (gửi thành công hoặc báo lỗi).")

        except Exception as e:
            self.logger.error(f"Lỗi trong quá trình flush producer: {e}", exc_info=True)
        # Không cần gọi close() với confluent-kafka, flush() là đủ trước khi thoát

        end_time = time.time()
        total_duration = end_time - start_time
        # actual_sent giờ là self.delivered_count (số callback thành công đã nhận)
        throughput = self.delivered_count / total_duration if total_duration > 0 else 0

        # In thống kê
        self.logger.info("--- THỐNG KÊ CUỐI CÙNG ---")
        self.logger.info(f"Tổng thời gian chạy: {total_duration:.2f} giây")
        self.logger.info(f"Số tin nhắn yêu cầu tạo: {n_transactions}")
        self.logger.info(f"Số lần gọi producer.produce(): {produced_count}")
        self.logger.info(f"Số tin nhắn gửi thành công (đã nhận callback xác nhận): {self.delivered_count}")
        self.logger.info(f"Số lỗi khi gửi (BufferError hoặc callback lỗi): {self.error_count}")
        self.logger.info(f"Số tin nhắn có thể chưa được xác nhận (do flush timeout/lỗi): {produced_count - self.delivered_count - self.error_count}")
        self.logger.info(f"Throughput (dựa trên delivered): {throughput:.2f} tin nhắn/giây")

        avg_gen_time_ms = (self.generation_time_total / n_transactions * 1000) if n_transactions > 0 else 0
        avg_produce_call_time_ms = (self.produce_call_time_total / produced_count * 1000) if produced_count > 0 else 0

        self.logger.info(f"Thời gian trung bình tạo 1 tin nhắn: {avg_gen_time_ms:.4f} ms")
        if produced_count > 0:
             self.logger.info(f"Thời gian trung bình gọi producer.produce(): {avg_produce_call_time_ms:.4f} ms")
        else:
             self.logger.info("Không có lần gọi producer.produce() nào thành công.")

    def close(self):
        """
        Đảm bảo producer được flush trước khi đối tượng bị hủy.
        Trong confluent-kafka, flush() là hành động chính cần làm.
        Không có phương thức close() tường minh như kafka-python.
        """
        if self.producer:
            self.logger.info("Đảm bảo producer đã flush trước khi kết thúc...")
            try:
                remaining = self.producer.flush(timeout=30) # Flush lần cuối với timeout ngắn hơn
                if remaining > 0:
                     self.logger.warning(f"Flush cuối cùng còn {remaining} tin nhắn chưa xử lý.")
            except Exception as e:
                 self.logger.error(f"Lỗi khi flush lần cuối: {e}")
            finally:
                self.producer = None # Đánh dấu là đã xử lý
                self.logger.info("Producer đã được flush (hoặc timeout).")


# --- Chạy Producer ---
if __name__ == "__main__":
    logger = setup_logging(LOG_FILE)
    logger.info("Bắt đầu ứng dụng Producer với confluent-kafka...")
    producer_instance = None # Khởi tạo để dùng trong finally

    try:
        # Khởi tạo đối tượng Producer
        producer_instance = TransactionProducer(
            bootstrap_servers=KAFKA_BROKER,
            api_key=API_KEY,
            api_secret=API_SECRET,
            topic=TOPIC_NAME,
            logger=logger
        )

        # Chạy quá trình tạo và gửi
        producer_instance.produce(n_transactions=NUM_TRANSACTIONS)

    except (ValueError, ConnectionError) as config_or_conn_err:
        # Lỗi đã được log trong quá trình khởi tạo
        logger.critical(f"Không thể khởi tạo hoặc kết nối Producer. Lỗi: {config_or_conn_err}. Thoát ứng dụng.")
        exit(1)
    except KeyboardInterrupt:
         logger.warning("Phát hiện KeyboardInterrupt. Đang dừng producer...")
         # Nếu đang chạy, finalize sẽ được gọi trong finally
    except Exception as e:
        logger.critical(f"Lỗi không mong muốn xảy ra trong luồng chính: {e}", exc_info=True)
        exit(1)
    finally:
        # Đảm bảo producer được flush ngay cả khi có lỗi hoặc ngắt giữa chừng
        if producer_instance and producer_instance.producer:
            logger.info("Thực hiện finalize/flush trong khối finally...")
            # Gọi finalize nếu chưa kết thúc bình thường, hoặc gọi close để flush lần cuối
            producer_instance.close() # Phương thức này giờ chỉ là wrapper cho flush
        logger.info("Hoàn thành quá trình producer.")