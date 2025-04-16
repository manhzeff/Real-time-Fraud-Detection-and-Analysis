# Dockerfile for the Kafka Producer

# Chọn base image Python (sử dụng phiên bản bạn đang phát triển hoặc phiên bản ổn định)
FROM python:3.11.4-slim

# Đặt thư mục làm việc bên trong container
WORKDIR /app

# Sao chép file requirements vào thư mục làm việc
COPY requirements.txt .

# Cài đặt các thư viện Python cần thiết
# --no-cache-dir giúp giảm kích thước image
RUN pip install --no-cache-dir -r requirements.txt

# Sao chép mã nguồn producer vào thư mục làm việc
# Lưu ý: Nếu bạn dùng volumes trong docker-compose để mount code, bước này có thể không cần thiết
# Nhưng việc copy code vào image giúp image tự chứa đựng hơn.
# COPY producer.py .



# Sao chép cả hai file script vào
COPY producer.py .
COPY consumer.py .

# Chạy producer trước, nếu thành công (&&) thì chạy consumer
CMD ["sh", "-c", "echo 'Starting producer...' && python producer.py && echo 'Producer finished. Starting consumer...' && python consumer.py && echo 'Consumer finished.'"]