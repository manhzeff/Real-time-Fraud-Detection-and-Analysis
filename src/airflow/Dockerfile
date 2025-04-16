# Thay thế dòng FROM cũ:
# FROM apache/airflow:2.10.5
# Bằng dòng mới với tag Python 3.11 (thay bằng tag chính xác bạn tìm thấy):
    FROM apache/airflow:2.10.5-python3.11

    # ----- Các phần còn lại của Dockerfile giữ nguyên -----
    
    # Switch to the root user
    USER root
    
    # Install Java 17, build tools, and essential packages in one RUN command
    # (Kiểm tra xem base image mới có cần cài lại Java không, có thể đã có sẵn)
        # Thay thế dòng RUN bị lỗi trong Dockerfile của bạn bằng dòng này:
    RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jdk ant && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*
      
    # Set JAVA_HOME for Java 17
    ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64/
    
    # Create directories if needed
    RUN mkdir -p /app/models


    # Switch back to the airflow user before installing Python packages
    USER airflow
    
    # Copy and install Python requirements
    COPY requirements.txt /tmp/requirements.txt
    # Lệnh pip này bây giờ sẽ sử dụng pip của Python 3.11 từ base image mới
    RUN pip install --no-cache-dir -r /tmp/requirements.txt
    
    # Final user should typically be airflow
    USER airflow