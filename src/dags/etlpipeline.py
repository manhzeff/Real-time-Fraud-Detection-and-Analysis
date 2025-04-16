# /opt/airflow/dags/minio_etl_dag.py

from __future__ import annotations

import pendulum
import os

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
# from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator # Alternative for K8s
# from airflow.models import Variable # Không cần import trực tiếp khi dùng Jinja

# Đường dẫn tới thư mục chứa scripts
DAGS_FOLDER = os.path.dirname(__file__)
SCRIPTS_PATH = os.path.join(DAGS_FOLDER, "scripts")

# ID của Airflow Connection bạn đã tạo cho MinIO
MINIO_CONN_ID = "minio_default"

# --- Tạo dictionary env cho các task ---
# Sử dụng Jinja templating để lấy thông tin từ connection và variables
common_minio_env = {
    # Lấy endpoint từ trường 'host' hoặc 'endpoint_url' trong Extra của S3 connection
    # Cách này hơi phức tạp hơn một chút vì phải kiểm tra cả hai
    # Lưu ý: Truy cập Extra cần Airflow phiên bản mới hơn và cú pháp có thể thay đổi nhẹ
    # Cách đơn giản hơn là đặt endpoint vào trường Host của connection nếu Airflow UI cho phép
    "MINIO_ENDPOINT": '{{ conn.' + MINIO_CONN_ID + '.extra_dejson.get("endpoint_url", conn.' + MINIO_CONN_ID + '.extra_dejson.get("host", conn.' + MINIO_CONN_ID + '.host)) }}', # Ưu tiên endpoint_url trong Extra, rồi host trong Extra, cuối cùng là trường Host chính
    "MINIO_ACCESS_KEY": '{{ conn.' + MINIO_CONN_ID + '.login }}', # Login field maps to Access Key
    "MINIO_SECRET_KEY": '{{ conn.' + MINIO_CONN_ID + '.password }}', # Password field maps to Secret Key
    # Lấy thông tin bucket từ Airflow Variables với giá trị mặc định
    "MINIO_SOURCE_BUCKET": '{{ var.value.get("minio_source_bucket", "raw") }}',
    "MINIO_RAW_BUCKET": '{{ var.value.get("minio_raw_bucket", "raw-data") }}',
    "MINIO_PROCESSED_BUCKET": '{{ var.value.get("minio_processed_bucket", "processed") }}',
    "MINIO_SANDBOX_BUCKET": '{{ var.value.get("minio_sandbox_bucket", "sandbox") }}',
    # Biến này không cần thiết nếu script không dùng .env nữa, nhưng giữ lại cũng không sao
    # "AIRFLOW_RUN": "true"
}


with DAG(
    # Quay lại ID gốc hoặc đặt ID mới nếu muốn giữ cả hai DAG
    dag_id="minio_etl_pyspark_pipeline_conn",
    schedule=None,
    start_date=pendulum.datetime(2023, 10, 1, tz="UTC"),
    catchup=False,
    tags=["minio", "etl", "pyspark", "connection"], # Cập nhật tag
    doc_md="""

    """.format(conn_id=MINIO_CONN_ID), # Định dạng docstring với conn_id
) as dag:
    # Task 1: Chạy script lấy dữ liệu nguồn đưa vào Raw
    fetch_raw_data = BashOperator(
        task_id="fetch_and_upload_raw",
        bash_command=f"python {SCRIPTS_PATH}/generate_and_upload_raw.py",
        env=common_minio_env, # Truyền biến môi trường vào task
        doc_md="Runs script to fetch source JSON and upload to MinIO 'raw' bucket."
    )

    # Task 2: Chạy script xử lý dữ liệu bằng PySpark từ Raw sang Processed
    process_raw_data_spark = BashOperator(
        task_id="process_raw_data_spark",
        bash_command=f"python {SCRIPTS_PATH}/process_raw_to_processed.py",
        env=common_minio_env, # Truyền biến môi trường vào task
        doc_md="""Runs PySpark script:
        - Reads JSON from 'raw'.
        - Processes data.
        - Writes Parquet to 'processed'.
        **Note:** Reads config from Airflow Connection. Requires PySpark and correct JARs."""
    )

    # Task 3: Chạy script load (copy) dữ liệu Parquet từ Processed sang Sandbox
    load_to_sandbox = BashOperator(
        task_id="load_processed_to_sandbox",
        bash_command=f"python {SCRIPTS_PATH}/load_processed_to_sandbox.py",
        env=common_minio_env, # Truyền biến môi trường vào task
        doc_md="Runs script to copy processed Parquet dir to 'sandbox' bucket. Reads config from Airflow Connection."
    )

    # Định nghĩa thứ tự chạy các task
    fetch_raw_data >> process_raw_data_spark >> load_to_sandbox