# -*- coding: utf-8 -*-
# /opt/airflow/dags/scripts/train_fraud_model_s3_source_minio_artifacts.py

import pandas as pd
import numpy as np
import time
import yaml
import matplotlib.pyplot as plt
import os
import warnings
import io
import sys # Thêm sys để xử lý lỗi tốt hơn

# AWS S3 Client
import boto3
from botocore.exceptions import ClientError as S3ClientError # Đổi tên để tránh trùng


# Preprocessing & Splitting
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline

# Models
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
import xgboost as xgb
import lightgbm as lgb

# Metrics & Display
from sklearn.metrics import (
    classification_report,
    roc_auc_score,
    average_precision_score,
    confusion_matrix,
    ConfusionMatrixDisplay
)

# Imbalance Handling
from imblearn.over_sampling import SMOTE
from imblearn.under_sampling import RandomUnderSampler

# MLflow
import mlflow
import mlflow.sklearn

# --- Bỏ qua các cảnh báo không quan trọng ---
warnings.filterwarnings('ignore', category=UserWarning)
warnings.filterwarnings('ignore', category=FutureWarning)

# === HÀM ĐỌC CẤU HÌNH ===
# Hàm load_config và get_config_value giữ nguyên như trước
def load_config(config_path: str = '/app/config.yaml') -> dict:
    print(f"Đang tải cấu hình từ: {config_path}")
    if not os.path.exists(config_path):
        print(f"Cảnh báo: Không tìm thấy file cấu hình '{config_path}'. Sử dụng mặc định và env var.")
        return {}
    try:
        with open(config_path, 'r') as f: config = yaml.safe_load(f)
        print("Tải cấu hình YAML thành công.")
        return config if config is not None else {}
    except yaml.YAMLError as e: print(f"Lỗi đọc YAML: {e}. Dùng mặc định và env var."); return {}
    except Exception as e: print(f"Lỗi tải cấu hình: {e}. Dùng mặc định và env var."); return {}

def get_config_value(config: dict, keys: list, env_var: str = None, default=None, expected_type=None):
    value = os.environ.get(env_var) if env_var else None; using_env = False
    if value is not None: using_env = True
    else:
        temp_val = config
        try:
            for key in keys: temp_val = temp_val[key]
            value = temp_val
            if value is None and default is not None: value = default
        except (KeyError, TypeError): value = default
    if value is None: value = default
    # Chuyển đổi kiểu dữ liệu, xử lý boolean từ string
    if expected_type is not None and value is not None:
        try:
            if expected_type == bool: value = str(value).lower() in ('true', '1', 'y', 'yes')
            else: value = expected_type(value)
        except (ValueError, TypeError):
            print(f"Cảnh báo: Không thể chuyển đổi giá trị '{value}' (từ {'env' if using_env else 'config/default'}) sang kiểu {expected_type}. Giữ nguyên giá trị.")
            # Giữ nguyên giá trị nếu không chuyển đổi được, có thể cần xử lý thêm nếu giá trị sai kiểu là nghiêm trọng
            pass
    return value


# === CÁC HÀM CHỨC NĂNG ===

# THAY THẾ: Hàm tải dữ liệu từ AWS S3
def load_data_from_s3(aws_region: str, bucket_name: str, object_key: str) -> pd.DataFrame:
    """
    Tải file Parquet từ AWS S3 và trả về DataFrame.
    """
    print(f"Đang kết nối đến AWS S3 region: {aws_region}")
    print(f"Đang tải dữ liệu Parquet từ bucket='{bucket_name}', object='{object_key}'")
    s3_client = None
    try:
        s3_client = boto3.client('s3', region_name=aws_region)

        response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        parquet_data = response['Body'].read()
        df = pd.read_parquet(io.BytesIO(parquet_data))
        print(f"Tải dữ liệu Parquet từ S3 thành công. Shape: {df.shape}")
        return df
    except S3ClientError as e:
        error_code = e.response.get("Error", {}).get("Code")
        if error_code == 'NoSuchKey':
            print(f"Lỗi S3: Object '{object_key}' không tìm thấy trong bucket '{bucket_name}'.")
        elif error_code == 'NoSuchBucket':
             print(f"Lỗi S3: Bucket '{bucket_name}' không tồn tại hoặc không có quyền truy cập.")
        else:
            print(f"Lỗi S3 ClientError khi tải dữ liệu: {e}")
        raise # Ném lại lỗi để dừng script
    except Exception as e:
        print(f"Lỗi không mong muốn khi tải dữ liệu S3: {e}")
        raise # Ném lại lỗi



def get_preprocessor(X: pd.DataFrame) -> ColumnTransformer:
    # ... (Nội dung hàm giữ nguyên) ...
    print("Đang xác định các cột số và hạng mục...")
    numerical_cols = X.select_dtypes(include=np.number).columns.tolist()
    categorical_cols = X.select_dtypes(include='object').columns.tolist()
    print(f" - Cột số ({len(numerical_cols)}): {numerical_cols}")
    print(f" - Cột hạng mục ({len(categorical_cols)}): {categorical_cols}")
    numerical_transformer = Pipeline(steps=[('scaler', StandardScaler())])
    categorical_transformer = Pipeline(steps=[('onehot', OneHotEncoder(handle_unknown='ignore', sparse_output=False))])
    preprocessor = ColumnTransformer(transformers=[('num', numerical_transformer, numerical_cols), ('cat', categorical_transformer, categorical_cols)],
                                     remainder='passthrough', verbose_feature_names_out=False)
    preprocessor.set_output(transform='pandas'); print("Đã tạo preprocessor.")
    return preprocessor

def preprocess_data(df: pd.DataFrame, target_col: str, resampling_method: str = 'none', test_size: float = 0.2, random_state: int = 42):
    # ... (Nội dung hàm giữ nguyên) ...
    print("\n--- Bắt đầu Tiền xử lý ---")
    if target_col not in df.columns: raise ValueError(f"Cột mục tiêu '{target_col}' không tồn tại.")
    if df.empty: raise ValueError("DataFrame đầu vào rỗng.")
    X = df.drop(target_col, axis=1); y = df[target_col]
    print(f"Phân phối y ban đầu:\n{y.value_counts(normalize=True)}")
    print("Loại bỏ các cột không cần thiết / gây rò rỉ...")
    columns_to_drop = ['transaction_id', 'customer_id', 'customer_name', 'customer_email', 'device_id', 'ip_address', 'merchant_id', 'merchant_name', 'user_agent', 'device_model', 'timestamp', 'transaction_status', 'fraud_rules', 'fraud_score', 'risk_score']
    existing_columns_to_drop = [col for col in columns_to_drop if col in X.columns]
    X = X.drop(columns=existing_columns_to_drop)
    print(f"Đã loại bỏ: {existing_columns_to_drop}. Số cột còn lại: {X.shape[1]}")
    print("Chia dữ liệu Train/Test...")
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=test_size, random_state=random_state, stratify=y)
    print(f"Kích thước: Train={len(X_train)}, Test={len(X_test)}")
    preprocessor = get_preprocessor(X_train)
    print("Fit/transform dữ liệu huấn luyện...")
    X_train_processed = preprocessor.fit_transform(X_train)
    print("Transform dữ liệu kiểm tra...")
    X_test_processed = preprocessor.transform(X_test)
    X_train_final, X_test_final = X_train_processed, X_test_processed
    print("Kích thước X_train_final:", X_train_final.shape); print("Kích thước X_test_final:", X_test_final.shape)
    y_train_final = y_train
    if resampling_method == 'smote':
        print(f"\nÁp dụng SMOTE (random_state={random_state})..."); smote = SMOTE(random_state=random_state)
        try: X_train_final, y_train_final = smote.fit_resample(X_train_final, y_train); print("Hoàn thành SMOTE.")
        except Exception as e: print(f"Lỗi SMOTE: {e}. Dùng dữ liệu gốc.")
    elif resampling_method == 'undersample':
        print(f"\nÁp dụng Random Undersampling (random_state={random_state})..."); rus = RandomUnderSampler(random_state=random_state, sampling_strategy='auto')
        try: X_train_final, y_train_final = rus.fit_resample(X_train_final, y_train); print("Hoàn thành Random Undersampling.")
        except Exception as e: print(f"Lỗi Random Undersampling: {e}. Dùng dữ liệu gốc.")
    else: print(f"\nKhông áp dụng resampling ('{resampling_method}').")
    print("Kích thước X_train_final sau resampling:", X_train_final.shape)
    if len(y_train_final) > 0: print(f"Phân phối y_train_final sau resampling:\n{pd.Series(y_train_final).value_counts(normalize=True)}")
    else: print("Cảnh báo: y_train_final rỗng.")
    print("--- Tiền xử lý hoàn tất ---")
    return X_train_final, y_train_final, X_test_final, y_test, preprocessor

def train_model(X_train, y_train, model_type: str, resampling_method: str, random_state: int = 42, model_params: dict = None):
    # ... (Nội dung hàm train_model giữ nguyên) ...
    print(f"\n--- Bắt đầu Huấn luyện: {model_type} ---")
    model_params = model_params or {}; handle_internal_imbalance = (resampling_method == 'none')
    print(f"Sử dụng xử lý imbalance tích hợp của model: {handle_internal_imbalance}")
    specific_params = model_params.get(model_type, {})
    print(f"Tham số bổ sung cho {model_type}: {specific_params}")
    if model_type == 'LogisticRegression':
        params = {'random_state': random_state, 'class_weight': 'balanced' if handle_internal_imbalance else None,'max_iter': 1000, **specific_params}; model = LogisticRegression(**params)
    elif model_type == 'RandomForest':
        params = {'random_state': random_state, 'n_estimators': 100, 'class_weight': 'balanced' if handle_internal_imbalance else None,'n_jobs': -1, **specific_params}; model = RandomForestClassifier(**params)
    elif model_type == 'XGBoost':
        scale_pos_weight = None
        if handle_internal_imbalance and len(y_train) > 0:
             try: counts = np.bincount(y_train); scale_pos_weight = counts[0] / counts[1] if len(counts) == 2 and counts[1] > 0 else None
             except Exception: pass
        params = {'random_state': random_state, 'use_label_encoder':False, 'scale_pos_weight': scale_pos_weight, 'eval_metric': 'logloss', **specific_params}; model = xgb.XGBClassifier(**params) # Thêm use_label_encoder=False
    elif model_type == 'LightGBM':
        params = {'random_state': random_state, 'class_weight': 'balanced' if handle_internal_imbalance else None, 'objective': 'binary', 'metric': 'binary_logloss', **specific_params}
        params.pop('verbose', None); model = lgb.LGBMClassifier(**params)
    else: raise ValueError(f"Loại mô hình '{model_type}' không được hỗ trợ.")
    start_time = time.time(); print(f"Huấn luyện {model_type} với tham số: {model.get_params()}"); model.fit(X_train, y_train); train_time = time.time() - start_time
    print(f"Thời gian huấn luyện: {train_time:.2f} giây"); print("--- Huấn luyện hoàn tất ---")
    return model, train_time


def evaluate_model(model, X_test, y_test, model_type):
    # ... (Nội dung hàm evaluate_model giữ nguyên) ...
    print("\n--- Bắt đầu Đánh giá ---"); y_pred = model.predict(X_test); y_pred_proba = None
    if hasattr(model, "predict_proba"):
        try: y_pred_proba = model.predict_proba(X_test)[:, 1]
        except Exception as e: print(f"Lỗi predict_proba: {e}")
    report_dict = classification_report(y_test, y_pred, output_dict=True, zero_division=0)
    print("Classification Report:\n", classification_report(y_test, y_pred, zero_division=0)); print("Confusion Matrix:")
    cm = confusion_matrix(y_test, y_pred); print(cm); plot_path = None
    try:
        disp = ConfusionMatrixDisplay(confusion_matrix=cm); fig, ax = plt.subplots()
        disp.plot(cmap=plt.cm.Blues, ax=ax); ax.set_title(f'Confusion Matrix - {model_type}')
        # Lưu vào thư mục tạm, MLflow sẽ upload lên artifact store (MinIO)
        temp_dir = "/tmp/plots"
        os.makedirs(temp_dir, exist_ok=True)
        plot_path = os.path.join(temp_dir, f"confusion_matrix_{model_type}_{time.strftime('%Y%m%d%H%M%S')}.png")
        plt.savefig(plot_path); plt.close(fig)
        print(f"Confusion matrix plot đã lưu tạm tại: {plot_path}")
    except Exception as e: print(f"Không thể tạo/lưu confusion matrix plot: {e}")
    roc_auc, auc_pr = None, None; n_classes_test = len(np.unique(y_test))
    if y_pred_proba is not None and n_classes_test > 1:
        try: roc_auc = roc_auc_score(y_test, y_pred_proba); print(f"AUC-ROC: {roc_auc:.4f}")
        except ValueError as e: print(f"Lỗi AUC-ROC: {e}")
        try: auc_pr = average_precision_score(y_test, y_pred_proba); print(f"AUC-PR: {auc_pr:.4f}")
        except ValueError as e: print(f"Lỗi AUC-PR: {e}")
    metrics = { "roc_auc": roc_auc,"auc_pr": auc_pr,"accuracy": report_dict["accuracy"],"precision_class_0": report_dict["0"]["precision"],"recall_class_0": report_dict["0"]["recall"],"f1_score_class_0": report_dict["0"]["f1-score"],"precision_class_1": report_dict["1"]["precision"],"recall_class_1": report_dict["1"]["recall"],"f1_score_class_1": report_dict["1"]["f1-score"],"macro_avg_precision": report_dict["macro avg"]["precision"],"macro_avg_recall": report_dict["macro avg"]["recall"],"macro_avg_f1_score": report_dict["macro avg"]["f1-score"],"weighted_avg_precision": report_dict["weighted avg"]["precision"],"weighted_avg_recall": report_dict["weighted avg"]["recall"],"weighted_avg_f1_score": report_dict["weighted avg"]["f1-score"],}; metrics = {k: v for k, v in metrics.items() if v is not None}
    print("--- Đánh giá hoàn tất ---"); return metrics, plot_path

def log_child_run_to_mlflow(run_metrics: dict, model, model_flavor: str, train_time: float, model_specific_params: dict, confusion_matrix_plot_path: str = None):
   
    print(f"\n--- Bắt đầu Ghi log MLflow cho Run Con ({model.__class__.__name__}) ---")
    try:
        mlflow.log_params({f'model_param_{k}': v for k, v in model_specific_params.items()})
        mlflow.log_param("actual_training_time", round(train_time, 3))
        print("Đã log parameters của run con.")
        mlflow.log_metrics(run_metrics)
        print("Đã log metrics của run con.")
        log_func = getattr(mlflow, model_flavor, mlflow.sklearn)
        log_func.log_model(model, "model") # Sẽ lưu vào MinIO
        print(f"Đã log model artifact (flavor: {model_flavor}) của run con (vào MinIO).")
        if confusion_matrix_plot_path and os.path.exists(confusion_matrix_plot_path):
            mlflow.log_artifact(confusion_matrix_plot_path, "evaluation_plots") # Sẽ lưu vào MinIO
            print("Đã log confusion matrix plot artifact của run con (vào MinIO).")
            # Không cần xóa file tạm ngay nếu muốn giữ lại để debug, nhưng nên xóa để tránh đầy disk
            try:
                os.remove(confusion_matrix_plot_path)
                print(f"Đã xóa file plot tạm: {confusion_matrix_plot_path}")
            except OSError as e:
                print(f"Lỗi khi xóa file plot tạm {confusion_matrix_plot_path}: {e}")
        else: print("Không tìm thấy file confusion matrix plot để log cho run con.")
        mlflow.set_tag("status", "completed"); print("Đã log tags của run con.")
        print("--- Ghi log MLflow cho Run Con hoàn tất ---")
    except Exception as e:
        print(f"Lỗi trong quá trình ghi log MLflow cho Run Con: {e}")
        mlflow.set_tag("status", "failed")


# === MAIN EXECUTION BLOCK ===
if __name__ == "__main__":

    # --- Tải Cấu hình từ YAML ---
    CONFIG_PATH = "/app/config.yaml"
    config = load_config(CONFIG_PATH)

    # --- Lấy Giá trị Cấu hình ---
    # --- Nguồn dữ liệu: AWS S3 ---
    aws_region = get_config_value(config, ['data_source', 's3', 'region'], 'AWS_REGION')
    aws_source_bucket = get_config_value(config, ['data_source', 's3', 'bucket'], 'AWS_SOURCE_BUCKET')
    aws_source_object = get_config_value(config, ['data_source', 's3', 'object'], 'AWS_SOURCE_OBJECT')
    # Thông tin xác thực AWS S3 nên được quản lý qua IAM Role hoặc biến môi trường AWS chuẩn
    # AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY sẽ được boto3 tự động tìm

    # --- MLflow Configuration ---
    mlflow_tracking_uri = get_config_value(config, ['mlflow_config', 'tracking_uri'], 'MLFLOW_TRACKING_URI') # Phải là URI của PostgreSQL
    mlflow_experiment_name = get_config_value(config, ['mlflow_config', 'experiment_name'], 'MLFLOW_EXPERIMENT_NAME', default='Fraud Detection Multi-Model S3 Source')

    # --- MinIO Configuration (CHO MLFLOW ARTIFACT STORE) ---
    # Lấy thông tin MinIO từ config/env để thiết lập biến môi trường cho MLflow
    minio_endpoint_url = get_config_value(config, ['mlflow_config', 'artifact_store', 'minio', 'endpoint_url'], 'MINIO_ENDPOINT_URL') # Ví dụ: http://minio.example.com:9000
    minio_access_key = os.environ.get("MINIO_ACCESS_KEY") # Nên lấy từ env var
    minio_secret_key = os.environ.get("MINIO_SECRET_KEY") # Nên lấy từ env var
    # minio_artifact_bucket = get_config_value(config, ['mlflow_config', 'artifact_store', 'minio', 'bucket'], 'MINIO_ARTIFACT_BUCKET') # Tên bucket MinIO cho artifacts
    minio_secure = get_config_value(config, ['mlflow_config', 'artifact_store', 'minio', 'secure'], 'MINIO_SECURE', default=False, expected_type=bool) # MinIO có dùng HTTPS không?

    # --- Pipeline Configuration ---
    target_column = get_config_value(config, ['pipeline', 'target_column'], 'TARGET_COLUMN', default='is_fraud')
    test_size = get_config_value(config, ['pipeline', 'test_size'], 'TEST_SIZE', default=0.2, expected_type=float)
    random_state = get_config_value(config, ['pipeline', 'random_state'], 'RANDOM_STATE', default=42, expected_type=int)
    resampling_method = get_config_value(config, ['pipeline', 'resampling_method'], 'RESAMPLING_METHOD', default='none')
    all_model_params_config = config.get('training', {}).get('model_params', {})

    # --- Kiểm tra các giá trị bắt buộc ---
    required_values = {
        'AWS Region': aws_region,
        'AWS Source Bucket': aws_source_bucket,
        'AWS Source Object': aws_source_object,
        'MLflow Tracking URI (PostgreSQL)': mlflow_tracking_uri,
        'MinIO Endpoint URL (for MLflow Artifacts)': minio_endpoint_url,
        'MinIO Access Key (for MLflow Artifacts - via Env Var MINIO_ACCESS_KEY)': minio_access_key,
        'MinIO Secret Key (for MLflow Artifacts - via Env Var MINIO_SECRET_KEY)': minio_secret_key,
        # 'MinIO Artifact Bucket': minio_artifact_bucket # Tên bucket thường nằm trong artifact_root của MLflow server/experiment
    }
    missing_values = [name for name, val in required_values.items() if val is None or str(val).strip() == '']
    if missing_values:
        print(f"Lỗi: Các giá trị cấu hình sau là bắt buộc: {', '.join(missing_values)}")
        sys.exit(1) # Thoát script

    # !!! --- QUAN TRỌNG: Thiết lập biến môi trường cho MLflow để sử dụng MinIO làm Artifact Store --- !!!
    print("\n--- Thiết lập biến môi trường cho MLflow S3 Artifact Store (MinIO) ---")
    os.environ['MLFLOW_S3_ENDPOINT_URL'] = minio_endpoint_url
    os.environ['AWS_ACCESS_KEY_ID'] = minio_access_key # MLflow dùng biến AWS chuẩn
    os.environ['AWS_SECRET_ACCESS_KEY'] = minio_secret_key # MLflow dùng biến AWS chuẩn
    # Nếu MinIO không dùng HTTPS (secure=False), cần báo MLflow bỏ qua TLS verify
    if not minio_secure:
        os.environ['MLFLOW_S3_IGNORE_TLS'] = 'true'
        print("MLFLOW_S3_IGNORE_TLS=true (do MinIO không dùng HTTPS)")
    else:
         # Đảm bảo biến này không tồn tại hoặc là false nếu dùng HTTPS
         if 'MLFLOW_S3_IGNORE_TLS' in os.environ:
              del os.environ['MLFLOW_S3_IGNORE_TLS']
         print("MinIO sử dụng HTTPS (mặc định hoặc cấu hình)")
    print(f"MLFLOW_S3_ENDPOINT_URL={os.environ['MLFLOW_S3_ENDPOINT_URL']}")
    print(f"AWS_ACCESS_KEY_ID set for MLflow.")
    print(f"AWS_SECRET_ACCESS_KEY set for MLflow.")
    print("-------------------------------------------------------------------")


    # --- Khởi tạo MLflow ---
    print(f"Thiết lập MLflow Tracking URI: {mlflow_tracking_uri}")
    mlflow.set_tracking_uri(mlflow_tracking_uri)
    print(f"Sử dụng MLflow Experiment: {mlflow_experiment_name}")
    mlflow.set_experiment(mlflow_experiment_name)
 


    # === BẮT ĐẦU PARENT MLFLOW RUN ===
    parent_run_name = f"MultiModel_{resampling_method}_{time.strftime('%Y%m%d_%H%M%S')}"
    with mlflow.start_run(run_name=parent_run_name) as parent_run:
        parent_run_id = parent_run.info.run_id
        print(f"\nMLflow Parent Run ID: {parent_run_id}")

        # --- Log các tham số chung vào Parent Run ---
        parent_params_to_log = {
            # Nguồn dữ liệu S3
            'data_source_type': 'aws_s3',
            'aws_region': aws_region,
            'aws_source_bucket': aws_source_bucket,
            'aws_source_object': aws_source_object,
            # MLflow artifact store (MinIO) details
            'mlflow_artifact_store_type': 'minio_s3',
            'mlflow_s3_endpoint_url': minio_endpoint_url,
            # Pipeline params
            'target_column': target_column,
            'test_size': test_size,
            'random_state': random_state,
            'resampling_method': resampling_method
        }
        mlflow.log_params(parent_params_to_log)
        if os.path.exists(CONFIG_PATH): mlflow.log_artifact(CONFIG_PATH) # Log file config lên MinIO
        print("Đã log tham số chung và config vào Parent Run.")

        try:
            # 1. Tải dữ liệu từ AWS S3
            df = load_data_from_s3(aws_region, aws_source_bucket, aws_source_object)

            # 2. Tiền xử lý (Chỉ một lần)
            X_train_final, y_train_final, X_test_final, y_test, preprocessor = preprocess_data(
                df, target_column, resampling_method, test_size, random_state
            )

            # --- Log preprocessor vào Parent Run (lên MinIO) ---
            print("Logging preprocessor artifact to Parent Run...")
            preprocess_pipeline = Pipeline([('preprocessor', preprocessor)])
            # MLflow sẽ lưu artifact này vào MinIO do cấu hình artifact store
            mlflow.sklearn.log_model(preprocess_pipeline, "fraud_preprocessor")
            print("Đã log preprocessor artifact vào Parent Run (lên MinIO).")


            # === VÒNG LẶP HUẤN LUYỆN CÁC MÔ HÌNH ===
            supported_models = ['LogisticRegression', 'RandomForest', 'XGBoost', 'LightGBM']
            model_results = {}

            for current_model_type in supported_models:
                # === Bắt đầu Child Run cho model hiện tại ===
                child_run_name = f"{current_model_type}"
                with mlflow.start_run(run_name=child_run_name, nested=True) as child_run:
                    child_run_id = child_run.info.run_id
                    print(f"\n--- Bắt đầu Child Run cho {current_model_type} (ID: {child_run_id}) ---")
                    mlflow.log_param("parent_run_id", parent_run_id)
                    mlflow.log_param("model_type", current_model_type)

                    try:
                        model_specific_params = all_model_params_config.get(current_model_type, {})

                        # 3. Huấn luyện model hiện tại
                        model, train_time = train_model(
                            X_train_final, y_train_final,
                            model_type=current_model_type,
                            resampling_method=resampling_method,
                            random_state=random_state,
                            model_params=all_model_params_config
                        )

                        # 4. Đánh giá model hiện tại
                        metrics, conf_matrix_path = evaluate_model(model, X_test_final, y_test, current_model_type)
                        model_results[current_model_type] = metrics

                        # 5. Ghi log vào Child Run hiện tại (Metrics vào PG, Artifacts vào MinIO)
                        flavor_map = {'LogisticRegression': 'sklearn','RandomForest': 'sklearn','XGBoost': 'xgboost','LightGBM': 'lightgbm'}
                        model_flavor = flavor_map.get(current_model_type, 'sklearn')
                        log_child_run_to_mlflow( # Hàm này giờ sẽ log artifacts lên MinIO
                            run_metrics=metrics,
                            model=model,
                            model_flavor=model_flavor,
                            train_time=train_time,
                            model_specific_params=model_specific_params,
                            confusion_matrix_plot_path=conf_matrix_path
                        )
                        print(f"--- Hoàn thành Child Run cho {current_model_type} ---")

                    except Exception as model_e:
                        print(f"\n*** LỖI TRONG CHILD RUN cho {current_model_type}: {model_e} ***")
                        mlflow.set_tag("status", "failed")
                        mlflow.log_param("error_message", str(model_e))
                        # Không raise lỗi ở đây để các model khác vẫn chạy

            # Kết thúc vòng lặp qua các model

            # (Tùy chọn) So sánh và log model tốt nhất vào Parent Run
            best_model_name = None
            best_f1_score = -1
            if model_results:
                print("\n--- So sánh kết quả các mô hình (dựa trên F1-Score lớp 1) ---")
                for model_name, metrics_dict in model_results.items():
                    f1_class_1 = metrics_dict.get('f1_score_class_1', -1)
                    print(f"- {model_name}: F1 (Class 1) = {f1_class_1:.4f}")
                    if f1_class_1 > best_f1_score:
                        best_f1_score = f1_class_1
                        best_model_name = model_name

                if best_model_name:
                    print(f"==> Mô hình tốt nhất: {best_model_name} (F1 = {best_f1_score:.4f})")
                    mlflow.set_tag("best_model", best_model_name)
                    mlflow.log_metric("best_model_f1_class_1", best_f1_score)
                else:
                    print("Không thể xác định mô hình tốt nhất.")


            print("\n*** QUÁ TRÌNH HUẤN LUYỆN TẤT CẢ MODEL HOÀN TẤT THÀNH CÔNG ***")
            mlflow.set_tag("status", "parent_completed")


        except Exception as e:
            print(f"\n*** LỖI TRONG PARENT RUN: {e} ***")
            import traceback
            traceback.print_exc() # In chi tiết lỗi
            mlflow.set_tag("status", "parent_failed")
            mlflow.log_param("parent_error_message", str(e))
            # Thoát script với lỗi nếu parent run lỗi
            sys.exit(1) # Báo lỗi cho Airflow/hệ thống điều phối