# ERP 資料備份系統

模組化的 ERP 資料備份系統，支援從 Ragic API 獲取資料並上傳至 Google BigQuery。

## 🏗️ 系統架構

系統分為五個獨立模組：

```
erp_backup_main.py          # 主程式整合檔案
├── ragic_client.py         # Ragic API 資料獲取模組
├── data_transformer.py     # 資料轉換模組
├── bigquery_uploader.py    # BigQuery 上傳模組
└── email_notifier.py       # Cloud Logging 電子郵件通知模組
```

## 📋 功能特色

- **模組化設計**：每個功能獨立成模組，易於維護和測試
- **錯誤處理**：完善的錯誤處理和重試機制
- **增量備份**：支援根據最後更新時間進行增量同步
- **批次處理**：支援大量資料的批次上傳
- **電子郵件通知**：自動發送最新日誌記錄至指定信箱
- **Cloud Function**：支援 Google Cloud Function 部署

## 📚 模組說明

### ragic_client.py - Ragic API 客戶端

負責從 Ragic API 獲取資料：

```python
from ragic_client import create_ragic_client

client = create_ragic_client(api_key="your-key", account="your-account")
data = client.fetch_data(sheet_id="your-sheet/1", last_timestamp=1234567890000)
```

**主要功能：**
- 支援分頁獲取大量資料
- 自動重試機制
- 連線測試功能
- 完善的錯誤處理

### data_transformer.py - 資料轉換器

將中文欄位名稱轉換為英文，並進行型別轉換：

```python
from data_transformer import create_transformer

transformer = create_transformer()
transformed = transformer.transform_data(ragic_data)
```

**主要功能：**
- 中文欄位映射為英文
- 自動型別轉換（FLOAT、INTEGER、BOOLEAN）
- 資料驗證和清理
- BigQuery Schema 管理

### bigquery_uploader.py - BigQuery 上傳器

負責將資料上傳至 BigQuery：

```python
from bigquery_uploader import create_uploader

uploader = create_uploader(project_id="your-project")
result = uploader.upload_data(data, dataset_id="dataset", table_id="table")
```

**主要功能：**
- 支援 MERGE 和 INSERT 操作
- 自動建立資料集和資料表
- 批次上傳大量資料
- 備用上傳方案

### email_notifier.py - 電子郵件通知器

從 Google Cloud Logging 獲取最新日誌並發送電子郵件通知：

```python
from email_notifier import create_notifier

notifier = create_notifier(project_id="your-project")
notifier.send_latest_logs(to_email="admin@example.com")
```

**主要功能：**
- 從 Cloud Logging 獲取最新日誌記錄
- 格式化日誌內容
- 發送 HTML 格式的電子郵件
- 支援錯誤和成功通知

### erp_backup_main.py - 主程式

整合所有模組，提供完整的備份流程並自動發送日誌通知：

```python
from erp_backup_main import ERPBackupManager

config = load_config_from_env()
manager = ERPBackupManager(config)
result = manager.run_backup()
```

## ⚙️ 環境變數設定

```bash
# Ragic 設定
export RAGIC_API_KEY="your-ragic-api-key"
export RAGIC_ACCOUNT="your-account"
export RAGIC_SHEET_ID="your-sheet/1"

# BigQuery 設定
export GCP_PROJECT_ID="your-project-id"
export BIGQUERY_DATASET="your_dataset"
export BIGQUERY_TABLE="erp_backup"

# 電子郵件通知設定（可選）
export NOTIFICATION_EMAIL="admin@example.com"
export SMTP_FROM_EMAIL="noreply@example.com"
export SMTP_FROM_PASSWORD="your-app-password"
export SMTP_SERVER="smtp.gmail.com"
export SMTP_PORT="587"

# 其他可選設定
export RAGIC_TIMEOUT="30"
export RAGIC_MAX_RETRIES="3"
export UPLOAD_BATCH_SIZE="1000"
export USE_MERGE="true"
export LOG_LEVEL="INFO"
```