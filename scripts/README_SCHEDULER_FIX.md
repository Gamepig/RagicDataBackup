# Cloud Scheduler OIDC 修復指南

## 概述

本指南提供完整的 Cloud Scheduler 觸發 Cloud Functions Gen2 OIDC 認證問題修復流程。

**問題症狀**：
- Cloud Scheduler 觸發 `erp-backup` 函式時出現 401 Unauthorized
- 函式逾時或無回應
- 日誌中無執行記錄

**根本原因**：
- Cloud Scheduler 的目標 URL 或 OIDC audience 與實際 Cloud Functions URL 不一致
- Gen2 函式背後使用 Cloud Run 安全機制，要求 ID Token 的 `aud` 與驗證端點完全匹配

**解決方案**：
- 將 Scheduler 的 `--uri` 和 `--oidc-token-audience` 都對齊到 Cloud Functions URL
- 使用 App Engine 預設服務帳號簽發 OIDC Token

## 檔案清單

```
scripts/
├── fix_scheduler_oidc.sh       # 主執行腳本（自動化修復流程）
├── verify_data_quality.sql     # BigQuery 資料品質檢查查詢
└── README_SCHEDULER_FIX.md     # 本說明文件
```

## 前置條件

### 1. 必要工具

```bash
# 檢查工具是否已安裝
which gcloud  # Google Cloud SDK
which curl    # HTTP 測試工具
which jq      # JSON 處理工具

# macOS 安裝方式
brew install --cask google-cloud-sdk
brew install curl jq
```

### 2. GCP 認證

```bash
# 登入 GCP
gcloud auth login

# 設定預設專案
gcloud config set project b25h01-ragic

# 驗證認證狀態
gcloud auth list
```

### 3. 必要權限

執行本腳本需要以下 GCP 權限：

- **Cloud Scheduler**：
  - `cloudscheduler.jobs.list`（列出工作）
  - `cloudscheduler.jobs.get`（查看詳細配置）
  - `cloudscheduler.jobs.update`（更新配置）
  - `cloudscheduler.jobs.run`（手動觸發）

- **Cloud Functions**：
  - `cloudfunctions.functions.get`（查看函式狀態）
  - `cloudfunctions.functions.invoke`（呼叫函式）

- **Cloud Logging**：
  - `logging.logEntries.list`（查看日誌）

- **Service Account**：
  - `iam.serviceAccounts.actAs`（模擬服務帳號）
  - `iam.serviceAccounts.getAccessToken`（取得 OIDC Token）

## 使用方式

### 快速開始（推薦）

執行完整流程（包含所有階段）：

```bash
# 賦予執行權限
chmod +x scripts/fix_scheduler_oidc.sh

# 執行完整流程
./scripts/fix_scheduler_oidc.sh all
```

腳本會在關鍵步驟前請求確認，安全可控。

### 分階段執行

如果希望更謹慎地逐步執行，可以分階段進行：

#### 階段一：準備與驗證（唯讀操作，無風險）

```bash
./scripts/fix_scheduler_oidc.sh prepare
```

**執行內容**：
- ✓ 檢查必要工具與 GCP 認證
- ✓ 列出所有 Cloud Scheduler 工作
- ✓ 自動尋找 `erp-backup` 相關工作
- ✓ 顯示當前 Scheduler 配置
- ✓ 檢查 Cloud Function 狀態與 URL

**預期輸出**：
```
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
階段一：準備與驗證（唯讀操作）
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

[INFO] 列出 Cloud Scheduler 工作...

NAME                          TYPE  URI                                              SCHEDULE
projects/.../locations/.../   HTTP  https://asia-east1-b25h01-ragic.cloud...         0 2 * * *

[SUCCESS] ✓ 找到目標工作: erp-backup-scheduler
[SUCCESS] ✓ Cloud Function 狀態: ACTIVE
[INFO] Cloud Function URL: https://asia-east1-b25h01-ragic.cloudfunctions.net/erp-backup
```

#### 階段二：修復配置（更新 Scheduler）

```bash
./scripts/fix_scheduler_oidc.sh fix
```

**執行內容**：
- ⚠️ 更新 Scheduler 的 URI 為 Cloud Functions URL
- ⚠️ 更新 OIDC 服務帳號為 `b25h01-ragic@appspot.iam.gserviceaccount.com`
- ⚠️ 更新 OIDC audience 為 Cloud Functions URL

**執行前確認**：
```
即將更新 Cloud Scheduler 工作配置：

  工作名稱: erp-backup-scheduler
  目標 URL: https://asia-east1-b25h01-ragic.cloudfunctions.net/erp-backup
  OIDC 服務帳號: b25h01-ragic@appspot.iam.gserviceaccount.com
  OIDC Audience: https://asia-east1-b25h01-ragic.cloudfunctions.net/erp-backup

確認執行更新？(yes/no):
```

#### 階段三：測試與驗證

```bash
./scripts/fix_scheduler_oidc.sh test
```

**執行內容**：
- 手動觸發 Scheduler 工作
- 查看最近 30 分鐘的 Cloud Run 日誌
- 查看最近 30 分鐘的 Cloud Function 日誌
- 使用 OIDC Token 直接呼叫函式（測試模式，不寄信）

**預期成功輸出**：
```
[SUCCESS] ✓ Scheduler 觸發成功
[SUCCESS] ✓ OIDC Token 取得成功
HTTP Status: 200
Response Body:
{
  "status": "success",
  "message": "備份完成",
  "records_fetched": 1234,
  "records_uploaded": 1234
}
[SUCCESS] ✓ OIDC 呼叫測試成功（HTTP 200）
```

#### 階段四：BigQuery 資料驗證

```bash
./scripts/fix_scheduler_oidc.sh verify
```

**執行內容**：
- 提示執行 BigQuery 資料品質檢查查詢
- 查詢檔案位置：`scripts/verify_data_quality.sql`

**手動執行 SQL 查詢**：

```bash
# 方式一：使用 bq 命令列
bq query --use_legacy_sql=false < scripts/verify_data_quality.sql

# 方式二：在 BigQuery Console 執行
# 開啟 https://console.cloud.google.com/bigquery
# 複製 verify_data_quality.sql 中的查詢並執行
```

## 執行流程圖

```
開始
  │
  ├─> [prepare] 準備與驗證
  │     ├─ 檢查工具與認證
  │     ├─ 列出 Scheduler 工作
  │     ├─ 找到目標工作
  │     ├─ 顯示當前配置
  │     └─ 檢查 Cloud Function
  │
  ├─> [fix] 修復配置
  │     ├─ 顯示即將更新的配置
  │     ├─ 請求用戶確認 (yes/no)
  │     ├─ 更新 Scheduler 配置
  │     └─ 顯示更新後的配置
  │
  ├─> [test] 測試與驗證
  │     ├─ 手動觸發 Scheduler
  │     ├─ 查看 Cloud Run 日誌
  │     ├─ 查看 Cloud Function 日誌
  │     ├─ 取得 OIDC Token
  │     └─ 直接呼叫函式（不寄信）
  │
  └─> [verify] BigQuery 驗證
        └─ 執行資料品質檢查查詢
```

## 故障排除

### 問題一：401 Unauthorized

**症狀**：
```
HTTP Status: 401
{"error": "Unauthorized"}
```

**檢查清單**：
1. 確認 `--uri` 與 `--oidc-token-audience` 完全一致
2. 確認兩者都使用 Cloud Functions URL（非 Cloud Run URL）
3. 確認服務帳號為 `b25h01-ragic@appspot.iam.gserviceaccount.com`
4. 確認函式部署在 `asia-east1` 區域

**解決方式**：
```bash
# 重新執行修復階段
./scripts/fix_scheduler_oidc.sh fix

# 驗證配置
gcloud scheduler jobs describe [JOB_NAME] \
  --location=asia-east1 \
  --format="yaml(httpTarget.uri, httpTarget.oidcToken)"
```

### 問題二：404 Not Found

**症狀**：
```
HTTP Status: 404
{"error": "Not Found"}
```

**可能原因**：
- Cloud Functions URL 錯誤（拼寫錯誤、區域錯誤）
- 函式名稱不一致

**解決方式**：
```bash
# 確認函式實際 URL
gcloud functions describe erp-backup \
  --region=asia-east1 \
  --gen2 \
  --format="value(url)"

# 更新腳本中的 CF_URL 變數（如需要）
```

### 問題三：無法取得 OIDC Token

**症狀**：
```
[ERROR] 無法取得 OIDC Token
ERROR: (gcloud.auth.print-identity-token) ...
```

**可能原因**：
- 沒有服務帳號模擬權限
- 服務帳號不存在

**解決方式**：
```bash
# 檢查服務帳號是否存在
gcloud iam service-accounts describe \
  b25h01-ragic@appspot.iam.gserviceaccount.com

# 檢查當前用戶是否有模擬權限
gcloud projects get-iam-policy b25h01-ragic \
  --flatten="bindings[].members" \
  --filter="bindings.members:user:$(gcloud config get-value account)"
```

### 問題四：Scheduler 觸發失敗

**症狀**：
```
ERROR: (gcloud.scheduler.jobs.run) PERMISSION_DENIED
```

**解決方式**：
```bash
# 確認 Scheduler 工作狀態
gcloud scheduler jobs describe [JOB_NAME] \
  --location=asia-east1 \
  --format="yaml(state)"

# 確認當前用戶權限
gcloud projects get-iam-policy b25h01-ragic \
  --flatten="bindings[].members" \
  --filter="bindings.role:roles/cloudscheduler.admin"
```

## 進階操作

### 手動 OIDC 測試（多表模式）

```bash
# 設定變數
PROJECT_ID="b25h01-ragic"
REGION="asia-east1"
CF_URL="https://${REGION}-${PROJECT_ID}.cloudfunctions.net/erp-backup"
SA_EMAIL="${PROJECT_ID}@appspot.iam.gserviceaccount.com"

# 取得 OIDC Token
TOKEN="$(gcloud auth print-identity-token \
  --audiences="${CF_URL}" \
  --impersonate-service-account="${SA_EMAIL}")"

# 測試多表同步（避免寄信）
curl -i -X POST "${CF_URL}" \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{"sheet": "ALL", "DISABLE_EMAIL": true}'
```

### 查看最近的執行日誌

```bash
# Cloud Run 日誌
gcloud logging read \
  'resource.type="cloud_run_revision" AND resource.labels.service_name="erp-backup"' \
  --project=b25h01-ragic \
  --limit=100 \
  --freshness=1h \
  --format='table(timestamp, severity, textPayload)'

# Cloud Function 日誌
gcloud logging read \
  'resource.type="cloud_function" AND resource.labels.function_name="erp-backup"' \
  --project=b25h01-ragic \
  --limit=100 \
  --freshness=1h \
  --format='table(timestamp, severity, textPayload)'
```

### BigQuery 資料品質快速檢查

```bash
# 檢查最大修改日期
bq query --use_legacy_sql=false \
  'SELECT MAX(last_modified_date) AS max_date
   FROM `b25h01-ragic.erp_backup.ragic_data`'

# 檢查是否有未來日期
bq query --use_legacy_sql=false \
  'SELECT COUNT(*) AS future_date_count
   FROM `b25h01-ragic.erp_backup.ragic_data`
   WHERE last_modified_date > TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)'
```

## 成功驗證檢查清單

完成修復後，確認以下項目：

- [ ] Scheduler 配置已更新
  - `httpTarget.uri` 為 Cloud Functions URL
  - `oidcToken.serviceAccountEmail` 為 App Engine SA
  - `oidcToken.audience` 為 Cloud Functions URL

- [ ] Scheduler 手動觸發成功
  - 返回 HTTP 200
  - Cloud Logging 有執行記錄

- [ ] OIDC 直接呼叫成功
  - Token 取得成功
  - HTTP 200 回應
  - JSON 回應包含 `status: success` 或 `status: info`

- [ ] BigQuery 資料正常
  - 無未來日期（或已清理）
  - 同步狀態與實際資料一致
  - 最近有新資料寫入

## 相關文件

- **原始修復文件**：[documents/Scheduler_to_GCF_Gen2_OIDC_Fix.md](../documents/Scheduler_to_GCF_Gen2_OIDC_Fix.md)
- **專案 README**：[README.md](../README.md)
- **CLAUDE 指南**：[CLAUDE.md](../CLAUDE.md)

## 支援與回饋

如遇到問題或需要協助，請：

1. 檢查本文件的「故障排除」章節
2. 查看 `scripts/logs/` 目錄中的執行日誌
3. 查看 Cloud Logging 中的詳細錯誤訊息
4. 聯繫專案維護者

---

**最後更新**：2025-10-29
**版本**：1.0.0
