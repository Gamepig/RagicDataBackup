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

## 📚 使用文件
- [操作手冊](./操作手冊.md) — 非技術使用者的一鍵部署與操作
- [技術手冊](./技術手冊.md) — 模組/函數/變數詳解與部署重點
- [文件索引](./DOCUMENTATION_INDEX.md) — 全部技術文件目錄與導覽

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
 - 單頁抓取 + 本地過濾一週（必要時自動往下一頁）增量策略
 - 支援「時間窗」增量（嚴格 dt > since，可選 until 上界避免重疊）

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

#### 上傳模式與門檻（新）
- `UPLOAD_MODE`: `auto`|`direct`|`staging_sp`（預設 `auto`）
  - `auto`: 當單次批次量 > `BATCH_THRESHOLD` 時自動切換至 `staging_sp`，否則使用直送（`MERGE`/`INSERT`）
  - `direct`: 一律使用直送（`MERGE`/`INSERT`）
  - `staging_sp`: 一律使用 staging 表 + 預儲程序（高吞吐）
- `BATCH_THRESHOLD`: 直送與 staging 切換門檻（筆數），預設 5000
- `STAGING_TABLE`: 指定 staging 表名（預設為 `<BIGQUERY_TABLE>_staging`）
- `MERGE_SP_NAME`: 預儲程序名稱，支援 `proc`、`dataset.proc`、`project.dataset.proc`（預設 `sp_upsert_ragic_data`）

建議：依每週增量的 13 週平均估算門檻。以目前測得（50/60/99 分別約 380、270、1281/週），維持 `auto` 與預設門檻 5000 即可。

## 🔁 增量擷取策略（where 與本地過濾）

- 若已知各表「最後修改」欄位 ID，可使用 `where=<欄位ID>,gt,<時間>` 精準增量。
- 若未知欄位 ID 或 `where` 無效，採用「單頁抓取 + 本地過濾一週」：
  - 先抓一頁（大表可用較大 `limit`，如 3000），以欄位名稱清單（「最後修改日期/時間/更新時間/最後更新時間」）嘗試解析該頁日期，篩出一週內資料。
  - 翻頁規則：若整頁「沒有一週外資料」→ 代表仍可能有一週內資料在下一頁，則自動往下一頁抓取；直到出現第一筆一週外紀錄、或該頁資料數低於 `limit`、或達到頁數上限為止。
  - 已實作於 `ragic_client.fetch_since_local_paged()`，測試腳本見 `test/fetch_last_week_where_cn.py`。

避免重複與跨週重疊：
- 本地過濾採「嚴格大於」邊界：只納入 `dt > since` 的資料；`dt <= since` 視為越界並停止翻頁。
- 可選上界 `until`：`dt > since 且 dt <= until`，用於相鄰兩段（如上上週、這週）切割測試與排程，確保零重疊。
- 區間驗證腳本：`test/run_api_incremental_window.py`（環境變數 `WINDOW_SINCE_DAYS`、`WINDOW_UNTIL_DAYS`）。

> 備註：若單頁資料皆無法解析日期（欄位缺失或格式異常），為避免無限翻頁，僅抓取該頁即停止。

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

## 🔎 未備份記錄查詢（Cloud Logging）

使用 `test/query_unbackup.py` 依 `batch_id` 查詢本批未備份紀錄（來源：Cloud Logging `invalid-records`）：

```bash
python test/query_unbackup.py --project <GCP_PROJECT_ID> --batch <BATCH_ID> --limit 50
```

輸出欄位：`sheet_code, ragic_id, errors`。建議於郵件中取得 `batch_id` 後執行。

### 未備份記錄（BigQuery 反查）

系統亦會將未備份記錄寫入 `ragic_backup.invalid_records`（含 raw 原文）。可用 SQL 反查：

```sql
SELECT sheet_code, ragic_id, record_index, errors, logged_at
FROM `ragicerp-databackup.ragic_backup.invalid_records`
WHERE batch_id = @batch_id
ORDER BY logged_at DESC
```

> 欄位：`sheet_code STRING, ragic_id STRING, record_index INT64, errors ARRAY<STRING>, raw JSON, batch_id STRING, logged_at TIMESTAMP`

### 表單識別（9 張表）

本系統將 9 個 Ragic 表資料彙整到 `erp_backup.ragic_data`，以 `sheet_code` 識別來源：

| sheet_code | 表單名稱（中文） | Ragic sheet_id（來源） |
| --- | --- | --- |
| 10 | 品牌管理 | forms8/5 |
| 20 | 通路管理 | forms8/4 |
| 30 | 金流管理 | forms8/7 |
| 40 | 物流管理 | forms8/1 |
| 41 | 縣市郵遞區號 | forms8/6 |
| 50 | 訂單管理 | forms8/17 |
| 60 | 客戶管理 | forms8/2 |
| 70 | 商品管理 | forms8/9 |
| 99 | 銷售總表 | forms8/3 |

> 備註：上述對應亦存在於 BigQuery `ragic_backup.backup_config` 中，後續若有新增/變更可直接更新該表而不需重新部署。

## ⚙️ 環境變數設定

```bash
# Ragic 設定
export RAGIC_API_KEY="your-ragic-api-key"
export RAGIC_ACCOUNT="your-account"
export RAGIC_SHEET_ID="your-sheet/1"
export RAGIC_SHEET_CODE=""                  # 單表模式可指定 10/20/...，將自動映射 sheet_id（可選）

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
export UPLOAD_MODE="auto"           # auto|direct|staging_sp
export BATCH_THRESHOLD="5000"       # 直送→staging 切換門檻
export STAGING_TABLE=""            # 可留空，預設 <BIGQUERY_TABLE>_staging
export MERGE_SP_NAME=""            # 預設 sp_upsert_ragic_data
export LOG_LEVEL="INFO"
export RAGIC_MAX_PAGES="50"        # 單頁抓取翻頁上限
export LAST_MODIFIED_FIELD_NAMES="最後修改日期,最後修改時間,更新時間,最後更新時間"

# 多表模式（sheet 對照設定）
# 若設定 RAGIC_SHEET_ID=ALL，將依下列對照表跑 9 張表
export RAGIC_SHEET_ID="ALL"
export SHEET_MAP_JSON='{"10":"forms8/5","20":"forms8/4","30":"forms8/7","40":"forms8/1","41":"forms8/6","50":"forms8/17","60":"forms8/2","70":"forms8/9","99":"forms8/3"}'
# 或使用檔案：export SHEET_MAP_FILE="/path/to/sheet_map.json"
```

## ⚠️ 使用前注意事項

**此為開發版本，使用前請注意：**

1. **測試環境優先**：請先在測試環境中完整驗證所有功能
2. **資料備份**：執行前請確保原始資料已有備份
3. **權限設定**：請謹慎設定 API 權限，避免過度授權
4. **監控檢查**：建議搭配監控工具觀察系統運行狀況
5. **錯誤處理**：如遇到問題請檢查日誌並適當調整設定

## 🚧 已知限制與未來改進

### 當前限制
- 尚未經過大量資料的壓力測試
- 電子郵件通知功能可能需要根據不同 SMTP 提供商調整
- Cloud Function 部署配置可能需要針對特定環境優化
- 錯誤恢復機制有待進一步完善

### ⚠️ 欄位對照表維護問題（重要）

**問題描述**：
當 Ragic 來源資料表新增或修改欄位時，系統的中英文欄位對照表將失效，導致資料轉換錯誤。

**當前解決方案**：
系統採用**三層配置策略**來處理此問題：

1. **Layer 1：Python 硬編碼對照表**（`config_field_mapping.py`）
   - 保護核心欄位永遠可用
   - 適用於 Cloud Function 無狀態環境
   - 修改需重新部署

2. **Layer 2：BigQuery 動態對照表**（可選啟用）
   - 支援動態新增/修改欄位映射
   - 無需重新部署即可更新
   - 適用於頻繁變動的欄位

3. **Layer 3：自動未知欄位處理**
   - 自動將未知中文欄位轉換為拼音（例：`客戶地址` → `auto_kehudizhi`）
   - 記錄未知欄位到 BigQuery `unknown_fields` 表
   - 透過 Cloud Logging 和 Email 通知管理員

**自動檢測與通知流程**：
```
Ragic 新增欄位
    ↓
系統偵測到未知欄位
    ↓
自動套用拼音轉換（臨時處理）
    ↓
記錄到 BigQuery unknown_fields 表
    ↓
發送 Email 通知管理員
    ↓
管理員手動在 BigQuery 新增正式對照規則
```

**手動修正步驟**：
```sql
-- 1. 查詢未知欄位
SELECT * FROM `grefun-testing.ragic_backup.unknown_fields`
ORDER BY first_seen_at DESC;

-- 2. 新增正式對照規則
INSERT INTO `grefun-testing.ragic_backup.field_mappings`
(sheet_code, chinese_field, english_field, enabled)
VALUES ('99', '新欄位名稱', 'new_field_name', TRUE);
```

**未來改進計劃**（AI 自動修正）：

> 📝 **記錄給未來的開發者**：
> 在資料清理階段，可以整合 AI 能力來自動修正欄位對照表：
>
> 1. 使用 Claude API 或 OpenAI API 分析未知欄位的語義
> 2. 自動產生合適的英文欄位名稱（符合 BigQuery 命名規範）
> 3. 自動更新 BigQuery `field_mappings` 表
> 4. 發送通知給管理員確認變更
>
> **實作提示**：
> - 在 `config_field_mapping.py` 中新增 `AIFieldTranslator` 類別
> - 使用 Anthropic Claude API 或 OpenAI GPT-4 進行語義理解
> - 參考現有對照表的命名模式（例：`客戶` → `customer_`, `訂單` → `order_`）
> - 實作人工審核機制（先標記為 `pending_review`，管理員確認後啟用）
>
> **範例 Prompt**：
> ```
> 請將以下中文資料庫欄位名稱轉換為符合 BigQuery 規範的英文欄位名稱：
> - 中文欄位：「客戶生日」
> - 參考對照：「客戶名稱」→ customer_name, 「生日」→ birthday
> - 要求：使用 snake_case，語義清晰，符合現有命名模式
> ```

詳細技術文件請參考：
- `config_field_mapping.py` - 三層配置策略實作
- `documents/field_mapping_analysis.md` - 欄位對照表方案分析
- `documents/field_mapping_README.md` - 欄位對照表使用指南

## 📞 技術支援

如在使用過程中遇到問題，請：
1. 檢查日誌輸出中的詳細錯誤訊息
2. 確認所有環境變數設定正確
3. 在 GitHub Issues 中回報問題並提供相關日誌

## 🔎 未備份資料查詢與修正（Ragic 原始資料）

- 從 Cloud Logging/JSONL 取得本批次未備份的 `sheet_code` 與 `_ragicId`。
- 依 `sheet_code` 對照 `sheet_id`，用 Ragic API 查單筆原始資料：
  - GET `https://ap6.ragic.com/{RAGIC_ACCOUNT}/{sheet_id}/{_ragicId}?api&v=3`
- 或在 Ragic 後台以 `_ragicId` 搜尋該表資料，修正欄位後儲存，再由下次備份自動納入。

## 🛡️ 正式環境移交所需資訊（GCP）

- GCP 專案 ID（已啟用計費）、BigQuery/Functions 地區（建議 `asia-east1`）
- 一組專用 Service Account 或授權我們的 Google 帳號；最小必要角色：
  - BigQuery Data Editor、BigQuery Job User、Logging Writer
  - Cloud Functions Admin/Invoker（若使用 CF），或 Cloud Run Admin（若使用 CR）
- （選）離線部署需該 Service Account 的 JSON 金鑰
- Ragic：`RAGIC_API_KEY`（Base64）、`RAGIC_ACCOUNT`
- 郵件：`NOTIFICATION_EMAIL`、`SMTP_FROM_EMAIL`、`SMTP_FROM_PASSWORD`
- 9 張表對照（如與預設不同）：`SHEET_MAP_JSON` 或對照檔案路徑