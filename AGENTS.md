# AGENTS.md

本文件提供在本倉庫內協作的 AI/代理（Agents）之統一工作規範與開發指引。其適用範圍為本檔案所在目錄為根的整個專案樹（整個倉庫）。

## 核心規範
- 一律使用繁體中文回答（包含說明、評論、提交訊息、PR 討論等）。
- 變更應聚焦、最小化、可回溯；避免不必要的重構或風格改動。
- 嚴禁在版本控制中新增或外洩機密（API Key、密碼、SA JSON 等）。若需使用祕密資訊，請改用環境變數或 GCP Secret Manager。

## 準則優先序（強制）
- 專案主要依據與準則：`CLAUDE.md`。
  - 所有代理（Claude Code、Gemini CLI、Codex CLI）必須以 `CLAUDE.md` 為統一標準進行同步協作（風格、架構、流程、設定策略）。
  - 若其他代理專用文件（例如 `GEMINI.md`）與 `CLAUDE.md` 有衝突，一律以 `CLAUDE.md` 為準。
- 指令優先原則：直接的系統/開發者/使用者指令優先於文件與此檔案；若臨時需求需偏離 `CLAUDE.md`，請在提交說明中明確標註偏離原因與影響。
- 文件分工：
  - `AGENTS.md`：流程與協作規範、優先序與執行要求（本文件）。
  - `CLAUDE.md`：內容與技術實作準則（架構決策、用語、設定策略、模組邊界）。
  - 其他代理文件（如 `GEMINI.md`）：可補充，但不得違背 `CLAUDE.md`。
- 嚴格執行：
  - 重大變更（影響流程/風格/設定策略）須先對齊並更新 `CLAUDE.md`，再實作與同步其他文件。
  - PR/變更審查需檢核是否遵循 `CLAUDE.md`；若不一致，應提出修正或說明偏離理由。
  - 每次任務開始前，代理需快速比對關鍵決策是否仍與 `CLAUDE.md` 一致；若發現缺口，先提議文件同步更新。

## 專案速覽（依據 CLAUDE.md 與 GEMINI.md）
- 目的：將 Ragic ERP 的 9 個表單資料備份到 BigQuery，支援 Cloud Function 自動化與 CLI 手動模式。
- 模組：
  - `ragic_client.py`：與 Ragic API 互動（增量抓取、本地過濾、分頁/重試）。
  - `data_transformer.py`：欄位映射（硬編碼 + BigQuery 動態 + 未知欄位拼音）、型別轉換、錯誤追蹤。
  - `bigquery_uploader.py`：自動建表、直送 MERGE 或暫存表 + SP、批次上傳。
  - `config_loader.py`：優先自 BigQuery `backup_config` 載入設定，失敗回退環境變數。
  - `erp_backup_main.py`：備份流程協調；Cloud Function HTTP 入口 `backup_erp_data(request)`；亦提供本地 `main()`。
  - `Manual_fetch_all_Ragic/`：手動抓取與上傳工具集。
- 動態配置：BigQuery `backup_config`、`field_mappings`、`unknown_fields` 等表；透過 SQL 可即時生效，無需重新部署。

## 變更原則
- 優先遵循動態配置策略：
  - 欄位對映請優先在 BigQuery `field_mappings` 維護；僅核心穩定欄位才修改 `config_field_mapping.py`。
  - 表單啟用/排序/備註等請調整 `backup_config`。
- 保持模組邊界：
  - 取數邏輯限定於 `ragic_client.py`；
  - 轉換限定於 `data_transformer.py`；
  - 上傳限定於 `bigquery_uploader.py`；
  - 請勿將跨層職責交叉耦合。
- 不變更 Cloud Function 簽章：`main.py` 之 `backup_erp_data(request)` 必須保留。
- 若需新增功能旗標，先考慮以環境變數或 HTTP payload 覆蓋（見 `erp_backup_main.py` 中既有參數模式）。

## 程式風格與品質
- 語言與版本：Python 3.10+，檔名/變數/函式採 snake_case，類別採 PascalCase。
- 型別標註：新增/修改之函式請補齊 `typing` 型別註記。
- 紀錄：使用 `logging`，避免 `print`（測試/CLI 範例除外）。
- 錯誤處理：明確擲出/記錄，避免隱性吞例外；保持錯誤訊息可診斷。
- 測試：
  - 針對變更區域優先撰寫或補齊最小必要測試；
  - 可使用 `test/` 及 `Manual_fetch_all_Ragic/` 工具做端到端驗證；
  - 非必要請勿大幅改動既有測試資料與流程。

## 時間與時區（重要）
- 解析 Ragic 時間：預設視為台北時間（`data_transformer.TAIPEI_TZ`），轉為 timezone-aware。
- 內部比較與儲存：使用 UTC 進行比較與上傳（程式內普遍以 UTC 比對；顯示層可轉回台北時間）。
- 禁止使用天真（naive）時間物件進行跨時區比較。

## Ragic 抓取準則
- 預設使用 `RagicClient.fetch_since_local_paged()`：
  - 以 `_ragicId,asc` 的固定排序逐頁抓取；
  - 在本地比較「最後修改時間」欄位（多候選名稱）；
  - 使用 `no_new_data_pages_threshold` 進行智慧提前停止；
  - 支援 `FORCE_SINCE_*`、`USE_RAGIC_WHERE` 等臨時覆蓋（環境變數或 HTTP payload）。
- 大表（50/60/99）建議增量；全量請搭配較大 `limit` 與節流間隔並觀察批次統計。

## 資料轉換準則
- 依三層對映策略處理欄位：
  1) Python 硬編碼（核心）、2) BigQuery `field_mappings`（建議）、3) 未知欄位自動拼音並記錄至 `unknown_fields`。
- 統一注入 `sheet_code` 以利九表彙整。
- 嚴格型別轉換：FLOAT/INTEGER/BOOLEAN/TIMESTAMP 等；無效紀錄需記錄並可追溯。
- **型別集合保護**（2025-10-30 新增）：`data_transformer.py` 已實作三層型別集合保護機制（初始化驗證、專用驗證方法、執行期安全檢查），防止型別集合變數被意外覆蓋為非可迭代型別。所有代理在修改型別相關邏輯時，必須維持此保護機制的完整性。

## BigQuery 上傳準則
- 優先使用 `BigQueryUploader.upload_data()` 與 `batch_upload_data()` 提供的策略：
  - `upload_mode=auto`：小批直送（MERGE/INSERT），大批走暫存表 + 儲存程序（SP）。
  - 自動建立 Dataset/Table 與 Schema 對齊（以 `data_transformer.BIGQUERY_SCHEMA` 為準）。
- 禁止在上傳路徑中引入與轉換無關的業務邏輯。

## 部署與排程
- 部署與 Scheduler 設定以 `scripts/migrate_to_gcp_cf.sh`、`scripts/fix_scheduler_oidc.sh` 為準；若需異動，請同步更新腳本與文件。
- 僅在必要時調整 Cloud Scheduler 或 Function 逾時/記憶體設定；保持地區與 Dataset Location 一致（預設 `asia-east1`）。

## 設定與祕密管理
- 設定載入順序：BigQuery `backup_config` > 環境變數 > 程式預設；請避免將硬值散落程式碼。
- 嚴禁提交真實祕密；本倉庫若已有示例值，視為示例用途，請在實際部署時改用 Secret Manager/部署管線注入。

## 常見任務建議流程
- 新增欄位映射：先在 BigQuery `field_mappings` 新增規則；必要時才補 `config_field_mapping.py` 與 `BIGQUERY_SCHEMA`。
- 新表/表單調整：於 `backup_config` 新增/調整 `sheet_code`、`sheet_id`、優先級與啟用狀態。
- 調整增量欄位：以環境變數或請求參數覆蓋 `LAST_MODIFIED_FIELD_NAMES`，避免重新部署。

## 禁用/避免事項
- 不要變更 `main.py` 的匯出與 Cloud Function 簽章。
- 不要將抓取、轉換、上傳的責任混合進單一模組。
- 不要在非測試場景使用 `print` 取代 `logging`。
- 不要將本地測試硬碼（e.g., 临时路徑、個人帳號）提交到版本控制。

## 最新修復狀態（2025-10-30）

### ✅ 型別錯誤已完全修復

**問題摘要**: 2025-10-30 系統發生大量 `argument of type 'int' is not iterable` TypeError，影響所有 9 個 Ragic 表單的備份作業。

**根本原因**:
- `data_transformer.py` 的型別集合變數（`float_fields`, `integer_fields`, `boolean_fields`, `date_fields`, `timestamp_fields` 等）在異常情況下可能被覆蓋為非可迭代型別（如 int、str）
- 當後續程式碼使用 `in` 運算子檢查欄位是否屬於某型別集合時，會觸發 TypeError
- 缺乏執行期保護機制，無法及時發現並修復此類異常

**修復方案**（三層型別保護）:
1. **初始化驗證**（第 386 行）：
   - 在 `DataTransformer.__init__` 結尾調用 `self._validate_type_collections()`
   - 確保類別初始化時所有型別集合都是正確的 set/dict 型別

2. **專用驗證方法**（第 769-798 行）：
   - 新增 `_validate_type_collections()` 方法
   - 逐一檢查 7 個型別集合的型別正確性
   - 若發現異常，記錄詳細錯誤訊息並自動重置為空集合

3. **執行期安全檢查**（第 571-595 行）：
   - 在 `_convert_field_type` 方法開頭加入型別檢查
   - 若發現集合型別異常，立即重置為預設硬編碼值
   - 確保即使驗證方法失敗，執行期仍有最後一道防線

**部署資訊**:
- 版本: `erp-backup-00092-dov`
- 部署時間: 2025-10-30 03:52 (UTC+8)
- Runtime: Python 3.11, 1GB 記憶體, 540s 逾時
- 地區: asia-east1

**驗證結果**:
- ✅ Python 語法檢查通過
- ✅ Cloud Function 部署成功
- ✅ 測試執行（sheet 99, LOG_PER_RECORD_FAILURES=true）無型別錯誤
- ✅ Cloud Logging 確認無 TypeError 發生

**代理協作要求**:
- 所有代理在修改 `data_transformer.py` 型別相關邏輯時，必須保持三層保護機制的完整性
- 任何對型別集合變數的賦值操作，必須確保賦值為正確的 set/dict 型別
- 新增型別集合時，必須在 `_validate_type_collections()` 中加入對應驗證邏輯

**詳細記錄**:
- 事件完整紀錄: `documents/Incident_2025-10-30_Full_Recovery_Record.md`
- 操作 SOP: `documents/Incident_2025-10-30_RagicBackup_Runbook.md`
- 技術細節: `CLAUDE.md`（已同步更新）

## 參考文件
- 專案總覽與指引：`README.md`、`CLAUDE.md`、`GEMINI.md`
- 手動工具：`Manual_fetch_all_Ragic/USAGE.md`
- BigQuery 初始化：`sql/setup_bigquery_config_tables.sql`
- 部署與排程維護：`scripts/README_SCHEDULER_FIX.md`
- **最新事件記錄**: `documents/Incident_2025-10-30_Full_Recovery_Record.md`、`documents/Incident_2025-10-30_RagicBackup_Runbook.md`
