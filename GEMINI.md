# GEMINI.md

## 專案概述

本專案 (`RagicDataBackup`) 是一個用於將 Ragic ERP 系統的資料備份到 Google BigQuery 的解決方案。它被設計為一個部署在 Google Cloud Function 上的無伺服器應用程式，並由 Cloud Scheduler 定時觸發。

## AI協作準則

**重要**: 本專案以 `CLAUDE.md` 檔案作為所有 AI 助理（包括 Gemini CLI, Claude code, Codex CLI 等）的唯一行為準則與最高優先級的規範文件。

所有 AI 在執行任何操作前，都必須優先參考並嚴格遵守 `CLAUDE.md` 中定義的開發慣例、程式碼風格、工作流程與所有規則。

**任何對專案的架構、工作流程、或主要邏輯的變動，都必須同步更新到 `CLAUDE.md` 主文檔中，以確保所有 AI 的認知與行為一致。**

此 `GEMINI.md` 檔案旨在提供專案的基本概觀，但所有具體的執行細節與準則均以 `CLAUDE.md` 為最終依歸。

## 主要功能

- **動態與增量備份**: 系統會根據 BigQuery 中 `backup_config` 表的設定，動態決定要備份的 Ragic 表單，並根據上次的同步時間戳進行增量資料備份。
- **三層式欄位對應**: 為了應對 Ragic 中頻繁變動的中英文欄位，系統採用了三層對應策略：
    1.  **靜態對應**: 核心且不常變動的欄位會硬編碼在 `config_field_mapping.py` 中。
    2.  **動態對應**: 大部分的欄位對應規則儲存在 BigQuery 的 `field_mappings` 表中，可以隨時透過 SQL 修改而無需重新部署。
    3.  **自動處理未知欄位**: 當遇到未知的欄位時，系統會自動將其轉換為拼音，並記錄在 `unknown_fields` 表中，以便日後修正。
- **模組化設計**: 專案被拆分為多個獨立的 Python 模組，各司其職，例如 `ragic_client.py` (Ragic API 客戶端)、`data_transformer.py` (資料轉換)、`bigquery_uploader.py` (BigQuery 上傳) 等。
- **手動工具集**: 在 `Manual_fetch_all_Ragic/` 目錄下提供了一套命令列工具，讓開發者可以在本地手動觸發資料抓取和上傳，方便測試與除錯。

## 專案結構

```
/
├── erp_backup_main.py          # Cloud Function 的主進入點
├── ragic_client.py             # 處理與 Ragic API 的所有互動
├── data_transformer.py         # 負責資料的轉換、清理與格式化
├── bigquery_uploader.py        # 負責將資料上傳到 BigQuery
├── email_notifier.py           # 備份完成後發送 email 通知
├── config_loader.py            # 從 BigQuery 或環境變數載入設定
├── config_field_mapping.py     # 包含靜態的欄位對應表
├── sql/                          # 存放所有 BigQuery 相關的 SQL 腳本
│   ├── setup_bigquery_config_tables.sql # 初始化設定表的腳本
│   └── ...
├── scripts/                      # 存放部署與維護用的 shell 腳本
│   ├── migrate_to_gcp_cf.sh    # 部署到 Cloud Function 的腳本
│   └── ...
├── Manual_fetch_all_Ragic/     # 手動執行的 CLI 工具
├── documents/                    # 專案的詳細文件
├── test/                         # 測試相關檔案
└── tests/                        # 單元測試與整合測試
```

## 如何開始

1.  **設定 BigQuery**: 執行 `sql/setup_bigquery_config_tables.sql` 來建立必要的設定表。
2.  **設定環境變數**: 根據 `操作手冊.md` 設定必要的環境變數。
3.  **部署**: 執行 `scripts/migrate_to_gcp_cf.sh` 來部署 Cloud Function。

## 日常維護

- **啟用/停用備份**: 直接修改 BigQuery `backup_config` 表中的 `enabled` 欄位。
- **新增/修改欄位對應**: 直接在 BigQuery `field_mappings` 表中新增或修改對應規則。

## 最新修復狀態（2025-10-30）

### ✅ 型別錯誤已修復

**問題**: 系統在 2025-10-30 發生大量 `argument of type 'int' is not iterable` 錯誤，影響所有 9 個表單的備份。

**根本原因**: `data_transformer.py` 的型別集合變數（`float_fields`, `integer_fields`, `boolean_fields` 等）缺乏執行期保護機制，在異常情況下可能被覆蓋為非可迭代型別（如 int），導致在使用 `in` 運算子時發生 TypeError。

**解決方案**: 實作三層型別保護機制
1. **初始化驗證**: 在 `DataTransformer.__init__` 中調用 `_validate_type_collections()` 驗證所有型別集合
2. **專用驗證方法**: 新增 `_validate_type_collections()` 方法（769-798 行），自動檢測異常並修復
3. **執行期安全檢查**: 在 `_convert_field_type` 方法開頭（571-595 行）加入執行期型別檢查，若發現異常立即重置為預設值

**部署版本**: `erp-backup-00092-dov`（2025-10-30 03:52 UTC+8 部署）

**驗證結果**: ✅ 已通過測試，型別錯誤完全消除

**詳細記錄**: 請參考以下文件
- `documents/Incident_2025-10-30_RagicBackup_Runbook.md`
- `documents/Incident_2025-10-30_Full_Recovery_Record.md`
- `CLAUDE.md`（主文檔，包含完整技術細節）

**重要提醒**: 依照專案規範，所有架構變更必須優先更新 `CLAUDE.md`。本次修復已同步更新主文檔。
