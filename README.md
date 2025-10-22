# ERP 資料備份系統

模組化的 ERP 資料備份系統，支援從 Ragic API 獲取資料並上傳至 Google BigQuery。系統採用動態配置，可透過 BigQuery 資料表進行管理，無需重新部署。

## 🏗️ 系統架構

系統分為多個獨立模組，並透過 BigQuery 進行動態配置：

```
erp_backup_main.py          # 主程式，Cloud Function 入口
├── ragic_client.py         # Ragic API 資料獲取模組
├── data_transformer.py     # 資料轉換模組
├── bigquery_uploader.py    # BigQuery 上傳模組
└── email_notifier.py       # 電子郵件通知模組

-- 配置核心 --
├── config_loader.py        # 從 BigQuery 或環境變數載入配置
└── sql/setup_bigquery_config_tables.sql # 配置初始化腳本
```

## 🚀 主要特色

- **動態配置**: 透過 BigQuery 表 (`backup_config`, `field_mappings`) 管理備份目標與欄位對應，無需重新部署。
- **模組化設計**: 每個功能獨立，易於維護和測試。
- **增量備份**: 根據 BigQuery 中的最後同步時間進行高效增量備份。
- **混合上傳策略**: 根據資料量自動選擇 `Direct MERGE` 或 `Staging Table + Stored Procedure` 模式。
- **手動工具集**: 提供 `Manual_fetch_all_Ragic/` CLI 工具，方便手動執行與驗證。

## 📚 使用文件
- [操作手冊](./操作手冊.md) — 非技術使用者的一鍵部署與操作指南。
- [技術手冊](./技術手冊.md) — 開發者快速上手指南。
- [技術手冊（詳細版）](./技術手冊_詳細版.md) — 完整系統設計、函數與類別詳解。

## ⚙️ 快速開始 (部署流程)

1.  **設定 GCP 專案**: 確保已安裝 `gcloud` CLI 並登入。

2.  **初始化 BigQuery 配置表**: 
    - 登入 GCP Console，打開 BigQuery UI。
    - 複製 `sql/setup_bigquery_config_tables.sql` 的內容並執行。這會建立 `backup_config` 和 `field_mappings` 兩個表，並填入預設的 9 個表單設定。

3.  **設定環境變數 (回退方案)**: 
    設定最基本的環境變數，主要用於部署腳本以及 BigQuery 連線失敗時的回退方案。
    ```bash
    export PROD_PROJECT_ID=your-gcp-project-id
    export RAGIC_API_KEY=your-ragic-api-key-base64
    export RAGIC_ACCOUNT=your-ragic-account
    # ... 其他郵件相關變數
    ```

4.  **執行部署腳本**:
    此腳本會部署 Cloud Function 並設定 Cloud Scheduler 定時觸發。
    ```bash
    bash scripts/migrate_to_gcp_cf.sh
    ```

## 🔩 配置管理

部署後，所有備份行為由 BigQuery `backup_config` 表控制：

- **查詢備份目標**: `SELECT * FROM erp_backup.backup_config WHERE enabled = true;`
- **停用某個表的備份**: `UPDATE erp_backup.backup_config SET enabled = false WHERE sheet_code = '99';`
- **修改欄位對應**: 直接在 `erp_backup.field_mappings` 表中新增或修改規則。

## ▶️ 手動執行 (CLI 工具集)

`Manual_fetch_all_Ragic/` 目錄提供了一套獨立的 CLI 工具，用於手動資料處理。

1.  **抓取資料** (`fetch_ragic_all.py`):
    從 Ragic 抓取資料存到本地 JSON。支援增量和全量模式。
    ```bash
    python Manual_fetch_all_Ragic/fetch_ragic_all.py --project-id <gcp-project> --since-days 7
    ```

2.  **上傳資料** (`upload_to_bigquery.py`):
    將本地 JSON 轉換後上傳到 BigQuery。
    ```bash
    python Manual_fetch_all_Ragic/upload_to_bigquery.py --project-id <gcp-project> --input-dir Manual_fetch_all_Ragic/data/<batch_id>
    ```

詳細用法請參考 `Manual_fetch_all_Ragic/USAGE.md`。

## 🚧 欄位對照表維護

當 Ragic 來源資料表新增或修改欄位時，系統採用**三層配置策略**來應對：

1.  **Layer 1: Python 硬編碼** (`config_field_mapping.py`): 核心欄位。
2.  **Layer 2: BigQuery 動態對照表** (`field_mappings`): **建議的管理方式**。在此表新增規則即可動態生效。
3.  **Layer 3: 自動未知欄位處理**: 自動將未知中文欄位轉換為拼音，並記錄以供後續修正。

**建議流程**: 發現新欄位 → 在 `field_mappings` 表新增一筆對照記錄 → 問題解決。

## 📞 技術支援

如在使用過程中遇到問題，請：
1.  優先檢查 Cloud Logging 中的詳細錯誤訊息。
2.  確認 BigQuery `backup_config` 表的設定是否正確。
3.  在 GitHub Issues 中回報問題並提供相關日誌。
