## 本地抓取與上傳流程

### 先決條件
- 已設定 Google 認證（例如匯出 `GOOGLE_APPLICATION_CREDENTIALS` 指向 service account JSON）。
- 安裝依賴：在專案根目錄安裝 requirements.txt 內套件。

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

### 1) 抓取 Ragic 9 表為本機 JSON

預設採「增量模式（近 7 天）」並使用 BigQuery `backup_config` 載入 9 表設定。

```bash
python Manual_fetch_all_Ragic/fetch_ragic_all.py \
  --project-id grefun-testing \
  --client-id grefun \
  --mode since_local_paged \
  --since-days 7
``;

- 產物：`Manual_fetch_all_Ragic/data/<批次時間戳>/`
  - `<sheet_code>_<sheet_name>.json`（陣列，中文欄位）
  - `manifest.json` 與 `batch_summary.json`

全量模式（適合小表；大表建議改以增量或分次切片）：

```bash
python Manual_fetch_all_Ragic/fetch_ragic_all.py \
  --project-id grefun-testing \
  --client-id grefun \
  --mode full \
  --limit 1000 --large-limit 10000 --page-sleep 0.8
```

> 全量模式會將每頁資料寫入 `<sheet_code>/page-*.json`，完成後合併為 `<sheet_code>_<sheet_name>.json`，可續傳。

常用參數：
- `--large-sheets "50,60,99"` 指定大表
- `--last-modified-fields "最後修改日期,最後修改時間,更新時間,最後更新時間"`（增量）
- `--out-dir Manual_fetch_all_Ragic/data/mybatch` 自訂輸出資料夾

若未提供 `--project-id`，程式會回退到環境變數載入配置（見 `config_loader.load_config_from_env`）。

### 2) 上傳至 BigQuery（單一目標表彙整）

```bash
python Manual_fetch_all_Ragic/upload_to_bigquery.py \
  --project-id grefun-testing \
  --dataset erp_backup \
  --table ragic_data \
  --input-dir Manual_fetch_all_Ragic/data/<批次時間戳> \
  --upload-mode auto \
  --use-merge true \
  --batch-threshold 5000 \
  --merge-sp-name erp_backup.sp_upsert_ragic_data \
  --emit-processed-json
```

- 轉換：使用 `DataTransformer`，自動注入來源欄位 `sheet_code`，確保九表彙整時可辨識來源。
- 上傳：`BigQueryUploader` 會自動建立 Dataset/Table；`auto` 模式下大批（>門檻）走 staging+SP，小批走直送（MERGE/INSERT）。
- 產物：`upload_summary.json`（各表上傳結果），若指定 `--emit-processed-json` 則輸出 `processed/<sheet_code>.json`（英文欄位）。

### 注意事項
- 大表（50/60/99）建議優先使用「增量模式」以降低逾時風險；全量需配合較高 `limit` 與間隔 `--page-sleep`，並觀察 `manifest.json`。
- BigQuery 目標表會包含 `sheet_code` 欄位以辨識來源表，欄位 schema 以 `data_transformer.BIGQUERY_SCHEMA` 為準。
- 若轉換有無效紀錄，會被略過並計入 `DataTransformer.invalid_records`；可在未來擴充輸出至 JSONL 以利修復流程。


