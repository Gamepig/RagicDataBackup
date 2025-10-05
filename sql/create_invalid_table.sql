-- 建立 invalid_records 表（記錄清理未通過資料），供 Cloud Function/批次重試使用
-- 使用方式：以環境變數替換 ${PROJECT_ID}、${DATASET_ID}

CREATE TABLE IF NOT EXISTS `${PROJECT_ID}.${DATASET_ID}.invalid_records`
(
  sheet_code STRING NOT NULL,
  ragic_id STRING,
  record_index INT64,
  errors ARRAY<STRING>,
  raw JSON,            -- 原始資料（方便 SQL 反查）
  batch_id STRING,
  logged_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY DATE(logged_at)
CLUSTER BY sheet_code, ragic_id;

-- 查詢最近未通過樣例：
-- SELECT sheet_code, ragic_id, record_index, logged_at, errors[OFFSET(0)] AS first_error
-- FROM `${PROJECT_ID}.${DATASET_ID}.invalid_records`
-- WHERE logged_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
-- ORDER BY logged_at DESC


