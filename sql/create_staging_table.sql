-- BigQuery staging 表建立範本
-- 說明：
-- 1) 以目標表為藍本建立 staging 表（LIKE 會複製欄位、分區與叢集設定）
-- 2) 附加批次欄位：batch_id, ingested_at
-- 3) 如目標表尚未建立，請先建立目標表後再執行本範本

-- 請將以下三個參數替換為實際名稱
--   ${PROJECT_ID}   例：grefun-testing
--   ${DATASET_ID}   例：erp_backup
--   ${TARGET_TABLE} 例：ragic_data

-- 建立 staging 表（複製目標表結構）
CREATE TABLE IF NOT EXISTS `${PROJECT_ID}.${DATASET_ID}.${TARGET_TABLE}_staging`
LIKE `${PROJECT_ID}.${DATASET_ID}.${TARGET_TABLE}`;

-- 附加批次欄位（若不存在）
ALTER TABLE `${PROJECT_ID}.${DATASET_ID}.${TARGET_TABLE}_staging`
ADD COLUMN IF NOT EXISTS batch_id STRING,
ADD COLUMN IF NOT EXISTS ingested_at TIMESTAMP;

-- 建議：若目標表未啟用分區/叢集，請於目標表設定：
--   PARTITION BY DATE(updated_at)
--   CLUSTER BY order_id
-- 然後重新以 LIKE 建立 staging 表，以獲得相同分區/叢集設定。


