-- BigQuery 預儲程序（Stored Procedure）範本：staging → 目標 MERGE + 清理 + 審計
-- 說明：
-- - 以 order_id 為鍵進行 Upsert
-- - 僅示意更新關鍵欄位，其他欄位請依 schema 擴充
-- - 需要先建立審計表（可選）：${DATASET_ID}.ragic_ingest_audit

-- 參數替換：
--   ${PROJECT_ID}   例：grefun-testing
--   ${DATASET_ID}   例：erp_backup
--   ${TARGET_TABLE} 例：ragic_data

CREATE OR REPLACE PROCEDURE `${PROJECT_ID}.${DATASET_ID}.sp_upsert_ragic_data`(IN batch_id STRING)
BEGIN
  -- Upsert 資料
  MERGE `${PROJECT_ID}.${DATASET_ID}.${TARGET_TABLE}` T
  USING (
    SELECT * FROM `${PROJECT_ID}.${DATASET_ID}.${TARGET_TABLE}_staging`
    WHERE batch_id = batch_id
  ) S
  ON T.order_id = S.order_id
  WHEN MATCHED THEN UPDATE SET
    status = S.status,
    export_status = S.export_status,
    updated_at = S.updated_at,
    updated_by = S.updated_by
    -- TODO: 其餘欄位請依實際 schema 附加
  WHEN NOT MATCHED THEN INSERT ROW;

  -- 審計紀錄（可選）
  BEGIN
    INSERT INTO `${PROJECT_ID}.${DATASET_ID}.ragic_ingest_audit`
    (batch_id, target_table, rows_merged, processed_at)
    SELECT
      batch_id,
      '${TARGET_TABLE}',
      (SELECT COUNT(1) FROM `${PROJECT_ID}.${DATASET_ID}.${TARGET_TABLE}_staging` WHERE batch_id = batch_id),
      CURRENT_TIMESTAMP();
  EXCEPTION WHEN ERROR THEN
    -- 若審計表不存在則忽略
  END;

  -- 清理該批次 staging（可按策略保留）
  DELETE FROM `${PROJECT_ID}.${DATASET_ID}.${TARGET_TABLE}_staging`
  WHERE batch_id = batch_id;
END;


