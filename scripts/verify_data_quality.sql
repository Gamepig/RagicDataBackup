-- ============================================================================
-- BigQuery 資料品質驗證查詢
-- 用途：驗證 Ragic 備份資料的品質與完整性
-- 參考文件：documents/Scheduler_to_GCF_Gen2_OIDC_Fix.md (步驟 6.3)
-- ============================================================================

-- 專案與資料集配置
-- PROJECT_ID: b25h01-ragic
-- DATASET: erp_backup
-- TABLE: ragic_data

-- ============================================================================
-- A. 檢查最大修改日期（是否仍有未來日期）
-- ============================================================================

-- A.1 查看最大修改日期
SELECT
    MAX(last_modified_date) AS max_last_modified,
    MIN(last_modified_date) AS min_last_modified,
    COUNT(*) AS total_records,
    COUNT(DISTINCT sheet_code) AS distinct_sheets
FROM `b25h01-ragic.erp_backup.ragic_data`;

-- A.2 列出仍在未來（> 現在+1天）的資料（若有）
SELECT
    sheet_code,
    order_id,
    last_modified_date,
    TIMESTAMP_DIFF(last_modified_date, CURRENT_TIMESTAMP(), DAY) AS days_in_future
FROM `b25h01-ragic.erp_backup.ragic_data`
WHERE last_modified_date > TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
ORDER BY last_modified_date DESC
LIMIT 100;

-- A.3 統計未來日期筆數（按 sheet_code 分組）
SELECT
    sheet_code,
    COUNT(*) AS future_date_count,
    MAX(last_modified_date) AS max_future_date
FROM `b25h01-ragic.erp_backup.ragic_data`
WHERE last_modified_date > TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
GROUP BY sheet_code
ORDER BY future_date_count DESC;

-- ============================================================================
-- B. 資料品質註記檢查
-- ============================================================================

-- B.1 抽查最近寫入且有資料品質註記的記錄
SELECT
    sheet_code,
    order_id,
    last_modified_date,
    data_quality_notes
FROM `b25h01-ragic.erp_backup.ragic_data`
WHERE data_quality_notes IS NOT NULL
ORDER BY last_modified_date DESC
LIMIT 100;

-- B.2 統計資料品質註記類型
SELECT
    sheet_code,
    REGEXP_EXTRACT(data_quality_notes, r'"issue":"([^"]+)"') AS issue_type,
    COUNT(*) AS issue_count
FROM `b25h01-ragic.erp_backup.ragic_data`
WHERE data_quality_notes IS NOT NULL
GROUP BY sheet_code, issue_type
ORDER BY issue_count DESC;

-- ============================================================================
-- C. 資料完整性檢查
-- ============================================================================

-- C.1 檢查每個 sheet_code 的資料量
SELECT
    sheet_code,
    COUNT(*) AS record_count,
    MIN(last_modified_date) AS earliest_date,
    MAX(last_modified_date) AS latest_date,
    COUNT(DISTINCT order_id) AS unique_orders
FROM `b25h01-ragic.erp_backup.ragic_data`
GROUP BY sheet_code
ORDER BY sheet_code;

-- C.2 檢查重複的 order_id（同一 sheet_code 內）
SELECT
    sheet_code,
    order_id,
    COUNT(*) AS duplicate_count,
    ARRAY_AGG(DISTINCT last_modified_date ORDER BY last_modified_date DESC LIMIT 5) AS modification_dates
FROM `b25h01-ragic.erp_backup.ragic_data`
GROUP BY sheet_code, order_id
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC, sheet_code
LIMIT 100;

-- C.3 檢查 NULL 值比例（關鍵欄位）
SELECT
    sheet_code,
    COUNT(*) AS total_records,
    COUNTIF(order_id IS NULL) AS null_order_id,
    COUNTIF(last_modified_date IS NULL) AS null_last_modified,
    ROUND(COUNTIF(order_id IS NULL) / COUNT(*) * 100, 2) AS null_order_id_pct,
    ROUND(COUNTIF(last_modified_date IS NULL) / COUNT(*) * 100, 2) AS null_last_modified_pct
FROM `b25h01-ragic.erp_backup.ragic_data`
GROUP BY sheet_code
ORDER BY sheet_code;

-- ============================================================================
-- D. 最近同步狀態檢查
-- ============================================================================

-- D.1 查看最近 7 天的資料更新量
SELECT
    sheet_code,
    DATE(last_modified_date) AS update_date,
    COUNT(*) AS record_count
FROM `b25h01-ragic.erp_backup.ragic_data`
WHERE last_modified_date >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
GROUP BY sheet_code, update_date
ORDER BY update_date DESC, sheet_code;

-- D.2 查看同步狀態表（sheet_sync_state）
SELECT
    sheet_code,
    last_sync_timestamp,
    TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), last_sync_timestamp, HOUR) AS hours_since_sync,
    updated_at
FROM `b25h01-ragic.ragic_backup.sheet_sync_state`
ORDER BY sheet_code;

-- D.3 比對同步狀態與實際資料（找出不一致）
SELECT
    s.sheet_code,
    s.last_sync_timestamp AS sync_state_timestamp,
    d.max_last_modified AS actual_max_timestamp,
    TIMESTAMP_DIFF(d.max_last_modified, s.last_sync_timestamp, HOUR) AS hours_difference
FROM `b25h01-ragic.ragic_backup.sheet_sync_state` s
LEFT JOIN (
    SELECT
        sheet_code,
        MAX(last_modified_date) AS max_last_modified
    FROM `b25h01-ragic.erp_backup.ragic_data`
    GROUP BY sheet_code
) d ON s.sheet_code = d.sheet_code
ORDER BY hours_difference DESC;

-- ============================================================================
-- E. 資料分佈分析
-- ============================================================================

-- E.1 查看每月資料量趨勢
SELECT
    sheet_code,
    FORMAT_TIMESTAMP('%Y-%m', last_modified_date) AS month,
    COUNT(*) AS record_count
FROM `b25h01-ragic.erp_backup.ragic_data`
WHERE last_modified_date IS NOT NULL
  AND last_modified_date <= CURRENT_TIMESTAMP()
GROUP BY sheet_code, month
ORDER BY sheet_code, month DESC
LIMIT 500;

-- E.2 查看最近 24 小時的新增/更新資料
SELECT
    sheet_code,
    COUNT(*) AS recent_records,
    MIN(last_modified_date) AS earliest_in_period,
    MAX(last_modified_date) AS latest_in_period
FROM `b25h01-ragic.erp_backup.ragic_data`
WHERE last_modified_date >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
  AND last_modified_date <= CURRENT_TIMESTAMP()
GROUP BY sheet_code
ORDER BY recent_records DESC;

-- ============================================================================
-- F. 清理與修正查詢（僅在必要時執行）
-- ============================================================================

-- F.1 清理未來日期資料（將其設為 NULL）
-- 警告：這會修改資料，請謹慎執行
/*
UPDATE `b25h01-ragic.erp_backup.ragic_data`
SET last_modified_date = NULL,
    data_quality_notes = CONCAT(
        IFNULL(data_quality_notes, ''),
        IF(data_quality_notes IS NULL, '', '; '),
        '{"issue":"future_date_cleaned","original_value":"',
        CAST(last_modified_date AS STRING),
        '","cleaned_at":"',
        CAST(CURRENT_TIMESTAMP() AS STRING),
        '"}'
    )
WHERE last_modified_date > TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 DAY);
*/

-- F.2 回寫同步狀態（以實際資料的最大日期為準）
-- 注意：這會更新 sheet_sync_state 表
/*
MERGE `b25h01-ragic.ragic_backup.sheet_sync_state` T
USING (
  SELECT
      sheet_code,
      MAX(last_modified_date) AS last_sync_timestamp
  FROM `b25h01-ragic.erp_backup.ragic_data`
  WHERE last_modified_date IS NOT NULL
    AND last_modified_date <= CURRENT_TIMESTAMP()
  GROUP BY sheet_code
) S
ON T.sheet_code = S.sheet_code
WHEN MATCHED THEN
    UPDATE SET
        last_sync_timestamp = S.last_sync_timestamp,
        updated_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (sheet_code, last_sync_timestamp, updated_at)
    VALUES (S.sheet_code, S.last_sync_timestamp, CURRENT_TIMESTAMP());
*/

-- ============================================================================
-- G. 特定 Sheet 的深入分析範例
-- ============================================================================

-- G.1 分析 Sheet 99（最大表單）的資料品質
SELECT
    'Sheet 99 統計' AS analysis,
    COUNT(*) AS total_records,
    COUNT(DISTINCT order_id) AS unique_orders,
    MIN(last_modified_date) AS earliest_date,
    MAX(last_modified_date) AS latest_date,
    COUNTIF(last_modified_date > CURRENT_TIMESTAMP()) AS future_dates,
    COUNTIF(data_quality_notes IS NOT NULL) AS records_with_notes
FROM `b25h01-ragic.erp_backup.ragic_data`
WHERE sheet_code = '99';

-- G.2 Sheet 41（郵遞區號靜態表）檢查
-- 注意：此表無時間欄位，所有 last_modified_date 應為 NULL
SELECT
    'Sheet 41 (郵遞區號)' AS analysis,
    COUNT(*) AS total_records,
    COUNTIF(last_modified_date IS NULL) AS null_dates,
    COUNTIF(last_modified_date IS NOT NULL) AS non_null_dates
FROM `b25h01-ragic.erp_backup.ragic_data`
WHERE sheet_code = '41';

-- ============================================================================
-- 使用說明
-- ============================================================================

/*
執行方式：

1. 使用 BigQuery Console：
   - 複製所需的查詢到 BigQuery Console
   - 按需要執行個別查詢或批次執行

2. 使用 bq 命令列工具：
   bq query --use_legacy_sql=false < scripts/verify_data_quality.sql

3. 使用腳本執行特定查詢：
   bq query --use_legacy_sql=false "SELECT ... FROM ..."

注意事項：
- 以 UPDATE/MERGE 開頭的修改查詢預設被註解，需手動取消註解執行
- 執行修改查詢前請先備份資料或在測試環境驗證
- 建議先執行唯讀查詢（A-E 區塊）確認問題後再執行修正查詢（F 區塊）
*/
