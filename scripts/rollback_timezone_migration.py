#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BigQuery 時區回滾腳本：回滾錯誤的 +8 小時遷移

⚠️ 重要提醒
============
此腳本用於修復錯誤的時區遷移：
- 錯誤：原先以為 Ragic 資料是 UTC，將所有時間 +8 小時
- 事實：Ragic 資料本來就是台北時間（UTC+8）
- 修復：將所有時間 -8 小時，恢復正確的台北時間

執行步驟：
1. 建立備份表（自動）
2. 回滾時間戳記（-8 小時）
3. 驗證回滾結果

"""

from google.cloud import bigquery
import logging
import os
import sys
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def rollback_timezone():
    """執行時區回滾（-8 小時）"""

    project_id = os.environ.get('GCP_PROJECT_ID', 'b25h01-ragic')
    dataset_id = os.environ.get('BIGQUERY_DATASET', 'erp_backup')
    table_id = os.environ.get('BIGQUERY_TABLE', 'ragic_data')

    client = bigquery.Client(project=project_id)
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    backup_table_ref = f"{project_id}.{dataset_id}.ragic_data_backup_error_20251022"

    logging.info("=" * 60)
    logging.info("BigQuery 時區回滾作業")
    logging.info("=" * 60)
    logging.info(f"專案: {project_id}")
    logging.info(f"資料表: {table_ref}")
    logging.info(f"備份表: {backup_table_ref}")
    logging.info("")

    # 步驟 1：檢查資料量
    logging.info("步驟 1: 檢查資料量")
    count_query = f"SELECT COUNT(*) as total FROM `{table_ref}`"
    count_result = client.query(count_query).result()
    total_rows = list(count_result)[0].total
    logging.info(f"總資料筆數: {total_rows:,}")
    logging.info("")

    # 步驟 2：建立備份表
    logging.info("步驟 2: 建立備份表")
    logging.info(f"備份表名: {backup_table_ref}")

    # 檢查備份表是否已存在
    try:
        client.get_table(backup_table_ref)
        logging.warning(f"⚠️  備份表已存在：{backup_table_ref}")
        overwrite = input("是否覆蓋現有備份表？ (yes/no): ")
        if overwrite.lower() != 'yes':
            logging.info("使用者取消操作")
            return
        # 刪除舊備份表
        client.delete_table(backup_table_ref)
        logging.info("已刪除舊備份表")
    except Exception:
        logging.info("備份表不存在，將建立新備份")

    # 建立備份
    backup_query = f"""
    CREATE TABLE `{backup_table_ref}`
    AS SELECT * FROM `{table_ref}`
    """

    try:
        logging.info("正在建立備份表...")
        backup_job = client.query(backup_query)
        backup_job.result()  # 等待完成
        logging.info(f"✅ 備份表建立成功：{backup_table_ref}")
    except Exception as e:
        logging.error(f"❌ 備份失敗：{e}")
        logging.error("為安全起見，中止回滾操作")
        sys.exit(1)
    logging.info("")

    # 步驟 3：顯示目前錯誤的時間範例
    logging.info("步驟 3: 顯示目前錯誤的時間（+8 小時）")
    sample_query = f"""
    SELECT
        sheet_code,
        MAX(last_modified_date) as latest_time
    FROM `{table_ref}`
    WHERE sheet_code IN ('50', '99')
    GROUP BY sheet_code
    ORDER BY sheet_code
    """
    sample_result = client.query(sample_query).result()
    logging.info("目前錯誤時間（應為實際時間 +8 小時）:")
    for row in sample_result:
        logging.info(f"  Sheet {row.sheet_code}: {row.latest_time}")
    logging.info("")

    # 步驟 4：確認執行回滾
    logging.warning("⚠️  即將執行回滾操作（所有時間 -8 小時）")
    logging.warning(f"⚠️  將影響 {total_rows:,} 筆資料")
    logging.info("此操作不可逆，但已建立備份表")
    logging.info(f"如需還原，請從備份表 {backup_table_ref} 恢復")
    logging.info("")

    confirm = input("確認執行回滾？ (yes/no): ")
    if confirm.lower() != 'yes':
        logging.info("使用者取消操作")
        return

    # 步驟 5：執行回滾（-8 小時）
    logging.info("")
    logging.info("步驟 4: 執行時間回滾（-8 小時）")
    rollback_query = f"""
    UPDATE `{table_ref}`
    SET
        created_at = TIMESTAMP_SUB(created_at, INTERVAL 8 HOUR),
        updated_at = TIMESTAMP_SUB(updated_at, INTERVAL 8 HOUR),
        last_modified_date = TIMESTAMP_SUB(last_modified_date, INTERVAL 8 HOUR)
    WHERE TRUE
    """

    logging.info("正在執行回滾（這可能需要數分鐘）...")
    try:
        query_job = client.query(rollback_query)
        result = query_job.result()
        affected_rows = query_job.num_dml_affected_rows
        logging.info(f"✅ 回滾完成！受影響的資料筆數: {affected_rows:,}")
    except Exception as e:
        logging.error(f"❌ 回滾失敗：{e}")
        logging.error(f"請從備份表恢復：{backup_table_ref}")
        sys.exit(1)
    logging.info("")

    # 步驟 6：驗證回滾結果
    logging.info("步驟 5: 驗證回滾結果")
    verify_query = f"""
    SELECT
        sheet_code,
        COUNT(*) as total,
        MAX(last_modified_date) as latest_time
    FROM `{table_ref}`
    WHERE sheet_code IN ('50', '99')
    GROUP BY sheet_code
    ORDER BY sheet_code
    """

    verify_result = client.query(verify_query).result()
    logging.info("回滾後的時間（應為正確的台北時間）:")
    logging.info("")
    logging.info("預期結果：")
    logging.info("  Sheet 50: ~2025-10-14 11:19:00（減少 8 小時）")
    logging.info("  Sheet 99: ~2025-10-14 11:21:00（減少 8 小時）")
    logging.info("")
    logging.info("實際結果：")
    for row in verify_result:
        logging.info(f"  Sheet {row.sheet_code}: {row.latest_time} ({row.total:,} 筆)")

    logging.info("")
    logging.info("=" * 60)
    logging.info("回滾作業完成")
    logging.info("=" * 60)
    logging.info(f"備份表: {backup_table_ref}")
    logging.info("下一步：執行增量備份以補齊遺失的 8 天資料")
    logging.info("=" * 60)

if __name__ == '__main__':
    try:
        rollback_timezone()
    except KeyboardInterrupt:
        logging.info("\n使用者中斷操作")
        sys.exit(1)
    except Exception as e:
        logging.error(f"執行失敗：{e}")
        sys.exit(1)
