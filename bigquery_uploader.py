# -*- coding: utf-8 -*-
"""
BigQuery 上傳模組
專門處理資料上傳至 BigQuery 的功能
"""

import logging
import datetime
from typing import List, Dict, Any, Optional, Tuple
from google.cloud import bigquery
from google.cloud.exceptions import NotFound, Conflict
from data_transformer import BIGQUERY_SCHEMA


class BigQueryUploader:
    """BigQuery 上傳器類別"""

    def __init__(self, project_id: str, location: str = "US"):
        """
        初始化 BigQuery 上傳器

        Args:
            project_id: GCP 專案 ID
            location: BigQuery 資料集位置

        Raises:
            Exception: 當無法建立 BigQuery 客戶端時
        """
        if not project_id:
            raise ValueError("專案 ID 不可為空")

        self.project_id = project_id
        self.location = location

        try:
            self.client = bigquery.Client(project=project_id)
            logging.info(f"BigQuery 客戶端初始化完成 - 專案: {project_id}")
        except Exception as e:
            raise Exception(f"無法建立 BigQuery 客戶端連線: {e}")

    def upload_data(self,
                   data: List[Dict[str, Any]],
                   dataset_id: str,
                   table_id: str,
                   schema: Optional[List[bigquery.SchemaField]] = None,
                   use_merge: bool = True) -> Dict[str, Any]:
        """
        上傳資料至 BigQuery

        Args:
            data: 要上傳的資料
            dataset_id: 資料集 ID
            table_id: 資料表 ID
            schema: BigQuery 架構，如果不提供則使用預設
            use_merge: 是否使用 MERGE 操作（Upsert），否則使用 INSERT

        Returns:
            Dict: 上傳結果統計

        Raises:
            Exception: 當上傳失敗時
        """
        if not data:
            logging.warning("沒有資料需要上傳到 BigQuery")
            return {"status": "no_data", "records_processed": 0}

        if not dataset_id or not table_id:
            raise ValueError("資料集 ID 或資料表 ID 不可為空")

        # 使用預設 schema 如果沒有提供
        if schema is None:
            schema = BIGQUERY_SCHEMA

        logging.info(f"開始上傳資料至 BigQuery - 資料集: {dataset_id}, 資料表: {table_id}")
        start_time = datetime.datetime.now()

        try:
            # 確保資料集和資料表存在
            table_ref = self._ensure_table_exists(dataset_id, table_id, schema)

            # 執行上傳
            if use_merge:
                result = self._upload_with_merge(data, table_ref, schema)
            else:
                result = self._upload_with_insert(data, table_ref, schema)

            end_time = datetime.datetime.now()
            duration = (end_time - start_time).total_seconds()

            result.update({
                "duration_seconds": duration,
                "upload_time": end_time.isoformat()
            })

            logging.info(f"BigQuery 上傳完成 - 耗時: {duration:.2f} 秒, 處理記錄: {result.get('records_processed', 0)}")
            return result

        except Exception as e:
            logging.error(f"BigQuery 上傳失敗: {e}")
            raise

    def _ensure_table_exists(self,
                           dataset_id: str,
                           table_id: str,
                           schema: List[bigquery.SchemaField]) -> str:
        """
        確保資料集和資料表存在

        Args:
            dataset_id: 資料集 ID
            table_id: 資料表 ID
            schema: BigQuery 架構

        Returns:
            str: 完整的資料表參考路徑

        Raises:
            Exception: 當建立失敗時
        """
        # 完整資料表參考
        table_ref = f"{self.project_id}.{dataset_id}.{table_id}"

        # 確保資料集存在
        self._ensure_dataset_exists(dataset_id)

        # 確保資料表存在
        try:
            self.client.get_table(table_ref)
            logging.info(f"資料表 {table_id} 已存在")
        except NotFound:
            logging.info(f"資料表 {table_id} 不存在，正在建立...")
            try:
                table = bigquery.Table(table_ref, schema=schema)
                self.client.create_table(table)
                logging.info(f"資料表 {table_id} 建立成功")
            except Exception as e:
                raise Exception(f"建立資料表失敗: {e}")

        return table_ref

    def _ensure_dataset_exists(self, dataset_id: str):
        """
        確保資料集存在

        Args:
            dataset_id: 資料集 ID

        Raises:
            Exception: 當建立失敗時
        """
        dataset_ref = f"{self.project_id}.{dataset_id}"

        try:
            self.client.get_dataset(dataset_ref)
            logging.info(f"資料集 {dataset_id} 已存在")
        except NotFound:
            logging.info(f"資料集 {dataset_id} 不存在，正在建立...")
            try:
                dataset = bigquery.Dataset(dataset_ref)
                dataset.location = self.location
                self.client.create_dataset(dataset)
                logging.info(f"資料集 {dataset_id} 建立成功")
            except Exception as e:
                raise Exception(f"建立資料集失敗: {e}")

    def _upload_with_merge(self,
                          data: List[Dict[str, Any]],
                          table_ref: str,
                          schema: List[bigquery.SchemaField]) -> Dict[str, Any]:
        """
        使用 MERGE 操作上傳資料（Upsert）

        Args:
            data: 要上傳的資料
            table_ref: 資料表參考路徑
            schema: BigQuery 架構

        Returns:
            Dict: 上傳結果

        Raises:
            Exception: 當 MERGE 操作失敗時
        """
        logging.info(f"使用 MERGE 操作上傳 {len(data)} 筆資料")

        try:
            # 動態生成所有欄位名稱
            all_fields = [field.name for field in schema]

            # 生成 UPDATE SET 子句
            update_statements = [f"T.{field} = S.{field}" for field in all_fields if field != 'order_id']
            update_clause = ",\n        ".join(update_statements)

            # 生成 INSERT 子句
            insert_fields = ", ".join(all_fields)
            insert_values = ", ".join([f"S.{field}" for field in all_fields])

            # 準備 MERGE SQL
            merge_query = f"""
            MERGE `{table_ref}` T
            USING (SELECT * FROM UNNEST(@new_data)) S
            ON T.order_id = S.order_id
            WHEN MATCHED THEN
              UPDATE SET
                {update_clause}
            WHEN NOT MATCHED THEN
              INSERT ({insert_fields})
              VALUES ({insert_values})
            """

            # 設定查詢參數
            query_params = [bigquery.ArrayQueryParameter('new_data', 'STRUCT', data)]
            job_config = bigquery.QueryJobConfig(
                query_parameters=query_params,
                job_timeout_ms=600000  # 10分鐘逾時
            )

            # 執行 MERGE
            query_job = self.client.query(merge_query, job_config=job_config)
            result = query_job.result()

            if query_job.errors:
                raise Exception(f"MERGE 操作執行錯誤: {query_job.errors}")

            # 獲取影響的行數
            affected_rows = getattr(query_job, 'num_dml_affected_rows', len(data))

            return {
                "status": "success",
                "method": "merge",
                "records_processed": len(data),
                "affected_rows": affected_rows
            }

        except Exception as e:
            logging.error(f"MERGE 操作失敗: {e}")
            # 嘗試備用的 INSERT 方案
            logging.info("嘗試使用 INSERT 操作作為備用方案...")
            return self._upload_with_insert(data, table_ref, schema, is_fallback=True)

    def _upload_with_insert(self,
                           data: List[Dict[str, Any]],
                           table_ref: str,
                           schema: List[bigquery.SchemaField],
                           is_fallback: bool = False) -> Dict[str, Any]:
        """
        使用 INSERT 操作上傳資料

        Args:
            data: 要上傳的資料
            table_ref: 資料表參考路徑
            schema: BigQuery 架構
            is_fallback: 是否為備用方案

        Returns:
            Dict: 上傳結果

        Raises:
            Exception: 當 INSERT 操作失敗時
        """
        method_name = "insert_fallback" if is_fallback else "insert"
        logging.info(f"使用 INSERT 操作上傳 {len(data)} 筆資料")

        try:
            job_config = bigquery.LoadJobConfig(
                schema=schema,
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND
            )

            job = self.client.load_table_from_json(
                data, table_ref, job_config=job_config
            )
            job.result()  # 等待完成

            if job.errors:
                raise Exception(f"INSERT 操作執行錯誤: {job.errors}")

            return {
                "status": "success",
                "method": method_name,
                "records_processed": len(data),
                "affected_rows": len(data)
            }

        except Exception as e:
            error_message = f"{'備用 ' if is_fallback else ''}INSERT 操作失敗: {e}"
            logging.error(error_message)
            if is_fallback:
                raise Exception(f"所有上傳方法都失敗: {e}")
            else:
                raise Exception(error_message)

    def get_last_sync_timestamp(self, dataset_id: str, table_id: str) -> int:
        """
        從 BigQuery 獲取最後一次同步時間戳

        Args:
            dataset_id: 資料集 ID
            table_id: 資料表 ID

        Returns:
            int: 最後同步時間戳（毫秒）
        """
        try:
            table_ref = f"{self.project_id}.{dataset_id}.{table_id}"
            query = f"""
            SELECT MAX(updated_at) as last_sync
            FROM `{table_ref}`
            WHERE updated_at IS NOT NULL
            """

            query_job = self.client.query(query)
            result = query_job.result()

            for row in result:
                if row.last_sync:
                    timestamp = int(row.last_sync.timestamp() * 1000)
                    logging.info(f"獲取到最後同步時間戳: {timestamp}")
                    return timestamp

            # 如果沒有資料，返回一週前
            last_week = datetime.datetime.now() - datetime.timedelta(weeks=1)
            timestamp = int(last_week.timestamp() * 1000)
            logging.info(f"資料表為空，使用預設時間戳: {timestamp}")
            return timestamp

        except Exception as e:
            logging.warning(f"無法獲取最後同步時間: {e}，使用預設值（一週前）")
            last_week = datetime.datetime.now() - datetime.timedelta(weeks=1)
            return int(last_week.timestamp() * 1000)

    def test_connection(self) -> bool:
        """
        測試 BigQuery 連線

        Returns:
            bool: 連線成功返回 True
        """
        try:
            # 嘗試列出資料集
            datasets = list(self.client.list_datasets(max_results=1))
            logging.info("BigQuery 連線測試成功")
            return True
        except Exception as e:
            logging.error(f"BigQuery 連線測試失敗: {e}")
            return False

    def get_table_info(self, dataset_id: str, table_id: str) -> Optional[Dict[str, Any]]:
        """
        獲取資料表資訊

        Args:
            dataset_id: 資料集 ID
            table_id: 資料表 ID

        Returns:
            Optional[Dict]: 資料表資訊，如果不存在則返回 None
        """
        try:
            table_ref = f"{self.project_id}.{dataset_id}.{table_id}"
            table = self.client.get_table(table_ref)

            return {
                "table_id": table.table_id,
                "created": table.created.isoformat() if table.created else None,
                "modified": table.modified.isoformat() if table.modified else None,
                "num_rows": table.num_rows,
                "num_bytes": table.num_bytes,
                "schema_fields": len(table.schema)
            }

        except NotFound:
            logging.info(f"資料表 {table_id} 不存在")
            return None
        except Exception as e:
            logging.error(f"獲取資料表資訊失敗: {e}")
            return None

    def close(self):
        """關閉連線"""
        if hasattr(self, 'client'):
            self.client.close()
            logging.info("BigQuery 客戶端連線已關閉")


def create_uploader(project_id: str, **kwargs) -> BigQueryUploader:
    """
    建立 BigQuery 上傳器的工廠函數

    Args:
        project_id: GCP 專案 ID
        **kwargs: 其他設定參數

    Returns:
        BigQueryUploader: BigQuery 上傳器實例
    """
    return BigQueryUploader(project_id, **kwargs)


def batch_upload_data(uploader: BigQueryUploader,
                     data: List[Dict[str, Any]],
                     dataset_id: str,
                     table_id: str,
                     batch_size: int = 1000) -> List[Dict[str, Any]]:
    """
    批次上傳大量資料

    Args:
        uploader: BigQuery 上傳器實例
        data: 要上傳的資料
        dataset_id: 資料集 ID
        table_id: 資料表 ID
        batch_size: 每批次大小

    Returns:
        List[Dict]: 每批次的上傳結果

    Raises:
        Exception: 當批次上傳失敗時
    """
    if not data:
        return []

    results = []
    total_batches = (len(data) + batch_size - 1) // batch_size

    logging.info(f"開始批次上傳，總計 {len(data)} 筆資料，分 {total_batches} 批次")

    for i in range(0, len(data), batch_size):
        batch_data = data[i:i + batch_size]
        batch_num = (i // batch_size) + 1

        logging.info(f"上傳第 {batch_num}/{total_batches} 批次，{len(batch_data)} 筆資料")

        try:
            result = uploader.upload_data(batch_data, dataset_id, table_id)
            result["batch_number"] = batch_num
            results.append(result)

        except Exception as e:
            logging.error(f"第 {batch_num} 批次上傳失敗: {e}")
            results.append({
                "batch_number": batch_num,
                "status": "error",
                "error": str(e),
                "records_processed": 0
            })
            # 可以選擇繼續或停止
            # raise  # 取消註解以在任何批次失敗時停止

    logging.info(f"批次上傳完成，成功批次: {sum(1 for r in results if r.get('status') == 'success')}")
    return results