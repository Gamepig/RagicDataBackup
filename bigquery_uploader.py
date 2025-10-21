# -*- coding: utf-8 -*-
"""
BigQuery 上傳模組
專門處理資料上傳至 BigQuery 的功能
"""

import logging
import datetime
import uuid
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
                   use_merge: bool = True,
                   upload_mode: str = "auto",
                   batch_threshold: int = 5000,
                   staging_table: Optional[str] = None,
                   merge_sp_name: Optional[str] = None) -> Dict[str, Any]:
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
            # 直送或 staging+SP 決策
            mode = (upload_mode or "auto").lower()

            if mode == "staging_sp" or (mode == "auto" and len(data) > batch_threshold):
                # 使用 staging + 預儲程序
                st_table = staging_table or f"{table_id}_staging"
                result = self._upload_via_staging(
                    data=data,
                    dataset_id=dataset_id,
                    target_table_id=table_id,
                    staging_table_id=st_table,
                    base_schema=schema,
                    merge_sp_name=merge_sp_name
                )
            else:
                # 直送（MERGE / INSERT）
                table_ref = self._ensure_table_exists(dataset_id, table_id, schema)
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

    def truncate_table(self, dataset_id: str, table_id: str, *, confirm: bool = False) -> None:
        """
        清空指定資料表（僅用於手動全量上傳前）。

        Args:
            dataset_id: 資料集 ID
            table_id: 資料表 ID
            confirm: 必須為 True 才允許執行，避免誤刪
        """
        if not confirm:
            raise ValueError("為避免誤刪，truncate_table 需要 confirm=True 才能執行")

        table_ref = f"{self.project_id}.{dataset_id}.{table_id}"
        logging.info(f"即將清空資料表: {table_ref}")
        try:
            # 先確保資料表存在
            self._ensure_table_exists(dataset_id, table_id, BIGQUERY_SCHEMA)

            # 優先使用 TRUNCATE TABLE，若不支援則改用 DELETE FROM
            try:
                qcfg = bigquery.QueryJobConfig(job_timeout_ms=600000)
                q = f"TRUNCATE TABLE `{table_ref}`"
                job = self.client.query(q, job_config=qcfg)
                job.result()
            except Exception:
                logging.warning("TRUNCATE TABLE 失敗，改用 DELETE FROM 方式清空")
                qcfg = bigquery.QueryJobConfig(job_timeout_ms=600000)
                q = f"DELETE FROM `{table_ref}` WHERE TRUE"
                job = self.client.query(q, job_config=qcfg)
                job.result()

            logging.info(f"資料表已清空: {table_ref}")
        except Exception as e:
            logging.error(f"清空資料表失敗: {e}")
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

    def _get_existing_table_schema(self, table_ref: str) -> Optional[List[bigquery.SchemaField]]:
        """
        讀取 BigQuery 目標表的現有 Schema，若不存在則回傳 None。
        """
        try:
            table = self.client.get_table(table_ref)
            return list(table.schema)
        except NotFound:
            return None
        except Exception as e:
            logging.warning(f"讀取資料表 Schema 失敗（{table_ref}）: {e}")
            return None

    def _project_records_to_schema(self, data: List[Dict[str, Any]], schema: List[bigquery.SchemaField]) -> List[Dict[str, Any]]:
        """
        僅保留 schema 中定義的欄位，避免 BigQuery 載入時出現未知欄位錯誤
        """
        allowed = {f.name for f in schema}
        projected: List[Dict[str, Any]] = []
        for rec in data:
            filtered = {k: v for k, v in rec.items() if k in allowed}
            projected.append(filtered)
        return projected

    def _get_staging_schema(self, base_schema: List[bigquery.SchemaField]) -> List[bigquery.SchemaField]:
        """
        取得 staging 表的 Schema：在基底 schema 基礎上，額外附加批次欄位

        Args:
            base_schema: 目標表的 BigQuery 架構

        Returns:
            List[bigquery.SchemaField]: staging 架構
        """
        field_names = {f.name for f in base_schema}
        staging_schema = list(base_schema)

        if 'batch_id' not in field_names:
            staging_schema.append(bigquery.SchemaField('batch_id', 'STRING'))
        if 'ingested_at' not in field_names:
            staging_schema.append(bigquery.SchemaField('ingested_at', 'TIMESTAMP'))

        return staging_schema

    def _resolve_sp_fqn(self, merge_sp_name: Optional[str], dataset_id: str) -> str:
        """
        解析預儲程序全名

        規則：
        - 若 merge_sp_name 含兩個點（project.dataset.proc），視為完整名稱
        - 若只含一個點（dataset.proc），自動補上本專案 ID
        - 若不含點，補上本專案與目前 dataset
        """
        if not merge_sp_name:
            # 預設名稱
            sp = 'sp_upsert_ragic_data'
        else:
            sp = merge_sp_name

        if sp.count('.') == 2:
            return sp
        elif sp.count('.') == 1:
            # dataset.proc
            return f"{self.project_id}.{sp}"
        else:
            # proc
            return f"{self.project_id}.{dataset_id}.{sp}"

    def _upload_via_staging(self,
                            data: List[Dict[str, Any]],
                            dataset_id: str,
                            target_table_id: str,
                            staging_table_id: str,
                            base_schema: List[bigquery.SchemaField],
                            merge_sp_name: Optional[str]) -> Dict[str, Any]:
        """
        透過 staging 表 + 預儲程序進行上傳（高吞吐、安全）
        """
        logging.info(f"使用 staging+SP 上傳 {len(data)} 筆資料 → staging 表: {staging_table_id}")

        # 生成批次 ID 與時間戳
        batch_id = f"{datetime.datetime.utcnow().strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:8]}"
        ingested_at = datetime.datetime.utcnow().isoformat()

        # 準備 staging schema 與資料
        staging_schema = self._get_staging_schema(base_schema)
        staging_table_ref = self._ensure_table_exists(dataset_id, staging_table_id, staging_schema)

        # 以實際 staging 表 schema 為準，避免未知欄位
        existing_staging_schema = self._get_existing_table_schema(staging_table_ref) or staging_schema

        # 附加批次欄位並投影到 schema（排除未知欄位如 _ragicId 等）
        payload = []
        allowed_fields = {f.name for f in existing_staging_schema}
        for item in data:
            enriched = {k: v for k, v in item.items() if k in allowed_fields}
            enriched['batch_id'] = batch_id
            enriched['ingested_at'] = ingested_at
            payload.append(enriched)

        # 載入至 staging（Append）
        job_config = bigquery.LoadJobConfig(
            schema=existing_staging_schema,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND
        )
        load_job = self.client.load_table_from_json(payload, staging_table_ref, job_config=job_config)
        load_job.result()
        if load_job.errors:
            raise Exception(f"載入 staging 失敗: {load_job.errors}")

        # 呼叫預儲程序執行 MERGE（在 BQ 端完成 Upsert / 清理 / 審計）
        sp_fqn = self._resolve_sp_fqn(merge_sp_name, dataset_id)
        logging.info(f"呼叫預儲程序: {sp_fqn} (batch_id={batch_id})")

        call_query = f"CALL `{sp_fqn}`(@batch_id)"
        qparams = [bigquery.ScalarQueryParameter('batch_id', 'STRING', batch_id)]
        qcfg = bigquery.QueryJobConfig(query_parameters=qparams, job_timeout_ms=600000)
        qjob = self.client.query(call_query, job_config=qcfg)
        qjob.result()
        if qjob.errors:
            raise Exception(f"預儲程序呼叫失敗: {qjob.errors}")

        return {
            "status": "success",
            "method": "staging_sp",
            "records_processed": len(data),
            "batch_id": batch_id,
            "staging_table": staging_table_id,
            "stored_procedure": sp_fqn
        }

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
            # 以目標表現有欄位為準，與提供 schema 取交集，避免未知欄位
            existing_schema = self._get_existing_table_schema(table_ref) or schema
            existing_names = {f.name for f in existing_schema}
            provided_by_name = {f.name: f for f in schema}
            effective_schema = [provided_by_name[name] for name in existing_names if name in provided_by_name]
            if not effective_schema:
                effective_schema = schema

            all_fields = [field.name for field in effective_schema]

            # 生成 UPDATE SET 子句
            update_statements = [f"T.{field} = S.{field}" for field in all_fields if field != 'order_id']
            update_clause = ",\n        ".join(update_statements)

            # 生成 INSERT 子句
            insert_fields = ", ".join(all_fields)
            insert_values = ", ".join([f"S.{field}" for field in all_fields])

            # 建立臨時表並載入資料（避免複雜的 STRUCT 參數型別問題）
            try:
                project_id, dataset_id, _ = table_ref.split('.')
            except ValueError:
                # 退而求其次：使用預設專案
                project_id = self.project_id
                parts = table_ref.split('.')
                dataset_id = parts[1] if len(parts) > 1 else self.location

            temp_table_id = f"__tmp_merge_{uuid.uuid4().hex[:12]}"
            temp_full_ref = f"{project_id}.{dataset_id}.{temp_table_id}"

            # 建立臨時表
            temp_table = bigquery.Table(temp_full_ref, schema=effective_schema)
            self.client.create_table(temp_table)

            try:
                # 載入資料至臨時表
                load_cfg = bigquery.LoadJobConfig(schema=effective_schema, write_disposition=bigquery.WriteDisposition.WRITE_APPEND)
                load_job = self.client.load_table_from_json(self._project_records_to_schema(data, effective_schema), temp_full_ref, job_config=load_cfg)
                load_job.result()
                if load_job.errors:
                    raise Exception(load_job.errors)

                # 使用臨時表進行 MERGE（來源端依 order_id 去重，取 updated_at 最新一筆）
                merge_query = f"""
                MERGE `{table_ref}` T
                USING (
                  SELECT * EXCEPT(row_num) FROM (
                    SELECT *, ROW_NUMBER() OVER (
                      PARTITION BY order_id
                      ORDER BY updated_at DESC NULLS LAST
                    ) AS row_num
                    FROM `{temp_full_ref}`
                  ) WHERE row_num = 1
                ) S
                ON T.order_id = S.order_id
                WHEN MATCHED THEN
                  UPDATE SET
                    {update_clause}
                WHEN NOT MATCHED THEN
                  INSERT ({insert_fields})
                  VALUES ({insert_values})
                """

                qcfg = bigquery.QueryJobConfig(job_timeout_ms=600000)
                qjob = self.client.query(merge_query, job_config=qcfg)
                qjob.result()
                if qjob.errors:
                    raise Exception(f"MERGE 操作執行錯誤: {qjob.errors}")

                affected_rows = getattr(qjob, 'num_dml_affected_rows', len(data))
                return {
                    "status": "success",
                    "method": "merge",
                    "records_processed": len(data),
                    "affected_rows": affected_rows
                }
            finally:
                # 確保臨時表被移除
                try:
                    self.client.delete_table(temp_full_ref, not_found_ok=True)
                except Exception:
                    logging.warning(f"無法刪除臨時表: {temp_full_ref}")

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
                self._project_records_to_schema(data, schema), table_ref, job_config=job_config
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

    def get_last_sync_timestamp(self, dataset_id: str, table_id: str) -> str:
        """
        從 BigQuery 獲取最後一次同步時間

        Args:
            dataset_id: 資料集 ID
            table_id: 資料表 ID

        Returns:
            str: Ragic 格式的日期時間 (yyyy/MM/dd HH:mm:ss)
        """
        try:
            table_ref = f"{self.project_id}.{dataset_id}.{table_id}"
            query = f"""
            SELECT MAX(COALESCE(last_modified_date, updated_at, created_at)) as last_sync
            FROM `{table_ref}`
            WHERE COALESCE(last_modified_date, updated_at, created_at) IS NOT NULL
            """

            query_job = self.client.query(query)
            result = query_job.result()

            for row in result:
                if row.last_sync:
                    # 轉為 Ragic 要求的日期格式
                    ragic_format = row.last_sync.strftime('%Y/%m/%d %H:%M:%S')
                    logging.info(f"獲取到最後同步時間: {ragic_format}")
                    return ragic_format

            # 如果沒有資料，返回一週前
            last_week = datetime.datetime.now() - datetime.timedelta(weeks=1)
            ragic_format = last_week.strftime('%Y/%m/%d %H:%M:%S')
            logging.info(f"資料表為空，使用預設時間: {ragic_format}")
            return ragic_format

        except Exception as e:
            logging.warning(f"無法獲取最後同步時間: {e}，使用預設值（一週前）")
            last_week = datetime.datetime.now() - datetime.timedelta(weeks=1)
            return last_week.strftime('%Y/%m/%d %H:%M:%S')

    def get_last_sync_timestamp_by_sheet(self, dataset_id: str, table_id: str, sheet_code: str) -> str:
        """
        依 sheet_code 從 BigQuery 取得最後同步時間（避免使用全表最大時間導致其他表全數被跳過）。

        Returns Ragic 格式 yyyy/MM/dd HH:mm:ss；若無資料，回傳一週前。
        """
        try:
            table_ref = f"{self.project_id}.{dataset_id}.{table_id}"
            query = f"""
            SELECT MAX(COALESCE(last_modified_date, updated_at, created_at)) as last_sync
            FROM `{table_ref}`
            WHERE sheet_code = @sheet_code
              AND COALESCE(last_modified_date, updated_at, created_at) IS NOT NULL
            """
            qcfg = bigquery.QueryJobConfig(query_parameters=[
                bigquery.ScalarQueryParameter('sheet_code', 'STRING', sheet_code)
            ])
            result = self.client.query(query, job_config=qcfg).result()
            for row in result:
                if row.last_sync:
                    # BigQuery TIMESTAMP 是 UTC，需轉換為台北時間（UTC+8）給 Ragic API 使用
                    taipei_time = row.last_sync + datetime.timedelta(hours=8)
                    logging.info(f"[sheet {sheet_code}] 最後同步時間（UTC）: {row.last_sync.strftime('%Y/%m/%d %H:%M:%S')}")
                    logging.info(f"[sheet {sheet_code}] 最後同步時間（台北）: {taipei_time.strftime('%Y/%m/%d %H:%M:%S')}")
                    return taipei_time.strftime('%Y/%m/%d %H:%M:%S')
            # 無歷史資料時，也需轉換為台北時間
            last_week_utc = datetime.datetime.now() - datetime.timedelta(weeks=1)
            last_week_taipei = last_week_utc + datetime.timedelta(hours=8)
            logging.info(f"[sheet {sheet_code}] 無歷史資料，使用一週前（台北時間）: {last_week_taipei.strftime('%Y/%m/%d %H:%M:%S')}")
            return last_week_taipei.strftime('%Y/%m/%d %H:%M:%S')
        except Exception as e:
            logging.warning(f"無法依表取得最後同步時間（{sheet_code}）: {e}，使用一週前")
            last_week = datetime.datetime.now() - datetime.timedelta(weeks=1)
            return last_week.strftime('%Y/%m/%d %H:%M:%S')

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