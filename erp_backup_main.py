# -*- coding: utf-8 -*-
"""
ERP 資料備份主程式
整合 Ragic API 資料獲取、資料轉換、BigQuery 上傳的完整流程
"""

import os
import json
import logging
import datetime
from typing import Dict, Any, Optional, List

# 導入自定義模組
from ragic_client import RagicClient
from data_transformer import create_transformer, DataTransformer
from bigquery_uploader import create_uploader, BigQueryUploader
from email_notifier import send_backup_notification


class ERPBackupManager:
    """ERP 資料備份管理器"""

    def __init__(self, config: Dict[str, Any]):
        """
        初始化備份管理器

        Args:
            config: 設定字典，包含所有必要的設定參數
        """
        self.config = config
        self.ragic_client: Optional[RagicClient] = None
        self.transformer: Optional[DataTransformer] = None
        self.uploader: Optional[BigQueryUploader] = None

        # 設定日誌
        self._setup_logging()

        # 驗證設定
        self._validate_config()

        logging.info("ERP 備份管理器初始化完成")

    def _setup_logging(self):
        """設定日誌格式"""
        log_level = self.config.get('log_level', 'INFO').upper()
        log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

        logging.basicConfig(
            level=getattr(logging, log_level),
            format=log_format,
            handlers=[
                logging.StreamHandler(),
                # 可以加入檔案處理器
                # logging.FileHandler('erp_backup.log')
            ]
        )

    def _validate_config(self):
        """驗證設定參數"""
        required_fields = [
            'ragic_api_key',
            'ragic_account',
            'ragic_sheet_id',
            'gcp_project_id',
            'bigquery_dataset',
            'bigquery_table'
        ]

        missing_fields = []
        for field in required_fields:
            if not self.config.get(field):
                missing_fields.append(field)

        if missing_fields:
            raise ValueError(f"缺少必要的設定欄位: {', '.join(missing_fields)}")

        logging.info("設定驗證完成")

    def initialize_clients(self):
        """初始化所有客戶端"""
        try:
            # 初始化 Ragic 客戶端
            self.ragic_client = RagicClient(
                api_key=self.config['ragic_api_key'],
                account=self.config['ragic_account'],
                timeout=self.config.get('ragic_timeout', 30),
                max_retries=self.config.get('ragic_max_retries', 3)
            )

            # 初始化資料轉換器
            self.transformer = create_transformer()

            # 初始化 BigQuery 上傳器
            self.uploader = create_uploader(
                project_id=self.config['gcp_project_id'],
                location=self.config.get('bigquery_location', 'US')
            )

            logging.info("所有客戶端初始化完成")

        except Exception as e:
            logging.error(f"客戶端初始化失敗: {e}")
            raise

    def test_connections(self) -> Dict[str, bool]:
        """
        測試所有連線

        Returns:
            Dict[str, bool]: 各服務的連線狀態
        """
        results = {}

        # 測試 Ragic 連線
        if self.ragic_client:
            results['ragic'] = self.ragic_client.test_connection()
        else:
            results['ragic'] = False

        # 測試 BigQuery 連線
        if self.uploader:
            results['bigquery'] = self.uploader.test_connection()
        else:
            results['bigquery'] = False

        logging.info(f"連線測試結果: {results}")
        return results

    def get_last_sync_timestamp(self) -> str:
        """
        獲取最後同步時間

        Returns:
            str: Ragic 格式的日期時間 (yyyy/MM/dd HH:mm:ss)
        """
        try:
            if self.uploader:
                return self.uploader.get_last_sync_timestamp(
                    self.config['bigquery_dataset'],
                    self.config['bigquery_table']
                )
            else:
                raise Exception("BigQuery 上傳器未初始化")

        except Exception as e:
            logging.warning(f"無法獲取最後同步時間: {e}，使用預設值")
            # 返回一週前的時間（Ragic 格式）
            last_week = datetime.datetime.now() - datetime.timedelta(weeks=1)
            return last_week.strftime('%Y/%m/%d %H:%M:%S')

    def fetch_ragic_data(self, last_sync_time: str) -> list:
        """
        從 Ragic 獲取資料

        Args:
            last_sync_time: 最後同步時間 (Ragic 格式: yyyy/MM/dd HH:mm:ss)

        Returns:
            list: 原始 Ragic 資料

        Raises:
            Exception: 當資料獲取失敗時
        """
        if not self.ragic_client:
            raise Exception("Ragic 客戶端未初始化")

        logging.info("開始從 Ragic 獲取資料...")

        try:
            # 預設採用：單頁抓取 + 本地過濾一週（必要時自動翻頁）
            # 1) 解析 last_sync_time（Ragic 格式）為 datetime
            since_dt = None
            for fmt in ['%Y/%m/%d %H:%M:%S', '%Y-%m-%d %H:%M:%S', '%Y/%m/%d', '%Y-%m-%d']:
                try:
                    since_dt = datetime.datetime.strptime(last_sync_time, fmt)
                    break
                except Exception:
                    continue
            if since_dt is None:
                # 無法解析時，退回一週前
                since_dt = datetime.datetime.utcnow() - datetime.timedelta(days=7)

            # 2) 決定最後修改欄位名稱清單（可由環境變數提供，否則使用預設）
            names_env = self.config.get('last_modified_field_names')
            if names_env:
                last_modified_field_names = [n.strip() for n in names_env.split(',') if n.strip()]
            else:
                last_modified_field_names = ['最後修改日期', '最後修改時間', '更新時間', '最後更新時間']

            # 3) 大表提高單頁 limit
            sid = self.config['ragic_sheet_id']
            default_limit = self.config.get('ragic_page_size', 1000)
            per_sheet_boost = {'forms8/17': 3000, 'forms8/2': 3000, 'forms8/3': 3000}
            limit = per_sheet_boost.get(sid, default_limit)
            max_pages = int(self.config.get('ragic_max_pages', 50))

            data = self.ragic_client.fetch_since_local_paged(
                sheet_id=sid,
                since_dt=since_dt,
                last_modified_field_names=last_modified_field_names,
                limit=limit,
                max_pages=max_pages
            )

            logging.info(f"成功獲取 {len(data)} 筆 Ragic 資料（local paged incremental）")
            return data

        except Exception as e:
            logging.error(f"Ragic 資料獲取失敗: {e}")
            raise

    def fetch_ragic_data_for_sheet(self, sheet_id: str, last_sync_time: str, last_modified_names: Optional[List[str]] = None, limit: Optional[int] = None, max_pages: Optional[int] = None) -> list:
        """針對指定 sheet 以「單頁抓取 + 本地過濾一週（必要時自動翻頁）」取得資料。"""
        if not self.ragic_client:
            raise Exception("Ragic 客戶端未初始化")

        # 解析 last_sync_time → datetime
        since_dt = None
        for fmt in ['%Y/%m/%d %H:%M:%S', '%Y-%m-%d %H:%M:%S', '%Y/%m/%d', '%Y-%m-%d']:
            try:
                since_dt = datetime.datetime.strptime(last_sync_time, fmt)
                break
            except Exception:
                continue
        if since_dt is None:
            since_dt = datetime.datetime.utcnow() - datetime.timedelta(days=7)

        # 欄位名稱清單
        if last_modified_names and isinstance(last_modified_names, list) and last_modified_names:
            names = last_modified_names
        else:
            names_env = self.config.get('last_modified_field_names')
            names = [n.strip() for n in names_env.split(',')] if names_env else ['最後修改日期', '最後修改時間', '更新時間', '最後更新時間']

        # 單頁限制與最大頁數
        default_limit = self.config.get('ragic_page_size', 1000)
        per_sheet_boost = {'forms8/17': 3000, 'forms8/2': 3000, 'forms8/3': 3000}
        lim = limit if isinstance(limit, int) and limit > 0 else per_sheet_boost.get(sheet_id, default_limit)
        maxp = int(max_pages) if isinstance(max_pages, int) and max_pages else int(self.config.get('ragic_max_pages', 50))

        data = self.ragic_client.fetch_since_local_paged(
            sheet_id=sheet_id,
            since_dt=since_dt,
            last_modified_field_names=names,
            limit=lim,
            max_pages=maxp
        )
        logging.info(f"[{sheet_id}] 取得 {len(data)} 筆（local paged incremental）")
        return data

    def transform_data(self, ragic_data: list) -> list:
        """
        轉換資料格式

        Args:
            ragic_data: 原始 Ragic 資料

        Returns:
            list: 轉換後的資料

        Raises:
            Exception: 當資料轉換失敗時
        """
        if not self.transformer:
            raise Exception("資料轉換器未初始化")

        logging.info("開始資料轉換...")

        try:
            transformed_data = self.transformer.transform_data(ragic_data)
            logging.info(f"成功轉換 {len(transformed_data)} 筆資料")
            return transformed_data

        except Exception as e:
            logging.error(f"資料轉換失敗: {e}")
            raise

    def upload_to_bigquery(self, transformed_data: list) -> Dict[str, Any]:
        """
        上傳資料至 BigQuery

        Args:
            transformed_data: 轉換後的資料

        Returns:
            Dict[str, Any]: 上傳結果

        Raises:
            Exception: 當上傳失敗時
        """
        if not self.uploader:
            raise Exception("BigQuery 上傳器未初始化")

        logging.info("開始上傳資料至 BigQuery...")

        try:
            # 根據資料量決定是否批次上傳與上傳模式
            batch_size = self.config.get('upload_batch_size', 1000)
            use_merge = self.config.get('use_merge', True)
            upload_mode = self.config.get('upload_mode', 'auto')
            batch_threshold = self.config.get('batch_threshold', 5000)
            staging_table = self.config.get('staging_table')
            merge_sp_name = self.config.get('merge_sp_name')

            if len(transformed_data) > batch_size:
                from bigquery_uploader import batch_upload_data
                results = batch_upload_data(
                    self.uploader,
                    transformed_data,
                    self.config['bigquery_dataset'],
                    self.config['bigquery_table'],
                    batch_size
                )

                # 彙總結果
                total_processed = sum(r.get('records_processed', 0) for r in results)
                success_batches = sum(1 for r in results if r.get('status') == 'success')

                return {
                    "status": "success" if success_batches == len(results) else "partial_success",
                    "method": "batch_upload",
                    "total_batches": len(results),
                    "success_batches": success_batches,
                    "total_records_processed": total_processed,
                    "batch_results": results
                }
            else:
                result = self.uploader.upload_data(
                    transformed_data,
                    self.config['bigquery_dataset'],
                    self.config['bigquery_table'],
                    use_merge=use_merge,
                    upload_mode=upload_mode,
                    batch_threshold=batch_threshold,
                    staging_table=staging_table,
                    merge_sp_name=merge_sp_name
                )

                logging.info(f"成功上傳 {result.get('records_processed', 0)} 筆資料")
                return result

        except Exception as e:
            logging.error(f"BigQuery 上傳失敗: {e}")
            raise

    def run_backup(self) -> Dict[str, Any]:
        """
        執行完整的備份流程

        Returns:
            Dict[str, Any]: 備份結果統計

        Raises:
            Exception: 當備份流程失敗時
        """
        start_time = datetime.datetime.now()
        logging.info(f"開始執行 ERP 資料備份流程 - {start_time}")

        # 初始化 result 變數，避免 finally 區塊中找不到變數
        result = {
            "status": "error",
            "start_time": start_time.isoformat(),
            "error": "未知錯誤"
        }

        try:
            # 初始化所有客戶端
            self.initialize_clients()

            # 測試連線
            connections = self.test_connections()
            if not all(connections.values()):
                failed_services = [k for k, v in connections.items() if not v]
                raise Exception(f"服務連線失敗: {', '.join(failed_services)}")

            # 獲取最後同步時間
            last_sync_time = self.get_last_sync_timestamp()
            logging.info(f"最後同步時間: {last_sync_time}")

            # 單表模式：從 Ragic 獲取資料
            ragic_data = self.fetch_ragic_data(last_sync_time)

            if not ragic_data:
                logging.info("Ragic 無新資料需要備份")
                end_time = datetime.datetime.now()
                return {
                    "status": "no_data",
                    "message": "Ragic 無新資料可以備份",
                    "start_time": start_time.isoformat(),
                    "end_time": end_time.isoformat(),
                    "duration_seconds": (end_time - start_time).total_seconds(),
                    "last_sync_time": last_sync_time,
                    "records_processed": 0
                }

            # 步驟 2: 轉換資料
            transformed_data = self.transform_data(ragic_data)

            if not transformed_data:
                logging.warning("轉換後沒有有效資料")
                return {
                    "status": "no_valid_data",
                    "start_time": start_time.isoformat(),
                    "end_time": datetime.datetime.now().isoformat(),
                    "duration_seconds": (datetime.datetime.now() - start_time).total_seconds(),
                    "records_fetched": len(ragic_data),
                    "records_processed": 0
                }

            # 步驟 3: 上傳至 BigQuery
            upload_result = self.upload_to_bigquery(transformed_data)

            # 計算總耗時
            end_time = datetime.datetime.now()
            duration = (end_time - start_time).total_seconds()

            # 整合結果（單表）
            result = {
                "status": "success",
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat(),
                "duration_seconds": duration,
                "records_fetched": len(ragic_data),
                "records_transformed": len(transformed_data),
                "records_processed": upload_result.get('records_processed', len(transformed_data)),
                "invalid_records": len(self.transformer.get_invalid_records()) if hasattr(self.transformer, 'get_invalid_records') else 0,
                "details": [
                    {
                        "sheet_code": self.config.get('sheet_code') or "",
                        "uploaded": upload_result.get('records_processed', len(transformed_data)),
                        "invalid": len(self.transformer.get_invalid_records()) if hasattr(self.transformer, 'get_invalid_records') else 0
                    }
                ],
                "upload_result": upload_result
            }

            logging.info(f"備份流程完成 - 耗時: {duration:.2f} 秒")
            return result

        except Exception as e:
            end_time = datetime.datetime.now()
            duration = (end_time - start_time).total_seconds()
            error_msg = f"備份失敗 - 耗時: {duration:.2f} 秒, 錯誤: {str(e)}"
            logging.error(error_msg)

            # 將錯誤原因以結構化形式寫入日誌，便於 Cloud Logging 過濾
            try:
                logging.error({
                    "type": "backup_error",
                    "sheet_id": self.config.get('ragic_sheet_id'),
                    "dataset": self.config.get('bigquery_dataset'),
                    "table": self.config.get('bigquery_table'),
                    "error": str(e),
                    "duration_seconds": duration,
                    "start_time": start_time.isoformat(),
                })
            except Exception:
                pass

            return {
                "status": "error",
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat(),
                "duration_seconds": duration,
                "error": str(e)
            }

        finally:
            # 清理資源
            self.cleanup()

            # 發送電子郵件通知（如果設定了）
            try:
                if self._should_send_notification():
                    self._send_email_notification(result)
            except Exception as e:
                logging.warning(f"發送電子郵件通知失敗: {e}")

    def _should_send_notification(self) -> bool:
        """檢查是否應該發送電子郵件通知"""
        if os.environ.get('DISABLE_EMAIL', 'false').lower() == 'true':
            return False
        return (
            self.config.get('notification_email') and
            self.config.get('smtp_from_email') and
            self.config.get('smtp_from_password')
        )

    def _send_email_notification(self, backup_result: Dict[str, Any]):
        """發送電子郵件通知"""
        smtp_config = {
            'from_email': self.config.get('smtp_from_email'),
            'from_password': self.config.get('smtp_from_password'),
            'smtp_server': self.config.get('smtp_server', 'smtp.gmail.com'),
            'smtp_port': self.config.get('smtp_port', 587)
        }

        success = send_backup_notification(
            project_id=self.config['gcp_project_id'],
            to_email=self.config['notification_email'],
            backup_result=backup_result,
            smtp_config=smtp_config
        )

        if success:
            logging.info(f"電子郵件通知已發送至: {self.config['notification_email']}")
        else:
            logging.error("電子郵件通知發送失敗")

    def cleanup(self):
        """清理資源"""
        try:
            if self.ragic_client:
                self.ragic_client.close()
            if self.uploader:
                self.uploader.close()
            logging.info("資源清理完成")
        except Exception as e:
            logging.warning(f"資源清理時發生錯誤: {e}")

    # ---- sheet 對照 ----
    def _get_sheet_map(self) -> Dict[str, str]:
        """取得 sheet_code → sheet_id 對照表，優先使用環境設定，其次內建預設。"""
        # 內建預設
        default_map = {
            '10': 'forms8/5',
            '20': 'forms8/4',
            '30': 'forms8/7',
            '40': 'forms8/1',
            '41': 'forms8/6',
            '50': 'forms8/17',
            '60': 'forms8/2',
            '70': 'forms8/9',
            '99': 'forms8/3',
        }
        # 1) 設定中已有解析完成的 dict
        m = self.config.get('sheet_map')
        if isinstance(m, dict) and m:
            return m
        # 2) JSON 字串
        mjson = self.config.get('sheet_map_json')
        if mjson:
            try:
                parsed = json.loads(mjson)
                if isinstance(parsed, dict) and parsed:
                    return parsed
            except Exception as e:
                logging.warning(f"SHEET_MAP_JSON 解析失敗，使用預設對照: {e}")
        # 3) 檔案
        mfile = self.config.get('sheet_map_file')
        if mfile and os.path.exists(mfile):
            try:
                with open(mfile, 'r', encoding='utf-8') as f:
                    parsed = json.load(f)
                    if isinstance(parsed, dict) and parsed:
                        return parsed
            except Exception as e:
                logging.warning(f"SHEET_MAP_FILE 載入失敗，使用預設對照: {e}")
        return default_map

    def run_backup_all_sheets(self) -> Dict[str, Any]:
        """多表流程：依固定 9 張表（或環境變數提供）進行一週增量抓取、轉換、上傳並彙總。"""
        start_time = datetime.datetime.now()
        logging.info(f"開始執行 ERP 多表備份流程 - {start_time}")

        result = {
            "status": "error",
            "start_time": start_time.isoformat(),
            "error": "未知錯誤"
        }

        # 9 張表對照（可由外部設定覆蓋）
        sheets = self._get_sheet_map()

        per_sheet_names = {
            'forms8/5': ['最後修改日期', '最後修改時間', '更新時間', '最後更新時間'],
            'forms8/4': ['最後修改日期', '最後修改時間', '更新時間', '最後更新時間'],
            'forms8/7': ['最後修改時間', '最後修改日期', '更新時間', '最後更新時間'],
            'forms8/1': ['最後修改日期', '最後修改時間', '更新時間', '最後更新時間'],
            'forms8/6': ['最後修改日期', '最後修改時間', '更新時間', '最後更新時間'],
            'forms8/17': ['最後修改日期', '最後修改時間', '更新時間', '最後更新時間'],
            'forms8/2': ['最後修改日期', '最後修改時間', '更新時間', '最後更新時間'],
            'forms8/9': ['最後修改日期', '最後修改時間', '更新時間', '最後更新時間'],
            'forms8/3': ['最後修改日期', '最後修改時間', '更新時間', '最後更新時間'],
        }

        # 連線與最後同步時間
        try:
            self.initialize_clients()
            connections = self.test_connections()
            if not all(connections.values()):
                failed_services = [k for k, v in connections.items() if not v]
                raise Exception(f"服務連線失敗: {', '.join(failed_services)}")

            last_sync_time = self.get_last_sync_timestamp()
            logging.info(f"最後同步時間: {last_sync_time}")

            total_uploaded = 0
            total_invalid = 0
            details: List[Dict[str, Any]] = []

            # 逐表處理
            for sheet_code, sheet_id in sheets.items():
                try:
                    records = self.fetch_ragic_data_for_sheet(
                        sheet_id=sheet_id,
                        last_sync_time=last_sync_time,
                        last_modified_names=per_sheet_names.get(sheet_id),
                        limit=None,
                        max_pages=None
                    )

                    transformer = create_transformer(sheet_code=sheet_code, project_id=self.config['gcp_project_id'], use_dynamic_mapping=False)
                    transformed = transformer.transform_data(records)

                    uploaded = 0
                    invalid = len(transformer.get_invalid_records()) if hasattr(transformer, 'get_invalid_records') else 0
                    if transformed:
                        up_res = self.uploader.upload_data(
                            data=transformed,
                            dataset_id=self.config['bigquery_dataset'],
                            table_id=self.config['bigquery_table'],
                            use_merge=self.config.get('use_merge', False),
                            upload_mode=self.config.get('upload_mode', 'direct')
                        )
                        uploaded = up_res.get('records_processed', len(transformed))

                    total_uploaded += uploaded
                    total_invalid += invalid
                    details.append({
                        'sheet_code': sheet_code,
                        'uploaded': uploaded,
                        'invalid': invalid
                    })

                except Exception as se:
                    logging.error(f"處理表 {sheet_code} 失敗: {se}")
                    details.append({
                        'sheet_code': sheet_code,
                        'uploaded': 0,
                        'invalid': 0,
                        'error': str(se)
                    })

            end_time = datetime.datetime.now()
            duration = (end_time - start_time).total_seconds()

            status = 'success' if total_uploaded > 0 else 'no_data'
            result = {
                'status': status,
                'start_time': start_time.isoformat(),
                'end_time': end_time.isoformat(),
                'duration_seconds': duration,
                'records_processed': total_uploaded,
                'invalid_records': total_invalid,
                'last_sync_time': last_sync_time,
                'details': details
            }
            logging.info(f"多表備份完成，共上傳 {total_uploaded} 筆，無效 {total_invalid} 筆")
            return result

        except Exception as e:
            end_time = datetime.datetime.now()
            duration = (end_time - start_time).total_seconds()
            logging.error(f"多表備份失敗: {e}")
            return {
                'status': 'error',
                'start_time': start_time.isoformat(),
                'end_time': end_time.isoformat(),
                'duration_seconds': duration,
                'error': str(e)
            }
        finally:
            self.cleanup()
            try:
                if self._should_send_notification():
                    self._send_email_notification(result)
            except Exception as e:
                logging.warning(f"發送電子郵件通知失敗: {e}")


def load_config_from_env() -> Dict[str, Any]:
    """
    從環境變數載入設定

    Returns:
        Dict[str, Any]: 設定字典

    Raises:
        ValueError: 當必要的環境變數缺失時
    """
    config = {
        'ragic_api_key': os.environ.get('RAGIC_API_KEY'),
        'ragic_account': os.environ.get('RAGIC_ACCOUNT', 'your-account'),
        'ragic_sheet_id': os.environ.get('RAGIC_SHEET_ID', 'your-sheet/1'),
        'ragic_timeout': int(os.environ.get('RAGIC_TIMEOUT', 30)),
        'ragic_max_retries': int(os.environ.get('RAGIC_MAX_RETRIES', 3)),
        'ragic_page_size': int(os.environ.get('RAGIC_PAGE_SIZE', 1000)),
        'ragic_max_pages': int(os.environ.get('RAGIC_MAX_PAGES', 50)),
        'last_modified_field_names': os.environ.get('LAST_MODIFIED_FIELD_NAMES'),
        # sheet 對照設定
        'sheet_map_json': os.environ.get('SHEET_MAP_JSON'),
        'sheet_map_file': os.environ.get('SHEET_MAP_FILE'),

        'gcp_project_id': os.environ.get('GCP_PROJECT_ID', 'your-project-id'),
        'bigquery_dataset': os.environ.get('BIGQUERY_DATASET', 'your_dataset'),
        'bigquery_table': os.environ.get('BIGQUERY_TABLE', 'erp_backup'),
        'bigquery_location': os.environ.get('BIGQUERY_LOCATION', 'US'),

        'upload_batch_size': int(os.environ.get('UPLOAD_BATCH_SIZE', 1000)),
        'use_merge': os.environ.get('USE_MERGE', 'true').lower() == 'true',
        'upload_mode': os.environ.get('UPLOAD_MODE', 'auto'),
        'batch_threshold': int(os.environ.get('BATCH_THRESHOLD', 5000)),
        'staging_table': os.environ.get('STAGING_TABLE'),
        'merge_sp_name': os.environ.get('MERGE_SP_NAME'),
        'log_level': os.environ.get('LOG_LEVEL', 'INFO'),

        # 電子郵件通知設定
        'notification_email': os.environ.get('NOTIFICATION_EMAIL'),
        'smtp_from_email': os.environ.get('SMTP_FROM_EMAIL'),
        'smtp_from_password': os.environ.get('SMTP_FROM_PASSWORD'),
        'smtp_server': os.environ.get('SMTP_SERVER', 'smtp.gmail.com'),
        'smtp_port': int(os.environ.get('SMTP_PORT', 587))
    }

    return config


def main():
    """
    主程式入口點
    """
    try:
        # 載入設定
        config = load_config_from_env()

        # 建立備份管理器
        backup_manager = ERPBackupManager(config)

        # 若 RAGIC_SHEET_ID 設為 ALL，走多表流程；否則走單表
        if (config.get('ragic_sheet_id') or '').upper() == 'ALL':
            result = backup_manager.run_backup_all_sheets()
        else:
            # 允許為單表指定 sheet_code 以利 email details 顯示
            try:
                config['sheet_code'] = os.environ.get('RAGIC_SHEET_CODE')
                # 若提供 sheet_code 且未提供明確 sheet_id，嘗試用對照表映射
                sc = config.get('sheet_code')
                sid = config.get('ragic_sheet_id')
                if sc and (not sid or sid == 'AUTO' or sid == 'your-sheet/1'):
                    # 臨時建立管理器以讀取對照
                    sm = backup_manager._get_sheet_map()
                    mapped = sm.get(sc)
                    if mapped:
                        config['ragic_sheet_id'] = mapped
                        # 同步到實例設定
                        backup_manager.config['ragic_sheet_id'] = mapped
                        logging.info(f"以對照表將 sheet_code={sc} 映射為 sheet_id={mapped}")
            except Exception:
                pass
            result = backup_manager.run_backup()

        # 輸出結果
        if result['status'] == 'success':
            print(f"✅ 備份成功完成")
            print(f"📊 獲取記錄: {result.get('records_fetched', 0)}")
            print(f"🔄 轉換記錄: {result.get('records_transformed', 0)}")
            print(f"⏱️  總耗時: {result.get('duration_seconds', 0):.2f} 秒")
        elif result['status'] in ['no_data', 'no_valid_data']:
            print(f"ℹ️  {result['status']}: 沒有需要同步的資料")
        else:
            print(f"❌ 備份失敗: {result.get('error', '未知錯誤')}")
            exit(1)

    except Exception as e:
        print(f"❌ 程式執行失敗: {e}")
        logging.error(f"主程式執行失敗: {e}")
        exit(1)


# Cloud Function 入口點
def backup_erp_data(request):
    """
    Google Cloud Function 入口點
    HTTP 觸發器函數，用於定時或手動觸發備份

    Args:
        request: HTTP 請求物件

    Returns:
        Tuple: (回應內容, HTTP 狀態碼)
    """
    try:
        # 載入設定
        config = load_config_from_env()

        # 建立備份管理器
        backup_manager = ERPBackupManager(config)

        # 執行備份
        result = backup_manager.run_backup()

        if result['status'] == 'success':
            return {
                "status": "success",
                "message": "ERP 資料備份完成",
                "data": {
                    "records_processed": result.get('records_transformed', 0),
                    "duration_seconds": result.get('duration_seconds', 0)
                }
            }, 200
        elif result['status'] in ['no_data', 'no_valid_data']:
            return {
                "status": "info",
                "message": "沒有新資料需要同步",
                "data": result
            }, 200
        else:
            return {
                "status": "error",
                "message": result.get('error', '備份失敗'),
                "data": result
            }, 500

    except Exception as e:
        logging.error(f"Cloud Function 執行失敗: {e}")
        return {
            "status": "error",
            "message": str(e)
        }, 500


# 執行主程式（本地測試使用）
if __name__ == "__main__":
    main()