# -*- coding: utf-8 -*-
"""
ERP 資料備份主程式
整合 Ragic API 資料獲取、資料轉換、BigQuery 上傳的完整流程
"""

import os
import json
import logging
import datetime
import time
import requests
from typing import Dict, Any, Optional, List
from google.cloud import bigquery

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

    def _diagnose_egress(self) -> Dict[str, Any]:
        """
        最小對外診斷：google.com、api.ipify.org、Ragic base。
        回傳各目標之狀態碼/逾時與耗時，並寫入日誌。
        """
        targets = [
            ("google", "https://www.google.com", 5, {}),
            ("ipify", "https://api.ipify.org", 5, {}),
            ("ragic_base", f"https://ap6.ragic.com/{self.config.get('ragic_account','')}", 15, {}),
        ]
        out: Dict[str, Any] = {}
        sess = requests.Session()
        # 若提供金鑰，附上 header（Ragic base 不一定需要，但不影響）
        ak = self.config.get('ragic_api_key')
        if ak:
            sess.headers['Authorization'] = f'Basic {ak}'
        for name, url, to, params in targets:
            t0 = time.time()
            try:
                r = sess.get(url, params=params, timeout=to)
                dt = round(time.time() - t0, 3)
                out[name] = {"ok": True, "status": r.status_code, "elapsed_s": dt}
                logging.info(f"egress diag {name}: {url} -> {r.status_code} in {dt}s")
            except Exception as e:
                dt = round(time.time() - t0, 3)
                out[name] = {"ok": False, "error": str(e), "elapsed_s": dt}
                logging.error(f"egress diag {name} error: {e} ({url}) in {dt}s")
        return out

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

            # 初始化資料轉換器（單表流程需帶入正確 sheet_code，避免預設為 99）
            sheet_code_cfg = self.config.get('sheet_code')
            if not sheet_code_cfg:
                # 嘗試由 sheet_id 反查 code
                try:
                    smap = self._get_sheet_map()
                    for sc, sid in smap.items():
                        if sid == self.config.get('ragic_sheet_id'):
                            sheet_code_cfg = sc
                            break
                except Exception:
                    pass
            self.transformer = create_transformer(sheet_code=sheet_code_cfg or '99')

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
            # 強制測試視窗：支援以環境變數覆蓋時間窗
            force_iso = os.environ.get('FORCE_SINCE_ISO')  # 例: 2025-09-01T00:00:00
            force_days = os.environ.get('FORCE_SINCE_DAYS')  # 例: 30
            if force_iso:
                try:
                    dt = datetime.datetime.fromisoformat(force_iso.replace('Z', '+00:00')).replace(tzinfo=None)
                    return dt.strftime('%Y/%m/%d %H:%M:%S')
                except Exception:
                    logging.warning(f"FORCE_SINCE_ISO 非法，忽略: {force_iso}")
            if force_days:
                try:
                    days = int(force_days)
                    dt = datetime.datetime.now() - datetime.timedelta(days=days)
                    return dt.strftime('%Y/%m/%d %H:%M:%S')
                except Exception:
                    logging.warning(f"FORCE_SINCE_DAYS 非法，忽略: {force_days}")

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

            # 上界（測試用）：FORCE_UNTIL_ISO / FORCE_UNTIL_DAYS
            until_dt = None
            fui = os.environ.get('FORCE_UNTIL_ISO')
            fud = os.environ.get('FORCE_UNTIL_DAYS')
            if fui:
                try:
                    until_dt = datetime.datetime.fromisoformat(fui.replace('Z', '+00:00')).replace(tzinfo=None)
                except Exception:
                    logging.warning(f"FORCE_UNTIL_ISO 非法，忽略: {fui}")
            elif fud:
                try:
                    d = int(fud)
                    until_dt = datetime.datetime.now() - datetime.timedelta(days=d)
                except Exception:
                    logging.warning(f"FORCE_UNTIL_DAYS 非法，忽略: {fud}")

            data = self.ragic_client.fetch_since_local_paged(
                sheet_id=sid,
                since_dt=since_dt,
                last_modified_field_names=last_modified_field_names,
                until_dt=until_dt,
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

        logging.info(f"[fetch_ragic_data_for_sheet] 開始處理 {sheet_id}")
        logging.info(f"[fetch_ragic_data_for_sheet] last_sync_time 輸入: {last_sync_time}")

        # 解析 last_sync_time → datetime
        since_dt = None
        for fmt in ['%Y/%m/%d %H:%M:%S', '%Y-%m-%d %H:%M:%S', '%Y/%m/%d', '%Y-%m-%d']:
            try:
                since_dt = datetime.datetime.strptime(last_sync_time, fmt)
                logging.info(f"[fetch_ragic_data_for_sheet] 成功解析日期，格式: {fmt}")
                break
            except Exception:
                continue
        if since_dt is None:
            since_dt = datetime.datetime.utcnow() - datetime.timedelta(days=7)
            logging.warning(f"[fetch_ragic_data_for_sheet] 無法解析日期，使用預設一週前: {since_dt.strftime('%Y-%m-%d %H:%M:%S')}")
        else:
            logging.info(f"[fetch_ragic_data_for_sheet] 解析後的 since_dt: {since_dt.strftime('%Y-%m-%d %H:%M:%S')}")

        # 欄位名稱清單
        if last_modified_names and isinstance(last_modified_names, list) and last_modified_names:
            names = last_modified_names
        else:
            names_env = self.config.get('last_modified_field_names')
            names = [n.strip() for n in names_env.split(',')] if names_env else ['最後修改日期', '最後修改時間', '更新時間', '最後更新時間']
        logging.info(f"[fetch_ragic_data_for_sheet] 日期欄位清單: {names}")

        # 單頁限制與最大頁數
        default_limit = self.config.get('ragic_page_size', 1000)
        per_sheet_boost = {'forms8/17': 3000, 'forms8/2': 3000, 'forms8/3': 3000}
        lim = limit if isinstance(limit, int) and limit > 0 else per_sheet_boost.get(sheet_id, default_limit)
        maxp = int(max_pages) if isinstance(max_pages, int) and max_pages else int(self.config.get('ragic_max_pages', 50))
        logging.info(f"[fetch_ragic_data_for_sheet] 分頁設定: limit={lim}, max_pages={maxp}")

        # 上界（測試用）：FORCE_UNTIL_ISO / FORCE_UNTIL_DAYS
        until_dt = None
        fui = os.environ.get('FORCE_UNTIL_ISO')
        fud = os.environ.get('FORCE_UNTIL_DAYS')
        if fui:
            try:
                until_dt = datetime.datetime.fromisoformat(fui.replace('Z', '+00:00')).replace(tzinfo=None)
                logging.info(f"[fetch_ragic_data_for_sheet] 使用 FORCE_UNTIL_ISO: {until_dt.strftime('%Y-%m-%d %H:%M:%S')}")
            except Exception:
                logging.warning(f"FORCE_UNTIL_ISO 非法，忽略: {fui}")
        elif fud:
            try:
                d = int(fud)
                until_dt = datetime.datetime.now() - datetime.timedelta(days=d)
                logging.info(f"[fetch_ragic_data_for_sheet] 使用 FORCE_UNTIL_DAYS: {until_dt.strftime('%Y-%m-%d %H:%M:%S')}")
            except Exception:
                logging.warning(f"FORCE_UNTIL_DAYS 非法，忽略: {fud}")

        data = self.ragic_client.fetch_since_local_paged(
            sheet_id=sheet_id,
            since_dt=since_dt,
            last_modified_field_names=names,
            until_dt=until_dt,
            limit=lim,
            max_pages=maxp
        )
        logging.info(f"[fetch_ragic_data_for_sheet] [{sheet_id}] 取得 {len(data)} 筆（local paged incremental）")
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
        # 測試模式：若環境變數 TRUNCATE_BEFORE=true，先清空目標表
        try:
            if os.environ.get('TRUNCATE_BEFORE', 'false').lower() == 'true' and self.uploader:
                logging.info("TRUNCATE_BEFORE=true，清空目標表以避免測試時時間差造成混淆")
                self.uploader.truncate_table(self.config['bigquery_dataset'], self.config['bigquery_table'])
        except Exception as e:
            logging.warning(f"預清空目標表失敗（非致命）：{e}")

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

    def _write_run_result(self, agg_id: Optional[str], sheet_code: str, result: Dict[str, Any]) -> None:
        try:
            client = bigquery.Client(project=self.config['gcp_project_id'])
            table = f"{self.config['gcp_project_id']}.erp_backup.run_results"
            details = (result.get('details') or [{}])
            d0 = details[0] if isinstance(details, list) and details else {}
            rows = [{
                'agg_id': agg_id or '',
                'sheet_code': sheet_code,
                'status': result.get('status'),
                'uploaded': d0.get('uploaded', result.get('records_processed', 0)),
                'invalid': d0.get('invalid', result.get('invalid_records', 0)),
                'fetched': d0.get('fetched', result.get('records_fetched', 0)),
                'error': result.get('error'),
                'start_time': result.get('start_time'),
                'end_time': result.get('end_time'),
            }]
            errors = client.insert_rows_json(table, rows)
            if errors:
                logging.warning(f"寫入 run_results 失敗: {errors}")
            else:
                logging.info(f"run_results 已記錄: agg_id={agg_id}, sheet={sheet_code}")
        except Exception as e:
            logging.warning(f"run_results 記錄失敗: {e}")

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
            # 先做對外診斷，協助釐清雲端連線狀況
            diagnostics = self._diagnose_egress()
            # 初始化所有客戶端
            self.initialize_clients()

            # 測試連線
            connections = self.test_connections()
            if not all(connections.values()):
                failed_services = [k for k, v in connections.items() if not v]
                raise Exception(f"服務連線失敗: {', '.join(failed_services)}")

            # 獲取最後同步時間（單表用全表最大即可）
            last_sync_time = self.get_last_sync_timestamp()
            logging.info(f"最後同步時間: {last_sync_time}")

            # 單表模式：從 Ragic 獲取資料
            ragic_data = self.fetch_ragic_data(last_sync_time)

            if not ragic_data:
                logging.info("Ragic 無新資料需要備份")
                end_time = datetime.datetime.now()
                result = {
                    "status": "no_data",
                    "message": "Ragic 無新資料可以備份",
                    "start_time": start_time.isoformat(),
                    "end_time": end_time.isoformat(),
                    "duration_seconds": (end_time - start_time).total_seconds(),
                    "last_sync_time": last_sync_time,
                    "records_processed": 0
                }
                # 記錄至 run_results（即使無資料）
                try:
                    agg_id = self.config.get('agg_id') or os.environ.get('AGG_ID')
                    # 從 sheet_map 反查表單代碼
                    sheet_id = self.config.get('ragic_sheet_id')
                    sheet_code = None
                    try:
                        smap = self._get_sheet_map()
                        for sc, sid in smap.items():
                            if sid == sheet_id:
                                sheet_code = sc
                                break
                    except Exception:
                        pass
                    if sheet_code:
                        self._write_run_result(agg_id, sheet_code, result)
                except Exception as e:
                    logging.warning(f"記錄 run_result 失敗（無資料情況）: {e}")
                return result

            # 步驟 2: 轉換資料
            transformed_data = self.transform_data(ragic_data)

            if not transformed_data:
                logging.warning("轉換後沒有有效資料")
                result = {
                    "status": "no_valid_data",
                    "start_time": start_time.isoformat(),
                    "end_time": datetime.datetime.now().isoformat(),
                    "duration_seconds": (datetime.datetime.now() - start_time).total_seconds(),
                    "records_fetched": len(ragic_data),
                    "records_processed": 0
                }
                # 記錄至 run_results（無有效資料情況）
                try:
                    agg_id = self.config.get('agg_id') or os.environ.get('AGG_ID')
                    # 從 sheet_map 反查表單代碼
                    sheet_id = self.config.get('ragic_sheet_id')
                    sheet_code = None
                    try:
                        smap = self._get_sheet_map()
                        for sc, sid in smap.items():
                            if sid == sheet_id:
                                sheet_code = sc
                                break
                    except Exception:
                        pass
                    if sheet_code:
                        self._write_run_result(agg_id, sheet_code, result)
                except Exception as e:
                    logging.warning(f"記錄 run_result 失敗（無有效資料情況）: {e}")
                return result

            # 步驟 3: 上傳至 BigQuery
            upload_result = self.upload_to_bigquery(transformed_data)

            # 計算總耗時
            end_time = datetime.datetime.now()
            duration = (end_time - start_time).total_seconds()

            # 整合結果（單表）
            # 取得 sheet_code 與 sheet_name 以供郵件顯示
            sheet_id = self.config.get('ragic_sheet_id')
            sheet_code_for_mail = self.config.get('sheet_code') or ''
            if not sheet_code_for_mail and sheet_id:
                try:
                    # 由對照表反查 code
                    smap = self._get_sheet_map()
                    for sc, sid in smap.items():
                        if sid == sheet_id:
                            sheet_code_for_mail = sc
                            break
                except Exception:
                    pass
            sheet_name_for_mail = sheet_code_for_mail or (sheet_id or '')

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
                        "sheet_code": sheet_code_for_mail,
                        "sheet_name": sheet_name_for_mail,
                        "uploaded": upload_result.get('records_processed', len(transformed_data)),
                        "invalid": len(self.transformer.get_invalid_records()) if hasattr(self.transformer, 'get_invalid_records') else 0,
                        "fetched": len(ragic_data)
                    }
                ],
                "upload_result": upload_result,
                "diagnostics": diagnostics
            }
            # 記錄至 run_results
            try:
                agg_id = self.config.get('agg_id') or os.environ.get('AGG_ID')
                if sheet_code_for_mail:
                    self._write_run_result(agg_id, sheet_code_for_mail, result)
            except Exception:
                pass

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

            result = {
                "status": "error",
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat(),
                "duration_seconds": duration,
                "error": str(e),
                "diagnostics": locals().get('diagnostics')
            }
            return result

        finally:
            # 清理資源
            self.cleanup()
            # 單表流程：此處郵件保留（兼容舊流程）
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
            # 將 Ragic 系統欄位 _ragicModified 放在第一順位，提升增量判定穩定度
            'forms8/5': ['_ragicModified', '最後修改日期', '最後修改時間', '更新時間', '最後更新時間'],
            'forms8/4': ['_ragicModified', '最後修改日期', '最後修改時間', '更新時間', '最後更新時間'],
            'forms8/7': ['_ragicModified', '最後修改時間', '最後修改日期', '更新時間', '最後更新時間'],
            'forms8/1': ['_ragicModified', '最後修改日期', '最後修改時間', '更新時間', '最後更新時間'],
            'forms8/6': ['_ragicModified', '最後修改日期', '最後修改時間', '更新時間', '最後更新時間'],
            'forms8/17': ['_ragicModified', '最後修改日期', '最後修改時間', '更新時間', '最後更新時間'],
            'forms8/2': ['_ragicModified', '最後修改日期', '最後修改時間', '更新時間', '最後更新時間'],
            'forms8/9': ['_ragicModified', '最後修改日期', '最後修改時間', '更新時間', '最後更新時間'],
            'forms8/3': ['_ragicModified', '最後修改日期', '最後修改時間', '更新時間', '最後更新時間'],
        }

        # 連線與最後同步時間
        try:
            self.initialize_clients()
            connections = self.test_connections()
            if not all(connections.values()):
                failed_services = [k for k, v in connections.items() if not v]
                raise Exception(f"服務連線失敗: {', '.join(failed_services)}")

            # 依各表的最後同步時間抓增量，避免因其他表時間較新導致本表被誤判為無新資料
            # 若表內無資料，退回一週前
            def _last_sync_for(sc: str) -> str:
                try:
                    return self.uploader.get_last_sync_timestamp_by_sheet(
                        self.config['bigquery_dataset'],
                        self.config['bigquery_table'],
                        sc
                    )
                except Exception:
                    return self.get_last_sync_timestamp()

            last_sync_time = self.get_last_sync_timestamp()
            logging.info(f"最後同步時間: {last_sync_time}")

            total_uploaded = 0
            total_invalid = 0
            details: List[Dict[str, Any]] = []

            # 逐表處理
            for sheet_code, sheet_id in sheets.items():
                try:
                    # 規則：若設 FORCE_SINCE_DAYS/ISO 則覆蓋 per-sheet；否則使用 per-sheet last sync。
                    force_iso = os.environ.get('FORCE_SINCE_ISO')
                    force_days = os.environ.get('FORCE_SINCE_DAYS')
                    if force_iso or force_days:
                        ls = self.get_last_sync_timestamp()  # 僅用來取得格式，不實際使用
                        # 將覆蓋視窗傳入 fetch_ragic_data_for_sheet
                        # 透過 last_sync_time 參數覆寫
                        if force_iso:
                            ls = force_iso.replace('Z', '')
                        elif force_days:
                            try:
                                days = int(force_days)
                                dt = datetime.datetime.now() - datetime.timedelta(days=days)
                                ls = dt.strftime('%Y/%m/%d %H:%M:%S')
                            except Exception:
                                pass
                    else:
                        ls = _last_sync_for(sheet_code)

                    # 切換：若 USE_RAGIC_WHERE=true，改用伺服端 where（避免排序影響）
                    use_where = os.environ.get('USE_RAGIC_WHERE', 'false').lower() == 'true'
                    if use_where:
                        # 直接用 fetch_data（where），限制 1 頁
                        per_limit = self.config.get('ragic_page_size', 1000)
                        # 允許提供每表 where 欄位（欄位 ID 或系統鍵）；預設 _ragicModified
                        per_sheet_where = {
                            'forms8/3': os.environ.get('RAGIC_WHERE_FIELD_99'),
                        }
                        wfield = per_sheet_where.get(sheet_id) or os.environ.get('RAGIC_WHERE_FIELD')
                        records = self.ragic_client.fetch_data(sheet_id, last_sync_time=ls, limit=per_limit, max_pages=1, where_field=wfield)
                    else:
                        records = self.fetch_ragic_data_for_sheet(
                            sheet_id=sheet_id,
                            last_sync_time=ls,
                            last_modified_names=per_sheet_names.get(sheet_id),
                            limit=None,
                            max_pages=None
                        )

                    # 若仍為 0：在啟用 skip_if_no_recent_days 時直接跳過；否則保留原先煙霧測試
                    if not records:
                        # 精準邏輯：無新資料即跳過，不做煙霧測試
                        logging.info(f"{sheet_code} 無新資料，跳過上傳（last_sync_used={ls})")
                        details.append({
                            'sheet_code': sheet_code,
                            'sheet_name': sheet_code,
                            'uploaded': 0,
                            'invalid': 0,
                            'fetched': 0,
                            'last_sync_used': ls,
                            'skipped': True,
                            'reason': 'no_new_data'
                        })
                        continue

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
                        'sheet_name': sheet_code,
                        'uploaded': uploaded,
                        'invalid': invalid,
                        'fetched': len(records),
                        'last_sync_used': ls
                    })

                except Exception as se:
                    logging.error(f"處理表 {sheet_code} 失敗: {se}")
                    details.append({
                        'sheet_code': sheet_code,
                        'sheet_name': sheet_code,
                        'uploaded': 0,
                        'invalid': 0,
                        'fetched': 0,
                        'last_sync_used': ls if 'ls' in locals() else None,
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
            # 僅在多表流程完成後一次性寄送通知
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
        # 近 N 天無新增則跳過（預設 7）
        'skip_if_no_recent_days': int(os.environ.get('SKIP_IF_NO_RECENT_DAYS', 7)),
        
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
        # 解析請求 payload（允許覆蓋部分行為）
        try:
            payload = request.get_json(silent=True) or {}
        except Exception:
            payload = {}
        mode = (payload.get('MODE') if isinstance(payload, dict) else None) or os.environ.get('MODE')
        agg_id_from_req = (payload.get('AGG_ID') if isinstance(payload, dict) else None) or os.environ.get('AGG_ID')
        # 單次執行可覆蓋是否寄信（避免單表流程各自寄信）
        if isinstance(payload, dict) and 'DISABLE_EMAIL' in payload:
            os.environ['DISABLE_EMAIL'] = str(payload.get('DISABLE_EMAIL')).lower()
        # 允許以請求覆蓋增量欄位/where 策略（避免重新佈署）
        if isinstance(payload, dict):
            if 'USE_RAGIC_WHERE' in payload:
                os.environ['USE_RAGIC_WHERE'] = str(payload.get('USE_RAGIC_WHERE')).lower()
            if 'LAST_MODIFIED_FIELD_NAMES' in payload and isinstance(payload.get('LAST_MODIFIED_FIELD_NAMES'), str):
                os.environ['LAST_MODIFIED_FIELD_NAMES'] = payload.get('LAST_MODIFIED_FIELD_NAMES')
            # 重要：將請求的 AGG_ID 寫入環境，讓單表流程能寫入 run_results
            if 'AGG_ID' in payload and isinstance(payload.get('AGG_ID'), str):
                os.environ['AGG_ID'] = payload.get('AGG_ID')

        # 若為彙總模式，僅彙總寄信，不執行備份
        if mode and mode.upper() == 'AGGREGATE':
            try:
                cfg = load_config_from_env()
                bq = bigquery.Client(project=cfg['gcp_project_id'])
                q = f"""
                SELECT sheet_code, status, uploaded, invalid, fetched, created_at
                FROM `{cfg['gcp_project_id']}.erp_backup.run_results`
                WHERE agg_id = @agg
                  AND created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
                ORDER BY sheet_code
                """
                job = bq.query(q, job_config=bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter('agg', 'STRING', agg_id_from_req or '')]))
                rows = list(job.result())
                details = [{
                    'sheet_code': r['sheet_code'],
                    'sheet_name': r['sheet_code'],
                    'uploaded': r['uploaded'],
                    'invalid': r['invalid'],
                    'fetched': r['fetched'],
                } for r in rows]
                agg_result = {
                    'status': 'success' if any((d.get('uploaded') or 0) > 0 for d in details) else 'no_data',
                    'records_processed': sum(d.get('uploaded') or 0 for d in details),
                    'invalid_records': sum(d.get('invalid') or 0 for d in details),
                    'details': details,
                }
                smtp_config = {
                    'from_email': cfg.get('smtp_from_email'),
                    'from_password': cfg.get('smtp_from_password'),
                    'smtp_server': cfg.get('smtp_server', 'smtp.gmail.com'),
                    'smtp_port': int(cfg.get('smtp_port', 587))
                }
                send_backup_notification(cfg['gcp_project_id'], cfg['notification_email'], agg_result, smtp_config)
                return { 'status': 'success', 'message': 'aggregated and mailed', 'data': agg_result }, 200
            except Exception as e:
                return { 'status': 'error', 'message': str(e) }, 500

        # 載入設定
        config = load_config_from_env()

        # 允許以請求覆寫單次 sheet（選填）
        try:
            if isinstance(payload, dict):
                override_sheet = payload.get('sheet')
                if isinstance(override_sheet, str) and override_sheet:
                    if override_sheet.upper() == 'ALL':
                        config['ragic_sheet_id'] = 'ALL'
                    else:
                        config['ragic_sheet_id'] = override_sheet
        except Exception:
            pass

        # 建立備份管理器
        backup_manager = ERPBackupManager(config)

        # 依設定選擇單表或多表
        if (config.get('ragic_sheet_id') or '').upper() == 'ALL':
            result = backup_manager.run_backup_all_sheets()
        else:
            result = backup_manager.run_backup()

        if result['status'] == 'success':
            return {
                "status": "success",
                "message": "ERP 資料備份完成",
                "data": {
                    "records_processed": result.get('records_transformed', 0) or result.get('records_processed', 0),
                    "duration_seconds": result.get('duration_seconds', 0),
                    "details": result.get('details')
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