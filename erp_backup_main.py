# -*- coding: utf-8 -*-
"""
ERP 資料備份主程式
整合 Ragic API 資料獲取、資料轉換、BigQuery 上傳的完整流程
"""

import os
import logging
import datetime
from typing import Dict, Any, Optional

# 導入自定義模組
from ragic_client import create_ragic_client, RagicClient
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
            self.ragic_client = create_ragic_client(
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

    def get_last_sync_timestamp(self) -> int:
        """
        獲取最後同步時間戳

        Returns:
            int: 最後同步時間戳（毫秒）
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
            # 返回一週前的時間戳
            last_week = datetime.datetime.now() - datetime.timedelta(weeks=1)
            return int(last_week.timestamp() * 1000)

    def fetch_ragic_data(self, last_timestamp: int) -> list:
        """
        從 Ragic 獲取資料

        Args:
            last_timestamp: 最後更新時間戳

        Returns:
            list: 原始 Ragic 資料

        Raises:
            Exception: 當資料獲取失敗時
        """
        if not self.ragic_client:
            raise Exception("Ragic 客戶端未初始化")

        logging.info("開始從 Ragic 獲取資料...")

        try:
            data = self.ragic_client.fetch_data(
                sheet_id=self.config['ragic_sheet_id'],
                last_timestamp=last_timestamp,
                limit=self.config.get('ragic_page_size', 1000)
            )

            logging.info(f"成功獲取 {len(data)} 筆 Ragic 資料")
            return data

        except Exception as e:
            logging.error(f"Ragic 資料獲取失敗: {e}")
            raise

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
            # 根據資料量決定是否批次上傳
            batch_size = self.config.get('upload_batch_size', 1000)
            use_merge = self.config.get('use_merge', True)

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
                    use_merge=use_merge
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

        try:
            # 初始化所有客戶端
            self.initialize_clients()

            # 測試連線
            connections = self.test_connections()
            if not all(connections.values()):
                failed_services = [k for k, v in connections.items() if not v]
                raise Exception(f"服務連線失敗: {', '.join(failed_services)}")

            # 獲取最後同步時間
            last_timestamp = self.get_last_sync_timestamp()
            logging.info(f"最後同步時間戳: {last_timestamp}")

            # 步驟 1: 從 Ragic 獲取資料
            ragic_data = self.fetch_ragic_data(last_timestamp)

            if not ragic_data:
                logging.info("沒有新資料需要同步")
                return {
                    "status": "no_data",
                    "start_time": start_time.isoformat(),
                    "end_time": datetime.datetime.now().isoformat(),
                    "duration_seconds": 0,
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

            # 整合結果
            result = {
                "status": "success",
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat(),
                "duration_seconds": duration,
                "records_fetched": len(ragic_data),
                "records_transformed": len(transformed_data),
                "upload_result": upload_result
            }

            logging.info(f"備份流程完成 - 耗時: {duration:.2f} 秒")
            return result

        except Exception as e:
            end_time = datetime.datetime.now()
            duration = (end_time - start_time).total_seconds()
            error_msg = f"備份失敗 - 耗時: {duration:.2f} 秒, 錯誤: {str(e)}"
            logging.error(error_msg)

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

        'gcp_project_id': os.environ.get('GCP_PROJECT_ID', 'your-project-id'),
        'bigquery_dataset': os.environ.get('BIGQUERY_DATASET', 'your_dataset'),
        'bigquery_table': os.environ.get('BIGQUERY_TABLE', 'erp_backup'),
        'bigquery_location': os.environ.get('BIGQUERY_LOCATION', 'US'),

        'upload_batch_size': int(os.environ.get('UPLOAD_BATCH_SIZE', 1000)),
        'use_merge': os.environ.get('USE_MERGE', 'true').lower() == 'true',
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

        # 執行備份
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