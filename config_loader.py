# -*- coding: utf-8 -*-
"""
配置載入模組
從 BigQuery 載入 Ragic API 配置（9 個表單的 API Key、Sheet ID 等）

更新日期：2025-10-02
功能：支援從 BigQuery backup_config 表動態載入配置，無需重新部署 Cloud Function
"""

import logging
from typing import List, Dict, Any, Optional
from google.cloud import bigquery


class BackupConfigLoader:
    """備份配置載入器"""

    def __init__(self, project_id: str, dataset_id: str = "ragic_backup"):
        """
        初始化配置載入器

        Args:
            project_id: GCP 專案 ID
            dataset_id: BigQuery Dataset ID，預設 ragic_backup
        """
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.client = bigquery.Client(project=project_id)
        self._cache = None  # 快取配置
        logging.info(f"配置載入器初始化完成（專案：{project_id}，Dataset：{dataset_id}）")

    def load_backup_config(
        self,
        client_id: str = "grefun",
        enabled_only: bool = True,
        force_reload: bool = False
    ) -> List[Dict[str, Any]]:
        """
        從 BigQuery 載入備份配置

        Args:
            client_id: 客戶識別碼，預設 grefun
            enabled_only: 是否只載入啟用的表單，預設 True
            force_reload: 是否強制重新載入（忽略快取），預設 False

        Returns:
            List[Dict]: 備份配置列表，每個元素包含：
                - client_id: 客戶識別碼
                - ragic_api_key: Ragic API Key（Base64）
                - ragic_account: Ragic 帳戶
                - sheet_code: 表單代碼（10, 20, 30...）
                - sheet_id: Sheet ID（forms8/5）
                - sheet_name: 表單名稱
                - backup_priority: 備份優先級

        Raises:
            Exception: 當查詢失敗時
        """
        # 檢查快取
        if not force_reload and self._cache is not None:
            logging.info("使用快取的備份配置")
            return self._cache

        try:
            # 建構查詢
            query = f"""
            SELECT
                client_id,
                ragic_api_key,
                ragic_account,
                sheet_code,
                sheet_id,
                sheet_name,
                backup_priority,
                notes
            FROM `{self.project_id}.{self.dataset_id}.backup_config`
            WHERE client_id = @client_id
            """

            if enabled_only:
                query += " AND enabled = TRUE"

            query += " ORDER BY backup_priority ASC, sheet_code ASC"

            # 設定查詢參數
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("client_id", "STRING", client_id)
                ]
            )

            # 執行查詢
            logging.info(f"從 BigQuery 載入備份配置（客戶：{client_id}，僅啟用：{enabled_only}）")
            results = self.client.query(query, job_config=job_config).result()

            # 轉換為列表
            config_list = []
            for row in results:
                config_list.append({
                    "client_id": row.client_id,
                    "ragic_api_key": row.ragic_api_key,
                    "ragic_account": row.ragic_account,
                    "sheet_code": row.sheet_code,
                    "sheet_id": row.sheet_id,
                    "sheet_name": row.sheet_name,
                    "backup_priority": row.backup_priority,
                    "notes": row.notes if hasattr(row, 'notes') else None
                })

            # 更新快取
            self._cache = config_list

            logging.info(f"成功載入 {len(config_list)} 個表單配置")
            return config_list

        except Exception as e:
            logging.error(f"載入備份配置失敗: {e}")
            raise

    def get_sheet_config(self, sheet_code: str, client_id: str = "grefun") -> Optional[Dict[str, Any]]:
        """
        獲取特定表單的配置

        Args:
            sheet_code: 表單代碼（10, 20, 30...）
            client_id: 客戶識別碼，預設 grefun

        Returns:
            Optional[Dict]: 表單配置，如果找不到則返回 None
        """
        try:
            configs = self.load_backup_config(client_id=client_id)
            for config in configs:
                if config["sheet_code"] == sheet_code:
                    return config
            logging.warning(f"找不到表單 {sheet_code} 的配置")
            return None

        except Exception as e:
            logging.error(f"獲取表單配置失敗: {e}")
            return None

    def get_api_credentials(self, client_id: str = "grefun") -> Optional[Dict[str, str]]:
        """
        獲取 Ragic API 認證資訊（API Key 和 Account）

        Args:
            client_id: 客戶識別碼，預設 grefun

        Returns:
            Optional[Dict]: 包含 api_key 和 account 的字典
        """
        try:
            configs = self.load_backup_config(client_id=client_id)
            if configs:
                # 所有表單使用相同的 API Key 和 Account
                first_config = configs[0]
                return {
                    "api_key": first_config["ragic_api_key"],
                    "account": first_config["ragic_account"]
                }
            else:
                logging.error(f"客戶 {client_id} 沒有任何配置")
                return None

        except Exception as e:
            logging.error(f"獲取 API 認證資訊失敗: {e}")
            return None

    def clear_cache(self):
        """清除快取，強制重新載入配置"""
        self._cache = None
        logging.info("配置快取已清除")

    def validate_config(self, config: Dict[str, Any]) -> bool:
        """
        驗證配置完整性

        Args:
            config: 配置字典

        Returns:
            bool: 配置有效返回 True
        """
        required_fields = ["ragic_api_key", "ragic_account", "sheet_code", "sheet_id"]
        for field in required_fields:
            if not config.get(field):
                logging.error(f"配置缺少必要欄位: {field}")
                return False
        return True


def create_config_loader(project_id: str, dataset_id: str = "ragic_backup") -> BackupConfigLoader:
    """
    建立配置載入器的工廠函數

    Args:
        project_id: GCP 專案 ID
        dataset_id: BigQuery Dataset ID，預設 ragic_backup

    Returns:
        BackupConfigLoader: 配置載入器實例

    Examples:
        >>> loader = create_config_loader(project_id="grefun-testing")
        >>> configs = loader.load_backup_config()
        >>> print(f"載入 {len(configs)} 個表單配置")
    """
    return BackupConfigLoader(project_id, dataset_id)


def load_config_from_bigquery(
    project_id: str,
    client_id: str = "grefun",
    dataset_id: str = "ragic_backup"
) -> List[Dict[str, Any]]:
    """
    快捷函數：從 BigQuery 載入配置

    Args:
        project_id: GCP 專案 ID
        client_id: 客戶識別碼，預設 grefun
        dataset_id: BigQuery Dataset ID，預設 ragic_backup

    Returns:
        List[Dict]: 備份配置列表

    Examples:
        >>> configs = load_config_from_bigquery(project_id="grefun-testing")
        >>> for config in configs:
        ...     print(f"表單 {config['sheet_code']}: {config['sheet_name']}")
    """
    loader = create_config_loader(project_id, dataset_id)
    return loader.load_backup_config(client_id=client_id)


# 環境變數回退方案
def load_config_from_env() -> Dict[str, Any]:
    """
    從環境變數載入配置（回退方案）

    當 BigQuery 配置表不可用時使用

    Returns:
        Dict: 包含 Ragic API 配置的字典

    Raises:
        ValueError: 當必要的環境變數缺失時
    """
    import os

    required_vars = ["RAGIC_API_KEY", "RAGIC_ACCOUNT", "RAGIC_SHEET_ID"]
    missing_vars = []

    for var in required_vars:
        if not os.environ.get(var):
            missing_vars.append(var)

    if missing_vars:
        raise ValueError(f"缺少必要的環境變數: {', '.join(missing_vars)}")

    # 解析表單清單（格式：10:forms8/5,20:forms8/4,...）
    sheets_str = os.environ.get("RAGIC_BACKUP_SHEETS", "")
    sheets = []

    if sheets_str:
        for sheet_pair in sheets_str.split(","):
            parts = sheet_pair.strip().split(":")
            if len(parts) == 2:
                sheets.append({
                    "sheet_code": parts[0],
                    "sheet_id": parts[1],
                    "ragic_api_key": os.environ["RAGIC_API_KEY"],
                    "ragic_account": os.environ["RAGIC_ACCOUNT"]
                })
    else:
        # 回退到單一表單模式
        sheets.append({
            "sheet_code": "99",
            "sheet_id": os.environ["RAGIC_SHEET_ID"],
            "ragic_api_key": os.environ["RAGIC_API_KEY"],
            "ragic_account": os.environ["RAGIC_ACCOUNT"]
        })

    logging.info(f"從環境變數載入 {len(sheets)} 個表單配置")
    return sheets


def load_config_with_fallback(
    project_id: Optional[str] = None,
    client_id: str = "grefun",
    use_bigquery: bool = True
) -> List[Dict[str, Any]]:
    """
    智慧配置載入：優先使用 BigQuery，失敗時回退到環境變數

    Args:
        project_id: GCP 專案 ID
        client_id: 客戶識別碼
        use_bigquery: 是否嘗試使用 BigQuery

    Returns:
        List[Dict]: 備份配置列表

    Examples:
        >>> # 優先 BigQuery，失敗則使用環境變數
        >>> configs = load_config_with_fallback(project_id="grefun-testing")

        >>> # 強制使用環境變數
        >>> configs = load_config_with_fallback(use_bigquery=False)
    """
    if use_bigquery and project_id:
        try:
            logging.info("嘗試從 BigQuery 載入配置...")
            return load_config_from_bigquery(project_id, client_id)
        except Exception as e:
            logging.warning(f"BigQuery 配置載入失敗: {e}，回退到環境變數")

    logging.info("從環境變數載入配置...")
    return load_config_from_env()
