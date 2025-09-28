# -*- coding: utf-8 -*-
"""
Ragic API 資料獲取模組
專門處理從 Ragic API 獲取資料的功能
"""

import requests
import logging
from typing import List, Dict, Any, Optional


class RagicClient:
    """Ragic API 客戶端類別"""

    def __init__(self, api_key: str, account: str, timeout: int = 30, max_retries: int = 3):
        """
        初始化 Ragic 客戶端

        Args:
            api_key: Ragic API 金鑰
            account: Ragic 帳戶名稱
            timeout: 請求逾時時間（秒）
            max_retries: 最大重試次數
        """
        if not api_key or not account:
            raise ValueError("API Key 或 Account 不可為空")

        self.api_key = api_key
        self.account = account
        self.timeout = timeout
        self.max_retries = max_retries
        self.session = requests.Session()
        self.session.auth = (api_key, '')  # Basic Auth

        logging.info(f"Ragic 客戶端初始化完成 - 帳戶: {account}")

    def fetch_data(self, sheet_id: str, last_timestamp: int, limit: int = 1000) -> List[Dict[str, Any]]:
        """
        從 Ragic API 取得資料

        Args:
            sheet_id: 表單 ID（例如：'your-sheet/1'）
            last_timestamp: 最後更新時間戳（毫秒）
            limit: 每頁資料筆數

        Returns:
            List[Dict]: 資料列表

        Raises:
            Exception: 當 API 呼叫失敗時
        """
        if not sheet_id:
            raise ValueError("Sheet ID 不可為空")

        base_url = f'https://www.ragic.com/{self.account}/{sheet_id}?api'
        all_data = []
        offset = 0
        retry_count = 0

        logging.info(f"開始從 Ragic 獲取資料 - Sheet: {sheet_id}, 時間戳: {last_timestamp}")

        while True:
            params = {
                'where': f'_ragicModified>{last_timestamp}',
                'limit': limit,
                'offset': offset,
                'naming': 'default'  # 使用中文欄位名稱
            }

            try:
                response = self.session.get(base_url, params=params, timeout=self.timeout)

                # 處理各種 HTTP 狀態碼
                if response.status_code == 401:
                    raise Exception("Ragic API 認證失敗，請檢查 API Key")
                elif response.status_code == 403:
                    raise Exception("Ragic API 權限不足，請檢查帳戶權限")
                elif response.status_code == 404:
                    raise Exception(f"找不到指定的 Ragic 表單: {sheet_id}")
                elif response.status_code != 200:
                    if retry_count < self.max_retries:
                        retry_count += 1
                        logging.warning(f"API 請求失敗 ({response.status_code})，重試第 {retry_count} 次...")
                        continue
                    else:
                        raise Exception(f"Ragic API 錯誤 ({response.status_code}): {response.text}")

                # 重置重試計數器
                retry_count = 0

                # 解析 JSON 回應
                try:
                    data = response.json()
                except ValueError as e:
                    raise Exception(f"無法解析 Ragic API 回應: {e}")

                # 檢查資料格式
                if not data:
                    logging.info("沒有更多資料")
                    break

                if not isinstance(data, list):
                    logging.warning(f"預期資料格式為列表，實際收到: {type(data)}")
                    break

                all_data.extend(data)
                offset += limit
                logging.info(f"已取得 {len(data)} 筆資料，總計 {len(all_data)} 筆")

            except requests.exceptions.Timeout:
                if retry_count < self.max_retries:
                    retry_count += 1
                    logging.warning(f"請求逾時，重試第 {retry_count} 次...")
                    continue
                else:
                    raise Exception("Ragic API 請求逾時，請稍後再試")

            except requests.exceptions.ConnectionError:
                if retry_count < self.max_retries:
                    retry_count += 1
                    logging.warning(f"連線錯誤，重試第 {retry_count} 次...")
                    continue
                else:
                    raise Exception("無法連接到 Ragic API，請檢查網路連線")

            except Exception as e:
                logging.error(f"Ragic API 請求失敗: {e}")
                raise

        logging.info(f"Ragic 資料獲取完成，總計 {len(all_data)} 筆")
        return all_data

    def test_connection(self) -> bool:
        """
        測試 Ragic API 連線

        Returns:
            bool: 連線成功返回 True，失敗返回 False
        """
        try:
            # 嘗試獲取帳戶資訊（使用簡單的 API 呼叫）
            test_url = f'https://www.ragic.com/{self.account}?api&limit=1'
            response = self.session.get(test_url, timeout=10)

            if response.status_code == 200:
                logging.info("Ragic API 連線測試成功")
                return True
            else:
                logging.error(f"Ragic API 連線測試失敗: {response.status_code}")
                return False

        except Exception as e:
            logging.error(f"Ragic API 連線測試異常: {e}")
            return False

    def close(self):
        """關閉連線"""
        if self.session:
            self.session.close()
            logging.info("Ragic 客戶端連線已關閉")


def create_ragic_client(api_key: str, account: str, **kwargs) -> RagicClient:
    """
    建立 Ragic 客戶端的工廠函數

    Args:
        api_key: Ragic API 金鑰
        account: Ragic 帳戶名稱
        **kwargs: 其他設定參數

    Returns:
        RagicClient: Ragic 客戶端實例
    """
    return RagicClient(api_key, account, **kwargs)