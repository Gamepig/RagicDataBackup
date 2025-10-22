# -*- coding: utf-8 -*-
"""Ragic API 資料獲取模組"""

import requests
import logging
import time
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone, timedelta
from data_transformer import TAIPEI_TZ # For robust timezone handling


class RagicClient:
    """Ragic API 客戶端"""

    def __init__(self, api_key: str, account: str, timeout: int = 30, max_retries: int = 3):
        """
        初始化 Ragic 客戶端

        Args:
            api_key: Ragic API 金鑰（Base64 編碼）
            account: Ragic 帳戶名稱
            timeout: 請求逾時時間（秒）
            max_retries: API 請求失敗時的最大重試次數
        """
        if not api_key or not account:
            raise ValueError("API Key 或 Account 不可為空")

        self.timeout = timeout
        self.max_retries = max_retries
        self.base_url = f'https://ap6.ragic.com/{account}'

        self.session = requests.Session()
        self.session.headers['Authorization'] = f'Basic {api_key}'

    def fetch_data(self, sheet_id: str, last_sync_time: Optional[str] = None, limit: int = 1000, max_pages: Optional[int] = None, where_field: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        從 Ragic API 取得資料（自動處理分頁和重試）

        Args:
            sheet_id: 表單 ID（例如：'forms8/5'）
            last_sync_time: 最後同步時間，Ragic 格式 (yyyy/MM/dd HH:mm:ss)
            limit: 每頁資料筆數，預設 1000

        Returns:
            List[Dict]: 資料列表
        """
        all_data = []
        offset = 0
        pages = 0
        url = f'{self.base_url}/{sheet_id}'

        while True:
            if max_pages is not None and pages >= max_pages:
                break
            params = {'api': '', 'v': 3, 'limit': limit, 'offset': offset}
            if last_sync_time:
                field = where_field or '_ragicModified'
                params['where'] = f'{field},gt,{last_sync_time}'

            # 重試機制
            for retry in range(self.max_retries + 1):
                try:
                    response = self.session.get(url, params=params, timeout=self.timeout)
                    response.raise_for_status()

                    result = response.json()

                    # Ragic API 直接回傳資料物件，需轉換為列表
                    if isinstance(result, dict):
                        data = list(result.values()) if result else []
                    else:
                        data = []
                    if not data:
                        return all_data

                    all_data.extend(data)
                    logging.info(f"取得 {len(data)} 筆，總計 {len(all_data)} 筆")

                    if len(data) < limit:
                        return all_data

                    pages += 1
                    offset += limit
                    time.sleep(0.5)  # 避免 API 限流
                    break

                except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
                    if retry == self.max_retries:
                        raise Exception(f"API 請求失敗（已重試 {self.max_retries} 次）: {e}")
                    time.sleep(2 ** retry)
                except requests.exceptions.HTTPError as e:
                    if e.response.status_code == 401:
                        raise Exception("API 認證失敗")
                    elif e.response.status_code == 404:
                        raise Exception(f"找不到表單: {sheet_id}")
                    raise Exception(f"HTTP 錯誤 ({e.response.status_code})")

    def close(self):
        """關閉連線"""
        self.session.close()

    def test_connection(self) -> bool:
        """簡易測試：嘗試連到帳號根網址，能連上即視為可用。
        不要求 200，常見 401/403/404 也代表網路與主機可達。
        """
        try:
            resp = self.session.get(self.base_url, timeout=self.timeout)
            logging.info(f"Ragic ping {self.base_url} -> {resp.status_code}")
            return resp.status_code in (200, 301, 302, 401, 403, 404)
        except Exception as e:
            logging.error(f"Ragic ping error for {self.base_url}: {e}")
            return False

    # ---- 新增：無 where 單頁掃描 + 本地過濾增量（以最後修改欄位） ----
    _DT_PATTERNS = [
        '%Y/%m/%d %H:%M:%S',
        '%Y-%m-%d %H:%M:%S',
        '%Y/%m/%d',
        '%Y-%m-%d',
    ]

    def _parse_dt(self, value: Any) -> Optional[datetime]:
        if not value:
            return None
        s = str(value).strip()
        dt_naive: Optional[datetime] = None

        try:
            # Try ISO format first, including timezone-aware
            if 'T' in s:
                dt_parsed = datetime.fromisoformat(s.replace('Z', '+00:00'))
                if dt_parsed.tzinfo is None: # If naive ISO
                    dt_naive = dt_parsed
                else: # If already timezone-aware, convert to UTC
                    return dt_parsed.astimezone(timezone.utc)
        except Exception:
            pass

        # Try common patterns
        for p in self._DT_PATTERNS:
            try:
                dt_naive = datetime.strptime(s, p)
                break
            except Exception:
                continue

        if dt_naive is None:
            return None # Could not parse

        # If parsed datetime is naive, assume Taipei and convert to UTC
        if dt_naive.tzinfo is None:
            dt_localized = dt_naive.replace(tzinfo=TAIPEI_TZ)
            return dt_localized.astimezone(timezone.utc)
        else:
            # Should not happen if fromisoformat handled it, but as a safeguard
            return dt_naive.astimezone(timezone.utc)

    def fetch_since_local_paged(
        self,
        sheet_id: str,
        since_dt: datetime,
        last_modified_field_names: List[str],
        until_dt: Optional[datetime] = None,
        limit: int = 1000,
        max_pages: int = 50,
        no_new_data_pages_threshold: int = 2 # 連續無新資料頁面閾值
    ) -> List[Dict[str, Any]]:
        """
        不使用 where，改以本地過濾增量且有頁面延伸規則：
        - 逐頁抓取，僅以 max_pages 與「不足一頁」為停止條件（避免因排序差異提早停止）。
        - 若整頁皆無法解析日期，為避免無限迴圈，只抓第一頁即停止。
        - 引入 no_new_data_pages_threshold，實現智慧提前停止。
        """
        # 詳細 logging 診斷
        logging.info(f"[fetch_since_local_paged] 開始抓取 {sheet_id}")
        logging.info(f"[fetch_since_local_paged] 原始 since_dt={since_dt.isoformat()}")
        if until_dt:
            logging.info(f"[fetch_since_local_paged] until_dt={until_dt.isoformat()}")
        logging.info(f"[fetch_since_local_paged] 日期欄位: {last_modified_field_names}")

        collected: List[Dict[str, Any]] = []
        offset = 0
        pages = 0
        url = f'{self.base_url}/{sheet_id}'
        consecutive_no_new_data_pages = 0 # 連續無新資料頁面計數

        while pages < max_pages:
            pages += 1
            params = {'api': '', 'v': 3, 'limit': limit, 'offset': offset}
            # 強制使用 _ragicId 進行遞增排序
            params['orderBy'] = '_ragicId,asc'

            try:
                r = self.session.get(url, params=params, timeout=self.timeout)
                r.raise_for_status()
                result = r.json()
            except requests.exceptions.ReadTimeout:
                # 單次快速重試一次
                try:
                    r = self.session.get(url, params=params, timeout=self.timeout)
                    r.raise_for_status()
                    result = r.json()
                except Exception as e:
                    logging.warning(f"Ragic 讀取逾時（offset={offset}, limit={limit}）：{e}")
                    break
            except Exception as e:
                logging.warning(f"Ragic 讀取失敗（offset={offset}, limit={limit}）：{e}")
                break
            data = list(result.values()) if isinstance(result, dict) else []
            logging.info(f"[fetch_since_local_paged] 第 {pages} 頁：API 返回 {len(data)} 筆資料")
            if not data:
                logging.info(f"[fetch_since_local_paged] 第 {pages} 頁無資料，停止")
                break

            any_parsed = False
            page_new_data_count = 0 # 記錄本頁符合原始 since_dt 的新資料筆數
            first_rec_dt = None
            last_rec_dt = None

            for rec in data:
                # 嘗試多個可能的「最後修改」欄位名稱
                dt = None
                for name in last_modified_field_names:
                    if name in rec:
                        dt = self._parse_dt(rec.get(name))
                        if dt:
                            any_parsed = True
                            if first_rec_dt is None:
                                first_rec_dt = dt
                            last_rec_dt = dt
                            break

                # 比較時使用原始的 since_dt (UTC)
                if dt and until_dt and dt > until_dt:
                    # 超過上界，不納入
                    pass
                elif dt and dt >= since_dt:  # 比較原始 since_dt，允許相等時間（MERGE 會去重）
                    collected.append(rec)
                    page_new_data_count += 1
                # else: 資料比原始 since_dt 舊，不處理

            # 詳細記錄每頁處理結果
            if first_rec_dt:
                logging.info(f"[fetch_since_local_paged] 第 {pages} 頁日期範圍: {first_rec_dt.isoformat()} ~ {last_rec_dt.isoformat()}")
            logging.info(f"[fetch_since_local_paged] 第 {pages} 頁符合原始 since_dt 條件: {page_new_data_count} 筆")

            # 智慧提前停止邏輯
            # 由於現在是按 _ragicId 遞增排序，如果一頁中沒有任何新資料，
            # 則可以合理推斷後續頁面也不會有新資料，因此可以提前停止。
            if page_new_data_count == 0:
                consecutive_no_new_data_pages += 1
                logging.info(f"[fetch_since_local_paged] 連續無新資料頁面計數: {consecutive_no_new_data_pages}")
            else:
                consecutive_no_new_data_pages = 0

            if consecutive_no_new_data_pages >= no_new_data_pages_threshold:
                logging.info(f"[fetch_since_local_paged] 連續 {no_new_data_pages_threshold} 頁無新資料，提前停止抓取")
                break

            if len(data) < limit:
                logging.info(f"[fetch_since_local_paged] 第 {pages} 頁資料不足一頁 ({len(data)} < {limit})，停止")
                break
            if not any_parsed:
                # 當頁皆無法解析日期，避免無限抓取
                logging.warning(f"[fetch_since_local_paged] 第 {pages} 頁無法解析任何日期，停止")
                break

            offset += limit

        logging.info(f"[fetch_since_local_paged] 完成抓取：共 {pages} 頁，保留 {len(collected)} 筆資料")
        return collected

    def fetch_first_page(self, sheet_id: str, limit: int = 1000) -> List[Dict[str, Any]]:
        """抓取第一頁原始資料（不套用時間窗），用於煙霧測試或無法判定時間欄位時。
        """
        url = f'{self.base_url}/{sheet_id}'
        params = {'api': '', 'v': 3, 'limit': limit, 'offset': 0}
        r = self.session.get(url, params=params, timeout=self.timeout)
        r.raise_for_status()
        result = r.json()
        data = list(result.values()) if isinstance(result, dict) else []
        return data
