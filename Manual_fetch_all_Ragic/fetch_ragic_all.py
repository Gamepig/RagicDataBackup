#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
抓取 Ragic 9 表資料到本機 JSON（支援增量/全量、分頁落地、批次摘要）

輸出目錄結構（預設）：
tests/data/<batch_timestamp>/
  ├─ manifest.json                 # 單批次執行摘要與每表統計
  ├─ batch_summary.json            # 簡化版摘要（便於人讀）
  ├─ <sheet_code>/page-00001.json  # 全量模式：每頁一檔（可續傳）
  └─ <sheet_code>_<sheet_name>.json# 合併檔案（陣列，中文欄位）
"""

import argparse
import json
import logging
import os
import sys
import time
import ast
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

# 確保從 tests/ 執行時能找到專案根目錄的模組
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

# 嘗試載入 BigQuery 設定載入器；若缺少 google 套件，改用環境變數方案
HAS_CONFIG_LOADER = False
try:
    from config_loader import load_config_with_fallback  # type: ignore
    HAS_CONFIG_LOADER = True
except Exception:
    HAS_CONFIG_LOADER = False

from ragic_client import RagicClient


def setup_logging(level: str = "INFO") -> None:
    numeric_level = getattr(logging, level.upper(), logging.INFO)
    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def now_batch_str() -> str:
    return datetime.now().strftime("%Y%m%d-%H%M%S")


def parse_csv(s: str) -> List[str]:
    return [x.strip() for x in s.split(",") if x.strip()] if s else []


def list_existing_pages(dir_path: Path) -> List[Path]:
    if not dir_path.exists():
        return []
    return sorted(dir_path.glob("page-*.json"))


def write_json(path: Path, obj: Any) -> None:
    with path.open("w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


def read_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def fetch_full_paged(
    client: RagicClient,
    sheet_code: str,
    sheet_name: str,
    sheet_id: str,
    out_root: Path,
    limit: int,
    page_sleep: float,
) -> Dict[str, Any]:
    """全量抓取（不使用 where），逐頁寫入 <sheet_code>/page-*.json，並合併輸出。

    回傳 per-sheet 摘要：pages, records, duration_seconds, combined_file
    """
    t0 = time.time()
    per_sheet_dir = out_root / sheet_code
    ensure_dir(per_sheet_dir)

    url = f"{client.base_url}/{sheet_id}"

    # 續傳：從現有頁數繼續
    existing_pages = list_existing_pages(per_sheet_dir)
    page_index = len(existing_pages)  # 0-based 計數，但檔名從 1 開始
    offset = page_index * limit

    total_records = 0
    while True:
        params = {"api": "", "v": 3, "limit": limit, "offset": offset}
        logging.info(f"[{sheet_code}] 取得頁面 offset={offset}, limit={limit}")

        # 使用既有 session 與 timeout
        r = client.session.get(url, params=params, timeout=client.timeout)
        r.raise_for_status()
        result = r.json()
        data = list(result.values()) if isinstance(result, dict) else []

        if not data:
            logging.info(f"[{sheet_code}] 無更多資料，結束全量抓取")
            break

        page_index += 1
        page_path = per_sheet_dir / f"page-{page_index:05d}.json"
        write_json(page_path, data)
        total_records += len(data)
        logging.info(
            f"[{sheet_code}] 寫入 {page_path.name} 筆數={len(data)}，累計={total_records}"
        )

        if len(data) < limit:
            logging.info(f"[{sheet_code}] 最後一頁（小於 limit），結束全量抓取")
            break

        offset += limit
        time.sleep(page_sleep)

    # 合併頁檔 → 單一陣列 JSON
    combined_file = out_root / f"{sheet_code}_{sheet_name}.json"
    combined: List[Dict[str, Any]] = []
    for p in list_existing_pages(per_sheet_dir):
        try:
            page_data = read_json(p)
            if isinstance(page_data, list):
                combined.extend(page_data)
        except Exception as e:
            logging.warning(f"[{sheet_code}] 讀取 {p.name} 失敗: {e}")

    write_json(combined_file, combined)

    return {
        "mode": "full",
        "pages": page_index,
        "records": len(combined),
        "duration_seconds": round(time.time() - t0, 2),
        "combined_file": str(combined_file),
    }


def fetch_incremental_since(
    client: RagicClient,
    sheet_code: str,
    sheet_name: str,
    sheet_id: str,
    out_root: Path,
    since_days: int,
    last_modified_field_names: List[str],
    limit: int,
    max_pages: int,
) -> Dict[str, Any]:
    """增量抓取（本地過濾 + 翻頁規則），直接輸出合併檔。"""
    t0 = time.time()
    since_dt = datetime.now() - timedelta(days=since_days)

    records = client.fetch_since_local_paged(
        sheet_id=sheet_id,
        since_dt=since_dt,
        last_modified_field_names=last_modified_field_names,
        until_dt=None,
        limit=limit,
        max_pages=max_pages,
    )

    combined_file = out_root / f"{sheet_code}_{sheet_name}.json"
    write_json(combined_file, records)

    return {
        "mode": "since_local_paged",
        "pages": None,
        "records": len(records),
        "duration_seconds": round(time.time() - t0, 2),
        "combined_file": str(combined_file),
    }


def load_configs_from_env() -> List[Dict[str, Any]]:
    """
    從環境變數載入 9 表配置，避免依賴 google 套件。

    需要：RAGIC_API_KEY、RAGIC_ACCOUNT。
    表單來源優先序：
    1) RAGIC_SHEET_ID=ALL + SHEET_MAP_JSON（code->sheet_id）
    2) RAGIC_BACKUP_SHEETS="10:forms8/5,20:forms8/4,..."
    3) 單一表：RAGIC_SHEET_ID
    """
    # 優先嘗試從 .env.*.yaml 載入必要環境變數
    _maybe_load_env_from_yaml()

    api_key = os.environ.get("RAGIC_API_KEY", "").strip()
    account = os.environ.get("RAGIC_ACCOUNT", "").strip()
    if not api_key or not account:
        raise RuntimeError("缺少環境變數 RAGIC_API_KEY / RAGIC_ACCOUNT")

    configs: List[Dict[str, Any]] = []
    sheet_id_env = os.environ.get("RAGIC_SHEET_ID", "").strip()

    if sheet_id_env.upper() == "ALL":
        # 需要 SHEET_MAP_JSON 或 SHEET_MAP_FILE
        sheet_map_json = os.environ.get("SHEET_MAP_JSON", "").strip()
        sheet_map_file = os.environ.get("SHEET_MAP_FILE", "").strip()
        mapping: Dict[str, str] = {}
        if sheet_map_json:
            try:
                mapping = json.loads(sheet_map_json)
            except Exception:
                # 兼容單引號/字典格式（例如 YAML 內寫成 {'10':'forms8/5',...}）
                try:
                    mapping = ast.literal_eval(sheet_map_json)
                    if not isinstance(mapping, dict):
                        raise ValueError("SHEET_MAP_JSON 不是字典")
                except Exception as e:
                    raise RuntimeError(f"SHEET_MAP_JSON 解析失敗: {e}")
        elif sheet_map_file:
            try:
                with open(sheet_map_file, "r", encoding="utf-8") as f:
                    mapping = json.load(f)
            except Exception as e:
                raise RuntimeError(f"SHEET_MAP_FILE 讀取失敗: {e}")
        else:
            raise RuntimeError("RAGIC_SHEET_ID=ALL 需要提供 SHEET_MAP_JSON 或 SHEET_MAP_FILE")

        for code, sid in mapping.items():
            configs.append({
                "client_id": os.environ.get("CLIENT_ID", "local"),
                "ragic_api_key": api_key,
                "ragic_account": account,
                "sheet_code": str(code),
                "sheet_id": str(sid),
                "sheet_name": str(code),
                "backup_priority": None,
            })
        return configs

    # 多表字串
    sheets_str = os.environ.get("RAGIC_BACKUP_SHEETS", "").strip()
    if sheets_str:
        for pair in sheets_str.split(","):
            parts = pair.strip().split(":")
            if len(parts) == 2:
                code, sid = parts
                configs.append({
                    "client_id": os.environ.get("CLIENT_ID", "local"),
                    "ragic_api_key": api_key,
                    "ragic_account": account,
                    "sheet_code": code.strip(),
                    "sheet_id": sid.strip(),
                    "sheet_name": code.strip(),
                    "backup_priority": None,
                })
        if configs:
            return configs

    # 單一表
    if sheet_id_env:
        code = os.environ.get("RAGIC_SHEET_CODE", "99").strip() or "99"
        configs.append({
            "client_id": os.environ.get("CLIENT_ID", "local"),
            "ragic_api_key": api_key,
            "ragic_account": account,
            "sheet_code": code,
            "sheet_id": sheet_id_env,
            "sheet_name": code,
            "backup_priority": None,
        })
        return configs

    raise RuntimeError("未提供任何表單設定（請設定 RAGIC_SHEET_ID 或 RAGIC_BACKUP_SHEETS 或 SHEET_MAP_JSON）")


def _maybe_load_env_from_yaml() -> None:
    """嘗試從專案根目錄的 .env.yaml / .env.complete.yaml / .env.fix.yaml 載入必要鍵。

    僅解析單層 key:value，忽略巢狀結構；允許值包覆單/雙引號。
    目標鍵：RAGIC_API_KEY、RAGIC_ACCOUNT、RAGIC_SHEET_ID、SHEET_MAP_JSON、RAGIC_BACKUP_SHEETS、RAGIC_SHEET_CODE
    """
    wanted = {
        "RAGIC_API_KEY",
        "RAGIC_ACCOUNT",
        "RAGIC_SHEET_ID",
        "SHEET_MAP_JSON",
        "RAGIC_BACKUP_SHEETS",
        "RAGIC_SHEET_CODE",
    }
    candidates = [".env.yaml", ".env.complete.yaml", ".env.fix.yaml"]
    for name in candidates:
        path = os.path.join(ROOT_DIR, name)
        if not os.path.isfile(path):
            continue
        try:
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    line_stripped = line.strip()
                    if not line_stripped or line_stripped.startswith("#"):
                        continue
                    if ":" not in line_stripped:
                        continue
                    k, v = line_stripped.split(":", 1)
                    key = k.strip()
                    if key not in wanted:
                        continue
                    val = v.strip()
                    # 移除可能的行尾註解
                    if " #" in val:
                        val = val.split(" #", 1)[0].strip()
                    # 移除首尾引號
                    if (val.startswith("'") and val.endswith("'")) or (val.startswith('"') and val.endswith('"')):
                        val = val[1:-1]
                    if key not in os.environ and val:
                        os.environ[key] = val
        except Exception:
            # 解析失敗不影響執行，交由後續檢查機制報錯
            continue


def main() -> None:
    parser = argparse.ArgumentParser(description="抓取 Ragic 9 表為本機 JSON")
    parser.add_argument("--project-id", type=str, default=None, help="GCP 專案（使用 BigQuery 載入配置）")
    parser.add_argument("--client-id", type=str, default="grefun", help="客戶識別碼")
    parser.add_argument("--out-dir", type=str, default=None, help="輸出資料夾（預設 tests/data/<batch>）")
    parser.add_argument("--mode", type=str, choices=["since_local_paged", "full"], default="since_local_paged")
    parser.add_argument("--since-days", type=int, default=7, help="增量模式：回溯天數")
    parser.add_argument("--limit", type=int, default=1000, help="每頁筆數（小表）")
    parser.add_argument("--large-limit", type=int, default=10000, help="每頁筆數（大表）")
    parser.add_argument("--max-pages", type=int, default=50, help="增量模式：最多頁數")
    parser.add_argument("--page-sleep", type=float, default=0.8, help="全量模式：每頁間隔秒數")
    parser.add_argument(
        "--large-sheets",
        type=str,
        default="50,60,99",
        help="視為大表的 sheet_code 清單（逗號分隔）",
    )
    parser.add_argument(
        "--last-modified-fields",
        type=str,
        default="最後修改日期,最後修改時間,更新時間,最後更新時間",
        help="增量模式：依序嘗試的時間欄位名稱（逗號分隔）",
    )
    parser.add_argument("--log-level", type=str, default="INFO", help="日誌層級")

    args = parser.parse_args()
    setup_logging(args.log_level)

    # 載入 9 表配置（若可用則使用 BigQuery，否則使用環境變數）
    if args.project_id and HAS_CONFIG_LOADER:
        try:
            configs = load_config_with_fallback(project_id=args.project_id, client_id=args.client_id, use_bigquery=True)
        except Exception as e:
            logging.warning(f"BigQuery 配置載入失敗: {e}，回退到環境變數")
            configs = load_configs_from_env()
    else:
        configs = load_configs_from_env()
    if not configs:
        logging.error("未取得任何表單配置，請確認 BigQuery/環境變數設定")
        sys.exit(1)

    # 決定 API 認證（同一批配置共用同一組）
    api_key = configs[0]["ragic_api_key"]
    account = configs[0]["ragic_account"]

    # 使用較寬鬆的 timeout/retry，以涵蓋大表
    client = RagicClient(api_key=api_key, account=account, timeout=60, max_retries=5)
    if not client.test_connection():
        logging.warning("Ragic 連線測試失敗，仍嘗試抓取（可能為 401/403/404）")

    batch_dir = Path(args.out_dir) if args.out_dir else Path("Manual_fetch_all_Ragic/data") / now_batch_str()
    ensure_dir(batch_dir)

    # 參數整理
    large_sheets = set(parse_csv(args.large_sheets))
    last_modified_field_names = parse_csv(args.last_modified_fields)

    manifest: Dict[str, Any] = {
        "batch_dir": str(batch_dir),
        "client_id": args.client_id,
        "project_id": args.project_id,
        "mode": args.mode,
        "started_at": datetime.now().isoformat(),
        "sheets": {},
    }

    total_records_all = 0
    for cfg in configs:
        sheet_code = str(cfg.get("sheet_code"))
        sheet_id = str(cfg.get("sheet_id"))
        sheet_name = str(cfg.get("sheet_name") or sheet_code)

        is_large = sheet_code in large_sheets
        per_limit = args.large_limit if is_large else args.limit

        logging.info(f"處理表單 {sheet_code} - {sheet_name}（sheet_id={sheet_id}，limit={per_limit}）")

        try:
            if args.mode == "full":
                stats = fetch_full_paged(
                    client=client,
                    sheet_code=sheet_code,
                    sheet_name=sheet_name,
                    sheet_id=sheet_id,
                    out_root=batch_dir,
                    limit=per_limit,
                    page_sleep=args.page_sleep,
                )
            else:
                stats = fetch_incremental_since(
                    client=client,
                    sheet_code=sheet_code,
                    sheet_name=sheet_name,
                    sheet_id=sheet_id,
                    out_root=batch_dir,
                    since_days=args.since_days,
                    last_modified_field_names=last_modified_field_names,
                    limit=per_limit,
                    max_pages=args.max_pages,
                )

            manifest["sheets"][sheet_code] = {
                "sheet_name": sheet_name,
                "sheet_id": sheet_id,
                **stats,
            }
            total_records_all += int(stats.get("records", 0))

        except Exception as e:
            logging.error(f"表單 {sheet_code} 抓取失敗: {e}")
            manifest["sheets"][sheet_code] = {
                "sheet_name": sheet_name,
                "sheet_id": sheet_id,
                "error": str(e),
            }

    manifest["finished_at"] = datetime.now().isoformat()
    manifest["total_records_all"] = total_records_all
    write_json(batch_dir / "manifest.json", manifest)

    # 產出簡易摘要（人讀）
    summary_lines = [
        f"批次資料夾: {manifest['batch_dir']}",
        f"模式: {manifest['mode']}",
        f"總筆數: {total_records_all}",
    ]
    for sc, info in manifest["sheets"].items():
        if "error" in info:
            summary_lines.append(f"- {sc} {info['sheet_name']}: 失敗 {info['error']}")
        else:
            summary_lines.append(
                f"- {sc} {info['sheet_name']}: {info.get('records', 0)} 筆, mode={info.get('mode')}"
            )
    write_json(batch_dir / "batch_summary.json", {"summary": summary_lines})

    logging.info("完成所有表單抓取")


if __name__ == "__main__":
    main()


