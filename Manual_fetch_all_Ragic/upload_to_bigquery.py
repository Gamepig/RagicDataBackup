#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
讀取 tests/data/<batch>/ 內各表原始 JSON（中文欄位），
以 DataTransformer 轉為 BigQuery 英文欄位後，透過 BigQueryUploader 上傳到單一目標表。

支援：
- 自動偵測合併檔（<sheet_code>_<sheet_name>.json）或分頁檔（<sheet_code>/page-*.json）
- 可選輸出 processed/ 下的轉換後 JSON（英文欄位，便於檢視與除錯）
"""

import argparse
import json
import logging
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# 確保從 tests/ 執行時能找到專案根目錄的模組
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from data_transformer import create_transformer
from bigquery_uploader import BigQueryUploader


def setup_logging(level: str = "INFO") -> None:
    numeric_level = getattr(logging, level.upper(), logging.INFO)
    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )


def read_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


def find_sheet_files(batch_dir: Path) -> Dict[str, Dict[str, Any]]:
    """尋找每個 sheet_code 的輸入來源。

    回傳 {sheet_code: {mode: 'combined'|'pages', 'combined': Path|None, 'pages': [Path]}}。
    """
    mapping: Dict[str, Dict[str, Any]] = {}

    # 1) 合併檔（<sheet_code>_*.json）
    for p in batch_dir.glob("*.json"):
        name = p.name
        if name in ("manifest.json", "batch_summary.json"):
            continue
        if "_" in name:
            sc = name.split("_", 1)[0]
            mapping.setdefault(sc, {"mode": None, "combined": None, "pages": []})
            mapping[sc]["combined"] = p
            mapping[sc]["mode"] = "combined"

    # 2) 分頁檔（<sheet_code>/page-*.json）
    for sub in batch_dir.iterdir():
        if sub.is_dir():
            sc = sub.name
            pages = sorted(sub.glob("page-*.json"))
            if pages:
                d = mapping.setdefault(sc, {"mode": None, "combined": None, "pages": []})
                d["pages"] = pages
                d["mode"] = d["mode"] or "pages"

    return mapping


def read_records_for_sheet(entry: Dict[str, Any]) -> List[Dict[str, Any]]:
    if entry.get("mode") == "combined" and entry.get("combined"):
        data = read_json(entry["combined"]) or []
        return data if isinstance(data, list) else []

    # pages 模式
    all_data: List[Dict[str, Any]] = []
    for p in entry.get("pages", []):
        try:
            page = read_json(p)
            if isinstance(page, list):
                all_data.extend(page)
        except Exception as e:
            logging.warning(f"讀取 {p.name} 失敗: {e}")
    return all_data


def main() -> None:
    parser = argparse.ArgumentParser(description="將 Ragic JSON 上傳 BigQuery（單一目標表）")
    parser.add_argument("--project-id", type=str, required=True, help="GCP 專案 ID")
    parser.add_argument("--dataset", type=str, required=True, help="BigQuery Dataset ID")
    parser.add_argument("--table", type=str, required=True, help="BigQuery Table ID")
    parser.add_argument("--input-dir", type=str, required=True, help="輸入資料夾（tests/data/<batch>）")
    parser.add_argument("--upload-mode", type=str, choices=["auto", "direct", "staging_sp"], default="auto")
    parser.add_argument("--use-merge", type=str, default="true", help="是否使用 MERGE（true/false）")
    parser.add_argument("--batch-threshold", type=int, default=5000, help="auto 模式分流門檻")
    parser.add_argument("--staging-table", type=str, default=None, help="staging 表名（可省略）")
    parser.add_argument("--merge-sp-name", type=str, default=None, help="預儲程序名稱（可省略）")
    parser.add_argument("--emit-processed-json", action="store_true", help="輸出 processed/ 英文欄位 JSON")
    parser.add_argument("--log-level", type=str, default="INFO", help="日誌層級")
    args = parser.parse_args()

    setup_logging(args.log_level)

    input_dir = Path(args.input_dir)
    if not input_dir.exists() or not input_dir.is_dir():
        logging.error(f"輸入資料夾不存在：{input_dir}")
        sys.exit(1)

    # 建立上傳器
    uploader = BigQueryUploader(project_id=args.project_id)
    if not uploader.test_connection():
        logging.warning("BigQuery 連線測試失敗，請確認認證與網路")

    # 找輸入檔
    sheet_map = find_sheet_files(input_dir)
    if not sheet_map:
        logging.error("找不到任何輸入檔")
        sys.exit(1)

    processed_dir = input_dir / "processed"
    summary: Dict[str, Any] = {
        "input_dir": str(input_dir),
        "dataset": args.dataset,
        "table": args.table,
        "upload_mode": args.upload_mode,
        "results": {},
    }

    # 逐表處理
    for sheet_code, entry in sheet_map.items():
        logging.info(f"處理上傳 sheet_code={sheet_code} mode={entry.get('mode')}")
        raw_records = read_records_for_sheet(entry)
        if not raw_records:
            logging.info(f"sheet_code={sheet_code} 無資料，略過")
            summary["results"][sheet_code] = {"status": "no_data", "records": 0}
            continue

        # 轉換（中文→英文欄位與型別）
        transformer = create_transformer(sheet_code=sheet_code, project_id=args.project_id, use_dynamic_mapping=False)
        if not transformer.validate_data_format(raw_records):
            logging.error(f"sheet_code={sheet_code} 資料格式無效，略過")
            summary["results"][sheet_code] = {"status": "invalid_format"}
            continue

        transformed = transformer.transform_data(raw_records)
        if args.emit_processed_json:
            out_path = processed_dir / f"{sheet_code}.json"
            write_json(out_path, transformed)

        # 上傳
        try:
            result = uploader.upload_data(
                data=transformed,
                dataset_id=args.dataset,
                table_id=args.table,
                schema=transformer.get_schema(),
                use_merge=(str(args.use_merge).lower() == "true"),
                upload_mode=args.upload_mode,
                batch_threshold=int(args.batch_threshold),
                staging_table=args.staging_table,
                merge_sp_name=args.merge_sp_name,
            )
            summary["results"][sheet_code] = {"status": "success", **result}
        except Exception as e:
            logging.error(f"sheet_code={sheet_code} 上傳失敗: {e}")
            summary["results"][sheet_code] = {"status": "error", "error": str(e)}

    write_json(input_dir / "upload_summary.json", summary)
    logging.info("完成所有表單上傳")


if __name__ == "__main__":
    main()


