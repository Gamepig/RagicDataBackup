# -*- coding: utf-8 -*-
"""
Sheet 時間欄位配置模組

根據 2025-10-21 全量資料分析結果，定義每個 Sheet 的時間欄位映射。
參考文件：Manual_fetch_all_Ragic/data/20251021-213544/data_structure_reference.yaml
"""

from typing import List, Dict

# Sheet 時間欄位配置
# 格式：{sheet_code: [優先欄位1, 欄位2, ...]}
# 系統會依序嘗試這些欄位，使用第一個找到且有值的欄位
SHEET_TIME_FIELDS: Dict[str, List[str]] = {
    "10": ["最後修改日期", "建檔日期"],
    "20": ["最後修改日期", "建檔日期"],
    "30": ["最後修改時間"],  # 注意：欄位名稱是「最後修改時間」而非「最後修改日期」
    "40": ["最後修改日期", "建檔日期"],
    "41": ["最後修改日期", "建檔日期"],  # 靜態表，實際值可能為空
    "50": ["最後修改日期", "訂單成立日期", "建立日期"],  # 優先使用「最後修改日期」
    "60": ["最後修改日期", "建檔日期"],
    "70": ["最後修改日期", "建檔日期"],
    "99": ["最後修改日期", "訂單成立日期", "建立日期"],  # 優先使用「最後修改日期」
}

# Sheet ID 對應（從 sheet_map.json）
SHEET_ID_MAP: Dict[str, str] = {
    "10": "forms8/5",
    "20": "forms8/4",
    "30": "forms8/7",
    "40": "forms8/1",
    "41": "forms8/6",
    "50": "forms8/17",
    "60": "forms8/2",
    "70": "forms8/9",
    "99": "forms8/3",
}

# 預設時間欄位（當 sheet_code 不在配置中時使用）
DEFAULT_TIME_FIELDS: List[str] = ["最後修改日期", "最後修改時間", "更新時間", "最後更新時間"]


def get_time_fields_for_sheet(sheet_code: str) -> List[str]:
    """
    獲取指定 Sheet 的時間欄位列表

    Args:
        sheet_code: Sheet 代碼（例如：'10', '20', '99'）

    Returns:
        List[str]: 時間欄位名稱列表（按優先級排序）

    Examples:
        >>> get_time_fields_for_sheet("99")
        ['最後修改日期', '訂單成立日期', '建立日期']

        >>> get_time_fields_for_sheet("30")
        ['最後修改時間']

        >>> get_time_fields_for_sheet("unknown")
        ['最後修改日期', '最後修改時間', '更新時間', '最後更新時間']
    """
    return SHEET_TIME_FIELDS.get(sheet_code, DEFAULT_TIME_FIELDS)


def get_sheet_id(sheet_code: str) -> str:
    """
    獲取 Sheet ID

    Args:
        sheet_code: Sheet 代碼（例如：'10', '20', '99'）

    Returns:
        str: Sheet ID（例如：'forms8/3'）

    Raises:
        ValueError: 當 sheet_code 不存在時

    Examples:
        >>> get_sheet_id("99")
        'forms8/3'

        >>> get_sheet_id("50")
        'forms8/17'
    """
    if sheet_code not in SHEET_ID_MAP:
        raise ValueError(f"未知的 sheet_code: {sheet_code}")
    return SHEET_ID_MAP[sheet_code]


def get_all_sheet_codes() -> List[str]:
    """
    獲取所有已配置的 Sheet 代碼

    Returns:
        List[str]: Sheet 代碼列表

    Examples:
        >>> get_all_sheet_codes()
        ['10', '20', '30', '40', '41', '50', '60', '70', '99']
    """
    return list(SHEET_ID_MAP.keys())


# 特殊 Sheet 標記
STATIC_SHEETS = {"41"}  # 郵遞區號靜態表，無時間欄位或時間欄位為空

def is_static_sheet(sheet_code: str) -> bool:
    """
    判斷是否為靜態表（無需增量更新）

    Args:
        sheet_code: Sheet 代碼

    Returns:
        bool: True 表示靜態表
    """
    return sheet_code in STATIC_SHEETS
