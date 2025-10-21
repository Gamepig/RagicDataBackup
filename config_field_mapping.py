# -*- coding: utf-8 -*-
"""
欄位對照表配置模組
支援三層配置策略：硬編碼 → BigQuery 動態 → 自動轉換
"""

import logging
import hashlib
from typing import Dict, Optional, Tuple
from google.cloud import bigquery
from pypinyin import lazy_pinyin

# ============================================================================
# Layer 1: 硬編碼對照表（基礎保護）
# ============================================================================
# 這是核心對照表，確保即使 BigQuery 連線失敗，系統仍可運作
FIELD_MAPPING_CORE = {
    # 系統欄位（所有表單共通）
    "使用狀態": "status",
    "建檔日期": "created_at",
    "建立日期": "created_at",
    "建檔時間": "created_at",
    "建檔人員": "created_by",
    "建立人員": "created_by",
    "最後修改日期": "last_modified_date",
    "最後修改時間": "last_modified_date",
    "最後修改人員": "last_modified_by",

    # 品牌相關（表單 10, 70, 99）
    "品牌名稱": "brand_name",
    "品牌編號": "brand_id",

    # 通路相關（表單 20, 99）
    "通路名稱": "channel_name",
    "通路編號": "channel_id",
    "電銷模式": "sales_model",
    "收款方": "payment_receiver",
    "通路類型": "channel_type",

    # 金流相關（表單 30, 99）
    "金流名稱": "payment_method_name",
    "金流編號": "payment_method_id",
    "支付方式": "payment_type",

    # 物流相關（表單 40, 99）
    "物流名稱": "logistics_name",
    "物流編號": "logistics_id",
    "運費收入": "shipping_fee_income",
    "物流廠商": "logistics_provider",
    "物流溫層": "temperature_layer",

    # 訂單相關（表單 50, 99）
    "訂單編號": "order_id",
    "訂單成立日期": "order_date",
    "訂單建議售價": "order_msrp",
    "訂單常態售價": "order_regular_price",
    "含運實收": "gross_revenue",
    "訂單實收": "net_revenue",

    # 客戶相關（表單 60, 99）
    "客戶名稱": "customer_name",
    "客戶編號": "customer_id",
    "E-mail": "email",
    "生日": "birthday",
    "統一編號": "tax_id",

    # 商品相關（表單 70, 99）
    "商品名稱": "product_name",
    "商品編號": "product_id",
    "數量": "quantity",
    "課稅別": "tax_type",

    # 收件人相關（表單 99）
    "收件人姓名": "recipient_name",
    "收件人電話": "recipient_phone",
    "郵遞區號": "postal_code",
    "縣市": "city",
    "鄉鎮市區": "district",
    "送貨完整地址": "shipping_address",

    # Ragic 系統鍵（保留原名，利於後續分析）
    "_ragicId": "_ragicId",
    "_star": "_star",
    "_dataTimestamp": "_dataTimestamp",
    "_index_title_": "_index_title_",
    "_index_calDates_": "_index_calDates_",
    "_index_": "_index_",
    "_seq": "_seq",
}

# 99 銷售總表完整欄位（234 個欄位中的 93 個）
FIELD_MAPPING_SALES_SUMMARY = {
    **FIELD_MAPPING_CORE,
    "匯出狀態": "export_status",
    "通路_動態屬性預留欄位_1": "channel_custom_attr_1",
    "通路_動態屬性預留欄位_2": "channel_custom_attr_2",
    "金流_靜態參數預留欄位_1": "payment_static_attr_1",
    "金流_動態屬性預留欄位_1": "payment_dynamic_attr_1",
    "發貨點": "shipping_point",
    "取貨點": "pickup_point",
    "運費支付方式": "shipping_fee_payment_method",
    "物流_動態屬性預留欄位_1": "logistics_dynamic_attr_1",
    "物流_動態屬性預留欄位_2": "logistics_dynamic_attr_2",
    "平台訂單號碼": "platform_order_id",
    "訂單備註": "order_notes",
    "開立發票與否": "is_invoice_issued",
    "發票捐贈": "is_invoice_donated",
    "發票捐贈代碼": "invoice_donation_code",
    "載具類別": "invoice_carrier_type",
    "載具編號": "invoice_carrier_id",
    "代收貨款": "cash_on_delivery_amount",
    "希望到達日": "requested_delivery_date",
    "希望配達時段": "requested_delivery_time",
    "行動電話": "mobile_phone",
    "通訊完整地址": "full_address",
    "市內電話": "phone_number",
    "客戶備註": "customer_notes",
    "客戶_靜態參數預留欄位_1": "customer_static_attr_1",
    "發票抬頭": "invoice_recipient",
    "客戶_靜態參數自定義_1": "customer_static_custom_1",
    "客戶_靜態參數自定義_2": "customer_static_custom_2",
    "買受人身份": "buyer_identity",
    "生日年份": "birth_year",
    "星座": "zodiac_sign",
    "客戶_動態屬性自定義_1": "customer_dynamic_custom_1",
    "客戶_動態屬性自定義_2": "customer_dynamic_custom_2",
    "商品規格_官方標示": "product_spec_official",
    "商品內容": "product_content",
    "商品建議售價": "product_msrp",
    "商品常態售價": "product_regular_price",
    "商品建議售價小計": "product_msrp_subtotal",
    "商品常態售價小計": "product_regular_price_subtotal",
    "商品結構": "product_structure",
    "商品系列": "product_series",
    "商品_動態屬性自定義_1": "product_dynamic_custom_1",
    "商品_動態屬性自定義_2": "product_dynamic_custom_2",
    # 系統更新時間（需保留）
    "「金流更新」執行時間": "payment_update_execution_time",
    # 活動文案欄位（純文字，不應映射為時間戳）
    "通路活動號碼1": "channel_promo_1",
    "通路活動號碼2": "channel_promo_2",
    "通路活動號碼3": "channel_promo_3",
    "通路活動號碼4": "channel_promo_4",
    "通路活動號碼5": "channel_promo_5",
    "物流訂單編號": "logistics_order_id",
    # Ragic 系統索引鍵（99 表常見，保留原名）
    "RAGIC_AUTOGEN_1622007913868": "RAGIC_AUTOGEN_1622007913868",
    "RAGIC_AUTOGEN_1622007913873": "RAGIC_AUTOGEN_1622007913873",
    "_index_": "_index_",
    "_index_calDates_": "_index_calDates_",
    "_index_title_": "_index_title_",
}

# 表單特定對照表
FIELD_MAPPING_BY_SHEET = {
    "10": {**FIELD_MAPPING_CORE,
            "合約起始日期": "contract_start_date",
            "合約終止日期": "contract_end_date",
            "內容說明": "description",
            "寄件人": "sender_name",
            "寄件人電話": "sender_phone",
            "寄件人地址": "sender_address",
    },
    "20": {**FIELD_MAPPING_CORE,
            "合作內容": "cooperation_details",
            "姓名": "contact_name",
            "職稱": "job_title",
            "即時通訊帳號": "im_account",
            # 額外保留欄位
            "電話": "phone_number",
            "手機": "mobile_phone",
            "通路_動態屬性預留欄位_1": "channel_dynamic_attr_1",
            "通路_動態屬性預留欄位_2": "channel_dynamic_attr_2",
    },
    "30": {**FIELD_MAPPING_CORE, "金流類型": "payment_category", "手續費率": "commission_rate",
            # 額外保留欄位
            "金流_靜態參數預留欄位_1": "payment_static_attr_1",
            "金流_動態屬性預留欄位_1": "payment_dynamic_attr_1",
    },
    "40": {**FIELD_MAPPING_CORE,
            "物流類型": "logistics_type",
            "運費": "shipping_fee",
            "物流客代名稱": "logistics_customer_name",
            "物流客代編號": "logistics_customer_id",
            # 額外保留欄位
            "發貨點": "shipping_point",
            "取貨點": "pickup_point",
            "運費支付方式": "shipping_fee_payment_method",
            "物流_動態屬性預留欄位_1": "logistics_dynamic_attr_1",
            "物流_動態屬性預留欄位_2": "logistics_dynamic_attr_2",
    },
    "41": {**FIELD_MAPPING_CORE, "郵遞區號": "postal_code", "縣市": "city", "鄉鎮市區": "district",
            # 額外保留欄位
            "縣市及鄉鎮市區": "city_and_district",
    },
    "50": {**FIELD_MAPPING_CORE, "平台訂單號碼": "platform_order_id", "訂單編號": "order_id"},
    "60": {**FIELD_MAPPING_CORE, "客戶名稱": "customer_name", "客戶編號": "customer_id"},
    "70": {**FIELD_MAPPING_CORE, "商品名稱": "product_name", "商品編號": "product_id",
            # 額外保留欄位（商品屬性與價格）
            "商品規格_官方標示": "product_spec_official",
            "商品內容": "product_content",
            "內容說明": "description",
            "商品建議售價": "product_msrp",
            "商品常態售價": "product_regular_price",
            "商品建議售價小計": "product_msrp_subtotal",
            "商品常態售價小計": "product_regular_price_subtotal",
            "商品結構": "product_structure",
            "商品系列": "product_series",
            "商品_動態屬性自定義_1": "product_dynamic_custom_1",
            "商品_動態屬性自定義_2": "product_dynamic_custom_2",
    },
    "99": FIELD_MAPPING_SALES_SUMMARY,
}


# ============================================================================
# Layer 2: BigQuery 動態對照表載入
# ============================================================================
class DynamicFieldMapper:
    """動態欄位對照表管理器"""

    def __init__(self, project_id: str, use_dynamic: bool = True):
        self.project_id = project_id
        self.use_dynamic = use_dynamic
        self.client = bigquery.Client(project=project_id) if use_dynamic else None
        self._cache = {}  # 快取機制

    def load_dynamic_mappings(self, sheet_code: str) -> Dict[str, str]:
        """
        從 BigQuery 載入動態對照表

        Args:
            sheet_code: 表單代碼（10, 20, 30...）

        Returns:
            Dict[中文欄位, 英文欄位]
        """
        if not self.use_dynamic:
            return {}

        # 檢查快取
        if sheet_code in self._cache:
            return self._cache[sheet_code]

        try:
            query = """
            SELECT chinese_field, english_field
            FROM `grefun-testing.ragic_backup.field_mappings`
            WHERE sheet_code = @sheet_code AND enabled = TRUE
            ORDER BY priority ASC
            """

            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("sheet_code", "STRING", sheet_code)
                ]
            )

            results = self.client.query(query, job_config=job_config).result()
            mappings = {row.chinese_field: row.english_field for row in results}

            # 更新快取
            self._cache[sheet_code] = mappings
            logging.info(f"載入 {len(mappings)} 個動態欄位對照（表單 {sheet_code}）")

            return mappings

        except Exception as e:
            logging.warning(f"無法載入動態對照表（表單 {sheet_code}）: {e}")
            return {}

    def log_unknown_field(self, sheet_code: str, chinese_field: str, temp_english: str, sample_value: str):
        """記錄未知欄位到 BigQuery"""
        if not self.use_dynamic:
            return

        try:
            query = """
            INSERT INTO `grefun-testing.ragic_backup.unknown_fields`
            (sheet_code, chinese_field, temp_english_field, first_seen_at, occurrence_count, sample_value)
            VALUES (@sheet_code, @chinese_field, @temp_english, CURRENT_TIMESTAMP(), 1, @sample_value)
            ON CONFLICT (sheet_code, chinese_field)
            DO UPDATE SET
                occurrence_count = unknown_fields.occurrence_count + 1,
                sample_value = @sample_value
            """

            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("sheet_code", "STRING", sheet_code),
                    bigquery.ScalarQueryParameter("chinese_field", "STRING", chinese_field),
                    bigquery.ScalarQueryParameter("temp_english", "STRING", temp_english),
                    bigquery.ScalarQueryParameter("sample_value", "STRING", str(sample_value)[:500])
                ]
            )

            self.client.query(query, job_config=job_config)
            logging.info(f"記錄未知欄位: {chinese_field} → {temp_english}")

        except Exception as e:
            logging.error(f"記錄未知欄位失敗: {e}")


# ============================================================================
# Layer 3: 自動未知欄位處理
# ============================================================================
def auto_convert_field_name(chinese_field: str, strategy: str = "pinyin") -> str:
    """
    自動轉換未知中文欄位為英文

    Args:
        chinese_field: 中文欄位名稱
        strategy: 轉換策略（pinyin | hash | preserve）

    Returns:
        英文欄位名稱
    """
    if strategy == "pinyin":
        # 策略 A：拼音轉換
        try:
            pinyin_parts = lazy_pinyin(chinese_field)
            english = '_'.join(pinyin_parts).lower()
            # 清理特殊字元
            english = ''.join(c if c.isalnum() or c == '_' else '_' for c in english)
            return f"auto_{english}"
        except Exception as e:
            logging.warning(f"拼音轉換失敗: {e}，回退到 hash 策略")
            strategy = "hash"

    if strategy == "hash":
        # 策略 B：Hash 編碼
        hash_value = hashlib.md5(chinese_field.encode()).hexdigest()[:8]
        return f"unknown_{hash_value}"

    # 策略 C：保留原名（不推薦，可能違反 BigQuery 命名規則）
    return f"raw_{chinese_field}"


def get_field_mapping(sheet_code: str, dynamic_mapper: Optional[DynamicFieldMapper] = None) -> Dict[str, str]:
    """
    獲取完整的欄位對照表（三層合併）

    Args:
        sheet_code: 表單代碼（10, 20, 30...）
        dynamic_mapper: 動態對照表管理器（可選）

    Returns:
        Dict[中文欄位, 英文欄位]
    """
    # Layer 1: 硬編碼基礎對照表
    mappings = FIELD_MAPPING_BY_SHEET.get(sheet_code, FIELD_MAPPING_CORE).copy()

    # Layer 2: BigQuery 動態對照表（覆蓋）
    if dynamic_mapper:
        dynamic_mappings = dynamic_mapper.load_dynamic_mappings(sheet_code)
        mappings.update(dynamic_mappings)

    return mappings


def translate_field(
    chinese_field: str,
    sheet_code: str,
    mappings: Dict[str, str],
    dynamic_mapper: Optional[DynamicFieldMapper] = None,
    sample_value: str = ""
) -> Tuple[str, bool]:
    """
    轉換單一欄位（支援自動處理未知欄位）

    Args:
        chinese_field: 中文欄位名稱
        sheet_code: 表單代碼
        mappings: 欄位對照表
        dynamic_mapper: 動態對照表管理器
        sample_value: 欄位範例值（用於記錄）

    Returns:
        Tuple[英文欄位名稱, 是否為未知欄位]
    """
    # 優先使用對照表
    if chinese_field in mappings:
        return mappings[chinese_field], False

    # Layer 3: 自動轉換未知欄位
    english_field = auto_convert_field_name(chinese_field, strategy="pinyin")

    # 記錄未知欄位
    logging.warning(f"未知欄位: {chinese_field} → {english_field} (表單 {sheet_code})")
    if dynamic_mapper:
        dynamic_mapper.log_unknown_field(sheet_code, chinese_field, english_field, sample_value)

    return english_field, True


# ============================================================================
# 工廠函數
# ============================================================================
def create_field_mapper(project_id: str, use_dynamic: bool = True) -> DynamicFieldMapper:
    """
    建立動態欄位對照表管理器

    Args:
        project_id: GCP 專案 ID
        use_dynamic: 是否啟用 BigQuery 動態對照表

    Returns:
        DynamicFieldMapper 實例
    """
    return DynamicFieldMapper(project_id, use_dynamic)
