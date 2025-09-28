# -*- coding: utf-8 -*-
"""
資料轉換模組
專門處理從 Ragic 中文資料轉換為 BigQuery 英文架構的功能
"""

import logging
from typing import List, Dict, Any, Optional, Set
from google.cloud import bigquery


# 定義欄位映射字典：根據實際的 CSV 樣板對照
# 基於 "99 銷售總表.csv" (中文) 和 "99_sales_summary.csv" (英文) 的對應關係
FIELD_MAPPING = {
    "使用狀態": "status",
    "匯出狀態": "export_status",
    "品牌名稱": "brand_name",
    "品牌編號": "brand_id",
    "通路名稱": "channel_name",
    "通路編號": "channel_id",
    "電銷模式": "sales_model",
    "收款方": "payment_receiver",
    "通路類型": "channel_type",
    "通路_動態屬性預留欄位_1": "channel_custom_attr_1",
    "通路_動態屬性預留欄位_2": "channel_custom_attr_2",
    "金流名稱": "payment_method_name",
    "金流編號": "payment_method_id",
    "金流_靜態參數預留欄位_1": "payment_static_attr_1",
    "支付方式": "payment_type",
    "金流_動態屬性預留欄位_1": "payment_dynamic_attr_1",
    "物流名稱": "logistics_name",
    "物流編號": "logistics_id",
    "運費收入": "shipping_fee_income",
    "物流廠商": "logistics_provider",
    "物流溫層": "temperature_layer",
    "發貨點": "shipping_point",
    "取貨點": "pickup_point",
    "運費支付方式": "shipping_fee_payment_method",
    "物流_動態屬性預留欄位_1": "logistics_dynamic_attr_1",
    "物流_動態屬性預留欄位_2": "logistics_dynamic_attr_2",
    "平台訂單號碼": "platform_order_id",
    "訂單編號": "order_id",
    "訂單成立日期": "order_date",
    "訂單建議售價": "order_msrp",
    "訂單常態售價": "order_regular_price",
    "含運實收": "gross_revenue",
    "訂單實收": "net_revenue",
    "收件人姓名": "recipient_name",
    "收件人電話": "recipient_phone",
    "郵遞區號": "postal_code",
    "縣市": "city",
    "鄉鎮市區": "district",
    "送貨完整地址": "shipping_address",
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
    "客戶名稱": "customer_name",
    "客戶編號": "customer_id",
    "E-mail": "email",
    "生日": "birthday",
    "通訊完整地址": "full_address",
    "市內電話": "phone_number",
    "客戶備註": "customer_notes",
    "客戶_靜態參數預留欄位_1": "customer_static_attr_1",
    "統一編號": "tax_id",
    "發票抬頭": "invoice_recipient",
    "客戶_靜態參數自定義_1": "customer_static_custom_1",
    "客戶_靜態參數自定義_2": "customer_static_custom_2",
    "買受人身份": "buyer_identity",
    "生日年份": "birth_year",
    "星座": "zodiac_sign",
    "客戶_動態屬性自定義_1": "customer_dynamic_custom_1",
    "客戶_動態屬性自定義_2": "customer_dynamic_custom_2",
    "商品名稱": "product_name",
    "商品編號": "product_id",
    "商品規格_官方標示": "product_spec_official",
    "商品內容": "product_content",
    "課稅別": "tax_type",
    "商品建議售價": "product_msrp",
    "商品常態售價": "product_regular_price",
    "數量": "quantity",
    "商品建議售價小計": "product_msrp_subtotal",
    "商品常態售價小計": "product_regular_price_subtotal",
    "商品結構": "product_structure",
    "商品系列": "product_series",
    "商品_動態屬性自定義_1": "product_dynamic_custom_1",
    "商品_動態屬性自定義_2": "product_dynamic_custom_2",
    # 實際CSV中的對應欄位（根據位置順序一一對應）
    "通路活動號碼1": "created_at",
    "通路活動號碼2": "created_by",
    "通路活動號碼3": "updated_at",
    "通路活動號碼4": "updated_by",
    "通路活動號碼5": "sync_sales_report_update_time",
    "物流訂單編號": "sync_sales_report_cancellation_time",
    "建立日期": "sync_order_mgmt_cancellation_time",
    "建立人員": "最後修改日期",
    "最後修改日期": "最後修改人員",
    "最後修改人員": "「金流更新」執行時間",
    "「金流更新」執行時間": "RAGIC_AUTOGEN_1622007913868",
    "RAGIC_AUTOGEN_1622007913868": "RAGIC_AUTOGEN_1622007913873"
}

# 定義 BigQuery 資料表 Schema（基於英文欄位，需確保型別正確）
BIGQUERY_SCHEMA = [
    # 基本狀態欄位
    bigquery.SchemaField("status", "STRING"),
    bigquery.SchemaField("export_status", "STRING"),

    # 品牌相關
    bigquery.SchemaField("brand_name", "STRING"),
    bigquery.SchemaField("brand_id", "STRING"),

    # 通路相關
    bigquery.SchemaField("channel_name", "STRING"),
    bigquery.SchemaField("channel_id", "STRING"),
    bigquery.SchemaField("sales_model", "STRING"),
    bigquery.SchemaField("payment_receiver", "STRING"),
    bigquery.SchemaField("channel_type", "STRING"),
    bigquery.SchemaField("channel_custom_attr_1", "STRING"),
    bigquery.SchemaField("channel_custom_attr_2", "STRING"),

    # 金流相關
    bigquery.SchemaField("payment_method_name", "STRING"),
    bigquery.SchemaField("payment_method_id", "STRING"),
    bigquery.SchemaField("payment_static_attr_1", "STRING"),
    bigquery.SchemaField("payment_type", "STRING"),
    bigquery.SchemaField("payment_dynamic_attr_1", "STRING"),

    # 物流相關
    bigquery.SchemaField("logistics_name", "STRING"),
    bigquery.SchemaField("logistics_id", "STRING"),
    bigquery.SchemaField("shipping_fee_income", "FLOAT"),
    bigquery.SchemaField("logistics_provider", "STRING"),
    bigquery.SchemaField("temperature_layer", "STRING"),
    bigquery.SchemaField("shipping_point", "STRING"),
    bigquery.SchemaField("pickup_point", "STRING"),
    bigquery.SchemaField("shipping_fee_payment_method", "STRING"),
    bigquery.SchemaField("logistics_dynamic_attr_1", "STRING"),
    bigquery.SchemaField("logistics_dynamic_attr_2", "STRING"),

    # 訂單相關
    bigquery.SchemaField("platform_order_id", "STRING"),
    bigquery.SchemaField("order_id", "STRING"),
    bigquery.SchemaField("order_date", "DATE"),
    bigquery.SchemaField("order_msrp", "FLOAT"),
    bigquery.SchemaField("order_regular_price", "FLOAT"),
    bigquery.SchemaField("gross_revenue", "FLOAT"),
    bigquery.SchemaField("net_revenue", "FLOAT"),

    # 收件人資訊
    bigquery.SchemaField("recipient_name", "STRING"),
    bigquery.SchemaField("recipient_phone", "STRING"),
    bigquery.SchemaField("postal_code", "STRING"),
    bigquery.SchemaField("city", "STRING"),
    bigquery.SchemaField("district", "STRING"),
    bigquery.SchemaField("shipping_address", "STRING"),
    bigquery.SchemaField("order_notes", "STRING"),

    # 發票相關
    bigquery.SchemaField("is_invoice_issued", "BOOLEAN"),
    bigquery.SchemaField("is_invoice_donated", "BOOLEAN"),
    bigquery.SchemaField("invoice_donation_code", "STRING"),
    bigquery.SchemaField("invoice_carrier_type", "STRING"),
    bigquery.SchemaField("invoice_carrier_id", "STRING"),

    # 配送相關
    bigquery.SchemaField("cash_on_delivery_amount", "FLOAT"),
    bigquery.SchemaField("requested_delivery_date", "DATE"),
    bigquery.SchemaField("requested_delivery_time", "STRING"),

    # 客戶資訊
    bigquery.SchemaField("mobile_phone", "STRING"),
    bigquery.SchemaField("customer_name", "STRING"),
    bigquery.SchemaField("customer_id", "STRING"),
    bigquery.SchemaField("email", "STRING"),
    bigquery.SchemaField("birthday", "DATE"),
    bigquery.SchemaField("full_address", "STRING"),
    bigquery.SchemaField("phone_number", "STRING"),
    bigquery.SchemaField("customer_notes", "STRING"),
    bigquery.SchemaField("customer_static_attr_1", "STRING"),
    bigquery.SchemaField("tax_id", "STRING"),
    bigquery.SchemaField("invoice_recipient", "STRING"),
    bigquery.SchemaField("customer_static_custom_1", "STRING"),
    bigquery.SchemaField("customer_static_custom_2", "STRING"),
    bigquery.SchemaField("buyer_identity", "STRING"),
    bigquery.SchemaField("birth_year", "INTEGER"),
    bigquery.SchemaField("zodiac_sign", "STRING"),
    bigquery.SchemaField("customer_dynamic_custom_1", "STRING"),
    bigquery.SchemaField("customer_dynamic_custom_2", "STRING"),

    # 商品相關
    bigquery.SchemaField("product_name", "STRING"),
    bigquery.SchemaField("product_id", "STRING"),
    bigquery.SchemaField("product_spec_official", "STRING"),
    bigquery.SchemaField("product_content", "STRING"),
    bigquery.SchemaField("description", "STRING"),  # 英文CSV額外欄位
    bigquery.SchemaField("tax_type", "STRING"),
    bigquery.SchemaField("product_msrp", "FLOAT"),
    bigquery.SchemaField("product_regular_price", "FLOAT"),
    bigquery.SchemaField("quantity", "INTEGER"),
    bigquery.SchemaField("product_msrp_subtotal", "FLOAT"),
    bigquery.SchemaField("product_regular_price_subtotal", "FLOAT"),
    bigquery.SchemaField("product_structure", "STRING"),
    bigquery.SchemaField("product_series", "STRING"),
    bigquery.SchemaField("product_dynamic_custom_1", "STRING"),
    bigquery.SchemaField("product_dynamic_custom_2", "STRING"),

    # 系統欄位
    bigquery.SchemaField("created_at", "TIMESTAMP"),
    bigquery.SchemaField("created_by", "STRING"),
    bigquery.SchemaField("updated_at", "TIMESTAMP"),
    bigquery.SchemaField("updated_by", "STRING"),
    bigquery.SchemaField("sync_sales_report_update_time", "TIMESTAMP"),
    bigquery.SchemaField("sync_sales_report_cancellation_time", "TIMESTAMP"),
    bigquery.SchemaField("sync_order_mgmt_cancellation_time", "TIMESTAMP"),

    # 保留中文欄位名稱（英文CSV中仍為中文）
    bigquery.SchemaField("最後修改日期", "TIMESTAMP"),
    bigquery.SchemaField("最後修改人員", "STRING"),
    bigquery.SchemaField("「金流更新」執行時間", "TIMESTAMP"),
    bigquery.SchemaField("RAGIC_AUTOGEN_1622007913868", "STRING"),
    bigquery.SchemaField("RAGIC_AUTOGEN_1622007913873", "STRING"),
]


class DataTransformer:
    """資料轉換器類別"""

    def __init__(self, field_mapping: Optional[Dict[str, str]] = None):
        """
        初始化資料轉換器

        Args:
            field_mapping: 欄位映射字典，如果不提供則使用預設映射
        """
        self.field_mapping = field_mapping or FIELD_MAPPING
        self.float_fields = {
            "product_msrp", "order_msrp", "order_regular_price", "gross_revenue",
            "net_revenue", "shipping_fee_income", "cash_on_delivery_amount",
            "product_regular_price", "product_msrp_subtotal", "product_regular_price_subtotal"
        }
        self.integer_fields = {"quantity", "birth_year"}
        self.boolean_fields = {"is_invoice_issued", "is_invoice_donated"}

        logging.info("資料轉換器初始化完成")

    def transform_data(self, ragic_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        轉換 Ragic 資料為 BigQuery 格式

        Args:
            ragic_data: 從 Ragic API 獲取的原始資料

        Returns:
            List[Dict]: 轉換後的資料列表

        Raises:
            ValueError: 當輸入資料無效時
        """
        if not ragic_data:
            logging.warning("輸入的 Ragic 資料為空")
            return []

        if not isinstance(ragic_data, list):
            raise ValueError("Ragic 資料必須是列表格式")

        transformed = []
        failed_records = []

        logging.info(f"開始轉換 {len(ragic_data)} 筆資料")

        for i, item in enumerate(ragic_data):
            try:
                transformed_item = self._transform_single_record(item, i)
                if transformed_item:
                    transformed.append(transformed_item)
                else:
                    failed_records.append(i)

            except Exception as e:
                logging.error(f"轉換記錄 {i} 時發生錯誤: {e}")
                failed_records.append(i)
                continue

        if failed_records:
            logging.warning(f"共有 {len(failed_records)} 筆記錄轉換失敗，記錄索引: {failed_records[:10]}...")

        logging.info(f"轉換完成 {len(transformed)} 筆資料，{len(failed_records)} 筆失敗")
        return transformed

    def _transform_single_record(self, item: Dict[str, Any], index: int) -> Optional[Dict[str, Any]]:
        """
        轉換單筆記錄

        Args:
            item: 單筆 Ragic 資料
            index: 記錄索引（用於錯誤報告）

        Returns:
            Optional[Dict]: 轉換後的記錄，如果失敗則返回 None
        """
        if not isinstance(item, dict):
            logging.warning(f"記錄 {index} 不是字典格式")
            return None

        transformed_item = {}

        for chinese_key, value in item.items():
            english_key = self.field_mapping.get(chinese_key, chinese_key)

            # 跳過空值或 None
            if value is None or value == "":
                transformed_item[english_key] = None
                continue

            # 型別轉換
            try:
                converted_value = self._convert_field_type(english_key, value)
                transformed_item[english_key] = converted_value

            except (ValueError, TypeError) as e:
                logging.warning(f"記錄 {index} 的欄位 {english_key} 轉換失敗: {value}, 錯誤: {e}")
                # 設定預設值
                transformed_item[english_key] = self._get_default_value(english_key)

        # 驗證必要欄位
        if not self._validate_required_fields(transformed_item, index):
            return None

        return transformed_item

    def _convert_field_type(self, field_name: str, value: Any) -> Any:
        """
        根據欄位類型轉換值

        Args:
            field_name: 欄位名稱
            value: 原始值

        Returns:
            Any: 轉換後的值
        """
        if field_name in self.float_fields:
            return float(str(value).replace(',', '')) if value else 0.0
        elif field_name in self.integer_fields:
            return int(float(str(value).replace(',', ''))) if value else 0
        elif field_name in self.boolean_fields:
            return str(value).lower() in ['true', '1', 'yes', '是', '開立']
        else:
            return str(value)

    def _get_default_value(self, field_name: str) -> Any:
        """
        獲取欄位的預設值

        Args:
            field_name: 欄位名稱

        Returns:
            Any: 預設值
        """
        if field_name in self.float_fields:
            return 0.0
        elif field_name in self.integer_fields:
            return 0
        elif field_name in self.boolean_fields:
            return False
        else:
            return ""

    def _validate_required_fields(self, record: Dict[str, Any], index: int) -> bool:
        """
        驗證必要欄位

        Args:
            record: 轉換後的記錄
            index: 記錄索引

        Returns:
            bool: 驗證通過返回 True
        """
        # 確保 order_id 不為空（作為主鍵）
        if not record.get('order_id'):
            logging.warning(f"記錄 {index} 缺少 order_id，跳過")
            return False

        return True

    def get_schema(self) -> List[bigquery.SchemaField]:
        """
        獲取 BigQuery Schema

        Returns:
            List[bigquery.SchemaField]: BigQuery 架構定義
        """
        return BIGQUERY_SCHEMA

    def get_field_mapping(self) -> Dict[str, str]:
        """
        獲取欄位映射

        Returns:
            Dict[str, str]: 欄位映射字典
        """
        return self.field_mapping.copy()

    def validate_data_format(self, data: List[Dict[str, Any]]) -> bool:
        """
        驗證資料格式

        Args:
            data: 要驗證的資料

        Returns:
            bool: 格式正確返回 True
        """
        if not isinstance(data, list):
            return False

        if not data:
            return True  # 空列表也是有效的

        # 檢查前幾筆資料的格式
        sample_size = min(5, len(data))
        for i in range(sample_size):
            if not isinstance(data[i], dict):
                logging.error(f"資料索引 {i} 不是字典格式")
                return False

        return True


def create_transformer(**kwargs) -> DataTransformer:
    """
    建立資料轉換器的工廠函數

    Args:
        **kwargs: 轉換器設定參數

    Returns:
        DataTransformer: 資料轉換器實例
    """
    return DataTransformer(**kwargs)