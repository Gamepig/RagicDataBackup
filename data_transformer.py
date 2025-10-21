# -*- coding: utf-8 -*-
"""
資料轉換模組
專門處理從 Ragic 中文資料轉換為 BigQuery 英文架構的功能

更新日期：2025-10-02
新增功能：整合三層配置策略（硬編碼 + BigQuery 動態 + 自動轉換）
"""

import logging
import json
from datetime import datetime
from typing import List, Dict, Any, Optional, Set
from google.cloud import bigquery

# 導入新的配置模組
try:
    from config_field_mapping import (
        DynamicFieldMapper,
        get_field_mapping,
        translate_field,
        FIELD_MAPPING_BY_SHEET,
        FIELD_MAPPING_SALES_SUMMARY
    )
    USE_NEW_CONFIG_SYSTEM = True
except ImportError:
    logging.warning("config_field_mapping 模組未找到，使用舊的硬編碼對照表")
    USE_NEW_CONFIG_SYSTEM = False


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
    # 系統欄位（對照實際 CSV：中文→英文）
    # 修正：通路活動號碼應對應通路活動文案，非系統時間欄位
    "通路活動號碼1": "channel_promo_1",
    "通路活動號碼2": "channel_promo_2",
    "通路活動號碼3": "channel_promo_3",
    "通路活動號碼4": "channel_promo_4",
    "通路活動號碼5": "channel_promo_5",
    # 修正：物流訂單編號對應 logistics_order_id（原誤設為取消時間）
    "物流訂單編號": "logistics_order_id",
    # 修正：建立日期/建立人員 對應 created_at/created_by
    "建立日期": "created_at",
    "建立人員": "created_by",
    # 以下三個欄位在英文 CSV 中原為中文，需轉換為英文
    "最後修改日期": "last_modified_date",
    "最後修改人員": "last_modified_by",
    "「金流更新」執行時間": "payment_update_execution_time",
    # Ragic 自動生成欄位
    "RAGIC_AUTOGEN_1622007913868": "RAGIC_AUTOGEN_1622007913868",
    "RAGIC_AUTOGEN_1622007913873": "RAGIC_AUTOGEN_1622007913873"
}

# 定義 BigQuery 資料表 Schema（基於英文欄位，需確保型別正確）
BIGQUERY_SCHEMA = [
    # 來源表代碼（新增）
    bigquery.SchemaField("sheet_code", "STRING"),
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
    bigquery.SchemaField("logistics_customer_name", "STRING"),
    bigquery.SchemaField("logistics_customer_id", "STRING"),

    # 訂單相關
    bigquery.SchemaField("platform_order_id", "STRING"),
    bigquery.SchemaField("logistics_order_id", "STRING"),
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
    bigquery.SchemaField("requested_delivery_date_raw", "STRING"),
    bigquery.SchemaField("requested_delivery_time", "STRING"),

    # 客戶資訊
    bigquery.SchemaField("mobile_phone", "STRING"),
    bigquery.SchemaField("customer_name", "STRING"),
    bigquery.SchemaField("customer_id", "STRING"),
    bigquery.SchemaField("email", "STRING"),
    bigquery.SchemaField("birthday", "DATE"),
    bigquery.SchemaField("birthday_raw", "STRING"),
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
    bigquery.SchemaField("sender_name", "STRING"),
    bigquery.SchemaField("sender_phone", "STRING"),
    bigquery.SchemaField("sender_address", "STRING"),

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

    # 通路活動文案（純文字）
    bigquery.SchemaField("channel_promo_1", "STRING"),
    bigquery.SchemaField("channel_promo_2", "STRING"),
    bigquery.SchemaField("channel_promo_3", "STRING"),
    bigquery.SchemaField("channel_promo_4", "STRING"),
    bigquery.SchemaField("channel_promo_5", "STRING"),

    # 系統欄位
    bigquery.SchemaField("created_at", "TIMESTAMP"),
    bigquery.SchemaField("created_at_raw", "STRING"),
    bigquery.SchemaField("created_by", "STRING"),
    bigquery.SchemaField("updated_at", "TIMESTAMP"),
    bigquery.SchemaField("updated_at_raw", "STRING"),
    bigquery.SchemaField("updated_by", "STRING"),
    bigquery.SchemaField("sync_sales_report_update_time", "TIMESTAMP"),
    bigquery.SchemaField("sync_sales_report_update_time_raw", "STRING"),
    bigquery.SchemaField("sync_sales_report_cancellation_time", "TIMESTAMP"),
    bigquery.SchemaField("sync_sales_report_cancellation_time_raw", "STRING"),
    bigquery.SchemaField("sync_order_mgmt_cancellation_time", "TIMESTAMP"),
    bigquery.SchemaField("sync_order_mgmt_cancellation_time_raw", "STRING"),

    # 最後修改欄位（原英文 CSV 中為中文，已轉換為英文）
    bigquery.SchemaField("last_modified_date", "TIMESTAMP"),
    bigquery.SchemaField("last_modified_date_raw", "STRING"),
    bigquery.SchemaField("last_modified_by", "STRING"),
    bigquery.SchemaField("payment_update_execution_time", "TIMESTAMP"),
    bigquery.SchemaField("payment_update_execution_time_raw", "STRING"),

    # Ragic 自動生成欄位
    bigquery.SchemaField("RAGIC_AUTOGEN_1622007913868", "STRING"),
    bigquery.SchemaField("RAGIC_AUTOGEN_1622007913873", "STRING"),
]


class DataTransformer:
    """
    資料轉換器類別

    支援三層配置策略：
    1. Layer 1: Python 硬編碼對照表（基礎保護）
    2. Layer 2: BigQuery 動態對照表（可選啟用）
    3. Layer 3: 自動未知欄位處理（拼音轉換）
    """

    def __init__(
        self,
        field_mapping: Optional[Dict[str, str]] = None,
        sheet_code: str = "99",
        project_id: Optional[str] = None,
        use_dynamic_mapping: bool = False,
        drop_unmapped: bool = False,
        log_per_record_failures: bool = False
    ):
        """
        初始化資料轉換器

        Args:
            field_mapping: 欄位映射字典，如果不提供則使用預設映射
            sheet_code: 表單代碼（10, 20, 30...），用於載入對應的欄位映射
            project_id: GCP 專案 ID（啟用動態對照表時需要）
            use_dynamic_mapping: 是否啟用 BigQuery 動態對照表（Layer 2）
        """
        self.sheet_code = sheet_code
        self.use_dynamic_mapping = use_dynamic_mapping and USE_NEW_CONFIG_SYSTEM
        self.unknown_fields_count = 0  # 記錄未知欄位數量
        self.unknown_field_counts: Dict[str, int] = {}
        self.invalid_records: List[Dict[str, Any]] = []  # 紀錄無效資料（索引與錯誤細節）
        self.failure_counts: Dict[str, int] = {}  # 各欄位轉換失敗計數
        self.drop_unmapped = drop_unmapped
        self.log_per_record_failures = log_per_record_failures

        # 初始化動態對照管理器（Layer 2）
        self.dynamic_mapper = None
        if self.use_dynamic_mapping and project_id:
            try:
                self.dynamic_mapper = DynamicFieldMapper(project_id, use_dynamic=True)
                logging.info(f"已啟用 BigQuery 動態欄位對照（專案：{project_id}）")
            except Exception as e:
                logging.warning(f"無法初始化動態對照管理器: {e}，回退到硬編碼對照表")
                self.dynamic_mapper = None

        # 載入欄位映射（Layer 1 + Layer 2 合併）
        if field_mapping:
            # 使用者自訂對照表
            self.field_mapping = field_mapping
        elif USE_NEW_CONFIG_SYSTEM:
            # 使用新的配置系統
            self.field_mapping = get_field_mapping(sheet_code, self.dynamic_mapper)
            logging.info(f"使用新配置系統載入表單 {sheet_code} 的欄位對照（共 {len(self.field_mapping)} 個欄位）")
        else:
            # 使用舊的硬編碼對照表
            self.field_mapping = FIELD_MAPPING
            logging.info(f"使用舊版硬編碼對照表（共 {len(self.field_mapping)} 個欄位）")

        # 型別欄位集合
        self.float_fields = {
            "product_msrp", "order_msrp", "order_regular_price", "gross_revenue",
            "net_revenue", "shipping_fee_income", "cash_on_delivery_amount",
            "product_regular_price", "product_msrp_subtotal", "product_regular_price_subtotal",
            "shipping_fee",
        }
        self.integer_fields = {"quantity", "birth_year"}
        self.boolean_fields = {"is_invoice_issued", "is_invoice_donated"}
        self.date_fields = {"order_date", "requested_delivery_date", "birthday"}
        self.timestamp_fields = {
            "created_at", "updated_at", "sync_sales_report_update_time",
            "sync_sales_report_cancellation_time", "sync_order_mgmt_cancellation_time",
            "last_modified_date", "payment_update_execution_time"
        }
        # 需保留原始字串的欄位對
        self.raw_pairs = {
            "requested_delivery_date": "requested_delivery_date_raw",
            "birthday": "birthday_raw",
            "created_at": "created_at_raw",
            "updated_at": "updated_at_raw",
            "sync_sales_report_update_time": "sync_sales_report_update_time_raw",
            "sync_sales_report_cancellation_time": "sync_sales_report_cancellation_time_raw",
            "sync_order_mgmt_cancellation_time": "sync_order_mgmt_cancellation_time_raw",
            "last_modified_date": "last_modified_date_raw",
            "payment_update_execution_time": "payment_update_execution_time_raw",
        }

        # 不允許為空的欄位（依需求：僅檢查 _ragicId）
        self.non_nullable_fields: Set[str] = {"_ragicId"}

        logging.info(f"資料轉換器初始化完成（表單 {sheet_code}，動態對照: {self.use_dynamic_mapping}）")

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
            logging.warning("[transform_data] 輸入的 Ragic 資料為空")
            return []

        if not isinstance(ragic_data, list):
            raise ValueError("Ragic 資料必須是列表格式")

        transformed = []
        failed_records = []

        logging.info(f"[transform_data] 開始轉換 {len(ragic_data)} 筆資料")

        # 記錄第一筆資料的樣本
        if len(ragic_data) > 0:
            sample = ragic_data[0]
            sample_keys = list(sample.keys())[:10]  # 只顯示前 10 個欄位
            logging.info(f"[transform_data] 第一筆資料欄位範例: {sample_keys}")
            # 檢查是否有日期欄位
            date_fields = ['最後修改日期', '最後修改時間', '更新時間', '最後更新時間']
            found_dates = [f for f in date_fields if f in sample]
            if found_dates:
                logging.info(f"[transform_data] 第一筆資料包含日期欄位: {found_dates}")
                for f in found_dates:
                    logging.info(f"[transform_data] {f} = {sample.get(f)}")

        for i, item in enumerate(ragic_data):
            try:
                transformed_item = self._transform_single_record(item, i)
                if transformed_item:
                    transformed.append(transformed_item)
                else:
                    failed_records.append(i)

            except Exception as e:
                logging.error(f"[transform_data] 轉換記錄 {i} 時發生錯誤: {e}")
                failed_records.append(i)
                continue

        if failed_records:
            logging.warning(f"[transform_data] 共有 {len(failed_records)} 筆記錄轉換失敗，記錄索引: {failed_records[:10]}...")

        logging.info(f"[transform_data] 轉換完成 {len(transformed)} 筆資料，{len(failed_records)} 筆失敗")
        return transformed

    def _transform_single_record(self, item: Dict[str, Any], index: int) -> Optional[Dict[str, Any]]:
        """
        轉換單筆記錄

        支援三層配置策略：
        1. Layer 1/2: 優先使用對照表（硬編碼或 BigQuery 動態）
        2. Layer 3: 對照表找不到時自動轉換未知欄位

        Args:
            item: 單筆 Ragic 資料
            index: 記錄索引（用於錯誤報告）

        Returns:
            Optional[Dict]: 轉換後的記錄，如果失敗則返回 None
        """
        if not isinstance(item, dict):
            logging.warning(f"記錄 {index} 不是字典格式")
            return None

        transformed_item: Dict[str, Any] = {}
        errors: List[str] = []
        invalid = False

        for chinese_key, value in item.items():
            # Layer 1/2: 查詢對照表
            if chinese_key in self.field_mapping:
                english_key = self.field_mapping[chinese_key]
                is_unknown = False
            else:
                # Layer 3: 自動處理未知欄位
                if USE_NEW_CONFIG_SYSTEM and self.dynamic_mapper:
                    english_key, is_unknown = translate_field(
                        chinese_field=chinese_key,
                        sheet_code=self.sheet_code,
                        mappings=self.field_mapping,
                        dynamic_mapper=self.dynamic_mapper,
                        sample_value=str(value)[:500]  # 限制長度
                    )
                    if is_unknown:
                        self.unknown_fields_count += 1
                        self.unknown_field_counts[chinese_key] = self.unknown_field_counts.get(chinese_key, 0) + 1
                        if self.drop_unmapped:
                            # 嚴格模式：丟棄未對應欄位（不輸出），僅統計
                            continue
                else:
                    # 回退方案
                    is_unknown = True
                    self.unknown_fields_count += 1
                    self.unknown_field_counts[chinese_key] = self.unknown_field_counts.get(chinese_key, 0) + 1
                    if self.drop_unmapped:
                        # 嚴格模式：丟棄未對應欄位（不輸出），僅統計
                        continue
                    # 非嚴格模式才保留原名輸出
                    english_key = chinese_key

            # 空值檢查：指定欄位不可為空
            if value is None or (isinstance(value, str) and value.strip() == ""):
                if english_key in self.non_nullable_fields:
                    errors.append(f"欄位 {english_key} 不可為空")
                    invalid = True
                # 仍記錄為 None 以利觀察
                transformed_item[english_key] = None
                continue

            # 型別轉換
            try:
                converted_value = self._convert_field_type(english_key, value, context=transformed_item)
                transformed_item[english_key] = converted_value
                # 保留原始值（僅在轉換失敗或屬於需保留清單時記錄）
                if english_key in self.raw_pairs:
                    # 僅在原始字串非空時保留
                    if isinstance(value, str) and value.strip() != "":
                        transformed_item[self.raw_pairs[english_key]] = str(value)

            except (ValueError, TypeError) as e:
                if self.log_per_record_failures:
                    logging.warning(f"記錄 {index} 的欄位 {english_key} 轉換失敗: {value}, 錯誤: {e}")
                errors.append(f"欄位 {english_key} 型別不符: {value}")
                # 清理策略：不視為無效列，改填預設值/None
                transformed_item[english_key] = self._get_default_value(english_key)
                # 轉換失敗則保留原始字串
                if english_key in self.raw_pairs:
                    transformed_item[self.raw_pairs[english_key]] = str(value)
                # 統計欄位失敗計數
                self.failure_counts[english_key] = self.failure_counts.get(english_key, 0) + 1

        # 驗證必要欄位（僅檢查 _ragicId）
        if not self._validate_required_fields(transformed_item, index):
            errors.append("缺少必要欄位：_ragicId")
            invalid = True

        if invalid:
            self.invalid_records.append({
                "index": index,
                "errors": errors,
                "raw": item
            })
            return None

        # 注入來源表代碼（用於 BigQuery 分表統計）
        transformed_item["sheet_code"] = self.sheet_code
        return transformed_item

    def _convert_field_type(self, field_name: str, value: Any, context: Optional[Dict[str, Any]] = None) -> Any:
        """
        根據欄位類型轉換值

        Args:
            field_name: 欄位名稱
            value: 原始值

        Returns:
            Any: 轉換後的值
        """
        # 檢查空值
        if not value or (isinstance(value, str) and value.strip() == ""):
            return self._get_default_value(field_name)

        # 清理數值字串（移除千分位逗號）
        cleaned_value = str(value).replace(',', '').strip()

        # 根據欄位類型進行轉換
        if field_name in self.float_fields:
            return float(cleaned_value)
        elif field_name in self.integer_fields:
            return int(float(cleaned_value))
        elif field_name in self.boolean_fields:
            return cleaned_value.lower() in ['true', '1', 'yes', '是', '開立']
        elif field_name in self.date_fields:
            # 特例：requested_delivery_date 支援月/日推斷年份
            if field_name == "requested_delivery_date":
                return self._normalize_date(cleaned_value, context=context, infer_year_from="order_date")
            return self._normalize_date(cleaned_value)
        elif field_name in self.timestamp_fields:
            return self._normalize_timestamp(cleaned_value)
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
        # 日期/時間戳欄位回傳 None，避免型別不符
        if field_name in self.date_fields or field_name in self.timestamp_fields:
            return None
        if field_name in self.float_fields:
            return 0.0
        elif field_name in self.integer_fields:
            return 0
        elif field_name in self.boolean_fields:
            return False
        else:
            return ""

    def _normalize_date(self, value: str, context: Optional[Dict[str, Any]] = None, infer_year_from: Optional[str] = None) -> str:
        """
        將常見日期格式正規化為 YYYY-MM-DD
        """
        if not isinstance(value, str):
            raise ValueError("日期欄位必須是字串")

        candidates = [
            "%Y-%m-%d",
            "%Y/%m/%d",
            "%Y%m%d",
            "%Y-%m-%d %H:%M:%S",
            "%Y/%m/%d %H:%M:%S",
            "%Y-%m-%d %H:%M",
            "%Y/%m/%d %H:%M",
        ]
        # 常見無效字串直接視為不可解析
        if value.strip() in {"不指定", "N/A", "NA", "-", "—", "null", "None", "ADDLINE"}:
            raise ValueError(f"無法解析日期: {value}")

        for fmt in candidates:
            try:
                dt = datetime.strptime(value.strip(), fmt)
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                continue
        # 支援月/日（省略年份）→ 以 context 的 infer_year_from 推斷年份
        v = value.strip()
        if infer_year_from and context is not None:
            # 簡單偵測 M/D 或 MM/DD
            parts = v.split('/') if '/' in v else v.split('-')
            if len(parts) == 2 and all(p.isdigit() for p in parts):
                m, d = parts
                try:
                    ref = context.get(infer_year_from)
                    if isinstance(ref, str) and ref:
                        # ref 可能是 YYYY-MM-DD 或 YYYY-MM-DDTHH:MM:SS
                        ref_year = ref[:4]
                        test = f"{ref_year}-{int(m):02d}-{int(d):02d}"
                        dt2 = datetime.strptime(test, "%Y-%m-%d")
                        return dt2.strftime("%Y-%m-%d")
                except Exception:
                    pass
        raise ValueError(f"無法解析日期: {value}")

    def _normalize_timestamp(self, value: str) -> str:
        """
        將常見日期時間格式正規化為 ISO 8601（YYYY-MM-DDTHH:MM:SS）
        （不附時區，BigQuery 視為 UTC）
        """
        if not isinstance(value, str):
            raise ValueError("時間戳欄位必須是字串")

        v = value.strip()
        if v in {"不指定", "N/A", "NA", "-", "—", "null", "None", "ADDLINE"}:
            raise ValueError(f"無法解析時間戳: {value}")
        candidates = [
            "%Y-%m-%d %H:%M:%S",
            "%Y/%m/%d %H:%M:%S",
            "%Y-%m-%d %H:%M",
            "%Y/%m/%d %H:%M",
            "%Y-%m-%d",
            "%Y/%m/%d",
        ]
        for fmt in candidates:
            try:
                dt = datetime.strptime(v, fmt)
                # 若僅日期提供則補 00:00:00
                return dt.strftime("%Y-%m-%dT%H:%M:%S")
            except ValueError:
                continue
        # 支援 14 碼數字時間戳（YYYYMMDDHHMMSS）
        if v.isdigit() and len(v) == 14:
            try:
                dt = datetime.strptime(v, "%Y%m%d%H%M%S")
                return dt.strftime("%Y-%m-%dT%H:%M:%S")
            except ValueError:
                pass
        # 常見的 ISO 格式直接返回
        try:
            # 若已是 ISO 8601，嘗試解析
            dt = datetime.fromisoformat(v.replace("Z", ""))
            return dt.strftime("%Y-%m-%dT%H:%M:%S")
        except ValueError:
            pass
        raise ValueError(f"無法解析時間戳: {value}")

    def _validate_required_fields(self, record: Dict[str, Any], index: int) -> bool:
        """
        驗證必要欄位

        Args:
            record: 轉換後的記錄
            index: 記錄索引

        Returns:
            bool: 驗證通過返回 True
        """
        # 僅檢查 _ragicId 不可為空
        v = record.get("_ragicId")
        if v is None or (isinstance(v, str) and v.strip() == ""):
            # 僅統計，不逐筆噴訊息
            self.failure_counts["_ragicId_missing"] = self.failure_counts.get("_ragicId_missing", 0) + 1
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

    def get_unknown_field_counts(self) -> Dict[str, int]:
        """
        取得未對應（未知）欄位統計
        """
        return dict(self.unknown_field_counts)

    def get_failure_counts(self) -> Dict[str, int]:
        """
        取得各欄位轉換失敗次數統計
        """
        return dict(self.failure_counts)

    def get_invalid_records(self) -> List[Dict[str, Any]]:
        """
        取得最新一次 transform 的無效資料清單（索引、錯誤、原始資料）
        """
        return list(self.invalid_records)

    def save_invalid_records(self, output_path: str) -> None:
        """
        將無效資料輸出為 JSON Lines 以利後續檢視與修正

        Args:
            output_path: 檔案路徑（.jsonl）
        """
        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                for rec in self.invalid_records:
                    f.write(json.dumps(rec, ensure_ascii=False) + "\n")
            logging.info(f"已輸出無效資料 {len(self.invalid_records)} 筆到 {output_path}")
        except Exception as e:
            logging.warning(f"輸出無效資料失敗: {e}")

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


def create_transformer(
    sheet_code: str = "99",
    project_id: Optional[str] = None,
    use_dynamic_mapping: bool = False,
    **kwargs
) -> DataTransformer:
    """
    建立資料轉換器的工廠函數

    Args:
        sheet_code: 表單代碼（10, 20, 30...），預設 99（銷售總表）
        project_id: GCP 專案 ID（啟用動態對照表時需要）
        use_dynamic_mapping: 是否啟用 BigQuery 動態對照表（Layer 2）
        **kwargs: 其他轉換器設定參數

    Returns:
        DataTransformer: 資料轉換器實例

    Examples:
        >>> # 使用硬編碼對照表（舊版相容）
        >>> transformer = create_transformer()

        >>> # 啟用動態對照表（推薦）
        >>> transformer = create_transformer(
        ...     sheet_code="99",
        ...     project_id="grefun-testing",
        ...     use_dynamic_mapping=True
        ... )

        >>> # 特定表單
        >>> transformer = create_transformer(sheet_code="50", project_id="grefun-testing")
    """
    return DataTransformer(
        sheet_code=sheet_code,
        project_id=project_id,
        use_dynamic_mapping=use_dynamic_mapping,
        **kwargs
    )