-- ============================================================================
-- RagicDataBackup BigQuery 配置表建立腳本
-- ============================================================================
-- 用途：建立動態配置表，支援無需重新部署即可修改配置
-- 執行環境：Google Cloud Console > BigQuery
-- 執行者：專案管理員
-- 最後更新：2025-10-02
-- ============================================================================

-- ============================================================================
-- 1. 建立 Dataset（如果不存在）
-- ============================================================================
-- 注意：請替換 'grefun-testing' 為你的實際專案 ID

CREATE SCHEMA IF NOT EXISTS `grefun-testing.ragic_backup`
OPTIONS (
  location = 'asia-east1',  -- 台灣區域
  description = 'Ragic 資料備份系統配置與資料儲存'
);

-- ============================================================================
-- 2. 建立 Ragic API 配置表（backup_config）
-- ============================================================================
-- 用途：儲存 9 個表單的 Ragic API 配置資訊
-- 更新方式：直接 UPDATE SQL，無需重新部署 Cloud Function

CREATE TABLE IF NOT EXISTS `grefun-testing.ragic_backup.backup_config` (
  client_id STRING NOT NULL COMMENT '客戶識別碼（例：grefun）',
  ragic_api_key STRING NOT NULL COMMENT 'Ragic API Key（Base64 編碼）',
  ragic_account STRING NOT NULL COMMENT 'Ragic 帳戶名稱（例：grefun）',
  sheet_code STRING NOT NULL COMMENT '表單代碼（10, 20, 30...）',
  sheet_id STRING NOT NULL COMMENT 'Sheet ID（例：forms8/5）',
  sheet_name STRING COMMENT '表單名稱（例：表單5、品牌管理）',
  enabled BOOLEAN DEFAULT TRUE COMMENT '是否啟用備份',
  backup_priority INT64 DEFAULT 50 COMMENT '備份優先級（1-100，數字越小越優先）',
  notes STRING COMMENT '備註說明',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP() COMMENT '建立時間',
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP() COMMENT '最後更新時間'
)
PARTITION BY DATE(created_at)
CLUSTER BY client_id, sheet_code;

-- 建立唯一索引
CREATE UNIQUE INDEX IF NOT EXISTS idx_backup_config_unique
ON `grefun-testing.ragic_backup.backup_config` (client_id, sheet_code);

-- ============================================================================
-- 3. 建立欄位對照表（field_mappings）
-- ============================================================================
-- 用途：動態管理中英文欄位對照，支援 Layer 2 動態配置
-- 優先級：BigQuery 動態對照 > Python 硬編碼對照

CREATE TABLE IF NOT EXISTS `grefun-testing.ragic_backup.field_mappings` (
  sheet_code STRING NOT NULL COMMENT '表單代碼（10, 20, 30, ... 或 * 表示全域）',
  chinese_field STRING NOT NULL COMMENT '中文欄位名稱',
  english_field STRING NOT NULL COMMENT '英文欄位名稱（snake_case）',
  data_type STRING COMMENT '資料型別（STRING, INTEGER, FLOAT, BOOLEAN, DATE, TIMESTAMP）',
  is_required BOOLEAN DEFAULT FALSE COMMENT '是否必填欄位',
  priority INT64 DEFAULT 100 COMMENT '優先級（數字越小越優先，用於處理重複定義）',
  enabled BOOLEAN DEFAULT TRUE COMMENT '是否啟用',
  notes STRING COMMENT '備註說明',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP() COMMENT '建立時間',
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP() COMMENT '最後更新時間'
)
PARTITION BY DATE(created_at)
CLUSTER BY sheet_code, chinese_field;

-- 建立查詢索引
CREATE INDEX IF NOT EXISTS idx_field_mappings_lookup
ON `grefun-testing.ragic_backup.field_mappings` (sheet_code, chinese_field, enabled);

-- ============================================================================
-- 4. 建立未知欄位記錄表（unknown_fields）
-- ============================================================================
-- 用途：自動記錄系統偵測到的未知欄位，供管理員審查

CREATE TABLE IF NOT EXISTS `grefun-testing.ragic_backup.unknown_fields` (
  sheet_code STRING NOT NULL COMMENT '表單代碼',
  chinese_field STRING NOT NULL COMMENT '中文欄位名稱',
  temp_english_field STRING NOT NULL COMMENT '臨時英文欄位名稱（自動產生）',
  first_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP() COMMENT '首次發現時間',
  last_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP() COMMENT '最後發現時間',
  occurrence_count INT64 DEFAULT 1 COMMENT '出現次數',
  sample_value STRING COMMENT '範例值（最多 500 字元）',
  status STRING DEFAULT 'pending' COMMENT '處理狀態（pending, reviewed, mapped, ignored）',
  mapped_to STRING COMMENT '已映射到的英文欄位（人工處理後）',
  notes STRING COMMENT '備註說明'
)
PARTITION BY DATE(first_seen_at)
CLUSTER BY sheet_code, status;

-- 建立唯一約束（使用 MERGE 更新）
-- BigQuery 不支援 UNIQUE 約束，需在應用層處理（MERGE ON sheet_code + chinese_field）

-- ============================================================================
-- 5. 插入初始資料：9 個表單的 Ragic API 配置
-- ============================================================================
-- 注意：請確認 API Key 和 Account 是否正確

INSERT INTO `grefun-testing.ragic_backup.backup_config`
(client_id, ragic_api_key, ragic_account, sheet_code, sheet_id, sheet_name, enabled, backup_priority, notes)
VALUES
  -- 小表（優先級較低）
  ('grefun', 'cmtrUVI5WkZxZXgvL283bGZneU9wMGxmcDV6LzFObHdVeWlpQXJxMncxVG5uOWFFVHU1K09zU2c1UXg3UUJLKw==', 'grefun', '10', 'forms8/5', '品牌管理', TRUE, 50, '7 筆資料'),
  ('grefun', 'cmtrUVI5WkZxZXgvL283bGZneU9wMGxmcDV6LzFObHdVeWlpQXJxMncxVG5uOWFFVHU1K09zU2c1UXg3UUJLKw==', 'grefun', '20', 'forms8/4', '通路管理', TRUE, 50, '393 筆資料'),
  ('grefun', 'cmtrUVI5WkZxZXgvL283bGZneU9wMGxmcDV6LzFObHdVeWlpQXJxMncxVG5uOWFFVHU1K09zU2c1UXg3UUJLKw==', 'grefun', '30', 'forms8/7', '金流管理', TRUE, 50, '8 筆資料'),
  ('grefun', 'cmtrUVI5WkZxZXgvL283bGZneU9wMGxmcDV6LzFObHdVeWlpQXJxMncxVG5uOWFFVHU1K09zU2c1UXg3UUJLKw==', 'grefun', '40', 'forms8/1', '物流管理', TRUE, 50, '28 筆資料'),
  ('grefun', 'cmtrUVI5WkZxZXgvL283bGZneU9wMGxmcDV6LzFObHdVeWlpQXJxMncxVG5uOWFFVHU1K09zU2c1UXg3UUJLKw==', 'grefun', '41', 'forms8/6', '縣市郵遞區號', TRUE, 50, '369 筆資料'),

  -- 中表（優先級中等）
  ('grefun', 'cmtrUVI5WkZxZXgvL283bGZneU9wMGxmcDV6LzFObHdVeWlpQXJxMncxVG5uOWFFVHU1K09zU2c1UXg3UUJLKw==', 'grefun', '70', 'forms8/9', '商品管理', TRUE, 30, '1,192 筆資料'),

  -- 大表（優先級最高）
  ('grefun', 'cmtrUVI5WkZxZXgvL283bGZneU9wMGxmcDV6LzFObHdVeWlpQXJxMncxVG5uOWFFVHU1K09zU2c1UXg3UUJLKw==', 'grefun', '50', 'forms8/17', '訂單管理', TRUE, 10, '84,498 筆資料（大表）'),
  ('grefun', 'cmtrUVI5WkZxZXgvL283bGZneU9wMGxmcDV6LzFObHdVeWlpQXJxMncxVG5uOWFFVHU1K09zU2c1UXg3UUJLKw==', 'grefun', '60', 'forms8/2', '客戶管理', TRUE, 10, '59,044 筆資料（大表）'),
  ('grefun', 'cmtrUVI5WkZxZXgvL283bGZneU9wMGxmcDV6LzFObHdVeWlpQXJxMncxVG5uOWFFVHU1K09zU2c1UXg3UUJLKw==', 'grefun', '99', 'forms8/3', '銷售總表', TRUE, 5, '295,700 筆資料（最大表）');

-- ============================================================================
-- 6. 插入範例欄位對照規則（可選）
-- ============================================================================
-- 用於示範如何新增動態欄位對照規則
-- 注意：這些規則會覆蓋 Python 硬編碼的 FIELD_MAPPING

-- 全域共通欄位（適用於所有表單）
INSERT INTO `grefun-testing.ragic_backup.field_mappings`
(sheet_code, chinese_field, english_field, data_type, is_required, priority, enabled, notes)
VALUES
  ('*', '使用狀態', 'status', 'STRING', TRUE, 1, TRUE, '全域系統欄位'),
  ('*', '建檔日期', 'created_at', 'TIMESTAMP', TRUE, 1, TRUE, '全域系統欄位'),
  ('*', '建立日期', 'created_at', 'TIMESTAMP', TRUE, 1, TRUE, '全域系統欄位（別名）'),
  ('*', '建檔人員', 'created_by', 'STRING', TRUE, 1, TRUE, '全域系統欄位'),
  ('*', '最後修改日期', 'updated_at', 'TIMESTAMP', TRUE, 1, TRUE, '全域系統欄位'),
  ('*', '最後修改人員', 'updated_by', 'STRING', TRUE, 1, TRUE, '全域系統欄位');

-- 表單 99（銷售總表）特定欄位範例
INSERT INTO `grefun-testing.ragic_backup.field_mappings`
(sheet_code, chinese_field, english_field, data_type, is_required, priority, enabled, notes)
VALUES
  ('99', '訂單編號', 'order_id', 'STRING', TRUE, 10, TRUE, '主鍵欄位'),
  ('99', '訂單實收', 'net_revenue', 'FLOAT', FALSE, 10, TRUE, '金額欄位'),
  ('99', '客戶名稱', 'customer_name', 'STRING', FALSE, 10, TRUE, '客戶資訊');

-- ============================================================================
-- 7. 驗證與查詢
-- ============================================================================

-- 7.1 查詢所有表單配置
SELECT
  client_id,
  sheet_code,
  sheet_name,
  sheet_id,
  enabled,
  backup_priority,
  notes
FROM `grefun-testing.ragic_backup.backup_config`
ORDER BY backup_priority ASC, sheet_code ASC;

-- 7.2 查詢特定表單的欄位對照
SELECT
  sheet_code,
  chinese_field,
  english_field,
  data_type,
  is_required,
  priority,
  enabled
FROM `grefun-testing.ragic_backup.field_mappings`
WHERE sheet_code IN ('99', '*')  -- 查詢表單 99 和全域規則
  AND enabled = TRUE
ORDER BY priority ASC, chinese_field ASC;

-- 7.3 查詢未知欄位（需等系統執行後）
SELECT
  sheet_code,
  chinese_field,
  temp_english_field,
  occurrence_count,
  first_seen_at,
  last_seen_at,
  status,
  sample_value
FROM `grefun-testing.ragic_backup.unknown_fields`
WHERE status = 'pending'
ORDER BY occurrence_count DESC, first_seen_at DESC;

-- ============================================================================
-- 8. 常用維護 SQL
-- ============================================================================

-- 8.1 停用特定表單備份
UPDATE `grefun-testing.ragic_backup.backup_config`
SET enabled = FALSE, updated_at = CURRENT_TIMESTAMP()
WHERE sheet_code = '50';

-- 8.2 新增欄位對照規則
INSERT INTO `grefun-testing.ragic_backup.field_mappings`
(sheet_code, chinese_field, english_field, data_type, enabled, notes)
VALUES ('99', '新欄位名稱', 'new_field_name', 'STRING', TRUE, '手動新增');

-- 8.3 標記未知欄位為已處理
UPDATE `grefun-testing.ragic_backup.unknown_fields`
SET status = 'mapped', mapped_to = 'correct_field_name', notes = '已手動處理'
WHERE sheet_code = '99' AND chinese_field = '未知欄位名稱';

-- 8.4 清理舊的未知欄位記錄（例：3 個月前）
DELETE FROM `grefun-testing.ragic_backup.unknown_fields`
WHERE first_seen_at < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)
  AND status IN ('ignored', 'mapped');

-- ============================================================================
-- 執行完成提示
-- ============================================================================
-- 1. 請確認所有表格建立成功
-- 2. 驗證初始資料已正確插入
-- 3. 檢查索引是否建立完成
-- 4. 更新應用程式環境變數（如需啟用動態對照表）
-- ============================================================================
