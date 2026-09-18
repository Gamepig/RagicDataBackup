# 📚 RagicDataBackup 技術文件索引

> 快速導航系統所有技術文件，幫助開發者與使用者快速找到所需資訊

**專案狀態**: 🚧 開發中（非 Production Ready）
**最後更新**: 2025-10-02

---

## 🚀 快速開始

| 文件 | 說明 | 適合對象 |
|-----|------|---------|
| [README.md](README.md) | 專案總覽與快速開始指南 | 所有人 ⭐ |
| [操作手冊.md](操作手冊.md) | 一鍵部署、驗證、排程與常見問題 | 非技術使用者 |
| [技術手冊.md](技術手冊.md) | 模組/函式/變數詳解與部署重點 | 技術維運 |
| [FIELD_MAPPING_SOLUTION.md](FIELD_MAPPING_SOLUTION.md) | 欄位對照表完整解決方案 | 開發者 ⭐ |
| [test/BACKUP_GUIDE.md](test/BACKUP_GUIDE.md) | Ragic 完整備份操作指南 | 操作人員 ⭐ |

---

## 📖 文件分類

### 1️⃣ 核心文件（必讀）

#### 專案概述
- **[README.md](README.md)**
  - 專案介紹與架構說明
  - 5 個核心模組功能
  - 環境變數設定指南
  - ⚠️ 已知限制與欄位對照表問題

#### 欄位對照表解決方案
- **[FIELD_MAPPING_SOLUTION.md](FIELD_MAPPING_SOLUTION.md)** ⭐ 重要
  - 問題背景：Cloud Function 環境限制
  - 三層設定策略詳解（硬編碼 + BigQuery + 自動轉換）
  - 完整實作細節與程式碼範例
  - 維護流程與未來 AI 自動化計劃
  - 常見問題排查

### 2️⃣ 測試與備份

#### 備份操作指南
- **[test/BACKUP_GUIDE.md](test/BACKUP_GUIDE.md)**
  - 漸進式斷點續傳備份方式
  - 自動化腳本使用說明
  - 9 個表單設定與預估時間
  - 檔案結構與進度管理

#### 測試報告
- **[test/TEST_SUMMARY.md](test/TEST_SUMMARY.md)**
  - Ragic API 測試結果（441,239 筆 / 1.4GB）
  - 9 個表單詳細統計
  - 效能數據與建議方案
  - 關鍵發現與下一步驟

- **[test/README.md](test/README.md)**
  - 測試專案總覽
  - 快速開始指令
  - 備份資料位置
  - 技術細節與斷點續傳機制

- **查詢未備份記錄**：`test/query_unbackup.py`（依 batch_id 查 Cloud Logging invalid-records）

### 3️⃣ 欄位對照表專區

#### 完整對照表
- **[documents/field_mapping_master.json](documents/field_mapping_master.json)**
  - 9 個表單 234 個欄位完整對照
  - JSON 格式，可程式化讀取
  - 包含 metadata 與統計資訊

#### 快速參考
- **[documents/field_mapping_quick_reference.md](documents/field_mapping_quick_reference.md)**
  - 一頁式快速查詢表
  - 9 個表單欄位清單
  - 共通欄位對照

#### 詳細說明
- **[documents/field_mapping_README.md](documents/field_mapping_README.md)**
  - 欄位對照表使用指南
  - Python 程式碼範例
  - 命令列查詢技巧

- **[documents/field_mapping_analysis.md](documents/field_mapping_analysis.md)**
  - 三種儲存方案分析（BigQuery vs 程式碼 vs JSON）
  - 效能評估與建議
  - 實作範例

- **[documents/FIELD_MAPPING_SUMMARY.md](documents/FIELD_MAPPING_SUMMARY.md)**
  - 欄位對照表建立任務報告
  - 統計資訊與檔案位置
  - 維護建議

- **[documents/field_mapping_reference.md](documents/field_mapping_reference.md)**
  - 欄位對照參考文件
  - 詳細欄位說明

### 4️⃣ Ragic API 文件

#### API 開發手冊
- **[documents/Ragic API 開發手冊.md](documents/Ragic%20API%20開發手冊.md)**
  - Ragic API 完整說明
  - 認證方式與請求格式
  - 分頁與過濾參數
  - 錯誤處理

#### 備份範圍
- **[documents/Ragic備份範圍.txt](documents/Ragic備份範圍.txt)**
  - 9 個表單清單
  - Sheet ID 與 Ragic URL
  - API Key 與 Account 資訊

#### 程式碼審查報告
- **[documents/Ragic API 資料擷取模組審查報告ChatGPT.md](documents/Ragic%20API%20資料擷取模組審查報告ChatGPT.md)**
- **[documents/Ragic API 資料擷取模組審查報告Gemini.md](documents/Ragic%20API%20資料擷取模組審查報告Gemini.md)**
- **[documents/Ragic API 資料擷取模組審查報告Grok.md](documents/Ragic%20API%20資料擷取模組審查報告Grok.md)**
  - ragic_client.py 程式碼審查
  - 各 AI 模型的審查意見
  - 改進建議

### 5️⃣ Cloud Function 部署

#### 部署指南
- **[documents/CloudFunctionGrok.md](documents/CloudFunctionGrok.md)** ⭐ 推薦
  - Cloud Function 完整部署步驟
  - 環境變數設定
  - 逾時與記憶體設定
  - 測試與監控

- **[documents/CloudFunctionChatGPT.md](documents/CloudFunctionChatGPT.md)**
  - Cloud Function 部署替代指南

#### 使用者文件（新）
- **[操作手冊.md](操作手冊.md)** ⭐ 建議先讀
  - 非技術人員用：一鍵部署、驗證、排程與常見問題
- **[技術手冊.md](技術手冊.md)**
  - 技術維運用：模組/函式/變數詳解、增量策略、部署重點

### 6️⃣ BigQuery 相關

#### BigQuery 設定資料表建立
- **[sql/setup_bigquery_config_tables.sql](sql/setup_bigquery_config_tables.sql)** ⭐ 重要
  - 建立 3 個設定資料表（backup_config、field_mappings、unknown_fields）
  - 插入 9 個表單初始資料
  - 索引與驗證 SQL
  - 常用維護 SQL

- **[sql/create_staging_table.sql](sql/create_staging_table.sql)** ⭐ 新增
  - 建立 `<TABLE>_staging` 表，附加 `batch_id`、`ingested_at`
  - 建議沿用目標資料表分區與叢集設定（`DATE(updated_at)` / `CLUSTER BY order_id`）

- **[sql/create_merge_sp.sql](sql/create_merge_sp.sql)** ⭐ 新增
  - 預儲程序 `sp_upsert_ragic_data(batch_id)`
  - staging → 目標 `MERGE`、審計（可選）與清理批次

#### 增量策略
- `ragic_client.fetch_since_local_paged()` - 單頁抓取 + 本地過濾一週（必要時自動往下一頁），嚴格 `dt > since`，支援 `until` 上界。
- `test/fetch_last_week_where_cn.py` - 一週驗證（大表較高 limit）
- `test/run_api_incremental_window.py` - 任意時間窗驗證（`WINDOW_SINCE_DAYS`/`WINDOW_UNTIL_DAYS`）

#### BigQuery 查詢
- **[documents/BigQuery查詢.md](documents/BigQuery查詢.md)**
  - BigQuery 查詢範例
  - 資料分析 SQL
  - 效能優化技巧

### 7️⃣ 系統設計

#### 提案文件
- **[documents/erp_backup_proposal_v2.md](documents/erp_backup_proposal_v2.md)**
  - 系統設計提案 v2
  - 架構規劃
  - 技術選型
  - 實作計劃

#### 文件索引
- **[documents/INDEX.md](documents/INDEX.md)**
  - documents 資料夾導航
  - 欄位對照表文件清單

- **[documents/README.md](documents/README.md)**
  - documents 資料夾說明

### 8️⃣ 內部文件（不上傳 GitHub）

這些文件位於 `.gitignore` 排除清單中：

- **CLAUDE.md** - Claude Code Agent 設定
- **Tasks.md** - 專案任務清單
- **.claude/memory/** - Claude 記憶檔案
- **test/** 資料夾 - 測試腳本與備份資料
- **documents/API_Keys.txt** - API 金鑰（敏感資訊）

---

## 🗂️ 文件地圖（按開發流程）

### 階段一：了解專案
1. [README.md](README.md) - 專案總覽
2. [documents/erp_backup_proposal_v2.md](documents/erp_backup_proposal_v2.md) - 設計提案
3. [documents/Ragic API 開發手冊.md](documents/Ragic%20API%20開發手冊.md) - API 說明

### 階段二：本地開發
1. [test/README.md](test/README.md) - 測試專案說明
2. [test/BACKUP_GUIDE.md](test/BACKUP_GUIDE.md) - 備份操作指南
3. [FIELD_MAPPING_SOLUTION.md](FIELD_MAPPING_SOLUTION.md) - 欄位對照表解決方案

### 階段三：設定管理
1. [sql/setup_bigquery_config_tables.sql](sql/setup_bigquery_config_tables.sql) - BigQuery 配置
2. [documents/field_mapping_master.json](documents/field_mapping_master.json) - 欄位對照表
3. [documents/field_mapping_README.md](documents/field_mapping_README.md) - 使用指南

### 階段四：部署上線
1. [documents/CloudFunctionGrok.md](documents/CloudFunctionGrok.md) - 部署指南
2. [documents/BigQuery查詢.md](documents/BigQuery查詢.md) - 查詢範例
3. [README.md](README.md) 環境變數章節 - 設定說明

---

## 📊 文件統計

| 類別 | 文件數量 | 說明 |
|-----|---------|------|
| 核心文件 | 3 | README、欄位對照表解決方案、備份指南 |
| 測試文件 | 3 | 測試總結、操作指南、專案說明 |
| 欄位對照表 | 7 | JSON + 5 個 MD 文件 |
| Ragic API | 5 | 開發手冊、審查報告、備份範圍 |
| 部署文件 | 2 | Cloud Function 部署指南 |
| BigQuery | 2 | SQL 腳本、查詢範例 |
| 系統設計 | 3 | 提案、索引、README |
| **總計** | **25** | 不含內部文件與 .claude/memory |

---

## 🔍 快速查詢指南

### 我想要...

#### 了解專案整體架構
→ [README.md](README.md) 的「系統架構」章節

#### 解決欄位對照表問題
→ [FIELD_MAPPING_SOLUTION.md](FIELD_MAPPING_SOLUTION.md)

#### 執行首次完整備份
→ [test/BACKUP_GUIDE.md](test/BACKUP_GUIDE.md)

#### 查詢特定欄位的英文名稱
→ [documents/field_mapping_quick_reference.md](documents/field_mapping_quick_reference.md)

#### 部署到 Cloud Function
→ [documents/CloudFunctionGrok.md](documents/CloudFunctionGrok.md)

#### 設定 BigQuery 配置表
→ [sql/setup_bigquery_config_tables.sql](sql/setup_bigquery_config_tables.sql)

#### 了解 Ragic API 使用方式
→ [documents/Ragic API 開發手冊.md](documents/Ragic%20API%20開發手冊.md)

#### 查看測試結果
→ [test/TEST_SUMMARY.md](test/TEST_SUMMARY.md)

#### 了解欄位對照表儲存方案
→ [documents/field_mapping_analysis.md](documents/field_mapping_analysis.md)

#### 執行 BigQuery 資料查詢
→ [documents/BigQuery查詢.md](documents/BigQuery查詢.md)

---

## 🎯 推薦閱讀順序

### 新手入門（第一次接觸專案）
1. [README.md](README.md) - 10 分鐘
2. [test/BACKUP_GUIDE.md](test/BACKUP_GUIDE.md) - 5 分鐘
3. [FIELD_MAPPING_SOLUTION.md](FIELD_MAPPING_SOLUTION.md) - 20 分鐘

### 開發人員（需要實作功能）
1. [FIELD_MAPPING_SOLUTION.md](FIELD_MAPPING_SOLUTION.md) - 完整閱讀
2. [documents/field_mapping_README.md](documents/field_mapping_README.md) - 使用範例
3. [sql/setup_bigquery_config_tables.sql](sql/setup_bigquery_config_tables.sql) - SQL 腳本
4. [documents/Ragic API 開發手冊.md](documents/Ragic%20API%20開發手冊.md) - API 說明

### 運維人員（負責部署與維護）
1. [README.md](README.md) 環境變數章節
2. [documents/CloudFunctionGrok.md](documents/CloudFunctionGrok.md) - 部署指南
3. [sql/setup_bigquery_config_tables.sql](sql/setup_bigquery_config_tables.sql) - 配置表建立
4. [documents/BigQuery查詢.md](documents/BigQuery查詢.md) - 查詢與監控

### 專案管理者（了解進度與規劃）
1. [test/TEST_SUMMARY.md](test/TEST_SUMMARY.md) - 測試結果
2. [documents/erp_backup_proposal_v2.md](documents/erp_backup_proposal_v2.md) - 設計提案
3. [README.md](README.md) 已知限制章節

---

## 📝 文件維護指南

### 何時更新文件

- ✅ 新增功能或模組時
- ✅ 修改核心配置或流程時
- ✅ 發現新問題或解決方案時
- ✅ 完成重要測試或部署時

### 文件命名規範

- 使用有意義的英文或中文名稱
- 核心文件使用大寫（如 README.md）
- 專題文件使用描述性名稱（如 FIELD_MAPPING_SOLUTION.md）
- 資料夾內文件使用小寫或 snake_case（如 field_mapping_README.md）

### 更新此索引

每次新增或移除文件時，請更新此 `DOCUMENTATION_INDEX.md`：
1. 在對應分類中新增文件連結
2. 更新「文件統計」表格
3. 更新「最後更新」日期

---

## 🔗 相關連結

- **GitHub Repository**: （待建立）
- **Cloud Function Console**: [Google Cloud Console](https://console.cloud.google.com/functions)
- **BigQuery Console**: [BigQuery Console](https://console.cloud.google.com/bigquery)
- **Ragic API 文件**: [Ragic API Docs](https://www.ragic.com/intl/zh-TW/doc-api/)

---

## 📞 聯絡資訊

如有文件相關問題或建議：
1. 檢查 [README.md](README.md) 的「技術支援」章節
2. 查閱相關技術文件
3. 建立 GitHub Issue（待建立）

---

**文件索引版本**: 1.0
**建立時間**: 2025-10-02
**維護者**: 專案團隊
**文件總數**: 25+

- **表單識別清單（9 表）**：見 README 的「表單識別（9 張表）」段落（sheet_code 與 sheet_id 對照）
