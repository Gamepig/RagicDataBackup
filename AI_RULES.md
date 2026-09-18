# AI 輔助工具使用規則 | AI Tools Usage Rules

**RagicDataBackup 專案統一規範**

---

## 🎯 核心原則

**CLAUDE.md 是本專案的唯一真理來源 (Single Source of Truth)**

所有 AI 輔助工具（Claude Code、Cursor、GitHub Copilot、Gemini CLI、Codex CLI 等）在處理本專案時，必須嚴格遵循本規則。

---

## 📋 強制性要求

### 1. 文件優先級

```
CLAUDE.md (主要準則)
    ↓
README.md (專案概覽)
    ↓
操作手冊.md / 技術手冊.md (具體操作)
    ↓
技術手冊_詳細版.md (深入細節)
    ↓
其他文檔 (補充資訊)
```

**規則**: 當文件之間有衝突或不一致時，以 `CLAUDE.md` 為準。

### 2. 工作流程

所有 AI 工具在執行任何任務前，**必須**：

1. ✅ **閱讀 CLAUDE.md**：理解專案架構、模組職責、關鍵決策
2. ✅ **遵循規範**：使用文件中定義的指令、命名、流程
3. ✅ **保持一致性**：不同 AI 工具的輸出必須基於相同準則
4. ✅ **🔄 強制更新規則**：任何架構、模組、流程、配置變更時，**必須先更新 CLAUDE.md**，再執行變更

---

## 🔄 強制更新規則（CRITICAL）

### 核心原則

**任何架構、模組、流程或配置變更時，必須先更新 CLAUDE.md，再執行變更。**

這不是建議，而是**強制性要求**。違反此規則會導致文件與實際架構不一致，破壞 AI 工具之間的同步性。

### 必須更新 CLAUDE.md 的時機

**立即更新**的情況（變更前必須先更新文件）：

| 變更類型 | 範例 | 必須更新 |
|---------|------|---------|
| 核心模組變更 | 新增 `xxx_client.py`、修改 `ragic_client.py` 邏輯 | ✅ 是 |
| 架構模式變更 | 新增第三種執行模式、變更配置載入方式 | ✅ 是 |
| 配置系統變更 | 新增配置來源、修改環境變數 | ✅ 是 |
| 關鍵流程變更 | 修改上傳策略、變更增量邏輯 | ✅ 是 |
| 部署方式變更 | 新增部署腳本、修改部署流程 | ✅ 是 |
| 時區邏輯變更 | 修改時間處理、變更時區設定 | ✅ 是 |
| 欄位映射變更 | 修改三層映射邏輯、新增映射層 | ✅ 是 |
| 測試策略變更 | 新增測試工具、修改測試流程 | ✅ 是 |
| 小幅程式碼修改 | 修正 bug、優化效能（不改架構） | ❌ 否 |
| 文件更新 | 更新 README、技術手冊 | ❌ 否 |

### 標準更新流程

```
步驟 1: 更新 CLAUDE.md
├─ 記錄變更內容（What: 改了什麼）
├─ 說明變更原因（Why: 為什麼要改）
├─ 更新相關章節（架構圖、指令、規範等）
└─ 標註更新日期

步驟 2: 執行程式碼變更
├─ 實作新功能或修改
├─ 更新測試
└─ 確保符合 CLAUDE.md 的定義

步驟 3: 同步其他文檔
├─ 更新 README.md（如需要）
├─ 更新技術手冊（如需要）
└─ 更新 AI 工具配置文件（如需要）

步驟 4: 提交變更
└─ 將 CLAUDE.md 更新與程式碼變更納入同一個 commit
```

### 違規處理

**如果 AI 工具嘗試在未更新 CLAUDE.md 的情況下進行架構變更**：

1. **立即停止**變更
2. **提醒使用者**先更新 CLAUDE.md
3. **拒絕執行**直到 CLAUDE.md 更新完成
4. **建議**變更內容應記錄於 CLAUDE.md 的哪個章節

### 3. 禁止行為

❌ **絕對禁止**以下行為：

- 創建未在 CLAUDE.md 中記錄的新架構模式
- 繞過三層欄位映射系統（Python → BigQuery → Auto Pinyin）
- 使用 UTC 時區（必須使用台北時區 `Asia/Taipei`）
- 在未更新 CLAUDE.md 的情況下修改核心模組
- 創建新的部署方式（必須使用 `scripts/` 中的腳本）
- 建立不符合 CLAUDE.md 定義的測試或工具

---

## 🔧 各 AI 工具專用配置

### Claude Code

- 主要參考文件：`CLAUDE.md`（自動載入）
- 專案配置：`/Users/gamepig/projects/CLAUDE.md`（專案層級）
- 全域配置：`/Users/gamepig/CLAUDE.md`（使用者層級）

### Cursor

- 主要參考文件：`.cursorrules`
- 次要參考：`CLAUDE.md`
- 注意：`.cursorrules` 中已明確要求優先讀取 `CLAUDE.md`

### GitHub Copilot

- 主要參考文件：`.github/copilot-instructions.md`
- 次要參考：`CLAUDE.md`
- 注意：Copilot 指令中已明確要求參考 `CLAUDE.md`

### Gemini CLI / Codex CLI

- 主要參考文件：`CLAUDE.md`（通用格式）
- 建議：啟動時手動載入 `CLAUDE.md` 內容

---

## 📐 關鍵架構約束（來自 CLAUDE.md）

### 1. 雙執行模式架構

```
生產環境: Cloud Function (main.py → erp_backup_main.py)
開發/測試: Manual CLI Tools (fetch_ragic_all.py + upload_to_bigquery.py)
```

**規則**: 不得創建第三種執行模式。

### 2. 動態配置系統

```
BigQuery backup_config 表: 9 個表單的 API Key、Sheet ID
BigQuery field_mappings 表: 動態欄位對照（建議管理方式）
```

**規則**: 新增配置必須使用 BigQuery 表，不得硬編碼（核心欄位除外）。

### 3. 三層欄位映射

```
Layer 1: Python 硬編碼 (config_field_mapping.py) → 核心欄位
Layer 2: BigQuery 動態對照 (field_mappings 表) → 建議新增欄位方式
Layer 3: 自動拼音轉換 → 未知欄位的回退方案
```

**規則**: 新欄位應在 Layer 2 新增，不得跳過映射系統。

### 4. 時區處理

```python
from data_transformer import TAIPEI_TZ, ensure_taipei_time

# ✅ 正確
dt = ensure_taipei_time(some_datetime)

# ❌ 錯誤 - 禁止使用 UTC
dt = datetime.utcnow()
```

**規則**: 所有時間處理必須使用台北時區。

### 5. 上傳策略

```
小批次 (< 5000 筆): Direct MERGE
大批次 (≥ 5000 筆): Staging Table + Stored Procedure
```

**規則**: 不得繞過 `bigquery_uploader.py` 的自動選擇邏輯。

---

## 🧪 測試規範

### 測試檔案位置

- **路徑**: `test/` 資料夾（已加入 `.gitignore`）
- **命名**: `test_*.py` 或 `*_test.py`
- **執行**: 參考 `CLAUDE.md` → "測試與診斷" 章節

### 現有測試腳本

參考這些現有測試作為範例：

```bash
test/test_ragic_connection.py       # Ragic 連線測試
test/diagnose_data_flow.py          # 資料流診斷
test/test_incremental_fetch.py      # 增量抓取邏輯
test/test_end_to_end.py             # 端到端測試
```

---

## 📝 程式碼生成指南

### 必須遵循的模式

#### 模組導入

```python
# ✅ 正確 - 遵循 CLAUDE.md 定義的模組結構
from ragic_client import RagicClient
from data_transformer import create_transformer, TAIPEI_TZ
from bigquery_uploader import create_uploader
from config_loader import load_config_with_fallback

# ❌ 錯誤 - 不存在於 CLAUDE.md 的模組
from custom_config import CustomConfigLoader
```

#### 時間處理

```python
# ✅ 正確
from data_transformer import TAIPEI_TZ, ensure_taipei_time
from datetime import datetime

now = datetime.now(tz=TAIPEI_TZ)
parsed_time = ensure_taipei_time(some_dt)

# ❌ 錯誤
now = datetime.utcnow()
```

#### 配置載入

```python
# ✅ 正確 - 使用 BigQuery 動態配置
from config_loader import BackupConfigLoader

loader = BackupConfigLoader(project_id="your-project")
configs = loader.load_backup_config(client_id="grefun")

# ❌ 錯誤 - 硬編碼配置
SHEETS = {
    "10": "forms8/5",
    "20": "forms8/4"
}
```

---

## 🔄 架構變更流程

當需要修改專案架構時，**嚴格遵循**以下順序：

### 步驟 1: 更新 CLAUDE.md（🔴 強制性第一步）

```
1. 在 CLAUDE.md 中記錄新的架構設計
2. 說明變更原因（Why）和變更內容（What）
3. 更新相關的指令、流程、最佳實踐
4. 標註變更日期
```

**⚠️ 關鍵**: 此步驟必須在任何程式碼變更之前完成。

### 步驟 2: 同步 AI 工具配置（如架構級變更）

```
1. 更新 .cursorrules（如需要）
2. 更新 .github/copilot-instructions.md（如需要）
3. 更新 AI_RULES.md（如架構約束變更）
```

**何時需要**: 當變更影響到 AI 工具的行為規範時（例如新增禁止模式、修改核心約束）。

### 步驟 3: 實作變更

```
1. 修改程式碼
2. 更新測試
3. 確保實作符合 CLAUDE.md 的定義
```

### 步驟 4: 同步其他文檔

```
1. 更新 README.md（如需要）
2. 更新技術手冊（如需要）
3. 更新部署文檔（如需要）
```

### 步驟 5: 驗證一致性

```
1. 確認所有 AI 工具配置文件與 CLAUDE.md 一致
2. 執行測試確保變更正確
3. 提交變更（CLAUDE.md 更新必須包含在同一個 commit）
```

### ⚠️ 違規檢查

在提交前，確認：
- [ ] CLAUDE.md 已更新並包含變更說明
- [ ] 程式碼實作符合 CLAUDE.md 的定義
- [ ] 所有相關文檔已同步更新
- [ ] CLAUDE.md 更新與程式碼變更在同一個 commit

---

## ⚠️ 常見錯誤與修正

### 錯誤 1: 忽略 CLAUDE.md

❌ **錯誤行為**: 直接基於 README.md 或猜測來工作

✅ **正確行為**: 先閱讀 CLAUDE.md，理解架構後再開始

### 錯誤 2: 創建新模式

❌ **錯誤行為**: 創建新的配置系統或部署方式

✅ **正確行為**: 使用現有的 BigQuery 配置表和 `scripts/` 部署腳本

### 錯誤 3: 時區混亂

❌ **錯誤行為**: 混用 UTC 和台北時區

✅ **正確行為**: 統一使用 `TAIPEI_TZ`，使用 `ensure_taipei_time()` 轉換

### 錯誤 4: 直接修改核心模組

❌ **錯誤行為**: 修改 `ragic_client.py` 等核心模組但不更新 CLAUDE.md

✅ **正確行為**: 先在 CLAUDE.md 記錄變更理由和設計，再實作

---

## 📊 專案關鍵資訊（來自 CLAUDE.md）

### 客戶環境

- **GCP Project**: `b25h01-ragic` (571015722523)
- **Dataset**: `erp_backup`
- **Table**: `ragic_data`
- **Location**: `asia-east1`

### 9 個 Ragic 表單

| Sheet Code | Sheet ID | 特性 | 記錄數 |
|-----------|---------|------|--------|
| 10-40, 70 | forms8/* | 小表 | < 500 |
| **41** | forms8/6 | **靜態表（無時間欄位）** | 369 |
| 50, 60 | forms8/17, forms8/2 | 大表 | 60K-86K |
| **99** | forms8/3 | **最大表（67% 資料量）** | 303K |

### 全量備份基準（2025-10-21）

- 總記錄數: 452,567 筆
- 執行時間: 約 43 分鐘
- 增量週期: 每週一次（約 2.1% 變動）

---

## 📚 快速參考連結

- **主要準則**: `CLAUDE.md`
- **專案概覽**: `README.md`
- **部署指南**: `操作手冊.md`
- **開發手冊**: `技術手冊.md`
- **詳細設計**: `技術手冊_詳細版.md`
- **CLI 工具**: `Manual_fetch_all_Ragic/USAGE.md`
- **欄位映射**: `documents/field_mapping_quick_reference.md`
- **問題修復**: `scripts/README_SCHEDULER_FIX.md`

---

## ✅ 檢查清單

在開始任何任務前，確認：

- [ ] 已閱讀 `CLAUDE.md` 的相關章節
- [ ] 理解專案的雙執行模式架構
- [ ] 知道要使用的模組和其職責
- [ ] 清楚時區處理規則（台北時區）
- [ ] 了解三層欄位映射系統
- [ ] 確認使用正確的部署/測試指令
- [ ] 知道配置變更應使用 BigQuery 表

---

**最後更新**: 2025-10-30
**維護者**: 專案開發團隊
**適用範圍**: 所有 AI 輔助工具

---

**記住：CLAUDE.md 是唯一真理來源。有疑問時，查閱 CLAUDE.md。**
