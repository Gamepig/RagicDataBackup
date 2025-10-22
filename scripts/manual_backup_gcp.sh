#!/bin/zsh
set -euo pipefail

# 手動備份指令：在 GCP 或本地執行一次增量備份（只抓未備份資料）
# 步驟：
# 1) 讀取必要環境變數
# 2) 設定預設對照（九張表）與上傳參數
# 3) 執行 python3 erp_backup_main.py（單/多表自動判斷）

SCRIPT_DIR=$(cd -- "$(dirname -- "$0")" && pwd)
PROJECT_ROOT=$(cd -- "$SCRIPT_DIR/.." && pwd)
cd "$PROJECT_ROOT"

# 必要環境變數檢查
req_vars=(RAGIC_API_KEY RAGIC_ACCOUNT GCP_PROJECT_ID BIGQUERY_DATASET BIGQUERY_TABLE)
for v in ${req_vars[@]}; do
  if [ -z "${!v:-}" ]; then
    echo "缺少必要環境變數: $v" >&2
    echo "請先 export $v=... 後重試" >&2
    exit 1
  fi
done

# 可選環境變數預設
export LOG_LEVEL=${LOG_LEVEL:-INFO}
export USE_MERGE=${USE_MERGE:-true}
export UPLOAD_MODE=${UPLOAD_MODE:-direct}
export BATCH_THRESHOLD=${BATCH_THRESHOLD:-5000}
export BIGQUERY_LOCATION=${BIGQUERY_LOCATION:-asia-east1}

# Ragic 抓取設定（預設九表，多表模式）
export RAGIC_SHEET_ID=${RAGIC_SHEET_ID:-ALL}
if [ -z "${SHEET_MAP_JSON:-}" ]; then
  export SHEET_MAP_JSON='{"10":"forms8/5","20":"forms8/4","30":"forms8/7","40":"forms8/1","41":"forms8/6","50":"forms8/17","60":"forms8/2","70":"forms8/9","99":"forms8/3"}'
fi

# 通知設定（可選，若提供則會寄信）
# 需設定：NOTIFICATION_EMAIL, SMTP_FROM_EMAIL, SMTP_FROM_PASSWORD

echo "開始手動備份（增量）：$PROJECT_ROOT"
python3 "$PROJECT_ROOT/erp_backup_main.py"

echo "手動備份完成"


