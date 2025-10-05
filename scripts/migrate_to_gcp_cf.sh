#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

# ----------------------------------------------------------------------------
# Ragic ERP 備份系統：Cloud Functions 自動遷移腳本（最小必要移轉）
# 只部署必需檔案，並以 --set-env-vars 注入設定。
# 需求：已安裝並登入 gcloud / bq；具備目標專案存取權。
# ----------------------------------------------------------------------------

# 預設參數（可由環境變數覆蓋）
PROD_PROJECT_ID="${PROD_PROJECT_ID:-}"
REGION="${REGION:-asia-east1}"
DATASET_ID="${DATASET_ID:-erp_backup}"
TABLE_ID="${TABLE_ID:-ragic_data}"
BIGQUERY_LOCATION="${BIGQUERY_LOCATION:-asia-east1}"

FUNCTION_NAME="${FUNCTION_NAME:-erp-backup}"
ENTRY_POINT="${ENTRY_POINT:-backup_erp_data}"
SCHEDULE_CRON="${SCHEDULE_CRON:-0 3 * * 1}"

# Ragic 與流程設定（以環境變數集中注入）
RAGIC_API_KEY="${RAGIC_API_KEY:-}"
RAGIC_ACCOUNT="${RAGIC_ACCOUNT:-}"
RAGIC_SHEET_ID="${RAGIC_SHEET_ID:-ALL}"
SHEET_MAP_JSON="${SHEET_MAP_JSON:-{"10":"forms8/5","20":"forms8/4","30":"forms8/7","40":"forms8/1","41":"forms8/6","50":"forms8/17","60":"forms8/2","70":"forms8/9","99":"forms8/3"}}"
RAGIC_MAX_PAGES="${RAGIC_MAX_PAGES:-50}"
LAST_MODIFIED_FIELD_NAMES="${LAST_MODIFIED_FIELD_NAMES:-最後修改日期,最後修改時間,更新時間,最後更新時間}"

# 郵件設定
NOTIFICATION_EMAIL="${NOTIFICATION_EMAIL:-}"
SMTP_FROM_EMAIL="${SMTP_FROM_EMAIL:-}"
SMTP_FROM_PASSWORD="${SMTP_FROM_PASSWORD:-}"

# 認證與排程（若提供 SCHEDULER_SA_EMAIL，使用 OIDC；否則允許未授權觸發）
SCHEDULER_SA_EMAIL="${SCHEDULER_SA_EMAIL:-}"

# 是否建立 invalid_records 表（true/false）
CREATE_INVALID_TABLE="${CREATE_INVALID_TABLE:-false}"

usage() {
  cat <<EOF
用法：
  PROD_PROJECT_ID=xxx \
  RAGIC_API_KEY=... RAGIC_ACCOUNT=... \
  NOTIFICATION_EMAIL=... SMTP_FROM_EMAIL=... SMTP_FROM_PASSWORD=... \
  ./scripts/migrate_to_gcp_cf.sh

可選參數（環境變數）：
  REGION=${REGION}  DATASET_ID=${DATASET_ID}  TABLE_ID=${TABLE_ID}
  BIGQUERY_LOCATION=${BIGQUERY_LOCATION}
  RAGIC_SHEET_ID=ALL  RAGIC_MAX_PAGES=${RAGIC_MAX_PAGES}
  LAST_MODIFIED_FIELD_NAMES='${LAST_MODIFIED_FIELD_NAMES}'
  SHEET_MAP_JSON='${SHEET_MAP_JSON}'
  FUNCTION_NAME=${FUNCTION_NAME}  ENTRY_POINT=${ENTRY_POINT}
  SCHEDULE_CRON='${SCHEDULE_CRON}'
  SCHEDULER_SA_EMAIL=<scheduler-oidc-sa@${PROD_PROJECT_ID}.iam.gserviceaccount.com>
  CREATE_INVALID_TABLE=${CREATE_INVALID_TABLE}
EOF
}

require() {
  local name="$1" val="${!1:-}"
  if [[ -z "$val" ]]; then
    echo "[ERROR] 缺少必要變數：$1" >&2
    usage
    exit 1
  fi
}

echo "==> 檢查必要變數"
require PROD_PROJECT_ID
require RAGIC_API_KEY
require RAGIC_ACCOUNT
require NOTIFICATION_EMAIL
require SMTP_FROM_EMAIL
require SMTP_FROM_PASSWORD

echo "==> 切換專案與啟用 API：$PROD_PROJECT_ID ($REGION)"
gcloud config set project "$PROD_PROJECT_ID" >/dev/null
gcloud services enable \
  bigquery.googleapis.com \
  cloudfunctions.googleapis.com \
  cloudscheduler.googleapis.com \
  logging.googleapis.com >/dev/null

echo "==> 建立/確保 BigQuery Dataset 存在：$DATASET_ID ($BIGQUERY_LOCATION)"
bq --location="$BIGQUERY_LOCATION" mk -d "$PROD_PROJECT_ID:$DATASET_ID" >/dev/null 2>&1 || true

if [[ "${CREATE_INVALID_TABLE}" == "true" ]]; then
  echo "==> 建立/確保 invalid_records 存在（如未存在）"
  if [[ -f "sql/create_invalid_table.sql" ]]; then
    # 嘗試以標準 SQL 建表（若已存在則忽略）
    bq query --nouse_legacy_sql < sql/create_invalid_table.sql || true
  else
    echo "[WARN] 找不到 sql/create_invalid_table.sql，略過建立 invalid_records"
  fi
fi

# 產生 .gcloudignore，避免將非必要資料夾佈署
if [[ ! -f .gcloudignore ]]; then
  echo "==> 產生 .gcloudignore 以排除不必要檔案"
  cat > .gcloudignore <<'IGN'
.git/
test/
documents/
.claude/
.DS_Store
*.log
IGN
fi

echo "==> 佈署 Cloud Function：$FUNCTION_NAME"
CF_FLAGS=(
  --region="$REGION"
  --runtime=python311
  --trigger-http
  --source=.
  --entry-point="$ENTRY_POINT"
)

if [[ -z "${SCHEDULER_SA_EMAIL}" ]]; then
  echo "[INFO] 未提供 SCHEDULER_SA_EMAIL，將允許未授權觸發（開發/測試用）"
  CF_FLAGS+=(--allow-unauthenticated)
fi

ENV_VARS=(
  "RAGIC_API_KEY=${RAGIC_API_KEY}"
  "RAGIC_ACCOUNT=${RAGIC_ACCOUNT}"
  "GCP_PROJECT_ID=${PROD_PROJECT_ID}"
  "BIGQUERY_DATASET=${DATASET_ID}"
  "BIGQUERY_TABLE=${TABLE_ID}"
  "BIGQUERY_LOCATION=${BIGQUERY_LOCATION}"
  "RAGIC_SHEET_ID=${RAGIC_SHEET_ID}"
  "SHEET_MAP_JSON=${SHEET_MAP_JSON}"
  "RAGIC_MAX_PAGES=${RAGIC_MAX_PAGES}"
  "LAST_MODIFIED_FIELD_NAMES=${LAST_MODIFIED_FIELD_NAMES}"
  "UPLOAD_MODE=auto"
  "BATCH_THRESHOLD=5000"
  "DISABLE_EMAIL=false"
  "NOTIFICATION_EMAIL=${NOTIFICATION_EMAIL}"
  "SMTP_FROM_EMAIL=${SMTP_FROM_EMAIL}"
  "SMTP_FROM_PASSWORD=${SMTP_FROM_PASSWORD}"
)

gcloud functions deploy "$FUNCTION_NAME" \
  "${CF_FLAGS[@]}" \
  --set-env-vars "$(IFS=, ; echo "${ENV_VARS[*]}")"

echo "==> 取得 Cloud Function URL"
CF_URL=$(gcloud functions describe "$FUNCTION_NAME" --region="$REGION" --format='value(httpsTrigger.url)')
echo "CF_URL=$CF_URL"

echo "==> 建立/更新 Cloud Scheduler Job（$SCHEDULE_CRON）"
SCHED_NAME="${SCHED_NAME:-${FUNCTION_NAME}-weekly}"

if gcloud scheduler jobs describe "$SCHED_NAME" >/dev/null 2>&1; then
  gcloud scheduler jobs delete "$SCHED_NAME" --quiet >/dev/null 2>&1 || true
fi

if [[ -n "${SCHEDULER_SA_EMAIL}" ]]; then
  gcloud scheduler jobs create http "$SCHED_NAME" \
    --schedule="$SCHEDULE_CRON" \
    --uri="$CF_URL" \
    --http-method=GET \
    --oidc-service-account-email="$SCHEDULER_SA_EMAIL"
else
  gcloud scheduler jobs create http "$SCHED_NAME" \
    --schedule="$SCHEDULE_CRON" \
    --uri="$CF_URL" \
    --http-method=GET
fi

echo "==> 遷移完成。請以下列指令驗證："
echo "   curl -sS \"$CF_URL\" | jq . || true"
echo "   bq query --nouse_legacy_sql 'SELECT COUNT(*) FROM \`${PROD_PROJECT_ID}.${DATASET_ID}.${TABLE_ID}\`'"

echo "完成。"


