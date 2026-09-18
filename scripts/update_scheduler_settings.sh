#!/bin/bash
#
# 更新 Cloud Scheduler 設定以暫停重試並暫時關閉寄信
# 使用方式：
#   bash scripts/update_scheduler_settings.sh [JOB_NAME]
# 參數：
#   JOB_NAME   預設 erp-backup-weekly

set -euo pipefail

JOB_NAME=${1:-erp-backup-weekly}
PROJECT_ID=${PROJECT_ID:-b25h01-ragic}
REGION=${REGION:-asia-east1}
FUNCTION_NAME=${FUNCTION_NAME:-erp-backup}
CF_URL=${CF_URL:-"https://${REGION}-${PROJECT_ID}.cloudfunctions.net/${FUNCTION_NAME}"}

echo "Updating Cloud Scheduler job: ${JOB_NAME} in ${PROJECT_ID}/${REGION}"

# 關閉重試並暫時帶上 DISABLE_EMAIL=true
gcloud scheduler jobs update http "${JOB_NAME}" \
  --location="${REGION}" \
  --uri="${CF_URL}" \
  --http-method=POST \
  --max-retry-attempts=0 \
  --attempt-deadline=600s \
  --message-body='{"DISABLE_EMAIL": true}'

echo "✓ Scheduler job updated. Retries disabled and email temporarily suppressed."


