#!/usr/bin/env bash
set -euo pipefail

# 用途：建立 BigQuery 測試環境（資料集、Service Account、staging 表與 SP）
# 使用方式：
#   ./scripts/setup_gcp_test_env.sh <PROJECT_ID> <DATASET_ID> <TARGET_TABLE> [LOCATION] [SERVICE_ACCOUNT_NAME]
# 例如：
#   ./scripts/setup_gcp_test_env.sh grefun-testing erp_backup ragic_data asia-east1 ragic-backup-test-sa

PROJECT_ID=${1:?"PROJECT_ID is required"}
DATASET_ID=${2:?"DATASET_ID is required"}
TARGET_TABLE=${3:?"TARGET_TABLE is required"}
LOCATION=${4:-US}
SA_NAME=${5:-ragic-backup-test-sa}

echo "[info] Project=${PROJECT_ID} Dataset=${DATASET_ID} Table=${TARGET_TABLE} Location=${LOCATION} SA=${SA_NAME}"

echo "[step] Enable BigQuery API"
gcloud services enable bigquery.googleapis.com --project "${PROJECT_ID}"

echo "[step] Create dataset if not exists"
# 使用完整標識避免不同預設專案問題
if ! bq --project_id="${PROJECT_ID}" --location="${LOCATION}" ls -d | awk '{print $1}' | grep -q "${PROJECT_ID}:${DATASET_ID}$"; then
  bq --project_id="${PROJECT_ID}" --location="${LOCATION}" mk -d "${PROJECT_ID}:${DATASET_ID}"
else
  echo "[info] Dataset exists: ${PROJECT_ID}:${DATASET_ID}"
fi

echo "[step] Create service account (if not exists)"
if ! gcloud iam service-accounts list --project "${PROJECT_ID}" --format="value(email)" | grep -q "${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"; then
  gcloud iam service-accounts create "${SA_NAME}" --project "${PROJECT_ID}" --display-name "Ragic Backup Test"
fi

SA_EMAIL="${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"

echo "[step] Grant BigQuery roles to SA"
gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/bigquery.dataEditor" >/dev/null
gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/bigquery.jobUser" >/dev/null

echo "[step] Create key file (JSON)"
KEY_FILE="./${SA_NAME}-key.json"
if [ ! -f "${KEY_FILE}" ]; then
  gcloud iam service-accounts keys create "${KEY_FILE}" --iam-account "${SA_EMAIL}" --project "${PROJECT_ID}"
fi

echo "[step] Apply SQL: create_staging_table.sql & create_merge_sp.sql (if base table exists)"
TABLE_EXISTS=$(bq --project_id="${PROJECT_ID}" --location="${LOCATION}" ls -t "${PROJECT_ID}:${DATASET_ID}" | awk '{print $1}' | grep -c "${PROJECT_ID}:${DATASET_ID}.${TARGET_TABLE}$" || true)
if [ "${TABLE_EXISTS}" -gt 0 ]; then
  TMP_SQL_1=$(mktemp)
  sed -e "s#\${PROJECT_ID}#${PROJECT_ID}#g" \
      -e "s#\${DATASET_ID}#${DATASET_ID}#g" \
      -e "s#\${TARGET_TABLE}#${TARGET_TABLE}#g" \
      sql/create_staging_table.sql > "${TMP_SQL_1}"
  bq --project_id="${PROJECT_ID}" query --use_legacy_sql=false --location="${LOCATION}" < "${TMP_SQL_1}"
  rm -f "${TMP_SQL_1}"

  TMP_SQL_2=$(mktemp)
  sed -e "s#\${PROJECT_ID}#${PROJECT_ID}#g" \
      -e "s#\${DATASET_ID}#${DATASET_ID}#g" \
      -e "s#\${TARGET_TABLE}#${TARGET_TABLE}#g" \
      sql/create_merge_sp.sql > "${TMP_SQL_2}"
  bq --project_id="${PROJECT_ID}" query --use_legacy_sql=false --location="${LOCATION}" < "${TMP_SQL_2}"
  rm -f "${TMP_SQL_2}"
else
  echo "[info] Base table ${PROJECT_ID}.${DATASET_ID}.${TARGET_TABLE} not found. Skip staging/SP creation for now."
  echo "       After the first upload creates the base table, re-run this script to create staging and SP."
fi

echo "[done] Test environment prepared. Export these env vars to run tests:"
echo "  export GOOGLE_APPLICATION_CREDENTIALS=\"$(pwd)/${KEY_FILE}\""
echo "  export GCP_PROJECT_ID=\"${PROJECT_ID}\""
echo "  export BIGQUERY_DATASET=\"${DATASET_ID}\""
echo "  export BIGQUERY_TABLE=\"${TARGET_TABLE}\""
echo "  export BIGQUERY_LOCATION=\"${LOCATION}\""


