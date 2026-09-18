#!/usr/bin/env bash
set -euo pipefail

# Migrate Cloud Scheduler jobs to the new Cloud Functions v2 entry URL
# - Old: https://asia-east1-<PROJECT>.cloudfunctions.net/backup_erp_data
# - New: https://asia-east1-<PROJECT>.cloudfunctions.net/erp-backup
#
# Usage:
#   PROJECT_ID=b25h01-ragic REGION=asia-east1 bash scripts/migrate_scheduler_entrypoint.sh

PROJECT_ID=${PROJECT_ID:-b25h01-ragic}
REGION=${REGION:-asia-east1}
NEW_URL=${NEW_URL:-"https://${REGION}-${PROJECT_ID}.cloudfunctions.net/erp-backup"}
SA=${SA:-"${PROJECT_ID}@appspot.iam.gserviceaccount.com"}

echo "[INFO] Project=${PROJECT_ID} Region=${REGION} NewURL=${NEW_URL} SA=${SA}"
gcloud config set project "${PROJECT_ID}" --quiet >/dev/null

echo "[INFO] Listing jobs using old path (backup_erp_data)"
JOBS=()
while IFS= read -r line; do
  [[ -n "$line" ]] && JOBS+=("$line")
done < <(gcloud scheduler jobs list \
  --location="${REGION}" \
  --format="value(name)" \
  --filter="httpTarget.uri~backup_erp_data")

if [[ ${#JOBS[@]} -eq 0 ]]; then
  echo "[INFO] No jobs found using old path. Nothing to migrate."
  exit 0
fi

echo "[INFO] Will migrate ${#JOBS[@]} job(s):"
printf ' - %s\n' "${JOBS[@]}"

for JOB in "${JOBS[@]}"; do
  echo "[STEP] Updating ${JOB} --> ${NEW_URL}"
  gcloud scheduler jobs update http "${JOB}" \
    --location="${REGION}" \
    --uri="${NEW_URL}" \
    --http-method=POST \
    --oidc-service-account-email="${SA}" \
    --oidc-token-audience="${NEW_URL}" \
    --attempt-deadline=600s \
    --quiet || {
      echo "[WARN] Failed to update ${JOB}. Check IAM: roles/cloudscheduler.admin";
    }
done

echo "[DONE] Migration attempted. Verify with:"
echo "  gcloud scheduler jobs list --location=${REGION} --format='table(name,httpTarget.uri,httpTarget.oidcToken.serviceAccountEmail)'"
