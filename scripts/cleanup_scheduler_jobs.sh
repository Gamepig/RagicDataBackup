#!/usr/bin/env bash
set -euo pipefail

# Cloud Scheduler 清理工具（plan/apply）
# 規則：刪除「未啟用」或「配置不正確」的 HTTP 工作：
# - state != ENABLED
# - httpMethod != POST
# - 缺少 OIDC（serviceAccountEmail 為空）
# - audience/uri 不包含 cloudfunctions.net/erp-backup（可依需求調整）
# - status.code 非空且非 0（最近嘗試失敗）
# 保留名單：以 KEEP_JOBS 陣列指定；命中者即使不符合規則也不刪除

PROJECT_ID=${PROJECT_ID:-"b25h01-ragic"}
REGION=${REGION:-"asia-east1"}
MODE=${1:-"plan"} # plan | apply

# 調整保留名單（正則樣式；任一命中即保留）
KEEP_JOBS=(
  "^projects/${PROJECT_ID}/locations/${REGION}/jobs/erp-backup-weekly$"
  "^projects/${PROJECT_ID}/locations/${REGION}/jobs/erp-backup-agg-weekly$"
)

echo "Project: ${PROJECT_ID} | Region: ${REGION} | Mode: ${MODE}" >&2

mapfile -t JOBS < <(gcloud scheduler jobs list \
  --location="${REGION}" \
  --format="value(name)" || true)

if ((${#JOBS[@]}==0)); then
  echo "No Scheduler jobs found in ${REGION}." >&2
  exit 0
fi

should_keep() {
  local name="$1"
  for pat in "${KEEP_JOBS[@]}"; do
    if [[ "$name" =~ $pat ]]; then return 0; fi
  done
  return 1
}

DEL=()
printf "%-70s %-6s %-28s %-6s %-6s %s\n" "NAME" "STATE" "SA(oidc)" "POST" "ERR" "URI"
printf -- "%.0s-" {1..140}; echo

for n in "${JOBS[@]}"; do
  # 只拿 HTTP 類型；若不是 HTTP，略過
  TYPE=$(gcloud scheduler jobs describe "$n" --location="$REGION" --format="value(type)" || true)
  [[ "$TYPE" != "HTTP" ]] && continue

  read -r STATE METHOD SA URI CODE < <(gcloud scheduler jobs describe "$n" \
    --location="$REGION" \
    --format='value(state, httpTarget.httpMethod, httpTarget.oidcToken.serviceAccountEmail, httpTarget.uri, status.code)')

  POST_OK=$([[ "$METHOD" == "POST" ]] && echo yes || echo no)
  OIDC_OK=$([[ -n "${SA:-}" ]] && echo yes || echo no)
  URI_OK=$([[ "${URI:-}" == *"cloudfunctions.net/erp-backup"* ]] && echo yes || echo no)
  ERR=$([[ -n "${CODE:-}" && "${CODE:-0}" != "0" ]] && echo yes || echo no)

  printf "%-70s %-6s %-28s %-6s %-6s %s\n" "$n" "${STATE:-}" "${SA:-}" "$POST_OK" "$ERR" "${URI:-}"

  # 判斷是否刪除
  if should_keep "$n"; then
    continue
  fi
  if [[ "${STATE:-}" != "ENABLED" || "$POST_OK" != "yes" || "$OIDC_OK" != "yes" || "$URI_OK" != "yes" || "$ERR" == "yes" ]]; then
    DEL+=("$n")
  fi
done

echo
echo "Candidates to delete: ${#DEL[@]}"
printf '%s\n' "${DEL[@]}"

if [[ "$MODE" == "apply" && ${#DEL[@]} -gt 0 ]]; then
  echo "Deleting..." >&2
  for n in "${DEL[@]}"; do
    gcloud scheduler jobs delete "$n" --quiet || true
  done
  echo "Done."
else
  echo "Running in plan mode. To delete, re-run: MODE=apply ./scripts/cleanup_scheduler_jobs.sh apply" >&2
fi


