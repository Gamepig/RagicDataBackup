#!/bin/bash
#
# Cloud Scheduler 重建腳本（包含 OIDC 配置）
# 用途：刪除並重建 erp-backup-weekly，確保 OIDC 正確配置
#

set -euo pipefail

# 顏色
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${BLUE}Cloud Scheduler 重建腳本（包含 OIDC 配置）${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""

# 設定參數
PROJECT_ID="b25h01-ragic"
REGION="asia-east1"
JOB_NAME="erp-backup-weekly"
CF_URL="https://asia-east1-${PROJECT_ID}.cloudfunctions.net/erp-backup"
SA_EMAIL="${PROJECT_ID}@appspot.iam.gserviceaccount.com"
SCHEDULE="0 3 * * 1"
TIMEZONE="Etc/UTC"

echo -e "${YELLOW}⚠️  即將執行以下操作：${NC}"
echo ""
echo "  1. 備份當前 Scheduler 配置"
echo "  2. 刪除 ${JOB_NAME}"
echo "  3. 重建 ${JOB_NAME}（包含 OIDC Token）"
echo ""
echo -e "${BLUE}新配置參數：${NC}"
echo "  專案: ${PROJECT_ID}"
echo "  區域: ${REGION}"
echo "  URL: ${CF_URL}"
echo "  HTTP 方法: POST"
echo "  服務帳號: ${SA_EMAIL}"
echo "  Audience: ${CF_URL}"
echo "  排程: ${SCHEDULE} (每週一 03:00 UTC)"
echo ""

read -p "確認執行？(yes/no): " -r
if [[ ! $REPLY =~ ^[Yy](es)?$ ]]; then
    echo -e "${YELLOW}取消操作${NC}"
    exit 0
fi

echo ""
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${BLUE}步驟 1: 備份當前配置${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

BACKUP_FILE="$(dirname "$0")/logs/erp-backup-weekly-backup-$(date +%Y%m%d-%H%M%S).yaml"
mkdir -p "$(dirname "$0")/logs"

gcloud scheduler jobs describe "${JOB_NAME}" \
  --location="${REGION}" \
  --format=yaml > "${BACKUP_FILE}"

echo -e "${GREEN}✓ 備份已儲存至: ${BACKUP_FILE}${NC}"
echo ""

echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${BLUE}步驟 2: 刪除現有工作${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

if gcloud scheduler jobs delete "${JOB_NAME}" \
  --location="${REGION}" \
  --quiet; then
  echo -e "${GREEN}✓ 已刪除 ${JOB_NAME}${NC}"
else
  echo -e "${RED}✗ 刪除失敗${NC}"
  exit 1
fi

echo ""
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${BLUE}步驟 3: 重建工作（包含 OIDC）${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

if gcloud scheduler jobs create http "${JOB_NAME}" \
  --location="${REGION}" \
  --schedule="${SCHEDULE}" \
  --time-zone="${TIMEZONE}" \
  --uri="${CF_URL}" \
  --http-method=POST \
  --oidc-service-account-email="${SA_EMAIL}" \
  --oidc-token-audience="${CF_URL}" \
  --attempt-deadline=180s; then
  echo -e "${GREEN}✓ 已成功重建 ${JOB_NAME}${NC}"
else
  echo -e "${RED}✗ 重建失敗${NC}"
  echo -e "${YELLOW}您可以從備份恢復: ${BACKUP_FILE}${NC}"
  exit 1
fi

echo ""
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${BLUE}步驟 4: 驗證新配置${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

echo ""
echo "完整配置："
gcloud scheduler jobs describe "${JOB_NAME}" \
  --location="${REGION}" \
  --format=yaml | grep -A 15 "httpTarget:"

echo ""
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${BLUE}驗證檢查${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

CONFIG=$(gcloud scheduler jobs describe "${JOB_NAME}" --location="${REGION}" --format=yaml)

if echo "$CONFIG" | grep -q "httpMethod: POST"; then
  echo -e "${GREEN}✅ HTTP 方法: POST${NC}"
else
  echo -e "${RED}❌ HTTP 方法錯誤${NC}"
fi

if echo "$CONFIG" | grep -q "oidcToken:"; then
  echo -e "${GREEN}✅ OIDC Token 已配置${NC}"
  echo "$CONFIG" | grep -A 2 "oidcToken:" | sed 's/^/   /'
else
  echo -e "${RED}❌ 缺少 OIDC Token${NC}"
fi

if echo "$CONFIG" | grep -q "audience: ${CF_URL}"; then
  echo -e "${GREEN}✅ Audience 正確${NC}"
else
  echo -e "${YELLOW}⚠️  Audience 可能不正確${NC}"
fi

echo ""
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${GREEN}重建完成！${NC}"
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""
echo "備份位置: ${BACKUP_FILE}"
echo ""
echo "下一步："
echo "  1. 手動觸發測試: gcloud scheduler jobs run ${JOB_NAME} --location=${REGION}"
echo "  2. 查看日誌驗證"
echo ""
