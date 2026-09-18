#!/bin/bash
#
# Cloud Scheduler OIDC 修復腳本
# 用途：修復 Cloud Scheduler 觸發 Cloud Functions Gen2 的 OIDC 認證問題
# 參考文件：documents/Scheduler_to_GCF_Gen2_OIDC_Fix.md
#
# 使用方式：
#   ./scripts/fix_scheduler_oidc.sh [stage]
#
# Stages:
#   prepare  - 準備與驗證（唯讀操作）
#   fix      - 執行修復（更新 Scheduler 配置）
#   test     - 測試與驗證
#   all      - 執行所有階段（預設）
#

set -euo pipefail  # 錯誤時立即退出

# 顏色輸出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 日誌函數
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_section() {
    echo ""
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo ""
}

# ============================================================================
# 一、前置參數設定
# ============================================================================

# 專案與地區
PROJECT_ID="b25h01-ragic"
REGION="asia-east1"

# Cloud Functions URL（作為目標與 OIDC audience）
FUNCTION_NAME="erp-backup"
CF_URL="https://${REGION}-${PROJECT_ID}.cloudfunctions.net/${FUNCTION_NAME}"

# 簽發 OIDC 的服務帳號（App Engine 預設 SA）
SA_EMAIL="${PROJECT_ID}@appspot.iam.gserviceaccount.com"

# 日誌與檢查點目錄
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_DIR="${SCRIPT_DIR}/logs"
CHECKPOINT_FILE="${LOG_DIR}/.checkpoint"

# 建立日誌目錄
mkdir -p "${LOG_DIR}"

# 執行階段
STAGE="${1:-all}"

# ============================================================================
# 二、前置檢查
# ============================================================================

check_prerequisites() {
    log_section "前置檢查"

    # 檢查必要工具
    local required_tools=("gcloud" "curl" "jq")
    for tool in "${required_tools[@]}"; do
        if ! command -v "$tool" &> /dev/null; then
            log_error "缺少必要工具: $tool"
            log_info "請安裝: brew install $tool"
            exit 1
        fi
        log_success "✓ $tool 已安裝"
    done

    # 檢查 GCP 認證
    log_info "檢查 GCP 認證狀態..."
    if ! gcloud auth list --filter=status:ACTIVE --format="value(account)" &> /dev/null; then
        log_error "GCP 認證失敗"
        log_info "請執行: gcloud auth login"
        exit 1
    fi

    local active_account
    active_account=$(gcloud auth list --filter=status:ACTIVE --format="value(account)" | head -n1)
    log_success "✓ 已認證帳號: ${active_account}"

    # 設定當前專案
    log_info "設定 GCP 專案: ${PROJECT_ID}"
    gcloud config set project "${PROJECT_ID}" --quiet

    # 驗證專案存取權限
    if ! gcloud projects describe "${PROJECT_ID}" &> /dev/null; then
        log_error "無法存取專案: ${PROJECT_ID}"
        exit 1
    fi
    log_success "✓ 專案存取權限正常"

    echo ""
}

# ============================================================================
# 三、準備階段（唯讀操作）
# ============================================================================

stage_prepare() {
    log_section "階段一：準備與驗證（唯讀操作）"

    # 1. 列出 Cloud Scheduler 工作
    log_info "列出 Cloud Scheduler 工作..."
    echo ""
    gcloud scheduler jobs list \
        --location="${REGION}" \
        --format="table(name, type, httpTarget.uri, schedule, state)"
    echo ""

    # 2. 自動尋找目標工作
    log_info "尋找與 erp-backup 相關的 Scheduler 工作..."

    # 優先尋找名稱為 "erp-backup-weekly" 的工作（主要週期性備份）
    JOB_NAME="$(gcloud scheduler jobs list \
        --location="${REGION}" \
        --filter='name:"erp-backup-weekly"' \
        --format='value(name)' | head -n1)"

    # 如果沒找到，嘗試尋找指向 cloudfunctions.net/erp-backup 的工作
    if [ -z "${JOB_NAME}" ]; then
        log_warning "未找到 'erp-backup-weekly'，嘗試尋找指向 erp-backup 函式的工作"
        JOB_NAME="$(gcloud scheduler jobs list \
            --location="${REGION}" \
            --format='table(name,uri)' | grep 'cloudfunctions.net/erp-backup' | awk '{print $1}' | head -n1)"
    fi

    # 如果還是沒找到，列出所有包含 erp-backup 的工作讓用戶選擇
    if [ -z "${JOB_NAME}" ]; then
        log_warning "自動查找失敗，以下是所有 erp-backup 相關工作："
        echo ""
        gcloud scheduler jobs list \
            --location="${REGION}" \
            --filter='name:"erp-backup"' \
            --format='table(name,uri)'
        echo ""
        log_error "請手動設定 JOB_NAME 環境變數後重新執行"
        log_info "範例: export JOB_NAME='erp-backup-weekly' && $0 prepare"
        exit 1
    fi

    log_success "✓ 找到目標工作: ${JOB_NAME}"

    # 儲存到環境變數檔案供後續階段使用
    echo "export JOB_NAME='${JOB_NAME}'" > "${LOG_DIR}/.env"
    echo "export PROJECT_ID='${PROJECT_ID}'" >> "${LOG_DIR}/.env"
    echo "export REGION='${REGION}'" >> "${LOG_DIR}/.env"
    echo "export CF_URL='${CF_URL}'" >> "${LOG_DIR}/.env"
    echo "export SA_EMAIL='${SA_EMAIL}'" >> "${LOG_DIR}/.env"

    # 3. 顯示當前配置
    log_info "當前 Scheduler 工作詳細配置："
    echo ""
    gcloud scheduler jobs describe "${JOB_NAME}" \
        --location="${REGION}" \
        --format="yaml(httpTarget.uri, httpTarget.oidcToken.serviceAccountEmail, httpTarget.oidcToken.audience, schedule, state)"
    echo ""

    # 4. 檢查 Cloud Function 是否存在
    log_info "檢查 Cloud Function 狀態..."
    if gcloud functions describe "${FUNCTION_NAME}" \
        --region="${REGION}" \
        --gen2 \
        --format="value(state)" &> /dev/null; then

        local func_state
        func_state=$(gcloud functions describe "${FUNCTION_NAME}" \
            --region="${REGION}" \
            --gen2 \
            --format="value(state)")
        log_success "✓ Cloud Function 狀態: ${func_state}"

        local func_url
        func_url=$(gcloud functions describe "${FUNCTION_NAME}" \
            --region="${REGION}" \
            --gen2 \
            --format="value(url)")
        log_info "Cloud Function URL: ${func_url}"

        if [ "${func_url}" != "${CF_URL}" ]; then
            log_warning "注意：實際 URL 與預期不符"
            log_warning "預期: ${CF_URL}"
            log_warning "實際: ${func_url}"
        fi
    else
        log_error "Cloud Function '${FUNCTION_NAME}' 不存在或無法存取"
        exit 1
    fi

    echo ""
    log_section "準備階段完成"
    log_info "目標 Scheduler 工作: ${JOB_NAME}"
    log_info "Cloud Functions URL: ${CF_URL}"
    log_info "服務帳號: ${SA_EMAIL}"
    echo ""

    # 儲存檢查點
    echo "prepare" > "${CHECKPOINT_FILE}"
}

# ============================================================================
# 四、修復階段（更新 Scheduler 配置）
# ============================================================================

stage_fix() {
    log_section "階段二：修復 Scheduler OIDC 配置"

    # 載入環境變數
    if [ -f "${LOG_DIR}/.env" ]; then
        source "${LOG_DIR}/.env"
    else
        log_error "請先執行 'prepare' 階段"
        exit 1
    fi

    log_warning "即將更新 Cloud Scheduler 工作配置："
    echo ""
    echo "  工作名稱: ${JOB_NAME}"
    echo "  目標 URL: ${CF_URL}"
    echo "  OIDC 服務帳號: ${SA_EMAIL}"
    echo "  OIDC Audience: ${CF_URL}"
    echo ""

    read -p "確認執行更新？(yes/no): " -r
    if [[ ! $REPLY =~ ^[Yy](es)?$ ]]; then
        log_info "取消更新"
        exit 0
    fi

    log_info "更新 Scheduler 工作..."
    if gcloud scheduler jobs update http "${JOB_NAME}" \
        --location="${REGION}" \
        --uri="${CF_URL}" \
        --http-method=POST \
        --oidc-service-account-email="${SA_EMAIL}" \
        --oidc-token-audience="${CF_URL}"; then

        log_success "✓ Scheduler 工作更新成功"
    else
        log_error "Scheduler 工作更新失敗"
        exit 1
    fi

    echo ""
    log_info "更新後的配置："
    gcloud scheduler jobs describe "${JOB_NAME}" \
        --location="${REGION}" \
        --format="yaml(httpTarget.uri, httpTarget.oidcToken.serviceAccountEmail, httpTarget.oidcToken.audience)"
    echo ""

    # 儲存檢查點
    echo "fix" > "${CHECKPOINT_FILE}"

    log_section "修復階段完成"
}

# ============================================================================
# 五、測試階段
# ============================================================================

stage_test() {
    log_section "階段三：測試與驗證"

    # 載入環境變數
    if [ -f "${LOG_DIR}/.env" ]; then
        source "${LOG_DIR}/.env"
    else
        log_error "請先執行 'prepare' 和 'fix' 階段"
        exit 1
    fi

    # 1. 手動觸發 Scheduler
    log_info "手動觸發 Scheduler 工作..."
    if gcloud scheduler jobs run "${JOB_NAME}" --location="${REGION}"; then
        log_success "✓ Scheduler 觸發成功"
    else
        log_error "Scheduler 觸發失敗"
    fi

    echo ""
    log_info "等待 10 秒讓執行完成..."
    sleep 10

    # 2. 查看 Cloud Run Revision 日誌
    log_info "查看最近 30 分鐘的 Cloud Run 日誌..."
    echo ""
    gcloud logging read \
        'resource.type="cloud_run_revision" AND resource.labels.service_name="'"${FUNCTION_NAME}"'"' \
        --project="${PROJECT_ID}" \
        --limit=50 \
        --freshness=30m \
        --format='table(timestamp, severity, textPayload.slice(0:100), jsonPayload.message.slice(0:100))' \
        2>/dev/null || log_warning "無 Cloud Run 日誌或查詢失敗"
    echo ""

    # 3. 查看 Cloud Function v2 日誌
    log_info "查看最近 30 分鐘的 Cloud Function 日誌..."
    echo ""
    gcloud logging read \
        'resource.type="cloud_function" AND resource.labels.function_name="'"${FUNCTION_NAME}"'"' \
        --project="${PROJECT_ID}" \
        --limit=50 \
        --freshness=30m \
        --format='table(timestamp, severity, textPayload.slice(0:100), jsonPayload.message.slice(0:100))' \
        2>/dev/null || log_warning "無 Cloud Function 日誌或查詢失敗"
    echo ""

    # 4. （可選）使用 OIDC 直接呼叫函式
    log_info "測試 OIDC 直接呼叫（避免寄信）..."
    echo ""

    if TOKEN="$(gcloud auth print-identity-token \
        --audiences="${CF_URL}" \
        --impersonate-service-account="${SA_EMAIL}" 2>&1)"; then

        log_success "✓ OIDC Token 取得成功"

        # 測試呼叫（單表，避免寄信）
        log_info "執行測試呼叫..."
        HTTP_RESPONSE=$(curl -s -w "\n%{http_code}" -X POST "${CF_URL}" \
            -H "Authorization: Bearer ${TOKEN}" \
            -H "Content-Type: application/json" \
            -d '{"DISABLE_EMAIL": true}')

        HTTP_CODE=$(echo "$HTTP_RESPONSE" | tail -n1)
        HTTP_BODY=$(echo "$HTTP_RESPONSE" | sed '$d')

        echo "HTTP Status: ${HTTP_CODE}"
        echo "Response Body:"
        echo "${HTTP_BODY}" | jq '.' 2>/dev/null || echo "${HTTP_BODY}"
        echo ""

        if [ "${HTTP_CODE}" = "200" ]; then
            log_success "✓ OIDC 呼叫測試成功（HTTP 200）"
        else
            log_warning "OIDC 呼叫返回 HTTP ${HTTP_CODE}"
        fi
    else
        log_error "無法取得 OIDC Token"
        log_warning "錯誤訊息: ${TOKEN}"
        log_info "請檢查服務帳號權限"
    fi

    echo ""

    # 儲存檢查點
    echo "test" > "${CHECKPOINT_FILE}"

    log_section "測試階段完成"
}

# ============================================================================
# 六、驗證階段（BigQuery 資料品質檢查）
# ============================================================================

stage_verify() {
    log_section "階段四：BigQuery 資料品質驗證"

    log_info "執行 BigQuery 資料品質檢查..."
    echo ""

    # 檢查 SQL 檔案是否存在
    SQL_FILE="${SCRIPT_DIR}/verify_data_quality.sql"
    if [ ! -f "${SQL_FILE}" ]; then
        log_warning "SQL 檔案不存在: ${SQL_FILE}"
        log_info "請手動執行 BigQuery 查詢驗證資料品質"
        return
    fi

    log_info "執行資料品質查詢（參考 ${SQL_FILE}）..."
    log_info "請手動在 BigQuery Console 或使用 bq 命令執行查詢"
    echo ""

    # 儲存檢查點
    echo "verify" > "${CHECKPOINT_FILE}"
}

# ============================================================================
# 主程式
# ============================================================================

main() {
    log_section "Cloud Scheduler OIDC 修復工具"
    log_info "專案: ${PROJECT_ID}"
    log_info "區域: ${REGION}"
    log_info "函式: ${FUNCTION_NAME}"
    log_info "執行階段: ${STAGE}"
    echo ""

    check_prerequisites

    case "${STAGE}" in
        prepare)
            stage_prepare
            ;;
        fix)
            stage_fix
            ;;
        test)
            stage_test
            ;;
        verify)
            stage_verify
            ;;
        all)
            stage_prepare
            echo ""
            read -p "繼續執行修復階段？(yes/no): " -r
            if [[ $REPLY =~ ^[Yy](es)?$ ]]; then
                stage_fix
                stage_test
                stage_verify
            else
                log_info "已停止，您可以稍後執行: $0 fix"
            fi
            ;;
        *)
            log_error "未知的階段: ${STAGE}"
            echo "使用方式: $0 [prepare|fix|test|verify|all]"
            exit 1
            ;;
    esac

    echo ""
    log_section "完成"
    log_success "所有階段執行完畢"
    log_info "日誌位置: ${LOG_DIR}"
    echo ""
}

main "$@"
