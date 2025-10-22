#!/bin/bash

# 確保腳本在遇到錯誤時終止
set -e

# 檢查 uv 是否已安裝
if ! command -v uv &> /dev/null
then
    echo "uv 尚未安裝。請執行以下指令安裝："
    echo "curl -LsSf https://astral.sh/uv/install.sh | sh"
    exit 1
fi

# 建立或更新虛擬環境
echo "正在建立或更新虛擬環境..."
uv venv

# 啟動虛擬環境
source .venv/bin/activate

# 安裝或更新依賴項
echo "正在安裝或更新依賴項..."
uv pip install -r requirements.txt

# 檢查 .env.local.yaml 是否存在
if [ ! -f ".env.local.yaml" ]; then
    echo "錯誤: .env.local.yaml 檔案不存在。請手動創建它並填寫配置。"
    exit 1
fi

# 執行 Python 程式，載入 .env.local.yaml 環境變數
echo "正在執行本地資料抓取測試..."
python -m dotenv -f .env.local.yaml python erp_backup_main.py

echo "本地資料抓取測試執行完成。請檢查上方日誌輸出。"
