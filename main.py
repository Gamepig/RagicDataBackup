"""
Cloud Function 入口點檔案
此檔案為 Google Cloud Functions 部署所需的標準入口檔案 (main.py)
實際業務邏輯在 erp_backup_main.py 中實作
"""

from erp_backup_main import backup_erp_data

# 匯出 Cloud Function 入口點
__all__ = ['backup_erp_data']
