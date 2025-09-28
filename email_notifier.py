# -*- coding: utf-8 -*-
"""
電子郵件通知模組
從 Google Cloud Logging 獲取最新日誌記錄並發送電子郵件通知
"""

import logging
import smtplib
import datetime
from email.mime.text import MimeText
from email.mime.multipart import MimeMultipart
from typing import List, Dict, Any, Optional
from google.cloud import logging as cloud_logging


class EmailNotifier:
    """電子郵件通知器類別"""

    def __init__(self,
                 project_id: str,
                 smtp_server: str = "smtp.gmail.com",
                 smtp_port: int = 587,
                 from_email: str = None,
                 from_password: str = None):
        """
        初始化電子郵件通知器

        Args:
            project_id: GCP 專案 ID
            smtp_server: SMTP 伺服器地址
            smtp_port: SMTP 埠號
            from_email: 發送者電子郵件
            from_password: 發送者電子郵件密碼或應用程式密碼
        """
        if not project_id:
            raise ValueError("專案 ID 不可為空")

        self.project_id = project_id
        self.smtp_server = smtp_server
        self.smtp_port = smtp_port
        self.from_email = from_email
        self.from_password = from_password

        # 初始化 Cloud Logging 客戶端
        try:
            self.logging_client = cloud_logging.Client(project=project_id)
            logging.info(f"Cloud Logging 客戶端初始化完成 - 專案: {project_id}")
        except Exception as e:
            raise Exception(f"無法建立 Cloud Logging 客戶端: {e}")

    def get_latest_logs(self,
                       filter_string: str = None,
                       limit: int = 50,
                       hours_back: int = 1) -> List[Dict[str, Any]]:
        """
        從 Cloud Logging 獲取最新日誌記錄

        Args:
            filter_string: 日誌過濾條件
            limit: 最大記錄數
            hours_back: 往回查詢的小時數

        Returns:
            List[Dict]: 日誌記錄列表
        """
        try:
            # 計算時間範圍
            end_time = datetime.datetime.now(datetime.timezone.utc)
            start_time = end_time - datetime.timedelta(hours=hours_back)

            # 建構過濾條件
            base_filter = f'timestamp >= "{start_time.isoformat()}" AND timestamp <= "{end_time.isoformat()}"'

            if filter_string:
                full_filter = f"{base_filter} AND ({filter_string})"
            else:
                # 預設過濾 ERP 備份相關日誌
                full_filter = f'{base_filter} AND (resource.type="cloud_function" OR jsonPayload.component="erp_backup" OR textPayload:"ERP" OR textPayload:"backup" OR textPayload:"Ragic" OR textPayload:"BigQuery")'

            logging.info(f"查詢日誌 - 時間範圍: {hours_back} 小時, 限制: {limit} 筆")

            # 查詢日誌
            entries = self.logging_client.list_entries(
                filter_=full_filter,
                order_by=cloud_logging.DESCENDING,
                max_results=limit
            )

            logs = []
            for entry in entries:
                log_data = {
                    "timestamp": entry.timestamp.isoformat() if entry.timestamp else None,
                    "severity": entry.severity,
                    "resource": getattr(entry.resource, 'type', 'unknown'),
                    "log_name": entry.log_name,
                    "text_payload": getattr(entry, 'payload', ''),
                    "json_payload": getattr(entry, 'json_payload', {}),
                    "source_location": getattr(entry, 'source_location', {}),
                    "http_request": getattr(entry, 'http_request', {})
                }
                logs.append(log_data)

            logging.info(f"成功獲取 {len(logs)} 筆日誌記錄")
            return logs

        except Exception as e:
            logging.error(f"獲取日誌失敗: {e}")
            raise

    def format_logs_to_html(self, logs: List[Dict[str, Any]], title: str = "ERP 備份系統日誌報告") -> str:
        """
        將日誌格式化為 HTML

        Args:
            logs: 日誌記錄列表
            title: 電子郵件標題

        Returns:
            str: HTML 格式的日誌內容
        """
        if not logs:
            return f"""
            <html>
            <body>
                <h2>{title}</h2>
                <p>在指定時間範圍內沒有找到相關日誌記錄。</p>
                <p>查詢時間: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            </body>
            </html>
            """

        # 統計日誌等級
        severity_counts = {}
        for log in logs:
            severity = log.get('severity', 'UNKNOWN')
            severity_counts[severity] = severity_counts.get(severity, 0) + 1

        # 建構 HTML
        html = f"""
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; }}
                .header {{ background-color: #f0f0f0; padding: 10px; border-radius: 5px; }}
                .summary {{ background-color: #e8f4fd; padding: 10px; border-radius: 5px; margin: 10px 0; }}
                .log-entry {{ border: 1px solid #ddd; margin: 10px 0; padding: 10px; border-radius: 5px; }}
                .severity-ERROR {{ border-left: 5px solid #dc3545; }}
                .severity-WARNING {{ border-left: 5px solid #ffc107; }}
                .severity-INFO {{ border-left: 5px solid #17a2b8; }}
                .severity-DEBUG {{ border-left: 5px solid #6c757d; }}
                .timestamp {{ color: #666; font-size: 0.9em; }}
                .severity {{ font-weight: bold; padding: 2px 8px; border-radius: 3px; color: white; }}
                .severity-ERROR {{ background-color: #dc3545; }}
                .severity-WARNING {{ background-color: #ffc107; color: black; }}
                .severity-INFO {{ background-color: #17a2b8; }}
                .severity-DEBUG {{ background-color: #6c757d; }}
                .message {{ margin-top: 5px; }}
                pre {{ background-color: #f8f9fa; padding: 10px; border-radius: 3px; overflow-x: auto; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h2>{title}</h2>
                <p>報告生成時間: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            </div>

            <div class="summary">
                <h3>📊 日誌統計</h3>
                <p>總計: {len(logs)} 筆記錄</p>
                <ul>
        """

        for severity, count in sorted(severity_counts.items()):
            html += f"<li>{severity}: {count} 筆</li>"

        html += """
                </ul>
            </div>

            <h3>📋 詳細日誌記錄</h3>
        """

        # 添加每筆日誌記錄
        for i, log in enumerate(logs[:30]):  # 限制顯示前30筆
            severity = log.get('severity', 'UNKNOWN')
            timestamp = log.get('timestamp', '')

            # 獲取日誌內容
            message = ""
            if log.get('text_payload'):
                message = str(log['text_payload'])
            elif log.get('json_payload'):
                import json
                message = json.dumps(log['json_payload'], indent=2, ensure_ascii=False)

            # 格式化時間戳
            try:
                if timestamp:
                    dt = datetime.datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                    formatted_time = dt.strftime('%Y-%m-%d %H:%M:%S')
                else:
                    formatted_time = "未知時間"
            except:
                formatted_time = timestamp

            html += f"""
            <div class="log-entry severity-{severity}">
                <div>
                    <span class="timestamp">{formatted_time}</span>
                    <span class="severity severity-{severity}">{severity}</span>
                    <span style="margin-left: 10px;">來源: {log.get('resource', 'unknown')}</span>
                </div>
                <div class="message">
                    <pre>{message}</pre>
                </div>
            </div>
            """

        if len(logs) > 30:
            html += f"<p><em>顯示前 30 筆記錄，總計 {len(logs)} 筆</em></p>"

        html += """
        </body>
        </html>
        """

        return html

    def send_email(self,
                  to_email: str,
                  subject: str,
                  html_content: str,
                  text_content: str = None) -> bool:
        """
        發送電子郵件

        Args:
            to_email: 收件人電子郵件
            subject: 郵件主旨
            html_content: HTML 內容
            text_content: 純文字內容（可選）

        Returns:
            bool: 發送成功返回 True
        """
        if not self.from_email or not self.from_password:
            logging.error("未設定發送者電子郵件或密碼")
            return False

        try:
            # 建立郵件物件
            msg = MimeMultipart('alternative')
            msg['From'] = self.from_email
            msg['To'] = to_email
            msg['Subject'] = subject

            # 添加內容
            if text_content:
                msg.attach(MimeText(text_content, 'plain', 'utf-8'))

            msg.attach(MimeText(html_content, 'html', 'utf-8'))

            # 連接 SMTP 伺服器並發送
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls()
                server.login(self.from_email, self.from_password)
                server.send_message(msg)

            logging.info(f"電子郵件發送成功 - 收件人: {to_email}")
            return True

        except Exception as e:
            logging.error(f"電子郵件發送失敗: {e}")
            return False

    def send_latest_logs(self,
                        to_email: str,
                        subject: str = None,
                        filter_string: str = None,
                        hours_back: int = 1,
                        limit: int = 50) -> bool:
        """
        發送最新日誌記錄

        Args:
            to_email: 收件人電子郵件
            subject: 郵件主旨（可選）
            filter_string: 日誌過濾條件
            hours_back: 往回查詢的小時數
            limit: 最大記錄數

        Returns:
            bool: 發送成功返回 True
        """
        try:
            # 獲取最新日誌
            logs = self.get_latest_logs(filter_string, limit, hours_back)

            # 生成郵件主旨
            if not subject:
                error_count = sum(1 for log in logs if log.get('severity') == 'ERROR')
                if error_count > 0:
                    subject = f"⚠️ ERP 備份系統日誌報告 - 發現 {error_count} 個錯誤"
                else:
                    subject = "✅ ERP 備份系統日誌報告 - 運行正常"

            # 格式化為 HTML
            html_content = self.format_logs_to_html(logs)

            # 發送郵件
            return self.send_email(to_email, subject, html_content)

        except Exception as e:
            logging.error(f"發送日誌報告失敗: {e}")
            return False

    def test_email_connection(self) -> bool:
        """
        測試電子郵件連線

        Returns:
            bool: 連線成功返回 True
        """
        if not self.from_email or not self.from_password:
            logging.error("未設定發送者電子郵件或密碼")
            return False

        try:
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls()
                server.login(self.from_email, self.from_password)

            logging.info("電子郵件連線測試成功")
            return True

        except Exception as e:
            logging.error(f"電子郵件連線測試失敗: {e}")
            return False


def create_notifier(project_id: str, **kwargs) -> EmailNotifier:
    """
    建立電子郵件通知器的工廠函數

    Args:
        project_id: GCP 專案 ID
        **kwargs: 其他設定參數

    Returns:
        EmailNotifier: 電子郵件通知器實例
    """
    return EmailNotifier(project_id, **kwargs)


def send_backup_notification(project_id: str,
                           to_email: str,
                           backup_result: Dict[str, Any],
                           smtp_config: Dict[str, str]) -> bool:
    """
    發送備份結果通知的便利函數

    Args:
        project_id: GCP 專案 ID
        to_email: 收件人電子郵件
        backup_result: 備份執行結果
        smtp_config: SMTP 設定（包含 from_email, from_password 等）

    Returns:
        bool: 發送成功返回 True
    """
    try:
        notifier = create_notifier(
            project_id=project_id,
            from_email=smtp_config.get('from_email'),
            from_password=smtp_config.get('from_password'),
            smtp_server=smtp_config.get('smtp_server', 'smtp.gmail.com'),
            smtp_port=int(smtp_config.get('smtp_port', 587))
        )

        # 根據備份結果決定郵件內容
        if backup_result.get('status') == 'success':
            subject = "✅ ERP 資料備份成功完成"
            filter_string = 'severity="INFO" OR severity="WARNING"'
        else:
            subject = "❌ ERP 資料備份執行失敗"
            filter_string = 'severity="ERROR" OR severity="WARNING"'

        return notifier.send_latest_logs(
            to_email=to_email,
            subject=subject,
            filter_string=filter_string,
            hours_back=2,
            limit=100
        )

    except Exception as e:
        logging.error(f"發送備份通知失敗: {e}")
        return False