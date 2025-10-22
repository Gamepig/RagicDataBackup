# -*- coding: utf-8 -*-
"""
電子郵件通知模組
從 Google Cloud Logging 獲取最新日誌記錄並發送電子郵件通知
"""

import logging
import smtplib
import datetime
import time
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
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

    def _format_logs_with_summary(self, logs: List[Dict[str, Any]], backup_result: Dict[str, Any]) -> str:
        """
        將日誌格式化為 HTML，並加入備份結果摘要

        Args:
            logs: 日誌記錄列表
            backup_result: 備份執行結果

        Returns:
            str: HTML 格式的內容
        """
        # 提取備份結果資訊
        status = backup_result.get('status', 'unknown')
        message = backup_result.get('message', '')
        start_time = backup_result.get('start_time', '')
        end_time = backup_result.get('end_time', '')
        duration = backup_result.get('duration_seconds', 0)
        records_processed = backup_result.get('records_processed', 0)
        last_sync_time = backup_result.get('last_sync_time', '')

        # 決定狀態顏色和圖示
        if status == 'success':
            status_color = '#28a745'
            status_icon = '✅'
            status_text = '成功'
        elif status == 'no_data':
            status_color = '#17a2b8'
            status_icon = 'ℹ️'
            status_text = '無新資料'
        else:
            status_color = '#dc3545'
            status_icon = '❌'
            status_text = '失敗'

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
                .header {{ background-color: #f0f0f0; padding: 15px; border-radius: 5px; }}
                .backup-summary {{ background-color: {status_color}15; padding: 15px; border-left: 5px solid {status_color}; border-radius: 5px; margin: 15px 0; }}
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
                .info-row {{ margin: 5px 0; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h2>ERP 資料備份執行報告</h2>
                <p>報告生成時間: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            </div>

            <div class="backup-summary">
                <h3>{status_icon} 備份執行結果：{status_text}</h3>
                <div class="info-row"><strong>狀態：</strong>{status_text}</div>
                <div class="info-row"><strong>開始時間：</strong>{start_time}</div>
                <div class="info-row"><strong>結束時間：</strong>{end_time}</div>
                <div class="info-row"><strong>耗時：</strong>{duration:.2f} 秒</div>
                <div class="info-row"><strong>處理記錄數：</strong>{records_processed} 筆</div>
        """

        if last_sync_time:
            html += f'<div class="info-row"><strong>上次同步時間（台北時間）：</strong>{last_sync_time}</div>'

        if message:
            html += f'<div class="info-row"><strong>訊息：</strong>{message}</div>'

        if status == 'error' and backup_result.get('error'):
            html += f'<div class="info-row" style="color: #dc3545;"><strong>錯誤訊息：</strong>{backup_result.get("error")}</div>'

        html += """
            </div>

            <div class="summary">
                <h3>📊 日誌統計</h3>
                <p>總計: {log_count} 筆記錄</p>
                <ul>
        """.format(log_count=len(logs))

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
                  text_content: str = None,
                  cc_emails: list = None) -> bool:
        """
        發送電子郵件

        Args:
            to_email: 收件人電子郵件（主要收件人）
            subject: 郵件主旨
            html_content: HTML 內容
            text_content: 純文字內容（可選）
            cc_emails: 副本收件人列表（可選）

        Returns:
            bool: 發送成功返回 True
        """
        if not self.from_email or not self.from_password:
            logging.error("未設定發送者電子郵件或密碼")
            return False

        try:
            # 建立郵件物件
            msg = MIMEMultipart('alternative')
            msg['From'] = self.from_email
            msg['To'] = to_email
            msg['Subject'] = subject
            
            # 添加副本收件人（若有）
            if cc_emails:
                msg['Cc'] = ', '.join(cc_emails)

            # 添加內容
            if text_content:
                msg.attach(MIMEText(text_content, 'plain', 'utf-8'))

            msg.attach(MIMEText(html_content, 'html', 'utf-8'))

            # 準備所有收件人列表（主收件人 + 副本）
            all_recipients = [to_email]
            if cc_emails:
                all_recipients.extend(cc_emails)

            # 連接 SMTP 伺服器並發送
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls()
                server.login(self.from_email, self.from_password)
                server.sendmail(self.from_email, all_recipients, msg.as_string())

            recipient_info = f"收件人: {to_email}"
            if cc_emails:
                recipient_info += f", 副本: {', '.join(cc_emails)}"
            logging.info(f"電子郵件發送成功 - {recipient_info}")
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
        status = backup_result.get('status')

        if status == 'success':
            subject = "✅ ERP 資料備份成功完成"
            filter_string = 'severity="INFO" OR severity="WARNING"'
        elif status == 'no_data':
            subject = "ℹ️ ERP 資料備份通知：Ragic 無新資料可以備份"
            filter_string = 'severity="INFO" OR severity="WARNING"'
        else:
            subject = "❌ ERP 資料備份執行失敗"
            filter_string = 'severity="ERROR" OR severity="WARNING"'

        # 獲取日誌
        logs = notifier.get_latest_logs(filter_string, 100, 2)

        # 以精簡摘要（備份筆數/未備份筆數/執行時間）為主，詳細日誌僅保留數筆錯誤
        backup_count = backup_result.get('records_processed', 0)
        unbackup_count = backup_result.get('invalid_records', 0)
        duration = backup_result.get('duration_seconds', 0)
        unbackup_ids = backup_result.get('unbackup_ids', [])
        error_message = backup_result.get('error')

        # 抽取最近的錯誤與 invalid 記錄
        brief_logs = []
        for log in logs:
            if log.get('severity') in ('ERROR', 'WARNING'):
                brief_logs.append(log)
            if len(brief_logs) >= 10:
                break

        # 針對常見英文錯誤提供中文說明
        def explain_error_zh(msg: str) -> str:
            if not msg:
                return ""
            m = msg.lower()
            if "memory limit" in m and "exceeded" in m:
                return "雲端函式記憶體不足，執行中被系統終止。建議：提高記憶體配額（例如 512Mi/1Gi），或改採單表序列處理以降低單次用量。"
            if "malformed response" in m and "too much memory" in m:
                return "執行過程因記憶體不足導致容器重啟，請調整記憶體或降低每次處理量（分頁/頁數）。"
            if "alts creds ignored" in m:
                return "此為資訊訊息（非錯誤），可忽略。"
            if "sheet_map_json" in m and "parse" in m:
                return "SHEET_MAP_JSON 解析失敗：請確認格式為有效 JSON 或改用 SHEET_MAP_FILE 注入。"
            return ""

        # 簡版 HTML
        html_parts = []
        # 明亮主題（白底、紫色漸層標頭、淺綠結果區塊）
        html_parts.append("<html><head><style>"
                          "body{font-family:Arial,'Noto Sans TC',sans-serif;background:#f3f4f6;color:#111827;margin:0;padding:24px;}"
                          ".card{background:#ffffff;border-radius:14px;overflow:hidden;box-shadow:0 8px 24px rgba(17,24,39,.08);}"
                          ".header{background:linear-gradient(135deg,#6d28d9,#8b5cf6);padding:18px 20px;border-bottom:1px solid #e5e7eb;color:#fff;}"
                          ".title{margin:0;font-size:20px;font-weight:800;letter-spacing:.3px;}"
                          ".badge{display:inline-block;margin-left:8px;padding:4px 12px;border-radius:9999px;background:#22c55e;color:#052e16;font-weight:800;font-size:12px;}"
                          ".body{padding:18px 20px;}"
                          ".result{background:#ecfdf5;border-left:6px solid #34d399;border-radius:12px;padding:14px 16px;margin-top:10px;margin-bottom:12px;}"
                          ".row{display:flex;gap:12px;flex-wrap:wrap;margin:-6px;} .stat{flex:1 1 160px;margin:6px;padding:14px 12px;background:#f9fafb;border:1px solid #e5e7eb;border-radius:10px;}"
                          ".label{color:#6b7280;font-size:12px;text-transform:uppercase;letter-spacing:.06em;margin-bottom:4px} .val{font-size:20px;font-weight:800;color:#111827}"
                          "ul{margin:10px 0 0 18px;padding:0;color:#111827} li{margin:6px 0}"
                          ".muted{color:#6b7280;font-size:12px;margin-top:12px}"
                          "</style></head><body>")
        html_parts.append("<div class='card'>")
        html_parts.append("<div class='header'>")
        html_parts.append(f"<h2 class='title'>ERP 備份結果<span class='badge'>完成</span></h2>")
        html_parts.append("</div>")
        html_parts.append("<div class='body'>")
        # 結果區塊
        html_parts.append("<div class='result'>")
        html_parts.append("<div class='row'>")
        html_parts.append(f"<div class='stat'><div class='label'>備份筆數</div><div class='val'>{backup_count}</div></div>")
        html_parts.append(f"<div class='stat'><div class='label'>未備份筆數</div><div class='val'>{unbackup_count}</div></div>")
        html_parts.append(f"<div class='stat'><div class='label'>執行時間</div><div class='val'>{duration:.2f} 秒</div></div>")
        html_parts.append("</div></div>")

        # 每表備份筆數摘要（若提供 details）
        details = backup_result.get('details') or []
        if isinstance(details, list) and details:
            html_parts.append("<h3 style='margin-top:14px;'>各表備份筆數</h3>")
            html_parts.append("<table style='width:100%;border-collapse:collapse;font-size:14px'>")
            html_parts.append("<thead><tr><th style='text-align:left;padding:8px;border-bottom:1px solid #e5e7eb'>sheet_code</th><th style='text-align:left;padding:8px;border-bottom:1px solid #e5e7eb'>sheet_name</th><th style='text-align:left;padding:8px;border-bottom:1px solid #e5e7eb'>status</th><th style='text-align:right;padding:8px;border-bottom:1px solid #e5e7eb'>uploaded</th><th style='text-align:right;padding:8px;border-bottom:1px solid #e5e7eb'>invalid</th><th style='text-align:left;padding:8px;border-bottom:1px solid #e5e7eb'>error</th></tr></thead>")
            html_parts.append("<tbody>")
            for d in details:
                sc = d.get('sheet_code', '')
                sn = d.get('sheet_name', '')
                up = d.get('uploaded', 0)
                iv = d.get('invalid', 0)
                st = '已更新' if (isinstance(up, int) and up > 0) else ('錯誤' if d.get('error') else '無新資料')
                err = (str(d.get('error') or '')[:120])
                html_parts.append(f"<tr><td style='padding:8px;border-bottom:1px solid #f3f4f6'>{sc}</td><td style='padding:8px;border-bottom:1px solid #f3f4f6'>{sn}</td><td style='padding:8px;border-bottom:1px solid #f3f4f6'>{st}</td><td style='padding:8px;border-bottom:1px solid #f3f4f6;text-align:right'>{up}</td><td style='padding:8px;border-bottom:1px solid #f3f4f6;text-align:right'>{iv}</td><td style='padding:8px;border-bottom:1px solid #f3f4f6'>{err}</td></tr>")
            html_parts.append("</tbody></table>")
        if status == 'error' and error_message:
            html_parts.append(f"<p style='color:#b91c1c'><strong>失敗原因：</strong>{error_message}</p>")
            # 額外中文解釋（若可辨識）
            zh = explain_error_zh(str(error_message))
            if zh:
                html_parts.append(f"<p style='color:#b91c1c'><strong>錯誤說明（中文）：</strong>{zh}</p>")
            # 若備份結果帶有 diagnostics，顯示對外連線自我檢查結果
            diag = backup_result.get('diagnostics') or {}
            if isinstance(diag, dict) and diag:
                html_parts.append("<h3 style='margin-top:14px;'>對外連線診斷</h3>")
                html_parts.append("<table style='width:100%;border-collapse:collapse;font-size:14px'>")
                html_parts.append("<thead><tr><th style='text-align:left;padding:8px;border-bottom:1px solid #e5e7eb'>target</th><th style='text-align:left;padding:8px;border-bottom:1px solid #e5e7eb'>status</th><th style='text-align:right;padding:8px;border-bottom:1px solid #e5e7eb'>elapsed(s)</th><th style='text-align:left;padding:8px;border-bottom:1px solid #e5e7eb'>detail</th></tr></thead>")
                html_parts.append("<tbody>")
                for k, v in diag.items():
                    ok = v.get('ok')
                    status = v.get('status')
                    err = v.get('error')
                    el = v.get('elapsed_s')
                    st = ("OK " + str(status)) if ok else ("ERR")
                    detail = str(err or '')
                    html_parts.append(f"<tr><td style='padding:8px;border-bottom:1px solid #f3f4f6'>{k}</td><td style='padding:8px;border-bottom:1px solid #f3f4f6'>{st}</td><td style='padding:8px;border-bottom:1px solid #f3f4f6;text-align:right'>{el}</td><td style='padding:8px;border-bottom:1px solid #f3f4f6'>{detail[:200]}</td></tr>")
                html_parts.append("</tbody></table>")
        if unbackup_ids:
            html_parts.append(f"<p><strong>未備份ID（前{min(len(unbackup_ids), 20)}筆）：</strong>{', '.join(unbackup_ids[:20])}</p>")
        else:
            html_parts.append("<p><strong>未備份ID：</strong>（本批次無或記錄為空，請參考附帶錯誤摘要）</p>")

        # 未備份樣本（來自備份結果）
        samples = backup_result.get('unbackup_samples') or []
        if samples:
            html_parts.append("<h3 style='margin-top:14px;'>未備份樣本（最多10筆）</h3>")
            html_parts.append("<ul>")
            for s in samples[:10]:
                rid = s.get('ragic_id') or 'unknown'
                err = s.get('error') or 'unknown error'
                sc = s.get('sheet_code') or ''
                html_parts.append(f"<li>[{sc}] ID={rid} - {err}</li>")
            html_parts.append("</ul>")

        if brief_logs:
            html_parts.append("<h3>簡短日誌（錯誤/警告，最多10筆）</h3>")
            html_parts.append("<ul>")
            for bl in brief_logs:
                ts = bl.get('timestamp') or ''
                sv = bl.get('severity') or ''
                msg = ''
                if bl.get('text_payload'):
                    msg = str(bl['text_payload'])
                elif bl.get('json_payload'):
                    import json as _json
                    msg = _json.dumps(bl['json_payload'], ensure_ascii=False)
                html_parts.append(f"<li>[{sv}] {ts} - {msg[:500]}</li>")
                # 就地提供中文說明（若辨識）
                zh = explain_error_zh(msg)
                if zh:
                    html_parts.append(f"<li style='color:#b91c1c'><em>{zh}</em></li>")
            html_parts.append("</ul>")

        html_parts.append("<p class='muted'>本郵件為系統自動通知。如需查詢完整未備份記錄，請使用 query_unbackup.py 以 batch_id 查詢。</p>")
        html_parts.append("</div></div></body></html>")
        html_content = "".join(html_parts)

        # 發送郵件（錯誤狀態才允許重送一次）
        # 添加第二位收件者（副本）
        cc_emails = ['gamepig1976@gmail.com']
        sent = notifier.send_email(to_email, subject, html_content, cc_emails=cc_emails)
        if not sent and status == 'error':
            logging.info("備份發生錯誤，郵件重送一次")
            time.sleep(2)
            sent = notifier.send_email(to_email, subject, html_content, cc_emails=cc_emails)
        return sent

    except Exception as e:
        logging.error(f"發送備份通知失敗: {e}")
        return False