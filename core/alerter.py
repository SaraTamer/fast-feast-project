import smtplib
import ssl
import os
import threading
from email.mime.text import MIMEText
from dotenv import load_dotenv
load_dotenv()

class Alert:
    def __init__(self):
        self.sender_email = os.getenv("SENDER_EMAIL")
        self.password = os.getenv("EMAIL_PASSWORD")
        self.admin_email = os.getenv("ADMIN_EMAIL")
        self.smtp_server = "smtp.gmail.com"
        self.port = 465

    def _send_email_sync(self, subject, message):
        if not all([self.sender_email, self.password, self.admin_email]):
            print("[ALERTER] ERROR: Missing credentials.")
            return
            
        msg = MIMEText(message)
        msg['Subject'] = subject
        msg['From'] = self.sender_email
        msg['To'] = self.admin_email
        
        context = ssl.create_default_context()
        try:
            with smtplib.SMTP_SSL(self.smtp_server, self.port, context=context) as server:
                server.login(self.sender_email, self.password)
                server.sendmail(self.sender_email, self.admin_email, msg.as_string())
            print(f"[ALERTER] Success: Alert sent to {self.admin_email}")
        except Exception as e:
            print(f"[ALERTER] Failed: {e}")

    def alert_mail(self, subject, message):
        print(f"[ALERTER] Kicking off background alert: {subject}")

        thread = threading.Thread(target=self._send_email_sync, args=(subject, message))
        thread.start()

