import asyncio
import smtplib
import ssl
import logging
import os
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from db import get_setting, log_error

logger = logging.getLogger("email-manager")


def _send_email_sync(smtp_host, smtp_port, smtp_username, smtp_password, smtp_from, display_name, to_email, subject, body):
    msg = MIMEMultipart()
    msg['From'] = f"{display_name} <{smtp_from}>" if display_name else smtp_from
    msg['To'] = to_email
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain', 'utf-8'))

    port = int(smtp_port)
    if port == 465:
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL(smtp_host, port, context=context, timeout=12) as server:
            server.login(smtp_username, smtp_password)
            server.sendmail(smtp_from, to_email, msg.as_string())
    else:
        # Defaults to STARTTLS (typically port 587 or other custom ports)
        with smtplib.SMTP(smtp_host, port, timeout=12) as server:
            server.ehlo()
            if port == 587 or "starttls" in smtp_host.lower():
                context = ssl.create_default_context()
                server.starttls(context=context)
                server.ehlo()
            server.login(smtp_username, smtp_password)
            server.sendmail(smtp_from, to_email, msg.as_string())


async def send_email_async(to_email: str, subject: str, body: str) -> bool:
    try:
        smtp_host = await get_setting("SMTP_HOST", "") or os.getenv("SMTP_HOST", "")
        smtp_port = await get_setting("SMTP_PORT", "") or os.getenv("SMTP_PORT", "")
        smtp_username = await get_setting("SMTP_USERNAME", "") or os.getenv("SMTP_USERNAME", "")
        smtp_password = await get_setting("SMTP_PASSWORD", "") or os.getenv("SMTP_PASSWORD", "")
        smtp_from = await get_setting("SMTP_FROM", "") or os.getenv("SMTP_FROM", "")
        display_name = await get_setting("SMTP_DISPLAY_NAME", "") or os.getenv("SMTP_DISPLAY_NAME", "")

        if not all([smtp_host, smtp_port, smtp_username, smtp_password, smtp_from]):
            logger.warning("SMTP settings not fully configured. Cannot send email.")
            await log_error(
                "email",
                "SMTP settings incomplete",
                f"host={smtp_host}, port={smtp_port}, from={smtp_from}",
                "warning"
            )
            return False

        # Run synchronous blocking SMTP calls inside an executor thread so we don't freeze the event loop.
        await asyncio.to_thread(
            _send_email_sync,
            smtp_host,
            smtp_port,
            smtp_username,
            smtp_password,
            smtp_from,
            display_name,
            to_email,
            subject,
            body
        )
        await log_error("email", "Email sent successfully", f"To: {to_email}, Subject: {subject}", "info")
        return True
    except Exception as exc:
        logger.exception("Failed to send email to %s", to_email)
        await log_error("email", "Email sending failed", f"To: {to_email}; Error: {exc}", "error")
        return False
