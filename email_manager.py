import smtplib
import logging
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Optional
import asyncio
from db import get_setting

logger = logging.getLogger("email-manager")


class EmailManager:
    @staticmethod
    def send_email_sync(
        to_email: str,
        subject: str,
        html_body: str,
        host: str,
        port: int,
        username: Optional[str],
        password: Optional[str],
        from_email: str,
        display_name: Optional[str] = None
    ) -> None:
        """Synchronously send an email using SMTP."""
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        
        sender = from_email
        if display_name:
            msg["From"] = f"{display_name} <{from_email}>"
        else:
            msg["From"] = from_email
            
        msg["To"] = to_email

        # Attach HTML body
        msg.attach(MIMEText(html_body, "html"))

        # Determine SSL vs STARTTLS based on port
        if port == 465:
            server = smtplib.SMTP_SSL(host, port, timeout=10)
        else:
            server = smtplib.SMTP(host, port, timeout=10)
            server.ehlo()
            try:
                server.starttls()
                server.ehlo()
            except Exception as exc:
                logger.warning("STARTTLS failed (non-fatal if port doesn't require it): %s", exc)

        if username and password:
            server.login(username, password)

        server.sendmail(sender, [to_email], msg.as_string())
        server.quit()
        logger.info("Email sent successfully to %s", to_email)

    @classmethod
    async def send_email_async(cls, to_email: str, subject: str, html_body: str) -> None:
        """Asynchronously send an email using the database settings."""
        try:
            host = await get_setting("SMTP_HOST", "")
            port_str = await get_setting("SMTP_PORT", "587")
            username = await get_setting("SMTP_USERNAME", "")
            password = await get_setting("SMTP_PASSWORD", "")
            from_email = await get_setting("SMTP_FROM_EMAIL", "")
            display_name = await get_setting("SMTP_DISPLAY_NAME", "")

            if not host or not from_email:
                logger.warning("SMTP not configured. Skipping email sending.")
                return

            try:
                port = int(port_str)
            except ValueError:
                port = 587

            loop = asyncio.get_running_loop()
            await loop.run_in_executor(
                None,
                lambda: cls.send_email_sync(
                    to_email=to_email,
                    subject=subject,
                    html_body=html_body,
                    host=host,
                    port=port,
                    username=username or None,
                    password=password or None,
                    from_email=from_email,
                    display_name=display_name or None
                )
            )
        except Exception as exc:
            logger.error("Failed to send email to %s: %s", to_email, exc)
            # Log error to database
            from db import log_error
            try:
                await log_error("email_manager", f"Failed to send email: {exc}", to_email, "error")
            except Exception:
                pass
