import asyncio
import smtplib
import ssl
import logging
import os
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from db import get_setting, log_error

logger = logging.getLogger("email-manager")

DEFAULT_BOOKING_EMAIL_SUBJECT_TEMPLATE = "Your appointment with {business_name} is confirmed"
DEFAULT_BOOKING_EMAIL_BODY_TEMPLATE = """Hi {lead_name},

Your {service_type} appointment has been booked.

Date: {date}
Time: {time}
Booking ID: {booking_id}

If you need to reschedule, please reply to this email.

Thank you,
{business_name}"""


class _SafeFormatDict(dict):
    def __missing__(self, key):
        return "{" + key + "}"


def _redact_email(email: str) -> str:
    if not email or "@" not in email:
        return "[missing]"
    local, domain = email.split("@", 1)
    if not local:
        return f"*@{domain}"
    return f"{local[:1]}***@{domain}"


def render_template(template: str, values: dict) -> str:
    return (template or "").replace("\\n", "\n").format_map(_SafeFormatDict(values))


async def render_booking_email(values: dict) -> tuple[str, str, str]:
    subject_template = (
        await get_setting("BOOKING_EMAIL_SUBJECT_TEMPLATE", "")
        or os.getenv("BOOKING_EMAIL_SUBJECT_TEMPLATE", "")
        or DEFAULT_BOOKING_EMAIL_SUBJECT_TEMPLATE
    )
    body_template = (
        await get_setting("BOOKING_EMAIL_BODY_TEMPLATE", "")
        or os.getenv("BOOKING_EMAIL_BODY_TEMPLATE", "")
        or DEFAULT_BOOKING_EMAIL_BODY_TEMPLATE
    )
    signature = await get_setting("BOOKING_EMAIL_SIGNATURE", "") or os.getenv("BOOKING_EMAIL_SIGNATURE", "")
    reply_to = await get_setting("BOOKING_EMAIL_REPLY_TO", "") or os.getenv("BOOKING_EMAIL_REPLY_TO", "")
    subject = render_template(subject_template, values)
    body = render_template(body_template, values)
    if signature:
        body = f"{body.rstrip()}\n\n{render_template(signature, values)}"
    return subject, body, reply_to


def _send_email_sync(smtp_host, smtp_port, smtp_username, smtp_password, smtp_from, display_name, to_email, subject, body, reply_to=""):
    msg = MIMEMultipart()
    msg['From'] = f"{display_name} <{smtp_from}>" if display_name else smtp_from
    msg['To'] = to_email
    msg['Subject'] = subject
    if reply_to:
        msg['Reply-To'] = reply_to
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


async def send_email_async(to_email: str, subject: str, body: str, booking_id: str = "", reply_to: str = "") -> bool:
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
                f"booking_id={booking_id or ''} recipient={_redact_email(to_email)} email_status=not_configured host_present={bool(smtp_host)} port_present={bool(smtp_port)} from_present={bool(smtp_from)}",
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
            body,
            reply_to
        )
        await log_error(
            "email",
            "Email sent successfully",
            f"booking_id={booking_id or ''} recipient={_redact_email(to_email)} email_status=sent subject={subject}",
            "info",
        )
        return True
    except Exception as exc:
        logger.exception("Failed to send email to %s", to_email)
        await log_error(
            "email",
            "Email sending failed",
            f"booking_id={booking_id or ''} recipient={_redact_email(to_email)} email_status=failed error={exc}",
            "error",
        )
        return False
