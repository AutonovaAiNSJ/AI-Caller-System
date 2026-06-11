import asyncio
import json
import os
import uuid
import logging
from contextvars import ContextVar
from dotenv import load_dotenv
load_dotenv(".env", override=False)
from datetime import datetime, timedelta
from typing import Optional
from collections import defaultdict

logger = logging.getLogger("db")

DEFAULTS = {
    "LIVEKIT_URL":             os.getenv("LIVEKIT_URL", ""),
    "LIVEKIT_API_KEY":         os.getenv("LIVEKIT_API_KEY", ""),
    "LIVEKIT_API_SECRET":      os.getenv("LIVEKIT_API_SECRET", ""),
    "GOOGLE_API_KEY":          os.getenv("GOOGLE_API_KEY", ""),
    "GEMINI_MODEL":            os.getenv("GEMINI_MODEL", "gemini-3.1-flash-live-preview"),
    "GEMINI_TTS_VOICE":        os.getenv("GEMINI_TTS_VOICE", "Aoede"),
    "USE_GEMINI_REALTIME":     os.getenv("USE_GEMINI_REALTIME", "true"),
    "VOBIZ_SIP_DOMAIN":        os.getenv("VOBIZ_SIP_DOMAIN", ""),
    "VOBIZ_USERNAME":          os.getenv("VOBIZ_USERNAME", ""),
    "VOBIZ_PASSWORD":          os.getenv("VOBIZ_PASSWORD", ""),
    "VOBIZ_OUTBOUND_NUMBER":   os.getenv("VOBIZ_OUTBOUND_NUMBER", ""),
    "OUTBOUND_TRUNK_ID":       os.getenv("OUTBOUND_TRUNK_ID", ""),
    "DEFAULT_TRANSFER_NUMBER": os.getenv("DEFAULT_TRANSFER_NUMBER", ""),
    "SUPABASE_URL":            os.getenv("SUPABASE_URL", ""),
    "SUPABASE_SERVICE_KEY":    os.getenv("SUPABASE_SERVICE_KEY", ""),
    "DEEPGRAM_API_KEY":        os.getenv("DEEPGRAM_API_KEY", ""),
}

def _default(key: str) -> str:
    return os.getenv(key, DEFAULTS.get(key, ""))

SUPABASE_URL = _default("SUPABASE_URL")
SUPABASE_KEY = _default("SUPABASE_SERVICE_KEY")

SENSITIVE_KEYS = {
    "LIVEKIT_API_KEY", "LIVEKIT_API_SECRET", "GOOGLE_API_KEY",
    "VOBIZ_PASSWORD", "TWILIO_AUTH_TOKEN", "SUPABASE_SERVICE_KEY",
    "AWS_SECRET_ACCESS_KEY", "S3_SECRET_ACCESS_KEY", "CALCOM_API_KEY",
    "DEEPGRAM_API_KEY", "GOOGLE_CALENDAR_SERVICE_ACCOUNT_JSON",
    "SMTP_PASSWORD",
}

DEFAULT_TENANT_ID = os.getenv("DEFAULT_TENANT_ID", "default").strip() or "default"
TENANT_STATUSES = {"ACTIVE", "SUSPENDED", "TRIAL", "PAYMENT_DUE", "DISABLED"}
BILLING_MODES = {"BYOK", "MANAGED"}
BRANDING_FIELDS = {
    "company_name", "company_logo", "favicon", "primary_color",
    "secondary_color", "support_email", "website_url", "company_logo_url", "favicon_url",
}
TENANT_FIELDS = BRANDING_FIELDS | {
    "slug", "status", "billing_mode", "wallet_balance",
    "wallet_low_balance_threshold", "is_active", "onboarded",
}
TENANT_API_KEY_FIELDS = {
    "LIVEKIT_URL", "LIVEKIT_API_KEY", "LIVEKIT_API_SECRET",
    "GOOGLE_API_KEY", "VOBIZ_SIP_DOMAIN", "VOBIZ_USERNAME",
    "VOBIZ_PASSWORD", "VOBIZ_OUTBOUND_NUMBER", "OUTBOUND_TRUNK_ID",
    "TWILIO_ACCOUNT_SID", "TWILIO_AUTH_TOKEN", "TWILIO_FROM_NUMBER",
    "SUPABASE_URL", "SUPABASE_SERVICE_KEY", "SMTP_HOST", "SMTP_PORT",
    "SMTP_USERNAME", "SMTP_PASSWORD", "SMTP_FROM", "SMTP_DISPLAY_NAME",
    "GOOGLE_CALENDAR_SERVICE_ACCOUNT_JSON", "GOOGLE_CALENDAR_ID",
    "GOOGLE_CALENDAR_SLOT_DURATION", "CALCOM_API_KEY",
    "CALCOM_EVENT_TYPE_ID", "CALCOM_TIMEZONE",
}

_current_tenant_id: ContextVar[str] = ContextVar("current_tenant_id", default=DEFAULT_TENANT_ID)
_current_user_email: ContextVar[str] = ContextVar("current_user_email", default="")
_current_user_role: ContextVar[str] = ContextVar("current_user_role", default="TENANT_ADMIN")

def _now() -> str:
    return datetime.now().isoformat()

def _clean_tenant_id(value: Optional[str] = None) -> str:
    tenant_id = (value or _current_tenant_id.get() or DEFAULT_TENANT_ID).strip()
    return tenant_id or DEFAULT_TENANT_ID

def get_current_tenant_id() -> str:
    return _clean_tenant_id()

def get_current_user_email() -> str:
    return _current_user_email.get() or ""

def get_current_user_role() -> str:
    return _current_user_role.get() or "TENANT_ADMIN"

def set_request_context(
    tenant_id: Optional[str] = None,
    user_email: str = "",
    role: str = "TENANT_ADMIN",
):
    return (
        _current_tenant_id.set(_clean_tenant_id(tenant_id)),
        _current_user_email.set((user_email or "").strip().lower()),
        _current_user_role.set((role or "TENANT_ADMIN").strip().upper()),
    )

def reset_request_context(tokens) -> None:
    if not tokens:
        return
    tenant_token, email_token, role_token = tokens
    _current_tenant_id.reset(tenant_token)
    _current_user_email.reset(email_token)
    _current_user_role.reset(role_token)

def _slugify(value: str) -> str:
    base = "".join(ch.lower() if ch.isalnum() else "-" for ch in (value or "tenant"))
    parts = [p for p in base.split("-") if p]
    return "-".join(parts)[:80] or f"tenant-{uuid.uuid4().hex[:8]}"

def _schema_missing(exc: Exception) -> bool:
    text = str(exc).lower()
    return (
        "tenant_id" in text
        or "tenants" in text and ("does not exist" in text or "schema cache" in text)
        or "tenant_audit_logs" in text
        or "tenant_api_keys" in text
    )

def _tenant_query(query, tenant_id: Optional[str] = None):
    return query.eq("tenant_id", _clean_tenant_id(tenant_id))

def _with_tenant(row: dict, tenant_id: Optional[str] = None) -> dict:
    row["tenant_id"] = _clean_tenant_id(tenant_id)
    return row

_sync_db_client = None
_async_db_clients: dict[int, object] = {}
_adb_locks: dict[int, asyncio.Lock] = {}

def _sdb():
    global _sync_db_client
    if _sync_db_client is None:
        from supabase import create_client
        _sync_db_client = create_client(_default("SUPABASE_URL"), _default("SUPABASE_SERVICE_KEY"))
    return _sync_db_client

async def _adb():
    loop = asyncio.get_running_loop()
    loop_key = id(loop)
    if loop_key not in _adb_locks:
        _adb_locks[loop_key] = asyncio.Lock()
    if loop_key not in _async_db_clients:
        async with _adb_locks[loop_key]:
            if loop_key not in _async_db_clients:
                from supabase._async.client import create_client as _ac
                _async_db_clients[loop_key] = await _ac(
                    _default("SUPABASE_URL"), _default("SUPABASE_SERVICE_KEY")
                )
    return _async_db_clients[loop_key]

def init_db() -> None:
    url = os.getenv("SUPABASE_URL", SUPABASE_URL)
    key = os.getenv("SUPABASE_SERVICE_KEY", SUPABASE_KEY)
    if not url or not key:
        print("[warn] SUPABASE_URL or SUPABASE_SERVICE_KEY not set.")
        return
    try:
        db = _sdb()
        db.table("settings").select("key").limit(1).execute()
        print("[ok] Supabase connected")
    except Exception as exc:
        print(f"[warn] Supabase connection failed: {exc}")
        print("   Run supabase_schema.sql in your Supabase Dashboard -> SQL Editor")

# ── Supabase Auth Users Integration ──

async def get_user_by_email(email: str) -> Optional[dict]:
    """Retrieve user context, role, and tenant from the users table."""
    db = await _adb()
    try:
        result = await db.table("users").select("*").eq("email", email.lower()).maybe_single().execute()
        return result.data if result and getattr(result, "data", None) else None
    except Exception as exc:
        logger.error(f"Failed to fetch user by email: {exc}")
        return None

async def update_user_profile(email: str, full_name: str) -> dict:
    """Update user's profile details (like full_name) in the users table."""
    db = await _adb()
    result = await db.table("users").update({"full_name": full_name}).eq("email", email.lower()).execute()
    return (result.data or [None])[0] or {}

async def get_or_create_user(uuid_str: str, email: str, full_name: Optional[str] = None) -> dict:
    """Get a user by email/ID, or create them if missing.
    Matches email against BOOTSTRAP_ADMIN_EMAIL to assign SUPER_ADMIN role.
    """
    db = await _adb()
    email_clean = email.strip().lower()
    
    # 1. Try to find user in the users table by ID
    try:
        res = await db.table("users").select("*").eq("id", uuid_str).maybe_single().execute()
        if res and getattr(res, "data", None):
            user = res.data
            if not user.get("tenant_id"):
                user["tenant_id"] = DEFAULT_TENANT_ID
                await db.table("users").update({"tenant_id": DEFAULT_TENANT_ID}).eq("id", uuid_str).execute()
            return user
    except Exception as exc:
        logger.error(f"Error checking user by ID: {exc}")

    # 2. Try to find user in the users table by email
    try:
        res = await db.table("users").select("*").eq("email", email_clean).maybe_single().execute()
        if res and getattr(res, "data", None):
            user = res.data
            if not user.get("tenant_id"):
                user["tenant_id"] = DEFAULT_TENANT_ID
            await db.table("users").update({"id": uuid_str, "tenant_id": user["tenant_id"]}).eq("email", email_clean).execute()
            return user
    except Exception as exc:
        logger.error(f"Error checking user by email: {exc}")

    # 3. Check pending_invites for pre-assigned role/tenant (invited users)
    invite_role = None
    invite_tenant_id = None
    try:
        inv_res = await db.table("pending_invites").select("*").eq("email", email_clean).maybe_single().execute()
        if inv_res and getattr(inv_res, "data", None):
            invite_role = inv_res.data.get("role") or "TENANT_ADMIN"
            invite_tenant_id = inv_res.data.get("tenant_id") or DEFAULT_TENANT_ID
            # Consume the invite
            await db.table("pending_invites").delete().eq("email", email_clean).execute()
            logger.info(f"Consumed pending invite for {email_clean}: role={invite_role} tenant={invite_tenant_id}")
    except Exception as exc:
        logger.error(f"Failed to check pending_invites: {exc}")

    # 4. Determine role: BOOTSTRAP_ADMIN_EMAIL → SUPER_ADMIN; invited → from invite; else TENANT_USER
    bootstrap_env = os.getenv("BOOTSTRAP_ADMIN_EMAIL", "").strip().lower()
    if bootstrap_env and email_clean == bootstrap_env:
        role = "SUPER_ADMIN"
        assigned_tenant = DEFAULT_TENANT_ID
    elif invite_role and invite_tenant_id:
        role = invite_role
        assigned_tenant = invite_tenant_id
    else:
        role = "TENANT_USER"
        assigned_tenant = DEFAULT_TENANT_ID

    # 5. Create the new user record
    new_user = {
        "id": uuid_str,
        "email": email_clean,
        "full_name": full_name or email_clean.split("@")[0].capitalize(),
        "role": role,
        "tenant_id": assigned_tenant,
        "is_active": True,
        "created_at": datetime.now().isoformat()
    }

    logger.info(f"Auto-creating user: {new_user}")
    await db.table("users").insert(new_user).execute()

    # 6. Insert matching record into tenant_users
    try:
        tenant_user_record = {
            "tenant_id": assigned_tenant,
            "user_id": uuid_str,
            "role": role
        }
        await db.table("tenant_users").insert(tenant_user_record).execute()
        logger.info(f"Auto-created tenant_user record: {tenant_user_record}")
    except Exception as exc:
        logger.error(f"Failed to insert tenant_users record: {exc}")

    return new_user

# ── Settings ─────────────────────────────────────────────────────────────────

async def get_all_settings() -> dict:
    db = await _adb()
    query = db.table("settings").select("key, value")
    try:
        result = await _tenant_query(query).execute()
    except Exception as exc:
        if not _schema_missing(exc):
            raise
        result = await query.execute()

    KNOWN_KEYS = [
        "LIVEKIT_URL", "LIVEKIT_API_KEY", "LIVEKIT_API_SECRET",
        "GOOGLE_API_KEY", "GEMINI_MODEL", "GEMINI_TTS_VOICE", "USE_GEMINI_REALTIME",
        "VOBIZ_SIP_DOMAIN", "VOBIZ_USERNAME", "VOBIZ_PASSWORD",
        "VOBIZ_OUTBOUND_NUMBER", "OUTBOUND_TRUNK_ID", "DEFAULT_TRANSFER_NUMBER",
        "DEEPGRAM_API_KEY", "TWILIO_ACCOUNT_SID", "TWILIO_AUTH_TOKEN", "TWILIO_FROM_NUMBER",
        "S3_ACCESS_KEY_ID", "S3_SECRET_ACCESS_KEY", "S3_ENDPOINT_URL", "S3_REGION", "S3_BUCKET",
        "CALCOM_API_KEY", "CALCOM_EVENT_TYPE_ID", "CALCOM_TIMEZONE",
        "GOOGLE_CALENDAR_SERVICE_ACCOUNT_JSON", "GOOGLE_CALENDAR_ID", "GOOGLE_CALENDAR_SLOT_DURATION",
        "ENABLED_TOOLS",
        "SMTP_HOST", "SMTP_PORT", "SMTP_USERNAME", "SMTP_PASSWORD", "SMTP_FROM", "SMTP_DISPLAY_NAME",
    ]
    out: dict = {}
    for k in KNOWN_KEYS:
        env_val = _default(k)
        if k in SENSITIVE_KEYS:
            out[k] = {"value": "", "configured": bool(env_val)}
        else:
            out[k] = {"value": env_val, "configured": bool(env_val)}
    for row in (result.data or []):
        k, v = row["key"], row["value"]
        if k == "TEST_KEY":
            continue
        if k in SENSITIVE_KEYS:
            out[k] = {"value": "", "configured": bool(v)}
        else:
            out[k] = {"value": v, "configured": bool(v)}
    return out

async def save_settings(data: dict) -> None:
    db = await _adb()
    updated_at = _now()
    rows = [
        _with_tenant({"key": k, "value": str(v), "updated_at": updated_at})
        for k, v in data.items()
        if v is not None and v != ""
    ]
    if rows:
        try:
            await db.table("settings").upsert(rows, on_conflict="tenant_id,key").execute()
        except Exception as exc:
            if not _schema_missing(exc):
                raise
            # Fallback to no tenant_id key
            rows_legacy = [
                {"key": r["key"], "value": r["value"], "updated_at": r["updated_at"]}
                for r in rows
            ]
            await db.table("settings").upsert(rows_legacy, on_conflict="key").execute()

async def get_setting(key: str, default: str = "") -> str:
    tenant_id = get_current_tenant_id()
    if key in TENANT_API_KEY_FIELDS and tenant_id != DEFAULT_TENANT_ID:
        tenant = await get_tenant(tenant_id)
        if tenant and tenant.get("billing_mode") == "MANAGED":
            tokens = set_request_context(DEFAULT_TENANT_ID, "", "TENANT_ADMIN")
            try:
                return await get_setting(key, default)
            finally:
                reset_request_context(tokens)

    db = await _adb()
    try:
        result = await db.table("settings").select("value").eq("tenant_id", tenant_id).eq("key", key).maybe_single().execute()
        if result and getattr(result, "data", None) and result.data.get("value"):
            return result.data["value"]
        return _default(key) or default
    except Exception as exc:
        if not _schema_missing(exc):
            raise
        result = await db.table("settings").select("value").eq("key", key).maybe_single().execute()
        if result and result.data:
            return result.data["value"]
        return _default(key) or default

async def set_setting(key: str, value: str) -> None:
    db = await _adb()
    row = _with_tenant({"key": key, "value": value, "updated_at": _now()})
    await db.table("settings").upsert(row, on_conflict="tenant_id,key").execute()

async def get_enabled_tools() -> Optional[list]:
    raw = await get_setting("ENABLED_TOOLS", "")
    if not raw:
        return None
    try:
        result = json.loads(raw)
        return result if isinstance(result, list) else []
    except Exception:
        return []

# ── Error logs ────────────────────────────────────────────────────────────────

async def log_error(source: str, message: str, detail: str = "", level: str = "error") -> None:
    try:
        db = await _adb()
        row = _with_tenant({
            "id": str(uuid.uuid4()),
            "source": source,
            "level": level,
            "message": message[:500],
            "detail": detail[:2000],
            "timestamp": _now(),
        })
        await db.table("error_logs").insert(row).execute()
    except Exception:
        pass

async def get_errors(limit: int = 100) -> list:
    db = await _adb()
    query = db.table("error_logs").select("*").order("timestamp", desc=True).limit(limit)
    result = await _tenant_query(query).execute()
    return result.data or []

async def get_logs(level: Optional[str] = None, source: Optional[str] = None, limit: int = 200) -> list:
    db = await _adb()
    query = db.table("error_logs").select("*").order("timestamp", desc=True).limit(limit)
    if level:
        query = query.eq("level", level)
    if source:
        query = query.eq("source", source)
    result = await _tenant_query(query).execute()
    return result.data or []

async def clear_errors() -> None:
    db = await _adb()
    query = db.table("error_logs").delete().neq("id", "")
    await _tenant_query(query).execute()

# ── Appointments ──────────────────────────────────────────────────────────────

async def insert_appointment(name: str, phone: str, email: Optional[str], date: str, time: str, service: str) -> str:
    full_id = str(uuid.uuid4())
    booking_id = full_id[:8].upper()
    db = await _adb()
    row = _with_tenant({
        "id": full_id, "name": name, "phone": phone,
        "date": date, "time": time, "service": service,
        "status": "booked", "created_at": _now(),
    })
    if email:
        row["email"] = email
    try:
        await db.table("appointments").insert(row).execute()
    except Exception as exc:
        err_msg = str(exc).lower()
        if email and "column" in err_msg and "email" in err_msg:
            row.pop("email", None)
            await db.table("appointments").insert(row).execute()
        else:
            raise
    return booking_id

async def update_appointment_gcal(booking_id: str, event_id: str, event_link: str) -> None:
    try:
        db = await _adb()
        query = db.table("appointments").update({
            "gcal_event_id": event_id,
            "gcal_event_link": event_link,
        }).like("id", f"{booking_id.lower()}%")
        try:
            await _tenant_query(query).execute()
        except Exception as exc:
            if not _schema_missing(exc):
                raise
            await query.execute()
    except Exception as exc:
        print(f"Google Calendar local appointment update failed: {exc}")

async def check_slot(date: str, time: str) -> bool:
    db = await _adb()
    query = db.table("appointments").select("id").eq("date", date).eq("time", time).eq("status", "booked")
    try:
        result = await _tenant_query(query).maybe_single().execute()
    except Exception as exc:
        if not _schema_missing(exc):
            raise
        result = await query.maybe_single().execute()
    return not (result and getattr(result, "data", None))

async def get_next_available(date: str, time: str) -> str:
    try:
        dt = datetime.strptime(f"{date} {time}", "%Y-%m-%d %H:%M")
    except ValueError:
        dt = datetime.now().replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
    for _ in range(7 * 24):
        dt += timedelta(hours=1)
        if 9 <= dt.hour < 18:
            if await check_slot(dt.strftime("%Y-%m-%d"), dt.strftime("%H:%M")):
                return f"{dt.strftime('%Y-%m-%d')} at {dt.strftime('%H:%M')}"
    return "no open slots found in the next 7 days"

async def get_all_appointments(date_filter: Optional[str] = None) -> list:
    db = await _adb()
    query = db.table("appointments").select("*").order("date").order("time")
    if date_filter:
        query = query.eq("date", date_filter)
    try:
        result = await _tenant_query(query).execute()
    except Exception as exc:
        if not _schema_missing(exc):
            raise
        result = await query.execute()
    return result.data or []

async def cancel_appointment(appointment_id: str) -> bool:
    db = await _adb()
    query = db.table("appointments").update({"status": "cancelled"}).eq("id", appointment_id).eq("status", "booked")
    try:
        result = await _tenant_query(query).execute()
    except Exception as exc:
        if not _schema_missing(exc):
            raise
        result = await query.execute()
    return len(result.data or []) > 0

async def get_appointments_by_phone(phone: str) -> list:
    db = await _adb()
    query = db.table("appointments").select("*").eq("phone", phone).order("date", desc=True)
    try:
        result = await _tenant_query(query).execute()
    except Exception as exc:
        if not _schema_missing(exc):
            raise
        result = await query.execute()
    return result.data or []

# ── Call logs ─────────────────────────────────────────────────────────────────

async def log_call(
    phone_number: str, lead_name: Optional[str], outcome: str, reason: str,
    duration_seconds: int, recording_url: Optional[str] = None, notes: Optional[str] = None,
    direction: str = "outbound", call_session_id: Optional[str] = None,
) -> None:
    db = await _adb()
    row = _with_tenant({
        "id": str(uuid.uuid4()), "phone_number": phone_number, "lead_name": lead_name,
        "outcome": outcome, "reason": reason, "duration_seconds": duration_seconds,
        "timestamp": _now(), "direction": direction or "outbound",
    })
    if call_session_id:
        row["call_session_id"] = call_session_id
    if recording_url:
        row["recording_url"] = recording_url
    if notes:
        row["notes"] = notes
    await db.table("call_logs").insert(row).execute()
    if call_session_id:
        await finalize_call_session(
            call_session_id=call_session_id,
            outcome=outcome,
            reason=reason,
            duration_seconds=duration_seconds,
            recording_url=recording_url,
        )

async def create_call_session(
    room_name: str,
    direction: str,
    phone_number: str,
    lead_name: Optional[str] = None,
    status: str = "dispatching",
    metadata: Optional[dict] = None,
) -> str:
    db = await _adb()
    session_id = str(uuid.uuid4())
    row = _with_tenant({
        "id": session_id,
        "room_name": room_name,
        "direction": direction or "outbound",
        "phone_number": phone_number,
        "lead_name": lead_name,
        "status": status,
        "started_at": _now(),
    })
    if metadata is not None:
        row["metadata"] = json.dumps(metadata)
    await db.table("call_sessions").insert(row).execute()
    return session_id

async def update_call_session(
    call_session_id: str,
    status: Optional[str] = None,
    connected_at: Optional[str] = None,
    ended_at: Optional[str] = None,
    outcome: Optional[str] = None,
    reason: Optional[str] = None,
    duration_seconds: Optional[int] = None,
    recording_url: Optional[str] = None,
) -> None:
    if not call_session_id:
        return
    db = await _adb()
    updates: dict = {"updated_at": _now()}
    if status:
        updates["status"] = status
    if connected_at:
        updates["connected_at"] = connected_at
    if ended_at:
        updates["ended_at"] = ended_at
    if outcome:
        updates["outcome"] = outcome
    if reason:
        updates["reason"] = reason
    if duration_seconds is not None:
        updates["duration_seconds"] = duration_seconds
    if recording_url:
        updates["recording_url"] = recording_url
    query = db.table("call_sessions").update(updates).eq("id", call_session_id)
    try:
        await _tenant_query(query).execute()
    except Exception as exc:
        if not _schema_missing(exc):
            raise
        await query.execute()

async def finalize_call_session(
    call_session_id: str,
    outcome: str,
    reason: str,
    duration_seconds: int,
    recording_url: Optional[str] = None,
) -> None:
    await update_call_session(
        call_session_id=call_session_id,
        status="ended",
        ended_at=_now(),
        outcome=outcome,
        reason=reason,
        duration_seconds=duration_seconds,
        recording_url=recording_url,
    )

async def save_transcript(room_name: str, speaker: str, message: str) -> None:
    if not (room_name and speaker and message):
        return
    db = await _adb()
    row = _with_tenant({
        "room_name": room_name,
        "speaker": speaker,
        "message": message,
        "created_at": _now(),
    })
    await db.table("call_transcripts").insert(row).execute()

async def get_recent_transcripts(limit: int = 120, room_name: Optional[str] = None) -> list:
    db = await _adb()
    query = db.table("call_transcripts").select("*").order("created_at", desc=True).limit(limit)
    if room_name:
        query = query.eq("room_name", room_name)
    result = await _tenant_query(query).execute()
    return result.data or []

async def get_all_calls(page: int = 1, limit: int = 20) -> list:
    db = await _adb()
    offset = (page - 1) * limit
    query = db.table("call_logs").select("*").order("timestamp", desc=True).range(offset, offset + limit - 1)
    result = await _tenant_query(query).execute()
    return result.data or []

async def get_calls_by_phone(phone: str) -> list:
    db = await _adb()
    query = db.table("call_logs").select("*").eq("phone_number", phone).order("timestamp", desc=True)
    result = await _tenant_query(query).execute()
    return result.data or []

async def update_call_notes(call_id: str, notes: str) -> bool:
    db = await _adb()
    query = db.table("call_logs").update({"notes": notes}).eq("id", call_id)
    result = await _tenant_query(query).execute()
    return len(result.data or []) > 0

async def get_contacts() -> list:
    db = await _adb()
    query = db.table("call_logs").select("*").order("timestamp", desc=True)
    result = await _tenant_query(query).execute()
    rows = result.data or []
    contacts: dict = {}
    for row in rows:
        phone = row["phone_number"]
        if phone not in contacts:
            contacts[phone] = {
                "phone_number": phone, "lead_name": row.get("lead_name"),
                "total_calls": 0, "booked": 0,
                "last_call": row["timestamp"], "last_outcome": row.get("outcome"),
            }
        contacts[phone]["total_calls"] += 1
        if row.get("outcome") == "booked":
            contacts[phone]["booked"] += 1
    return sorted(contacts.values(), key=lambda c: c["last_call"], reverse=True)

# ── Stats ─────────────────────────────────────────────────────────────────────

async def get_stats() -> dict:
    db = await _adb()
    query = db.table("call_logs").select("outcome, duration_seconds, timestamp, direction")
    try:
        rows = (await _tenant_query(query).execute()).data or []
    except Exception as exc:
        if not _schema_missing(exc):
            raise
        rows = (await query.execute()).data or []
    total_calls    = len(rows)
    inbound_calls  = sum(1 for r in rows if (r.get("direction") or "outbound") == "inbound")
    outbound_calls = sum(1 for r in rows if (r.get("direction") or "outbound") != "inbound")
    booked         = sum(1 for r in rows if r.get("outcome") == "booked")
    not_interested = sum(1 for r in rows if r.get("outcome") == "not_interested")
    durations      = [r["duration_seconds"] for r in rows if r.get("duration_seconds")]
    avg_dur        = sum(durations) / len(durations) if durations else 0
    booking_rate   = round((booked / total_calls * 100) if total_calls else 0, 1)
    outcomes: dict = {}
    for r in rows:
        o = r.get("outcome") or "unknown"
        outcomes[o] = outcomes.get(o, 0) + 1
    daily: dict = defaultdict(int)
    for r in rows:
        ts = (r.get("timestamp") or "")[:10]
        if ts:
            daily[ts] += 1
    today = datetime.now().date()
    timeline = [{"date": (today - timedelta(days=i)).isoformat(), "count": daily.get((today - timedelta(days=i)).isoformat(), 0)} for i in range(13, -1, -1)]
    dur_sum: dict = defaultdict(float)
    dur_cnt: dict = defaultdict(int)
    for r in rows:
        o = r.get("outcome") or "unknown"
        sec = r.get("duration_seconds")
        if sec:
            dur_sum[o] += sec
            dur_cnt[o] += 1
    duration_by_outcome = {o: dur_sum[o] / dur_cnt[o] for o in dur_sum}
    return {
        "total_calls": total_calls, "booked": booked, "not_interested": not_interested,
        "avg_duration_seconds": round(avg_dur, 1), "booking_rate_percent": booking_rate,
        "outcomes": outcomes, "timeline": timeline, "duration_by_outcome": duration_by_outcome,
        "inbound_calls": inbound_calls, "outbound_calls": outbound_calls,
    }

# ── Campaigns ─────────────────────────────────────────────────────────────────

async def create_campaign(
    name: str, contacts_json: str, schedule_type: str = "once",
    schedule_time: str = "09:00", call_delay_seconds: int = 3,
    system_prompt: Optional[str] = None, agent_profile_id: Optional[str] = None,
) -> str:
    campaign_id = str(uuid.uuid4())
    db = await _adb()
    row = _with_tenant({
        "id": campaign_id, "name": name, "status": "active",
        "contacts_json": contacts_json, "schedule_type": schedule_type,
        "schedule_time": schedule_time, "call_delay_seconds": call_delay_seconds,
        "created_at": _now(), "total_dispatched": 0, "total_failed": 0,
    })
    if system_prompt:
        row["system_prompt"] = system_prompt
    if agent_profile_id:
        row["agent_profile_id"] = agent_profile_id
    try:
        await db.table("campaigns").insert(row).execute()
    except Exception as exc:
        if not _schema_missing(exc):
            raise
        row.pop("tenant_id", None)
        await db.table("campaigns").insert(row).execute()
    return campaign_id

async def get_all_campaigns() -> list:
    db = await _adb()
    query = db.table("campaigns").select("*").order("created_at", desc=True)
    result = await _tenant_query(query).execute()
    return result.data or []

async def get_all_campaigns_unscoped() -> list:
    db = await _adb()
    try:
        result = await db.table("campaigns").select("*").execute()
        return result.data or []
    except Exception:
        return []

async def get_campaign(campaign_id: str) -> Optional[dict]:
    db = await _adb()
    query = db.table("campaigns").select("*").eq("id", campaign_id)
    result = await _tenant_query(query).maybe_single().execute()
    return result.data if result and getattr(result, "data", None) else None

async def get_campaign_for_worker(campaign_id: str) -> Optional[dict]:
    """Retrieve campaign details for the global scheduling worker."""
    db = await _adb()
    try:
        result = await db.table("campaigns").select("id, tenant_id").eq("id", campaign_id).maybe_single().execute()
        return result.data if result and getattr(result, "data", None) else None
    except Exception:
        return None

async def update_campaign_status(campaign_id: str, status: str) -> bool:
    db = await _adb()
    query = db.table("campaigns").update({"status": status}).eq("id", campaign_id)
    result = await _tenant_query(query).execute()
    return len(result.data or []) > 0

async def update_campaign_run_stats(campaign_id: str, dispatched: int, failed: int, status: str = "completed") -> None:
    db = await _adb()
    query = db.table("campaigns").update({
        "last_run_at": _now(),
        "total_dispatched": dispatched, "total_failed": failed, "status": status,
    }).eq("id", campaign_id)
    await _tenant_query(query).execute()

async def delete_campaign(campaign_id: str) -> bool:
    db = await _adb()
    query = db.table("campaigns").delete().eq("id", campaign_id)
    result = await _tenant_query(query).execute()
    return len(result.data or []) > 0

# ── Contact Memory ────────────────────────────────────────────────────────────

async def add_contact_memory(phone: str, insight: str) -> None:
    db = await _adb()
    row = _with_tenant({
        "id": str(uuid.uuid4()), "phone_number": phone,
        "insight": insight[:1000], "created_at": _now(),
    })
    await db.table("contact_memory").insert(row).execute()

async def get_contact_memory(phone: str) -> list:
    db = await _adb()
    query = db.table("contact_memory").select("insight, created_at").eq("phone_number", phone).order("created_at", desc=True).limit(20)
    result = await _tenant_query(query).execute()
    return result.data or []

async def compress_contact_memory(phone: str, compressed: str) -> None:
    db = await _adb()
    await _tenant_query(db.table("contact_memory").delete()).eq("phone_number", phone).execute()
    row = _with_tenant({
        "id": str(uuid.uuid4()), "phone_number": phone,
        "insight": compressed[:2000], "created_at": _now(),
    })
    await db.table("contact_memory").insert(row).execute()

# ── Agent Profiles ────────────────────────────────────────────────────────────

async def get_all_agent_profiles() -> list:
    db = await _adb()
    query = db.table("agent_profiles").select("*").order("created_at")
    result = await _tenant_query(query).execute()
    return result.data or []

async def get_agent_profile(profile_id: str) -> Optional[dict]:
    db = await _adb()
    query = db.table("agent_profiles").select("*").eq("id", profile_id)
    result = await _tenant_query(query).maybe_single().execute()
    return result.data if result and getattr(result, "data", None) else None

async def get_default_agent_profile() -> Optional[dict]:
    db = await _adb()
    query = db.table("agent_profiles").select("*").eq("is_default", 1).limit(1)
    result = await _tenant_query(query).maybe_single().execute()
    return result.data if result and getattr(result, "data", None) else None

async def create_agent_profile(
    name: str, voice: str = "Aoede", model: str = "gemini-3.1-flash-live-preview",
    system_prompt: Optional[str] = None, enabled_tools: Optional[str] = None, is_default: bool = False,
) -> str:
    profile_id = str(uuid.uuid4())
    db = await _adb()
    if is_default:
        await _tenant_query(db.table("agent_profiles").update({"is_default": 0})).neq("id", "placeholder").execute()
    row = _with_tenant({
        "id": profile_id, "name": name, "voice": voice, "model": model,
        "system_prompt": system_prompt, "enabled_tools": enabled_tools,
        "is_default": 1 if is_default else 0, "created_at": _now(),
    })
    await db.table("agent_profiles").insert(row).execute()
    return profile_id

async def update_agent_profile(profile_id: str, updates: dict) -> bool:
    db = await _adb()
    query = db.table("agent_profiles").update(updates).eq("id", profile_id)
    result = await _tenant_query(query).execute()
    return len(result.data or []) > 0

async def delete_agent_profile(profile_id: str) -> bool:
    db = await _adb()
    query = db.table("agent_profiles").delete().eq("id", profile_id)
    result = await _tenant_query(query).execute()
    return len(result.data or []) > 0

async def set_default_agent_profile(profile_id: str) -> None:
    db = await _adb()
    await _tenant_query(db.table("agent_profiles").update({"is_default": 0})).neq("id", "placeholder").execute()
    await _tenant_query(db.table("agent_profiles").update({"is_default": 1})).eq("id", profile_id).execute()

# ── Multi-Tenant Management Helpers ──

async def ensure_default_tenant() -> dict:
    db = await _adb()
    billing_mode = os.getenv("DEFAULT_BILLING_MODE", "MANAGED").upper()
    if billing_mode not in BILLING_MODES:
        billing_mode = "MANAGED"
    row = {
        "id": DEFAULT_TENANT_ID,
        "company_name": os.getenv("DEFAULT_COMPANY_NAME", "Default Tenant"),
        "slug": os.getenv("DEFAULT_TENANT_SLUG", "default"),
        "status": "ACTIVE",
        "billing_mode": billing_mode,
        "wallet_balance": float(os.getenv("DEFAULT_WALLET_BALANCE", "0") or 0.0),
        "wallet_low_balance_threshold": float(os.getenv("DEFAULT_WALLET_LOW_BALANCE_THRESHOLD", "0") or 0.0),
        "is_active": True,
        "onboarded": True,
        "created_at": _now(),
    }
    try:
        result = await db.table("tenants").select("*").eq("id", DEFAULT_TENANT_ID).maybe_single().execute()
        if result and getattr(result, "data", None):
            return result.data
        await db.table("tenants").insert(row).execute()
        await audit_log("Tenant Created", DEFAULT_TENANT_ID, {"source": "ensure_default_tenant"})
    except Exception:
        pass
    return row

async def get_tenant(tenant_id: Optional[str] = None) -> Optional[dict]:
    tid = _clean_tenant_id(tenant_id)
    if tid == DEFAULT_TENANT_ID:
        await ensure_default_tenant()
    db = await _adb()
    try:
        result = await db.table("tenants").select("*").eq("id", tid).maybe_single().execute()
        return result.data if result and getattr(result, "data", None) else None
    except Exception as exc:
        if tid == DEFAULT_TENANT_ID and _schema_missing(exc):
            return {
                "id": DEFAULT_TENANT_ID,
                "company_name": "OutboundAI",
                "slug": "default",
                "status": "ACTIVE",
                "billing_mode": "MANAGED",
                "wallet_balance": 0.0,
                "wallet_low_balance_threshold": 0.0,
                "is_active": True,
            }
        raise

async def require_active_tenant(operation: str = "operation", tenant_id: Optional[str] = None, check_wallet: bool = True) -> dict:
    tenant = await get_tenant(tenant_id)
    if not tenant:
        raise PermissionError("Tenant account suspended.")
    status = (tenant.get("status") or "").upper()
    if status != "ACTIVE" or tenant.get("is_active") is False:
        await audit_log("Tenant Operation Blocked", tenant.get("id"), {"operation": operation, "status": status})
        raise PermissionError("Tenant account suspended.")
    # Wallet gate: MANAGED tenants must have balance > 0
    if check_wallet and tenant.get("billing_mode") == "MANAGED":
        balance = float(tenant.get("wallet_balance") or 0.0)
        if balance <= 0:
            await audit_log("Wallet Gate Blocked", tenant.get("id"), {"operation": operation, "balance": balance})
            raise PermissionError(f"Insufficient wallet balance for {operation}. Please top up your account.")
    return tenant

async def list_tenants() -> list:
    await ensure_default_tenant()
    db = await _adb()
    result = await db.table("tenants").select("*").order("created_at", desc=True).execute()
    return result.data or []

async def invite_tenant_admin(tenant_id: str, admin_email: str, invited_by: str = "") -> dict:
    """Send a Supabase invite email to a new tenant admin and record a pending invite."""
    db = await _adb()
    email_clean = admin_email.strip().lower()
    if not email_clean or "@" not in email_clean:
        raise ValueError("Invalid admin_email address")

    # Upsert pending invite so get_or_create_user() picks up role on first login
    invite_id = str(uuid.uuid4())
    invite_row = {
        "id": invite_id,
        "email": email_clean,
        "tenant_id": tenant_id,
        "role": "TENANT_ADMIN",
        "invited_by": invited_by,
    }
    try:
        await db.table("pending_invites").upsert(invite_row, on_conflict="email").execute()
    except Exception as exc:
        logger.error(f"Failed to write pending_invite: {exc}")
        raise ValueError(f"Could not store invite record: {exc}")

    # Send Supabase invite email via Admin API
    app_url = os.getenv("APP_URL", "").strip() or os.getenv("SITE_URL", "").strip()
    if not app_url:
        raise RuntimeError("APP_URL not configured")
    redirect_url = f"{app_url.rstrip('/')}/ui/login.html"

    try:
        invite_res = await db.auth.admin.invite_user_by_email(
            email_clean,
            options={"redirect_to": redirect_url}
        )
        logger.info(f"Supabase invite sent to {email_clean} for tenant {tenant_id} (redirect to {redirect_url}): {invite_res}")
    except Exception as exc:
        # Clean up the pending invite if Supabase rejects the email
        try:
            await db.table("pending_invites").delete().eq("email", email_clean).execute()
        except Exception:
            pass
        logger.error(f"Supabase invite failed for {email_clean}: {exc}")
        raise ValueError(f"Failed to send invite email: {exc}")

    await audit_log("Tenant Admin Invited", tenant_id, {"admin_email": email_clean}, invited_by)
    return {"status": "invited", "email": email_clean, "tenant_id": tenant_id}


async def create_tenant(data: dict, user_email: str = "") -> dict:
    db = await _adb()
    payload = {}
    for k in TENANT_FIELDS:
        if k in data and data[k] is not None:
            payload[k] = data[k]
    tenant_id = payload.get("id") or str(uuid.uuid4())
    company_name = str(payload.get("company_name") or "").strip()
    if not company_name:
        raise ValueError("company_name is required")
    status = payload.get("status") or "TRIAL"
    payload.update({
        "id": tenant_id,
        "company_name": company_name,
        "slug": payload.get("slug") or _slugify(company_name),
        "status": status,
        "billing_mode": payload.get("billing_mode") or "MANAGED",
        "wallet_balance": float(payload.get("wallet_balance") or 0.0),
        "wallet_low_balance_threshold": float(payload.get("wallet_low_balance_threshold") or 0.0),
        "is_active": status == "ACTIVE",
        "created_at": _now(),
        "updated_at": _now(),
    })
    await db.table("tenants").insert(payload).execute()
    await audit_log("Tenant Created", tenant_id, {"company_name": company_name}, user_email)

    # Auto-create default tenant resources: default agent profile and default system prompt setting
    tokens = set_request_context(tenant_id, user_email, "TENANT_ADMIN")
    try:
        await create_agent_profile(
            name="Priya - Default",
            voice="Aoede",
            model="gemini-3.1-flash-live-preview",
            system_prompt=None,
            enabled_tools=None,
            is_default=True
        )
        from prompts import DEFAULT_SYSTEM_PROMPT
        await set_setting("system_prompt", DEFAULT_SYSTEM_PROMPT)
    except Exception as exc:
        await log_error("server", f"Failed to initialize default resources for tenant {tenant_id}: {exc}", level="warning")
    finally:
        reset_request_context(tokens)

    # Auto-send invite if admin_email provided
    admin_email = data.get("admin_email", "").strip().lower() if isinstance(data, dict) else ""
    invite_result = None
    if admin_email and "@" in admin_email:
        try:
            invite_result = await invite_tenant_admin(tenant_id, admin_email, user_email)
        except Exception as exc:
            # Log but do not fail tenant creation — admin can invite separately
            await log_error("server", f"Auto-invite failed for {admin_email}: {exc}", level="warning")

    return {**payload, "invite": invite_result}

async def update_tenant(tenant_id: str, updates: dict, user_email: str = "") -> Optional[dict]:
    db = await _adb()
    payload = {}
    for k in TENANT_FIELDS:
        if k in updates and updates[k] is not None:
            payload[k] = updates[k]
    if "status" in payload:
        status = str(payload["status"]).strip().upper()
        if status not in TENANT_STATUSES:
            raise ValueError(f"Invalid tenant status: {payload['status']}")
        payload["status"] = status
        payload["is_active"] = status == "ACTIVE"
    if "billing_mode" in payload:
        mode = str(payload["billing_mode"]).strip().upper()
        if mode not in BILLING_MODES:
            raise ValueError(f"Invalid billing mode: {payload['billing_mode']}")
        payload["billing_mode"] = mode
    for money_field in ("wallet_balance", "wallet_low_balance_threshold"):
        if money_field in payload:
            payload[money_field] = float(payload[money_field] or 0.0)
    if not payload:
        return await get_tenant(tenant_id)
    payload["updated_at"] = _now()
    result = await db.table("tenants").update(payload).eq("id", tenant_id).execute()
    if not (result.data or []):
        return None
        
    has_branding = any(k in BRANDING_FIELDS for k in payload)
    has_other = any(k not in BRANDING_FIELDS for k in payload if k != "updated_at")
    if has_branding and not has_other:
        await audit_log("Branding Changed", tenant_id, payload, user_email)
    else:
        await audit_log("Tenant Updated", tenant_id, payload, user_email)
        
    return (result.data or [None])[0]

async def update_tenant_status(tenant_id: str, status: str, user_email: str = "") -> Optional[dict]:
    status = (status or "").upper()
    if status not in TENANT_STATUSES:
        raise ValueError(f"Invalid tenant status: {status}")
    tenant = await update_tenant(tenant_id, {"status": status, "is_active": status == "ACTIVE"}, user_email)
    action = {
        "ACTIVE": "Tenant Activated",
        "SUSPENDED": "Tenant Suspended",
        "DISABLED": "Tenant Disabled",
    }.get(status, "Tenant Status Changed")
    await audit_log(action, tenant_id, {"status": status}, user_email)
    return tenant

async def update_tenant_branding(updates: dict, user_email: str = "", tenant_id: Optional[str] = None) -> dict:
    tid = _clean_tenant_id(tenant_id)
    payload = {k: v for k, v in (updates or {}).items() if k in BRANDING_FIELDS}
    if not payload:
        return await get_tenant(tid) or {}
    tenant = await update_tenant(tid, payload, user_email)
    await audit_log("Branding Changed", tid, payload, user_email)
    return tenant or {}

async def get_tenant_branding(tenant_id: Optional[str] = None) -> dict:
    tenant = await get_tenant(tenant_id) or {}
    keys = BRANDING_FIELDS | {"id", "status", "billing_mode"}
    defaults = {
        "company_name": "Autonova",
        "company_logo": "/ui/ai_voice_logo.png",
        "favicon": "/ui/ai_voice_logo.png",
        "primary_color": "#5B5BD6",
        "secondary_color": "#00b894",
        "support_email": "support@autonova.ai",
        "website_url": "https://autonova.ai",
    }
    
    company_logo_file = tenant.get("company_logo") or ""
    company_logo_url = tenant.get("company_logo_url") or ""
    favicon_file = tenant.get("favicon") or ""
    favicon_url = tenant.get("favicon_url") or ""
    
    resolved_logo = company_logo_file if company_logo_file else (company_logo_url if company_logo_url else defaults["company_logo"])
    resolved_favicon = favicon_file if favicon_file else (favicon_url if favicon_url else defaults["favicon"])
    
    res = {}
    for k in keys:
        val = tenant.get(k)
        if not val and k in defaults:
            val = defaults[k]
        res[k] = val or ""
        
    res["company_logo"] = resolved_logo
    res["favicon"] = resolved_favicon
    res["company_logo_file"] = company_logo_file
    res["company_logo_url"] = company_logo_url
    res["favicon_file"] = favicon_file
    res["favicon_url"] = favicon_url
    return res

async def add_wallet_credits(tenant_id: str, amount: float, user_email: str = "", reason: str = "") -> dict:
    if amount <= 0:
        raise ValueError("amount must be greater than zero")
    tenant = await get_tenant(tenant_id)
    if not tenant:
        raise ValueError("Tenant not found")
    balance = float(tenant.get("wallet_balance") or 0.0) + float(amount)
    updated = await update_tenant(tenant_id, {"wallet_balance": balance}, user_email)
    await audit_log("Wallet Updated", tenant_id, {"action": "add", "amount": amount, "reason": reason, "balance": balance}, user_email)
    return updated or {}

async def get_platform_pricing() -> dict:
    """Read platform pricing settings from the default tenant's settings rows."""
    tokens = set_request_context(DEFAULT_TENANT_ID, "", "SUPER_ADMIN")
    try:
        attempt = float(await get_setting("PRICE_PER_CALL_ATTEMPT", "0") or 0)
        appt = float(await get_setting("PRICE_PER_APPOINTMENT", "0") or 0)
    except Exception:
        attempt = 0.0
        appt = 0.0
    finally:
        reset_request_context(tokens)
    return {
        "price_per_call_attempt": attempt,
        "price_per_appointment": appt,
    }


async def deduct_wallet_for_event(
    tenant_id: str,
    event_type: str,
    amount: float,
    user_email: str = "",
) -> Optional[dict]:
    """Deduct wallet and write a billing audit entry. Returns updated tenant or None."""
    if amount <= 0:
        return None
    try:
        updated = await deduct_wallet_credits(
            tenant_id, amount, user_email,
            reason=f"Auto-billing: {event_type} (Rs {amount:.2f})"
        )
        await audit_log(
            "Wallet Auto-Deducted",
            tenant_id,
            {"event": event_type, "amount": amount, "new_balance": float(updated.get("wallet_balance") or 0)},
            user_email,
        )
        return updated
    except Exception as exc:
        logger.error(f"deduct_wallet_for_event failed: tenant={tenant_id} event={event_type} amount={amount} err={exc}")
        return None


async def deduct_wallet_credits(tenant_id: str, amount: float, user_email: str = "", reason: str = "") -> dict:
    if amount <= 0:
        raise ValueError("amount must be greater than zero")
    tenant = await get_tenant(tenant_id)
    if not tenant:
        raise ValueError("Tenant not found")
    balance = float(tenant.get("wallet_balance") or 0.0) - float(amount)
    updated = await update_tenant(tenant_id, {"wallet_balance": balance}, user_email)
    await audit_log("Wallet Updated", tenant_id, {"action": "deduct", "amount": amount, "reason": reason, "balance": balance}, user_email)
    return updated or {}

async def get_tenant_api_keys(tenant_id: str) -> dict:
    db = await _adb()
    try:
        result = await db.table("tenant_api_keys").select("key, value, mode, updated_at").eq("tenant_id", tenant_id).execute()
        out: dict = {}
        for row in result.data or []:
            key = row.get("key")
            if not key:
                continue
            value = row.get("value") or ""
            out[key] = {
                "value": "" if key in SENSITIVE_KEYS else value,
                "configured": bool(value),
                "mode": row.get("mode") or "BYOK",
                "updated_at": row.get("updated_at"),
            }
        return out
    except Exception:
        return {}

async def save_tenant_api_keys(tenant_id: str, values: dict, mode: str = "BYOK", user_email: str = "") -> None:
    db = await _adb()
    for key, value in values.items():
        if key not in TENANT_API_KEY_FIELDS:
            continue
        row = {
            "tenant_id": tenant_id,
            "key": key,
            "value": value,
            "mode": mode,
            "updated_at": _now()
        }
        await db.table("tenant_api_keys").upsert(row).execute()
    await audit_log("API Keys Updated", tenant_id, {"keys": list(values.keys()), "mode": mode}, user_email)

async def get_super_admin_summary() -> dict:
    db = await _adb()
    tenants = await list_tenants()
    active = [t for t in tenants if t.get("status") == "ACTIVE"]
    suspended = [t for t in tenants if t.get("status") == "SUSPENDED"]
    trial = [t for t in tenants if t.get("status") == "TRIAL"]
    try:
        calls = (await db.table("call_logs").select("id").execute()).data or []
        campaigns = (await db.table("campaigns").select("id").execute()).data or []
        appointments = (await db.table("appointments").select("id").execute()).data or []
    except Exception:
        calls, campaigns, appointments = [], [], []
    return {
        "total_tenants": len(tenants),
        "active_tenants": len(active),
        "suspended_tenants": len(suspended),
        "trial_tenants": len(trial),
        "total_calls": len(calls),
        "total_campaigns": len(campaigns),
        "total_appointments": len(appointments),
    }

async def audit_log(action: str, tenant_id: Optional[str] = None, detail: object = "", user_email: Optional[str] = None) -> None:
    try:
        db = await _adb()
        detail_value = json.dumps(detail) if isinstance(detail, (dict, list)) else str(detail or "")
        await db.table("tenant_audit_logs").insert({
            "id": str(uuid.uuid4()),
            "tenant_id": _clean_tenant_id(tenant_id),
            "user_email": (user_email if user_email is not None else get_current_user_email()),
            "action": action,
            "detail": detail_value[:4000],
            "timestamp": _now(),
        }).execute()
    except Exception:
        pass
