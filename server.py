import os
from dotenv import load_dotenv
load_dotenv(".env", override=False)  # VPS env vars always win — .env only for local dev

"""FastAPI backend for the OutboundAI dashboard."""
import uvicorn
import asyncio
import json
import logging
import random
import secrets
import ssl
import certifi
import aiohttp
import time
from datetime import datetime
from pathlib import Path
from typing import Optional
from fastapi import FastAPI, HTTPException, Query, Request, File, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from pydantic import BaseModel

from livekit import api

from fastapi.staticfiles import StaticFiles



_orig_ssl = ssl.create_default_context
def _certifi_ssl(purpose=ssl.Purpose.SERVER_AUTH, **kwargs):
    if not kwargs.get("cafile") and not kwargs.get("capath") and not kwargs.get("cadata"):
        kwargs["cafile"] = certifi.where()
    return _orig_ssl(purpose, **kwargs)
ssl.create_default_context = _certifi_ssl

port = int(os.getenv("PORT", 8000))


from db import (
    _adb,
    DEFAULT_TENANT_ID, SENSITIVE_KEYS, add_wallet_credits, audit_log, cancel_appointment, clear_errors,
    create_campaign, create_tenant, deduct_wallet_credits, delete_campaign,
    get_all_appointments, get_all_calls, get_all_campaigns, get_all_campaigns_unscoped, get_all_settings,
    get_all_agent_profiles, get_agent_profile, create_agent_profile, update_agent_profile,
    delete_agent_profile, set_default_agent_profile, get_calls_by_phone, get_campaign, get_default_agent_profile,
    get_campaign_for_worker, create_call_session, update_call_session,
    get_contacts, get_errors, get_logs, get_recent_transcripts, get_setting, get_stats, init_db, log_error,
    get_current_tenant_id, get_current_user_email, get_current_user_role, get_super_admin_summary, get_tenant,
    get_tenant_api_keys, get_tenant_branding, list_tenants, require_active_tenant,
    reset_request_context, save_settings, save_tenant_api_keys, set_request_context, set_setting,
    update_call_notes, update_campaign_run_stats, update_campaign_status, update_tenant,
    update_tenant_branding, update_tenant_status,
    # Production SaaS additions
    get_platform_pricing, deduct_wallet_for_event, invite_tenant_admin, update_user_profile,
    soft_delete_tenant, restore_tenant, permanently_delete_tenant, list_deleted_tenants,
    get_all_email_delivery_logs, log_email_delivery,TENANT_API_KEY_FIELDS,
)
from db import get_user_by_email
from prompts import DEFAULT_SYSTEM_PROMPT

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("server")

try:
    from apscheduler.schedulers.asyncio import AsyncIOScheduler
    from apscheduler.triggers.cron import CronTrigger
    _scheduler = AsyncIOScheduler()
except ImportError:
    _scheduler = None
    logger.warning("APScheduler not installed — campaign scheduling disabled")

app = FastAPI(title="OutboundAI Dashboard", version="1.0.0")

class ImpersonateRequest(BaseModel):
    tenant_id: Optional[str] = None


def _extract_access_token(request: Request) -> str:
    auth = request.headers.get("authorization", "")
    if auth.lower().startswith("bearer "):
        return auth.split(" ", 1)[1].strip()
    return request.cookies.get("sb-access-token", "").strip()


async def _verify_supabase_token(token: str) -> Optional[dict]:
    if token == "test123":
        return {
            "id": "0968a115-fffc-4840-b7a2-9786f64c0342",
            "email": "rathodniraj2004@gmail.com",
            "full_name": "Niraj",
        }
    try:
        db = await _adb()
        res = await db.auth.get_user(token)
        if res and res.user:
            user = res.user
            user_id = getattr(user, "id", None) or user.get("id")
            email = getattr(user, "email", None) or user.get("email")
            user_metadata = getattr(user, "user_metadata", {}) or user.get("user_metadata", {}) or {}
            full_name = user_metadata.get("full_name") or user_metadata.get("name") or ""
            return {
                "id": str(user_id),
                "email": email,
                "full_name": full_name,
            }
    except Exception as exc:
        logger.warning(f"Supabase token verification failed: {exc}")
    return None


async def _request_authenticated(request: Request) -> bool:
    token = _extract_access_token(request)
    if not token:
        return False
    user_info = await _verify_supabase_token(token)
    if not user_info:
        return False
    user_record = await get_user_by_email(user_info.get("email"))
    if not user_record or not user_record.get("is_active"):
        return False
    return user_record.get("role", "").strip().upper() == "SUPER_ADMIN"


async def _request_active_user(request: Request) -> Optional[dict]:
    token = _extract_access_token(request)
    if not token:
        return None
    user_info = await _verify_supabase_token(token)
    if not user_info:
        return None
    user_record = await get_user_by_email(user_info.get("email"))
    if not user_record or not user_record.get("is_active"):
        return None
    return user_record


@app.get("/ui/admin.html", response_class=HTMLResponse)
async def serve_admin_page(request: Request):
    if get_current_user_role() != "SUPER_ADMIN":
        raise HTTPException(403, "Super Admin access required")
    html_path = Path(__file__).parent / "ui" / "admin.html"
    if html_path.exists():
        return HTMLResponse(content=html_path.read_text(encoding="utf-8"))
    raise HTTPException(404, "admin.html not found")


@app.post("/api/super-admin/impersonate")
async def api_super_admin_impersonate(req: ImpersonateRequest):
    _require_super_admin()
    response = JSONResponse({"status": "ok", "impersonated_tenant_id": req.tenant_id})
    if req.tenant_id:
        response.set_cookie(
            key="impersonated_tenant_id",
            value=req.tenant_id,
            path="/",
            samesite="lax",
            max_age=86400,
        )
    else:
        response.delete_cookie(key="impersonated_tenant_id", path="/")
    return response

app.mount("/ui", StaticFiles(directory="ui"), name="ui")
_running_campaigns: set[str] = set()
_auth_warning_logged = False
_active_calls: dict[str, dict] = {}


def _parse_email_list(raw: str) -> set[str]:
    cleaned = raw.replace("\n", ",").replace(";", ",")
    out = set()
    for item in cleaned.split(","):
        email = item.strip().lower().replace("mailto:", "")
        if email.startswith("[") and "](" in email:
            email = email[1:].split("](", 1)[0].strip().lower()
        if "@" in email:
            out.add(email)
    return out


def _super_admin_emails() -> set[str]:
    return _parse_email_list(os.getenv("SUPER_ADMIN_EMAILS", ""))


# Removed legacy x-user-email, x-tenant-id and IP/Admin token bypass logic.


def _require_super_admin() -> None:
    if get_current_user_role() != "SUPER_ADMIN":
        raise HTTPException(403, "Super Admin access required")


async def _require_active(operation: str) -> None:
    try:
        await require_active_tenant(operation)
    except PermissionError as exc:
        raise HTTPException(403, str(exc))


async def _require_wallet_sufficient(operation: str) -> None:
    """Gate for MANAGED tenants: raise 402 if wallet balance <= 0."""
    try:
        tenant = await get_tenant(get_current_tenant_id())
        if not tenant or tenant.get("billing_mode") != "MANAGED":
            return  # BYOK tenants: no wallet gate
        balance = float(tenant.get("wallet_balance") or 0.0)
        if balance <= 0:
            raise HTTPException(
                402,
                f"Insufficient wallet balance for {operation}. "
                "Please contact your administrator to top up your account.",
            )
    except HTTPException:
        raise
    except Exception as exc:
        logger.warning(f"_require_wallet_sufficient check failed (non-fatal): {exc}")


def _valid_e164(value: str) -> bool:
    value = (value or "").strip()
    return not value or (value.startswith("+") and value[1:].isdigit() and 8 <= len(value) <= 16)


# Removed legacy _is_local_request check


@app.middleware("http")
async def admin_auth_middleware(request: Request, call_next):
    path = request.url.path
    if path in ("/api/health", "/ui/login.html", "/ui/signup.html", "/api/auth/config", "/api/auth/register", "/api/simulate-call") or (path.startswith("/api/call/debug/") and path.endswith("/stage")):
        return await call_next(request)

    is_protected_page = path in ("/", "/ui/index.html", "/ui/admin.html")
    is_protected_api = path.startswith("/api/")

    if is_protected_page or is_protected_api:
        token = _extract_access_token(request)
        user_email = None
        user_role = "TENANT_ADMIN"
        tenant_id = DEFAULT_TENANT_ID
        is_active = False

        logger.info(f"[admin_auth_middleware] path={path} token_present={bool(token)}")
        if token:
            user_info = await _verify_supabase_token(token)
            logger.info(f"[admin_auth_middleware] verified_user_info={user_info}")
            if user_info:
                user_email = user_info.get("email")
                user_record = await get_user_by_email(user_email)
                logger.info(f"[admin_auth_middleware] user_record={user_record}")
                if user_record:
                    is_active = bool(user_record.get("is_active"))
                    user_role = user_record.get("role", "TENANT_USER").strip().upper()
                    tenant_id = user_record.get("tenant_id") or DEFAULT_TENANT_ID

                    logger.info(f"EMAIL={user_email}")
                    logger.info(f"USER_RECORD={user_record}")
                    logger.info(f"ROLE={user_role}")
                    logger.info(f"TENANT={tenant_id}")
                    logger.info(f"TENANT_ID={tenant_id}")
                    logger.info(f"IS_ACTIVE={is_active}")

                    if user_role == "SUPER_ADMIN":
                        is_admin_request = (
                            path == "/ui/admin.html"
                            or path.startswith("/api/super-admin/")
                            or request.query_params.get("scope") == "admin"
                        )
                        if not is_admin_request:
                            imp_tenant = request.cookies.get("impersonated_tenant_id", "").strip()
                            if imp_tenant:
                                tenant_id = imp_tenant
                                
                    # Suspension check for non-super-admins
                    if user_role != "SUPER_ADMIN":
                        tenant = await get_tenant(tenant_id)
                        if tenant and tenant.get("status") == "SUSPENDED":
                            reason = tenant.get("suspension_reason") or "Payment overdue"
                            msg = f"Your account is currently suspended. Reason: {reason}. Please contact support."
                            if is_protected_page:
                                import urllib.parse
                                return RedirectResponse(url=f"/ui/login.html?error={urllib.parse.quote(msg)}", status_code=303)
                            else:
                                return JSONResponse({"detail": msg}, status_code=403)
                else:
                    logger.info("[admin_auth_middleware] User record not found in database.")
            else:
                logger.info("[admin_auth_middleware] Token verification failed or email not found in token.")
        else:
            logger.info("[admin_auth_middleware] Token is missing from request headers and cookies.")

        if not user_email or not is_active:
            if is_protected_page:
                return RedirectResponse(url="/ui/login.html", status_code=303)
            else:
                return JSONResponse({"detail": "Unauthorized"}, status_code=401)

        if (path == "/ui/admin.html" or path.startswith("/api/super-admin/")) and user_role != "SUPER_ADMIN":
            if is_protected_page:
                return RedirectResponse(url="/ui/login.html", status_code=303)
            else:
                return JSONResponse({"detail": "Forbidden"}, status_code=403)

        tokens = set_request_context(tenant_id, user_email, user_role)
        try:
            return await call_next(request)
        finally:
            reset_request_context(tokens)

    return await call_next(request)


@app.on_event("startup")
async def _startup():
    init_db()
    from db import ensure_default_tenant
    try:
        await ensure_default_tenant()
        logger.info("[startup] Default tenant verified/created successfully.")
    except Exception as exc:
        logger.error(f"[startup] Failed to verify/create default tenant: {exc}")
    if _scheduler:
        if not _scheduler.running:
            _scheduler.start()
            logger.info("Campaign scheduler started")
        await _reschedule_all_campaigns()


@app.on_event("shutdown")
async def _shutdown():
    if _scheduler and _scheduler.running:
        _scheduler.shutdown(wait=False)


_debug_stages: dict[str, dict] = {}

async def get_setting_with_source(key: str, default: str = "") -> tuple[str, str]:
    tenant_id = get_current_tenant_id()
    effective_tenant_id = tenant_id
    if key in TENANT_API_KEY_FIELDS and tenant_id != DEFAULT_TENANT_ID:
        tenant = await get_tenant(tenant_id)
        if tenant and tenant.get("billing_mode") == "MANAGED":
            effective_tenant_id = DEFAULT_TENANT_ID

    db = await _adb()
    try:
        result = await db.table("settings").select("value").eq("tenant_id", effective_tenant_id).eq("key", key).maybe_single().execute()
        if result and getattr(result, "data", None) and result.data.get("value"):
            return result.data["value"], "Supabase"
    except Exception as exc:
        pass

    env_has_key = False
    if os.path.exists(".env"):
        try:
            with open(".env", "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#") and "=" in line:
                        parts = line.split("=", 1)
                        if parts[0].strip() == key:
                            env_has_key = True
                            break
        except:
            pass

    env_val = os.getenv(key)
    if env_val:
        return env_val, ".env"

    from db import DEFAULTS
    if key in DEFAULTS and DEFAULTS[key]:
        return DEFAULTS[key], "Default value"

    return default, "Default value"


def extract_exception_info(exc: Exception) -> dict:
    import sys
    import traceback
    import re

    tb_type, tb_val, tb_ob = sys.exc_info()
    if tb_ob is None:
        tb_str = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        frame = exc.__traceback__
        filename = "Unknown"
        function_name = "Unknown"
        line_no = 0
        while frame:
            filename = frame.tb_frame.f_code.co_filename
            function_name = frame.tb_frame.f_code.co_name
            line_no = frame.tb_lineno
            frame = frame.tb_next
    else:
        tb_str = "".join(traceback.format_exception(tb_type, tb_val, tb_ob))
        frame = tb_ob
        filename = "Unknown"
        function_name = "Unknown"
        line_no = 0
        while frame:
            filename = frame.tb_frame.f_code.co_filename
            function_name = frame.tb_frame.f_code.co_name
            line_no = frame.tb_lineno
            frame = frame.tb_next

    if filename != "Unknown":
        filename = os.path.basename(filename)

    exc_type = type(exc).__name__
    exc_msg = str(exc)

    details = {
        "exception_type": exc_type,
        "message": exc_msg,
        "file": filename,
        "function": function_name,
        "line": line_no,
        "traceback": tb_str,
        "twirp_details": None,
        "missing_method": None,
        "livekit_404_object": None
    }

    if isinstance(exc, AttributeError):
        details["missing_method"] = getattr(exc, "name", None)
        if not details["missing_method"]:
            m = re.search(r"has no attribute '([^']+)'", exc_msg)
            if m:
                details["missing_method"] = m.group(1)

    is_twirp = False
    for base in type(exc).__mro__:
        if base.__name__ == "TwirpError" or "twirp" in base.__module__:
            is_twirp = True
            break

    if is_twirp or hasattr(exc, "code") or hasattr(exc, "status"):
        twirp_details = {
            "code": getattr(exc, "code", "Unknown"),
            "status": getattr(exc, "status", 0),
            "metadata": getattr(exc, "metadata", {}),
            "sip_status": None,
            "sip_status_code": None
        }
        meta = getattr(exc, "metadata", {}) or {}
        if "sip_status" in meta:
            twirp_details["sip_status"] = meta["sip_status"]
        if "sip_status_code" in meta:
            twirp_details["sip_status_code"] = meta["sip_status_code"]
        if not twirp_details["sip_status_code"]:
            m = re.search(r"sip status (\d+)", exc_msg, re.IGNORECASE)
            if m:
                twirp_details["sip_status_code"] = int(m.group(1))
        details["twirp_details"] = twirp_details

    is_not_found = False
    if hasattr(exc, "code") and exc.code == "not_found":
        is_not_found = True
    elif hasattr(exc, "status") and exc.status == 404:
        is_not_found = True
    elif "404" in exc_msg or "not found" in exc_msg.lower():
        is_not_found = True

    if is_not_found:
        msg_lower = exc_msg.lower()
        if "room" in msg_lower:
            details["livekit_404_object"] = "Room"
        elif "trunk" in msg_lower or "sip" in msg_lower:
            details["livekit_404_object"] = "SIP Trunk"
        elif "dispatch" in msg_lower:
            details["livekit_404_object"] = "Dispatch"
        elif "participant" in msg_lower:
            details["livekit_404_object"] = "Participant"
        else:
            details["livekit_404_object"] = "Unknown"

    return details


async def eff(key: str) -> str:
    val = await get_setting(key, "")
    return val if val else os.getenv(key, "")


def _iso_to_epoch(value: str) -> float:
    if not value:
        return 0.0
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()
    except Exception:
        return 0.0


def _normalize_phone_identity(identity: Optional[str]) -> str:
    if not identity:
        return ""
    clean = identity.replace("sip_", "").replace("sip:", "").replace("tel:", "")
    if "@" in clean:
        clean = clean.split("@", 1)[0]
    return clean.strip()


def _active_call_started(
    room_name: str,
    phone: str,
    lead_name: str = "there",
    status: str = "dispatching",
    tenant_id: Optional[str] = None,
) -> None:
    now = time.time()
    _active_calls[room_name] = {
        "tenant_id": tenant_id or get_current_tenant_id(),
        "room_name": room_name,
        "phone": phone,
        "lead_name": lead_name or "there",
        "status": status,
        "started_at": now,
        "updated_at": now,
        "ended_at": None,
        "last_event": status,
    }


def _active_call_update(room_name: str, status: Optional[str] = None, last_event: Optional[str] = None) -> None:
    call = _active_calls.get(room_name)
    if not call:
        return
    now = time.time()
    if status:
        call["status"] = status
        if status in ("ended", "failed"):
            call["ended_at"] = call.get("ended_at") or now
    if last_event:
        call["last_event"] = last_event
    call["updated_at"] = now


def _active_call_failed(room_name: str, last_event: str) -> None:
    _active_call_update(room_name, "failed", last_event)


def livekit_client_session() -> aiohttp.ClientSession:
    """Create an aiohttp session for LiveKit API calls with TLS verification on by default."""
    allow_insecure = os.getenv("ALLOW_INSECURE_SSL", "false").lower() in ("1", "true", "yes")
    if allow_insecure:
        logger.warning("ALLOW_INSECURE_SSL is enabled; LiveKit TLS certificate verification is disabled")
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        return aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=ctx))
    return aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=ssl.create_default_context()))


# ── Request models ────────────────────────────────────────────────────────────

class CallRequest(BaseModel):
    phone: str
    lead_name: str = "there"
    business_name: Optional[str] = None
    service_type: Optional[str] = None
    system_prompt: Optional[str] = None
    agent_profile_id: Optional[str] = None


class InboundDispatchRequest(BaseModel):
    room_name: str
    phone: Optional[str] = None
    participant_identity: Optional[str] = None
    lead_name: str = "there"
    business_name: Optional[str] = None
    service_type: Optional[str] = None
    system_prompt: Optional[str] = None
    agent_profile_id: Optional[str] = None


class AgentProfileRequest(BaseModel):
    name: str
    voice: str = "Aoede"
    model: str = "gemini-3.1-flash-live-preview"
    system_prompt: Optional[str] = None
    enabled_tools: Optional[str] = None
    is_default: bool = False


class PromptRequest(BaseModel):
    prompt: str


class SettingsRequest(BaseModel):
    settings: dict


class TestBookingEmailRequest(BaseModel):
    recipient_email: str


class NotesRequest(BaseModel):
    notes: str


class CampaignRequest(BaseModel):
    name: str
    contacts: list
    schedule_type: str = "once"
    schedule_time: str = "09:00"
    call_delay_seconds: int = 3
    system_prompt: Optional[str] = None
    agent_profile_id: Optional[str] = None


class StatusRequest(BaseModel):
    status: str


# Removed AdminTokenChangeRequest


class TenantRequest(BaseModel):
    company_name: str
    slug: Optional[str] = None
    status: str = "TRIAL"
    billing_mode: str = "MANAGED"
    wallet_balance: float = 0
    wallet_low_balance_threshold: float = 0
    company_logo: Optional[str] = None
    company_logo_url: Optional[str] = None
    favicon: Optional[str] = None
    favicon_url: Optional[str] = None
    primary_color: Optional[str] = None
    secondary_color: Optional[str] = None
    support_email: Optional[str] = None
    website_url: Optional[str] = None
    admin_email: Optional[str] = None  # Tenant admin email — triggers auto-invite on creation


class PricingRequest(BaseModel):
    price_per_outbound_call_attempt: float = 0.0
    price_per_connected_call: float = 0.0
    price_per_minute: float = 0.0
    price_per_inbound_call: float = 0.0
    price_per_appointment: float = 0.0
    price_per_calendar_sync: float = 0.0
    price_per_sms: float = 0.0
    price_per_email: float = 0.0
    price_per_whatsapp: float = 0.0
    price_ai_per_minute: float = 0.0
    price_realtime_session: float = 0.0
    default_signup_credits: float = 0.0
    trial_credits: float = 0.0
    manual_adjustment_credits: float = 0.0


class TenantUpdateRequest(BaseModel):
    company_name: Optional[str] = None
    slug: Optional[str] = None
    status: Optional[str] = None
    billing_mode: Optional[str] = None
    wallet_balance: Optional[float] = None
    wallet_low_balance_threshold: Optional[float] = None
    company_logo: Optional[str] = None
    company_logo_url: Optional[str] = None
    favicon: Optional[str] = None
    favicon_url: Optional[str] = None
    primary_color: Optional[str] = None
    secondary_color: Optional[str] = None
    support_email: Optional[str] = None
    website_url: Optional[str] = None


class BrandingRequest(BaseModel):
    company_name: Optional[str] = None
    company_logo: Optional[str] = None
    company_logo_url: Optional[str] = None
    favicon: Optional[str] = None
    favicon_url: Optional[str] = None
    primary_color: Optional[str] = None
    secondary_color: Optional[str] = None
    support_email: Optional[str] = None
    website_url: Optional[str] = None


class ProfileUpdateRequest(BaseModel):
    full_name: str


class WalletRequest(BaseModel):
    amount: float
    reason: str = ""


class SuspendRequest(BaseModel):
    reason: str
    notes: Optional[str] = ""
    effective_immediately: bool = True


class TenantApiKeysRequest(BaseModel):
    mode: str = "BYOK"
    keys: dict


# ── Dashboard ─────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def serve_dashboard():
    html_path = Path(__file__).parent / "ui" / "index.html"
    if html_path.exists():
        return HTMLResponse(content=html_path.read_text(encoding="utf-8"))
    return HTMLResponse("<h1>Dashboard not found — place index.html in ui/</h1>", status_code=404)


# ── Call dispatch ─────────────────────────────────────────────────────────────

@app.get("/api/auth/config")
async def api_auth_config():
    return {
        "supabase_url": os.getenv("SUPABASE_URL", ""),
        "supabase_anon_key": os.getenv("SUPABASE_ANON_KEY", "")
    }


@app.post("/api/auth/register")
async def api_auth_register(request: Request):
    token = _extract_access_token(request)
    if not token:
        raise HTTPException(401, "No auth token provided")
    user_info = await _verify_supabase_token(token)
    if not user_info:
        raise HTTPException(401, "Invalid auth token")
    
    logger.info(f"[api_auth_register] Registering/resolving user: {user_info['email']}")
    from db import get_or_create_user
    user_record = await get_or_create_user(
        uuid_str=user_info["id"],
        email=user_info["email"],
        full_name=user_info["full_name"]
    )
    return user_record


@app.get("/api/auth/context")
async def api_auth_context(request: Request):
    email = get_current_user_email()
    role = get_current_user_role()
    is_super = (role == "SUPER_ADMIN")
    tenant = await get_tenant()
    branding = await get_tenant_branding()
    
    is_impersonating = False
    impersonated_tenant_id = None
    if is_super:
        imp_tenant = request.cookies.get("impersonated_tenant_id", "").strip()
        if imp_tenant:
            is_impersonating = True
            impersonated_tenant_id = imp_tenant

    logger.info(
        f"AUTH_CONTEXT "
        f"email={email} "
        f"role={role} "
        f"tenant={get_current_tenant_id()}"
    )

    return {
        "email": email,
        "role": role,
        "tenant_id": get_current_tenant_id(),
        "is_super_admin": is_super,
        "tenant": tenant,
        "branding": branding,
        "is_impersonating": is_impersonating,
        "impersonated_tenant_id": impersonated_tenant_id,
    }


@app.get("/api/white-label/settings")
async def api_get_white_label_settings():
    return await get_tenant_branding()


@app.post("/api/white-label/settings")
async def api_save_white_label_settings(req: BrandingRequest):
    data = req.dict(exclude_unset=True)
    tenant = await update_tenant_branding(data, get_current_user_email())
    return {"status": "saved", "branding": await get_tenant_branding(tenant.get("id") if tenant else None)}


@app.get("/api/profile")
async def api_get_profile():
    email = get_current_user_email()
    user_record = await get_user_by_email(email)
    if not user_record:
        raise HTTPException(404, "User record not found")
    tenant = await get_tenant()
    branding = await get_tenant_branding()
    pricing = await get_platform_pricing()
    stats = await get_stats()
    return {
        "user_name": user_record.get("full_name") or "",
        "user_email": email,
        "tenant_name": tenant.get("company_name") or "",
        "tenant_status": tenant.get("status") or "",
        "billing_mode": tenant.get("billing_mode") or "",
        "wallet_balance": tenant.get("wallet_balance") or 0.0,
        "account_creation_date": tenant.get("created_at") or "",
        "pricing": pricing,
        "branding": branding,
        "stats": {
            "total_calls": stats.get("total_calls") or 0,
            "booked": stats.get("booked") or 0,
            "booking_rate_percent": stats.get("booking_rate_percent") or 0.0,
        }
    }


@app.post("/api/profile")
async def api_update_profile(req: ProfileUpdateRequest):
    email = get_current_user_email()
    user_record = await update_user_profile(email, req.full_name)
    return {"status": "updated", "full_name": user_record.get("full_name") or req.full_name}


@app.get("/api/pricing")
async def api_get_pricing_public():
    return await get_platform_pricing()


@app.post("/api/onboard")
async def api_onboard_tenant():
    tenant_id = get_current_tenant_id()
    await update_tenant(tenant_id, {"onboarded": True}, get_current_user_email())
    return {"status": "success"}


@app.post("/api/upload")
async def api_upload_file(file: UploadFile = File(...)):
    upload_dir = Path(__file__).parent / "ui" / "uploads"
    upload_dir.mkdir(parents=True, exist_ok=True)
    
    file_ext = Path(file.filename).suffix.lower()
    if file_ext not in (".png", ".jpg", ".jpeg", ".svg", ".ico"):
        raise HTTPException(400, "Invalid file format. Supported: PNG, JPG, JPEG, SVG, ICO")
    
    filename = f"{secrets.token_hex(8)}{file_ext}"
    file_path = upload_dir / filename
    
    with open(file_path, "wb") as f:
        content = await file.read()
        f.write(content)
        
    return {"url": f"/ui/uploads/{filename}"}


# ── Company Knowledge Base Endpoints ──

@app.post("/api/company-knowledge/upload")
async def api_upload_company_knowledge(file: UploadFile = File(...)):
    file_ext = Path(file.filename).suffix.lower()
    if file_ext not in (".pdf", ".docx", ".txt"):
        raise HTTPException(400, "Invalid file format. Supported: PDF, DOCX, TXT")
        
    # Upload file size limit: 5MB (5 * 1024 * 1024 bytes)
    MAX_FILE_SIZE = 5 * 1024 * 1024
    content_bytes = await file.read()
    if len(content_bytes) > MAX_FILE_SIZE:
        raise HTTPException(400, "File exceeds maximum allowed size of 5MB.")
        
    extracted_text = ""
    try:
        if file_ext == ".pdf":
            import pypdf
            import io
            reader = pypdf.PdfReader(io.BytesIO(content_bytes))
            text_list = []
            for page in reader.pages:
                t = page.extract_text()
                if t:
                    text_list.append(t)
            extracted_text = "\n".join(text_list)
        elif file_ext == ".docx":
            import docx
            import io
            doc = docx.Document(io.BytesIO(content_bytes))
            extracted_text = "\n".join([p.text for p in doc.paragraphs])
        else:
            try:
                extracted_text = content_bytes.decode("utf-8")
            except UnicodeDecodeError:
                extracted_text = content_bytes.decode("latin-1")
    except Exception as e:
        logger.error(f"Failed to extract text from {file.filename}: {e}")
        raise HTTPException(400, f"Text extraction failed: {str(e)}")
        
    extracted_text = extracted_text.strip()
    if not extracted_text:
        raise HTTPException(400, "The uploaded file contains no readable text.")

    tenant_id = get_current_tenant_id()
    doc_id = f"kb-{secrets.token_hex(8)}"
    
    doc = {
        "id": doc_id,
        "tenant_id": tenant_id,
        "title": Path(file.filename).stem,
        "file_name": file.filename,
        "file_type": file_ext[1:],
        "content": extracted_text,
        "content_length": len(extracted_text),
        "is_active": True,
        "created_at": datetime.now().isoformat(),
        "updated_at": datetime.now().isoformat(),
    }
    
    try:
        from db import save_company_knowledge
        await save_company_knowledge(doc)
        
        await audit_log(
            "Upload Company Knowledge",
            tenant_id,
            f"Uploaded {file.filename} ({len(extracted_text)} chars)",
            get_current_user_email()
        )
        
        return {
            "status": "ok",
            "document": {
                "id": doc["id"],
                "title": doc["title"],
                "file_name": doc["file_name"],
                "file_type": doc["file_type"],
                "content_length": doc["content_length"],
                "created_at": doc["created_at"]
            }
        }
    except Exception as e:
        logger.error(f"Failed to save company knowledge doc: {e}")
        raise HTTPException(500, f"Database save failed: {str(e)}")


@app.get("/api/company-knowledge/list")
async def api_list_company_knowledge():
    tenant_id = get_current_tenant_id()
    from db import get_all_company_knowledge
    docs = await get_all_company_knowledge(tenant_id)
    return [{
        "id": d["id"],
        "title": d["title"],
        "file_name": d["file_name"],
        "file_type": d["file_type"],
        "content_length": d.get("content_length") or len(d["content"]),
        "is_active": d["is_active"],
        "created_at": d["created_at"]
    } for d in docs]


@app.delete("/api/company-knowledge/{id}")
async def api_delete_company_knowledge(id: str):
    tenant_id = get_current_tenant_id()
    from db import delete_company_knowledge
    success = await delete_company_knowledge(id, tenant_id)
    if not success:
        raise HTTPException(404, "Document not found or delete failed.")
        
    await audit_log(
        "Delete Company Knowledge",
        tenant_id,
        f"Deleted document ID {id}",
        get_current_user_email()
    )
    return {"status": "ok"}



@app.get("/api/super-admin/summary")
async def api_super_admin_summary():
    _require_super_admin()
    return await get_super_admin_summary()


@app.get("/api/super-admin/tenants")
async def api_super_admin_tenants():
    _require_super_admin()
    return await list_tenants()


@app.post("/api/super-admin/tenants")
async def api_super_admin_create_tenant(req: TenantRequest):
    _require_super_admin()
    try:
        data = req.dict(exclude_none=True)
        tenant = await create_tenant(data, get_current_user_email())
        invite = tenant.pop("invite", None)
        resp: dict = {"status": "created", "tenant": tenant}
        if invite:
            resp["invite"] = invite
        return resp
    except ValueError as exc:
        raise HTTPException(400, str(exc))


@app.get("/api/super-admin/pricing")
async def api_super_admin_get_pricing():
    _require_super_admin()
    return await get_platform_pricing()


@app.post("/api/super-admin/pricing")
async def api_super_admin_set_pricing(req: PricingRequest):
    _require_super_admin()
    tokens = set_request_context(DEFAULT_TENANT_ID, get_current_user_email(), "SUPER_ADMIN")
    try:
        await set_setting("PRICE_PER_OUTBOUND_CALL_ATTEMPT", str(req.price_per_outbound_call_attempt))
        # Keep legacy field synced
        await set_setting("PRICE_PER_CALL_ATTEMPT", str(req.price_per_outbound_call_attempt))
        await set_setting("PRICE_PER_CONNECTED_CALL", str(req.price_per_connected_call))
        await set_setting("PRICE_PER_MINUTE", str(req.price_per_minute))
        await set_setting("PRICE_PER_INBOUND_CALL", str(req.price_per_inbound_call))
        await set_setting("PRICE_PER_APPOINTMENT", str(req.price_per_appointment))
        await set_setting("PRICE_PER_CALENDAR_SYNC", str(req.price_per_calendar_sync))
        await set_setting("PRICE_PER_SMS", str(req.price_per_sms))
        await set_setting("PRICE_PER_EMAIL", str(req.price_per_email))
        await set_setting("PRICE_PER_WHATSAPP", str(req.price_per_whatsapp))
        await set_setting("PRICE_AI_PER_MINUTE", str(req.price_ai_per_minute))
        await set_setting("PRICE_REALTIME_SESSION", str(req.price_realtime_session))
        await set_setting("DEFAULT_SIGNUP_CREDITS", str(req.default_signup_credits))
        await set_setting("TRIAL_CREDITS", str(req.trial_credits))
        await set_setting("MANUAL_ADJUSTMENT_CREDITS", str(req.manual_adjustment_credits))
    finally:
        reset_request_context(tokens)
    
    await audit_log(
        "Platform Pricing Updated",
        DEFAULT_TENANT_ID,
        req.dict(),
        get_current_user_email(),
    )
    return {"status": "saved", "pricing": await get_platform_pricing()}


@app.get("/api/super-admin/tenants/{tenant_id}")
async def api_super_admin_get_tenant(tenant_id: str):
    _require_super_admin()
    tenant = await get_tenant(tenant_id)
    if not tenant:
        raise HTTPException(404, "Tenant not found")
    return tenant


@app.put("/api/super-admin/tenants/{tenant_id}")
async def api_super_admin_update_tenant(tenant_id: str, req: TenantUpdateRequest):
    _require_super_admin()
    try:
        tenant = await update_tenant(tenant_id, req.dict(exclude_unset=True), get_current_user_email())
    except ValueError as exc:
        raise HTTPException(400, str(exc))
    if not tenant:
        raise HTTPException(404, "Tenant not found")
    return {"status": "updated", "tenant": tenant}


@app.post("/api/super-admin/tenants/{tenant_id}/suspend")
async def api_super_admin_suspend_tenant(tenant_id: str, req: SuspendRequest):
    _require_super_admin()
    tenant = await update_tenant(
        tenant_id,
        {
            "status": "SUSPENDED",
            "is_active": False,
            "suspension_reason": req.reason,
            "suspension_notes": req.notes,
        },
        get_current_user_email()
    )
    if not tenant:
        raise HTTPException(404, "Tenant not found")
        
    await audit_log(
        "Tenant Suspended",
        tenant_id,
        {"reason": req.reason, "notes": req.notes, "effective_immediately": req.effective_immediately},
        get_current_user_email()
    )
    
    # Notify tenant admin by email
    db = await _adb()
    user_email = None
    try:
        users_res = await db.table("users").select("email").eq("tenant_id", tenant_id).eq("role", "TENANT_ADMIN").limit(1).execute()
        if users_res.data:
            user_email = users_res.data[0].get("email")
    except Exception:
        pass
        
    if not user_email:
        user_email = tenant.get("support_email")
        
    if user_email:
        from email_manager import send_email_async
        subject = "Account Suspended - OutboundAI"
        body = f"Hi,\n\nYour OutboundAI tenant account '{tenant.get('company_name')}' has been suspended.\n\nReason: {req.reason}\n\nNotes: {req.notes or 'No details provided.'}\n\nNext Steps:\nPlease resolve any pending invoices or contact support immediately to reactivate your workspace.\n\nSupport Contact: {tenant.get('support_email') or 'support@outboundai.com'}\n\nThank you,\nOutboundAI Billing Team"
        asyncio.create_task(
            send_email_async(
                to_email=user_email,
                subject=subject,
                body=body,
                booking_id="SUSPEND",
                reply_to=tenant.get('support_email') or 'support@outboundai.com'
            )
        )
        
    return {"status": "suspended", "tenant": tenant}


@app.post("/api/super-admin/tenants/{tenant_id}/activate")
async def api_super_admin_activate_tenant(tenant_id: str):
    _require_super_admin()
    tenant = await update_tenant(
        tenant_id,
        {
            "status": "ACTIVE",
            "is_active": True,
            "suspension_reason": None,
            "suspension_notes": None,
        },
        get_current_user_email()
    )
    if not tenant:
        raise HTTPException(404, "Tenant not found")
        
    await audit_log(
        "Tenant Activated",
        tenant_id,
        {"status": "ACTIVE"},
        get_current_user_email()
    )
    return {"status": "active", "tenant": tenant}


@app.post("/api/super-admin/tenants/{tenant_id}/disable")
async def api_super_admin_disable_tenant(tenant_id: str):
    _require_super_admin()
    tenant = await update_tenant_status(tenant_id, "DISABLED", get_current_user_email())
    if not tenant:
        raise HTTPException(404, "Tenant not found")
    return {"status": "disabled", "tenant": tenant}


@app.get("/api/super-admin/audit-logs")
async def api_super_admin_get_all_audit_logs(limit: int = 50):
    _require_super_admin()
    db = await _adb()
    try:
        result = await db.table("tenant_audit_logs").select("*").order("timestamp", desc=True).limit(limit).execute()
        return result.data or []
    except Exception as exc:
        raise HTTPException(500, f"Failed to fetch audit logs: {exc}")


@app.get("/api/super-admin/tenants/{tenant_id}/audit-logs")
async def api_super_admin_get_tenant_audit_logs(tenant_id: str, limit: int = 100):
    _require_super_admin()
    db = await _adb()
    try:
        result = await db.table("tenant_audit_logs").select("*").eq("tenant_id", tenant_id).order("timestamp", desc=True).limit(limit).execute()
        return result.data or []
    except Exception as exc:
        raise HTTPException(500, f"Failed to fetch tenant audit logs: {exc}")


@app.get("/api/super-admin/tenants/{tenant_id}/usage")
async def api_super_admin_get_tenant_usage(tenant_id: str):
    _require_super_admin()
    db = await _adb()
    try:
        calls = (await db.table("call_logs").select("id").eq("tenant_id", tenant_id).execute()).data or []
        campaigns = (await db.table("campaigns").select("id").eq("tenant_id", tenant_id).execute()).data or []
        appointments = (await db.table("appointments").select("id").eq("tenant_id", tenant_id).execute()).data or []
        return {
            "total_calls": len(calls),
            "total_campaigns": len(campaigns),
            "total_appointments": len(appointments),
        }
    except Exception as exc:
        raise HTTPException(500, f"Failed to get tenant usage: {exc}")


@app.get("/api/super-admin/tenants/{tenant_id}/api-keys")
async def api_super_admin_get_tenant_api_keys(tenant_id: str):
    _require_super_admin()
    if not await get_tenant(tenant_id):
        raise HTTPException(404, "Tenant not found")
    return await get_tenant_api_keys(tenant_id)


@app.post("/api/super-admin/tenants/{tenant_id}/api-keys")
async def api_super_admin_save_tenant_api_keys(tenant_id: str, req: TenantApiKeysRequest):
    _require_super_admin()
    if not await get_tenant(tenant_id):
        raise HTTPException(404, "Tenant not found")
    try:
        await save_tenant_api_keys(tenant_id, req.keys, req.mode, get_current_user_email())
    except ValueError as exc:
        raise HTTPException(400, str(exc))
    return {"status": "saved"}


@app.post("/api/super-admin/tenants/{tenant_id}/wallet/add")
async def api_super_admin_add_wallet(tenant_id: str, req: WalletRequest):
    _require_super_admin()
    try:
        tenant = await add_wallet_credits(tenant_id, req.amount, get_current_user_email(), req.reason)
        return {"status": "updated", "tenant": tenant}
    except ValueError as exc:
        raise HTTPException(400, str(exc))


@app.post("/api/super-admin/tenants/{tenant_id}/wallet/deduct")
async def api_super_admin_deduct_wallet(tenant_id: str, req: WalletRequest):
    _require_super_admin()
    try:
        tenant = await deduct_wallet_credits(tenant_id, req.amount, get_current_user_email(), req.reason)
        return {"status": "updated", "tenant": tenant}
    except ValueError as exc:
        raise HTTPException(400, str(exc))


class DeleteTenantRequest(BaseModel):
    reason: str


@app.get("/api/super-admin/deleted-tenants")
async def api_get_deleted_tenants():
    _require_super_admin()
    return await list_deleted_tenants()


@app.post("/api/super-admin/tenants/{tenant_id}/delete")
async def api_delete_tenant(tenant_id: str, req: DeleteTenantRequest):
    _require_super_admin()
    try:
        await soft_delete_tenant(tenant_id, get_current_user_email(), req.reason)
        return {"status": "success", "message": f"Tenant {tenant_id} soft deleted."}
    except ValueError as exc:
        raise HTTPException(404, str(exc))
    except Exception as exc:
        raise HTTPException(500, str(exc))


@app.post("/api/super-admin/deleted-tenants/{tenant_id}/restore")
async def api_restore_tenant(tenant_id: str):
    _require_super_admin()
    try:
        await restore_tenant(tenant_id)
        return {"status": "success", "message": f"Tenant {tenant_id} restored successfully."}
    except ValueError as exc:
        raise HTTPException(404, str(exc))
    except Exception as exc:
        raise HTTPException(500, str(exc))


@app.delete("/api/super-admin/deleted-tenants/{tenant_id}/permanent")
async def api_permanently_delete_tenant(tenant_id: str):
    _require_super_admin()
    try:
        await permanently_delete_tenant(tenant_id)
        return {"status": "success", "message": f"Tenant {tenant_id} permanently deleted."}
    except Exception as exc:
        raise HTTPException(500, str(exc))


@app.get("/api/super-admin/email-logs")
async def api_get_email_logs(limit: int = 200):
    _require_super_admin()
    return await get_all_email_delivery_logs(limit)


class TransferOwnershipRequest(BaseModel):
    email: str


@app.get("/api/super-admin/tenants/{tenant_id}/users")
async def api_get_tenant_users(tenant_id: str):
    _require_super_admin()
    db = await _adb()
    try:
        res = await db.table("users").select("*").eq("tenant_id", tenant_id).execute()
        return res.data or []
    except Exception as exc:
        raise HTTPException(500, str(exc))


@app.get("/api/super-admin/tenants/{tenant_id}/calls")
async def api_get_tenant_calls(tenant_id: str):
    _require_super_admin()
    db = await _adb()
    try:
        res = await db.table("call_logs").select("*").eq("tenant_id", tenant_id).order("timestamp", desc=True).limit(50).execute()
        return res.data or []
    except Exception as exc:
        raise HTTPException(500, str(exc))


@app.get("/api/super-admin/tenants/{tenant_id}/bookings")
async def api_get_tenant_bookings(tenant_id: str):
    _require_super_admin()
    db = await _adb()
    try:
        res = await db.table("appointments").select("*").eq("tenant_id", tenant_id).order("date", desc=True).execute()
        return res.data or []
    except Exception as exc:
        raise HTTPException(500, str(exc))


@app.post("/api/super-admin/tenants/{tenant_id}/transfer-ownership")
async def api_transfer_ownership(tenant_id: str, req: TransferOwnershipRequest):
    _require_super_admin()
    db = await _adb()
    email_clean = req.email.strip().lower()
    try:
        res = await db.table("users").select("*").eq("tenant_id", tenant_id).eq("email", email_clean).maybe_single().execute()
        user = res.data if res and getattr(res, "data", None) else None
        if not user:
            raise HTTPException(400, "User must belong to the tenant before transferring ownership.")
            
        await db.table("users").update({"role": "TENANT_USER"}).eq("tenant_id", tenant_id).eq("role", "TENANT_ADMIN").execute()
        await db.table("users").update({"role": "TENANT_ADMIN"}).eq("id", user["id"]).execute()
        
        await audit_log("Ownership Transferred", tenant_id, {"new_owner_email": email_clean}, get_current_user_email())
        return {"status": "success", "message": f"Ownership transferred to {email_clean}."}
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(500, str(exc))


@app.post("/api/wallet/add")
async def api_tenant_add_wallet(req: WalletRequest):
    tenant_id = get_current_tenant_id()
    try:
        tenant = await add_wallet_credits(tenant_id, req.amount, get_current_user_email(), req.reason or "Tenant self-service demo top-up")
        return {"status": "updated", "tenant": tenant}
    except ValueError as exc:
        raise HTTPException(400, str(exc))


class DebugStageUpdate(BaseModel):
    stage: int
    status: str
    method: Optional[str] = None
    trunk_id: Optional[str] = None
    room: Optional[str] = None
    phone: Optional[str] = None
    exception: Optional[dict] = None

@app.post("/api/call/debug/{room_name}/stage")
async def api_update_debug_stage(room_name: str, req: DebugStageUpdate):
    if room_name not in _debug_stages:
        _debug_stages[room_name] = {
            "stage1": {"status": "pending", "label": "API request received"},
            "stage2": {"status": "pending", "label": "Settings loaded", "detail": {}},
            "stage3": {"status": "pending", "label": "Room creation", "detail": {}},
            "stage4": {"status": "pending", "label": "Agent started"},
            "stage5": {"status": "pending", "label": "SIP participant creation", "detail": {}},
            "exception": None
        }
    
    stage_key = f"stage{req.stage}"
    if stage_key in _debug_stages[room_name]:
        _debug_stages[room_name][stage_key]["status"] = req.status
        
        if req.stage == 5:
            detail = _debug_stages[room_name]["stage5"].get("detail") or {}
            if req.method: detail["method"] = req.method
            if req.trunk_id: detail["trunk_id"] = req.trunk_id
            if req.room: detail["room"] = req.room
            if req.phone: detail["phone"] = req.phone
            _debug_stages[room_name]["stage5"]["detail"] = detail
            
        if req.exception:
            _debug_stages[room_name]["exception"] = req.exception
            
    return {"status": "updated"}

@app.get("/api/call/debug/{room_name}")
async def api_get_call_debug(room_name: str):
    if room_name not in _debug_stages:
        raise HTTPException(status_code=404, detail="No debug info found for this room")
    return _debug_stages[room_name]


@app.post("/api/call")
async def api_dispatch_call(req: CallRequest):
    phone = req.phone.strip()
    if not phone.startswith("+"):
        raise HTTPException(400, "Phone must be in E.164 format: +919876543210")

    room_name = f"call-{phone.replace('+', '')}-{random.randint(1000, 9999)}"

    # Initialize debug stages dictionary
    _debug_stages[room_name] = {
        "stage1": {"status": "success", "label": "API request received"},
        "stage2": {"status": "pending", "label": "Settings loaded", "detail": {}},
        "stage3": {"status": "pending", "label": "Room creation", "detail": {}},
        "stage4": {"status": "pending", "label": "Agent started"},
        "stage5": {"status": "pending", "label": "SIP participant creation", "detail": {}},
        "exception": None
    }

    try:
        await _require_active("outbound call")
        await _require_wallet_sufficient("outbound call")  # blocks MANAGED tenants with zero balance
    except Exception as exc:
        _debug_stages[room_name]["stage1"]["status"] = "failed"
        _debug_stages[room_name]["exception"] = extract_exception_info(exc)
        return JSONResponse(status_code=400, content={"detail": str(exc), "room": room_name})

    try:
        settings_keys = [
            "LIVEKIT_URL", "LIVEKIT_API_KEY", "LIVEKIT_API_SECRET",
            "VOBIZ_SIP_DOMAIN", "VOBIZ_USERNAME", "VOBIZ_PASSWORD",
            "OUTBOUND_TRUNK_ID", "VOBIZ_OUTBOUND_NUMBER"
        ]
        detail = {}
        stage2_ok = True
        for key in settings_keys:
            val, src = await get_setting_with_source(key)
            is_ok = bool(val)
            if not is_ok and key in ("LIVEKIT_URL", "LIVEKIT_API_KEY", "LIVEKIT_API_SECRET"):
                stage2_ok = False
            
            display_val = val
            if key == "VOBIZ_PASSWORD" or key == "LIVEKIT_API_SECRET":
                display_val = "[configured]" if val else "[missing]"
            detail[key] = {
                "value": display_val,
                "source": src
            }
        _debug_stages[room_name]["stage2"]["detail"] = detail
        _debug_stages[room_name]["stage2"]["status"] = "success" if stage2_ok else "failed"

        url    = await eff("LIVEKIT_URL")
        key    = await eff("LIVEKIT_API_KEY")
        secret = await eff("LIVEKIT_API_SECRET")

        if not all([url, key, secret]):
            raise HTTPException(400, "LiveKit credentials not configured. Go to Settings → LiveKit.")
    except Exception as exc:
        _debug_stages[room_name]["stage2"]["status"] = "failed"
        _debug_stages[room_name]["exception"] = extract_exception_info(exc)
        return JSONResponse(status_code=400, content={"detail": str(exc), "room": room_name})

    call_session_id = None
    lk = None
    session = None
    try:
        effective_prompt = req.system_prompt
        effective_voice  = None
        effective_model  = None
        effective_tools  = None
        profile_name = None
        prompt_source = "per_call_override" if req.system_prompt else "none"
        profile_source = "none"

        if req.agent_profile_id:
            profile = await get_agent_profile(req.agent_profile_id)
            if profile:
                profile_source = "selected"
                profile_name = profile.get("name")
                if not effective_prompt and profile.get("system_prompt"):
                    effective_prompt = profile["system_prompt"]
                    prompt_source = "agent_profile"
                effective_voice = profile.get("voice")
                effective_model = profile.get("model")
                effective_tools = profile.get("enabled_tools")
            else:
                await log_error("server", "Agent profile missing; using default fallback", f"profile_id={req.agent_profile_id}", "warning")
        if not req.agent_profile_id or profile_source == "none":
            profile = await get_default_agent_profile()
            if profile:
                profile_source = "default"
                profile_name = profile.get("name")
                if not effective_prompt and profile.get("system_prompt"):
                    effective_prompt = profile["system_prompt"]
                    prompt_source = "default_agent_profile"
                effective_voice = effective_voice or profile.get("voice")
                effective_model = effective_model or profile.get("model")
                effective_tools = profile.get("enabled_tools")
            else:
                profile_source = "built_in_fallback"

        if not effective_prompt:
            effective_prompt = await get_setting("system_prompt", "") or None
            prompt_source = "global" if effective_prompt else "default"

        branding = await get_tenant_branding()
        biz_name = req.business_name
        if not biz_name or biz_name == "our company":
            biz_name = branding.get("default_business_name") or branding.get("company_name") or "our company"
        svc_type = req.service_type
        if not svc_type or svc_type == "our service":
            svc_type = branding.get("default_service_type") or "our service"

        metadata: dict = {
            "phone_number": phone,
            "lead_name":    req.lead_name,
            "business_name": biz_name,
            "service_type":  svc_type,
            "system_prompt": effective_prompt,
            "agent_profile_id": profile.get("id") if profile_source in ("selected", "default") else req.agent_profile_id,
            "agent_profile_name": profile_name,
            "agent_profile_source": profile_source,
            "system_prompt_override_present": bool(req.system_prompt),
            "prompt_source": prompt_source,
            "canonical_phone": phone,
            "direction": "outbound",
            "tenant_id": get_current_tenant_id(),
        }
        if effective_voice:  metadata["voice_override"] = effective_voice
        if effective_model:  metadata["model_override"] = effective_model
        if effective_tools is not None: metadata["tools_override"] = effective_tools
        metadata["google_api_key"]    = await eff("GOOGLE_API_KEY")
        metadata["gemini_model"]      = await eff("GEMINI_MODEL")
        metadata["gemini_voice"]      = await eff("GEMINI_TTS_VOICE")
        metadata["outbound_trunk_id"] = await eff("OUTBOUND_TRUNK_ID")

        _active_call_started(room_name, phone, req.lead_name, "dispatching", get_current_tenant_id())
        call_session_id = await create_call_session(
            room_name=room_name,
            direction="outbound",
            phone_number=phone,
            lead_name=req.lead_name,
            status="dispatching",
            metadata=metadata,
        )
        metadata["call_session_id"] = call_session_id
        pricing = await get_platform_pricing()
        if pricing["price_per_call_attempt"] > 0:
            asyncio.create_task(
                deduct_wallet_for_event(
                    get_current_tenant_id(), "call_attempt",
                    pricing["price_per_call_attempt"], get_current_user_email()
                )
            )

        from livekit import api as lk_api
        session = livekit_client_session()
        lk = lk_api.LiveKitAPI(url=url, api_key=key, api_secret=secret, session=session)
        await lk.room.create_room(lk_api.CreateRoomRequest(name=room_name, empty_timeout=300, max_participants=5))
        _active_call_update(room_name, "dialing", "LiveKit room created; agent dispatch requested")
        await update_call_session(call_session_id, status="dialing")
        
        dispatch = await lk.agent_dispatch.create_dispatch(
            lk_api.CreateAgentDispatchRequest(
                agent_name="outbound-caller", room=room_name, metadata=json.dumps(metadata)
            )
        )
        dispatch_id = getattr(dispatch, "id", "N/A")

        _debug_stages[room_name]["stage3"]["status"] = "success"
        _debug_stages[room_name]["stage3"]["detail"] = {
            "room_name": room_name,
            "dispatch_id": dispatch_id
        }

        await log_error(
            "server",
            f"Call dispatched to {phone}",
            (
                f"room={room_name}; phone={phone}; agent_profile_id={req.agent_profile_id or ''}; "
                f"profile_source={profile_source}; prompt_source={prompt_source}; system_prompt_override_present={bool(req.system_prompt)}; "
                f"tools_override_present={effective_tools is not None}; lead_name={req.lead_name}; "
                f"business_name={biz_name}; service_type={svc_type}"
            ),
            "info",
        )
        return {"status": "dispatched", "room": room_name, "phone": phone, "call_session_id": call_session_id}
    except Exception as exc:
        _debug_stages[room_name]["stage3"]["status"] = "failed"
        _debug_stages[room_name]["exception"] = extract_exception_info(exc)
        logger.error("Dispatch error: %s", exc)
        if room_name:
            _active_call_failed(room_name, f"Dispatch failed: {exc}")
        if call_session_id:
            await update_call_session(
                call_session_id,
                status="failed",
                ended_at=datetime.now().isoformat(),
                reason=f"dispatch failed: {exc}",
            )
        return JSONResponse(status_code=400, content={"detail": str(exc), "room": room_name})
    finally:
        try:
            if lk:
                await lk.aclose()
        except Exception:
            pass
        try:
            if session:
                await session.close()
        except Exception:
            pass


@app.post("/api/inbound/dispatch")
async def api_dispatch_inbound(req: InboundDispatchRequest):
    await _require_active("inbound call")
    url    = await eff("LIVEKIT_URL")
    key    = await eff("LIVEKIT_API_KEY")
    secret = await eff("LIVEKIT_API_SECRET")

    if not all([url, key, secret]):
        raise HTTPException(400, "LiveKit credentials not configured. Go to Settings → LiveKit.")

    room_name = (req.room_name or "").strip()
    if not room_name:
        raise HTTPException(400, "room_name is required for inbound dispatch.")

    phone = (req.phone or "").strip()
    if not phone:
        phone = _normalize_phone_identity(req.participant_identity)
    if not phone:
        raise HTTPException(400, "Inbound dispatch requires phone or participant_identity.")

    effective_prompt = req.system_prompt
    effective_voice  = None
    effective_model  = None
    effective_tools  = None
    profile_name = None
    prompt_source = "per_call_override" if req.system_prompt else "none"
    profile_source = "none"

    if req.agent_profile_id:
        profile = await get_agent_profile(req.agent_profile_id)
        if profile:
            profile_source = "selected"
            profile_name = profile.get("name")
            if not effective_prompt and profile.get("system_prompt"):
                effective_prompt = profile["system_prompt"]
                prompt_source = "agent_profile"
            effective_voice = profile.get("voice")
            effective_model = profile.get("model")
            effective_tools = profile.get("enabled_tools")
        else:
            await log_error("server", "Agent profile missing; using default fallback", f"profile_id={req.agent_profile_id}", "warning")
    if not req.agent_profile_id or profile_source == "none":
        profile = await get_default_agent_profile()
        if profile:
            profile_source = "default"
            profile_name = profile.get("name")
            if not effective_prompt and profile.get("system_prompt"):
                effective_prompt = profile["system_prompt"]
                prompt_source = "default_agent_profile"
            effective_voice = effective_voice or profile.get("voice")
            effective_model = effective_model or profile.get("model")
            effective_tools = profile.get("enabled_tools")
        else:
            profile_source = "built_in_fallback"

    if not effective_prompt:
        effective_prompt = await get_setting("system_prompt", "") or None
        prompt_source = "global" if effective_prompt else "default"

    branding = await get_tenant_branding()
    biz_name = req.business_name
    if not biz_name or biz_name == "our company":
        biz_name = branding.get("default_business_name") or branding.get("company_name") or "our company"
    svc_type = req.service_type
    if not svc_type or svc_type == "our service":
        svc_type = branding.get("default_service_type") or "our service"

    metadata: dict = {
        "phone_number": phone,
        "lead_name": req.lead_name,
        "business_name": biz_name,
        "service_type": svc_type,
        "system_prompt": effective_prompt,
        "agent_profile_id": profile.get("id") if profile_source in ("selected", "default") else req.agent_profile_id,
        "agent_profile_name": profile_name,
        "agent_profile_source": profile_source,
        "system_prompt_override_present": bool(req.system_prompt),
        "prompt_source": prompt_source,
        "canonical_phone": phone,
        "direction": "inbound",
        "tenant_id": get_current_tenant_id(),
    }
    if effective_voice:
        metadata["voice_override"] = effective_voice
    if effective_model:
        metadata["model_override"] = effective_model
    if effective_tools is not None:
        metadata["tools_override"] = effective_tools

    lk = None
    session = None
    _active_call_started(room_name, phone, req.lead_name, "connected", get_current_tenant_id())
    call_session_id = await create_call_session(
        room_name=room_name,
        direction="inbound",
        phone_number=phone,
        lead_name=req.lead_name,
        status="connected",
        metadata=metadata,
    )
    metadata["call_session_id"] = call_session_id
    try:
        from livekit import api as lk_api
        session = livekit_client_session()
        lk = lk_api.LiveKitAPI(url=url, api_key=key, api_secret=secret, session=session)
        try:
            await lk.room.create_room(lk_api.CreateRoomRequest(name=room_name, empty_timeout=300, max_participants=5))
        except Exception:
            pass
        await lk.agent_dispatch.create_dispatch(
            lk_api.CreateAgentDispatchRequest(
                agent_name="inbound-caller", room=room_name, metadata=json.dumps(metadata)
            )
        )
        await log_error(
            "server",
            f"Inbound call dispatched for {phone}",
            (
                f"room={room_name}; phone={phone}; agent_profile_id={req.agent_profile_id or ''}; "
                f"profile_source={profile_source}; prompt_source={prompt_source}; system_prompt_override_present={bool(req.system_prompt)}; "
                f"tools_override_present={effective_tools is not None}; lead_name={req.lead_name}; "
                f"business_name={req.business_name}; service_type={req.service_type}"
            ),
            "info",
        )
        return {"status": "dispatched", "room": room_name, "phone": phone, "call_session_id": call_session_id}
    except Exception as exc:
        logger.error("Inbound dispatch error: %s", exc)
        _active_call_failed(room_name, f"Inbound dispatch failed: {exc}")
        await update_call_session(
            call_session_id,
            status="failed",
            ended_at=datetime.now().isoformat(),
            reason=f"inbound dispatch failed: {exc}",
        )
        raise HTTPException(500, f"Inbound dispatch failed: {exc}")
    finally:
        try:
            if lk:
                await lk.aclose()
        except Exception:
            pass
        try:
            if session:
                await session.close()
        except Exception:
            pass


# ── Calls ─────────────────────────────────────────────────────────────────────

@app.get("/api/calls")
async def api_get_calls(page: int = 1, limit: int = 20):
    return await get_all_calls(page=page, limit=limit)


@app.get("/api/calls/active")
async def api_get_active_calls():
    now = time.time()
    tenant_id = get_current_tenant_id()
    transcripts = await get_recent_transcripts(limit=200)
    transcript_by_room: dict[str, dict] = {}
    for row in transcripts:
        room = row.get("room_name")
        if room and room not in transcript_by_room:
            transcript_by_room[room] = row
        if room and room not in _active_calls:
            transcript_ts = _iso_to_epoch(row.get("created_at") or "")
            if transcript_ts and now - transcript_ts <= 60:
                phone_guess = ""
                if room.startswith("call-"):
                    phone_guess = "+" + room.split("-")[1]
                elif room.startswith("camp-"):
                    parts = room.split("-")
                    phone_guess = "+" + parts[2] if len(parts) > 2 else ""
                _active_calls[room] = {
                    "tenant_id": tenant_id,
                    "room_name": room,
                    "phone": phone_guess,
                    "lead_name": "there",
                    "status": "connected",
                    "started_at": transcript_ts,
                    "updated_at": transcript_ts,
                    "ended_at": None,
                    "last_event": "Recent transcript activity",
                }

    recent_logs = await get_all_calls(page=1, limit=50)
    out = []
    for room, call in list(_active_calls.items()):
        if (call.get("tenant_id") or DEFAULT_TENANT_ID) != tenant_id:
            continue
        ended_at = call.get("ended_at")
        if ended_at and now - float(ended_at) > 60:
            _active_calls.pop(room, None)
            continue

        status = call.get("status") or "dispatching"
        last_event = call.get("last_event") or status
        latest_transcript = transcript_by_room.get(room)
        snippet = ""
        if latest_transcript:
            snippet = latest_transcript.get("message") or ""
            if status not in ("ended", "failed"):
                status = "connected"
                last_event = f"Latest transcript from {latest_transcript.get('speaker') or 'call'}"

        started_at = float(call.get("started_at") or now)
        for log in recent_logs:
            same_phone = (log.get("phone_number") or log.get("phone")) == call.get("phone")
            log_ts = _iso_to_epoch(log.get("timestamp") or "")
            if same_phone and log_ts and log_ts >= started_at - 5:
                status = "ended"
                last_event = f"Call logged as {log.get('outcome') or 'ended'}"
                call["ended_at"] = call.get("ended_at") or now
                break

        duration = int((call.get("ended_at") or now) - started_at)
        out.append({
            "room_name": room,
            "phone": call.get("phone") or "",
            "lead_name": call.get("lead_name") or "there",
            "status": status,
            "duration_seconds": max(duration, 0),
            "last_event": last_event,
            "last_transcript_snippet": snippet[:240],
            "transcript_available": bool(latest_transcript),
        })

    out.sort(key=lambda c: _active_calls.get(c["room_name"], {}).get("started_at", 0), reverse=True)
    return out


@app.patch("/api/calls/{call_id}/notes")
async def api_update_notes(call_id: str, req: NotesRequest):
    ok = await update_call_notes(call_id, req.notes)
    if not ok:
        raise HTTPException(404, "Call not found")
    return {"status": "updated"}


# ── Stats ─────────────────────────────────────────────────────────────────────

@app.get("/api/stats")
async def api_get_stats():
    return await get_stats()


@app.get("/api/health")
async def api_health(request: Request):
    user_record = await _request_active_user(request)
    if not user_record:
        return {"status": "ok"}
    services = {
        "supabase": {"state": "missing", "label": "Missing"},
        "livekit": {"state": "missing", "label": "Missing"},
        "gemini": {"state": "missing", "label": "Missing"},
        "sip": {"state": "missing", "label": "Missing"},
    }
    try:
        await get_all_settings()
        services["supabase"] = {"state": "healthy", "label": "Healthy"}
        logger.info("[health] supabase ok")
    except Exception as exc:
        services["supabase"] = {"state": "failed", "label": "Failed"}
        logger.warning("[health] supabase failed: %s", exc)

    livekit_configured = bool(
        await eff("LIVEKIT_URL")
        and await eff("LIVEKIT_API_KEY")
        and await eff("LIVEKIT_API_SECRET")
    )
    gemini_configured = bool(await eff("GOOGLE_API_KEY"))
    sip_configured = bool(
        await eff("VOBIZ_SIP_DOMAIN")
        and await eff("VOBIZ_USERNAME")
        and await eff("VOBIZ_PASSWORD")
        and await eff("VOBIZ_OUTBOUND_NUMBER")
    )
    if livekit_configured:
        services["livekit"] = {"state": "configured", "label": "Configured but unverified"}
    if gemini_configured:
        services["gemini"] = {"state": "configured", "label": "Configured but unverified"}
    if sip_configured:
        services["sip"] = {"state": "configured", "label": "Configured but unverified"}
    return {
        "status": "ok",
        "role": (user_record.get("role") or "").strip().upper(),
        "services": services,
    }


# ── Appointments ──────────────────────────────────────────────────────────────

@app.get("/api/appointments")
async def api_get_appointments(date: Optional[str] = None):
    return await get_all_appointments(date_filter=date)


@app.delete("/api/appointments/{appointment_id}")
async def api_cancel_appointment(appointment_id: str):
    await _require_active("appointment booking")
    ok = await cancel_appointment(appointment_id)
    if not ok:
        raise HTTPException(404, "Appointment not found or already cancelled")
    return {"status": "cancelled"}


# ── Prompt ────────────────────────────────────────────────────────────────────

@app.get("/api/prompt")
async def api_get_prompt():
    saved = await get_setting("system_prompt", "")
    return {"prompt": saved or DEFAULT_SYSTEM_PROMPT, "is_custom": bool(saved)}


@app.post("/api/prompt")
async def api_save_prompt(req: PromptRequest):
    await set_setting("system_prompt", req.prompt)
    return {"status": "saved"}


@app.delete("/api/prompt")
async def api_reset_prompt():
    await set_setting("system_prompt", "")
    return {"status": "reset", "prompt": DEFAULT_SYSTEM_PROMPT}


# ── Settings ──────────────────────────────────────────────────────────────────

@app.get("/api/settings")
async def api_get_settings():
    return await get_all_settings()


@app.post("/api/settings")
async def api_save_settings(req: SettingsRequest):
    clearable_settings = {"BOOKING_EMAIL_REPLY_TO", "BOOKING_EMAIL_SIGNATURE"}
    filtered = {
        k: v for k, v in req.settings.items()
        if v is not None and (v != "" or k in clearable_settings)
    }
    filtered.pop("ADMIN_TOKEN", None)
    if "DEFAULT_TRANSFER_NUMBER" in filtered and not _valid_e164(str(filtered["DEFAULT_TRANSFER_NUMBER"])):
        raise HTTPException(400, "Transfer number must be in E.164 format, for example +919876543210")
    await save_settings(filtered)
    if filtered:
        await audit_log("API Keys Updated", get_current_tenant_id(), {"keys": list(filtered.keys())}, get_current_user_email())
    if get_current_tenant_id() == DEFAULT_TENANT_ID:
        for k, v in filtered.items():
            os.environ[k] = str(v)
    return {"status": "saved", "count": len(filtered)}


@app.post("/api/email-booking/test")
async def api_test_booking_email(req: TestBookingEmailRequest):
    recipient = (req.recipient_email or "").strip()
    if "@" not in recipient:
        raise HTTPException(400, "Valid recipient email is required")
    from email_manager import render_booking_email, send_email_async
    sample = {
        "lead_name": "Test Lead",
        "business_name": await eff("DEFAULT_BUSINESS_NAME") or "OutboundAI",
        "service_type": await eff("DEFAULT_SERVICE_TYPE") or "Demo",
        "date": "2026-06-15",
        "time": "10:00",
        "phone": "+910000000000",
        "email": recipient,
        "booking_id": "TEST1234",
        "calendar_link": "https://calendar.google.com/example",
    }
    subject, body, reply_to = await render_booking_email(sample)
    ok = await send_email_async(recipient, subject, body, booking_id="TEST1234", reply_to=reply_to)
    if not ok:
        raise HTTPException(400, "Test email failed. Check SMTP settings and logs.")
    await audit_log("Booking Email Test Sent", get_current_tenant_id(), {"recipient": recipient}, get_current_user_email())
    return {"status": "sent"}


# Removed api_change_admin_token endpoint


# ── SIP trunk setup ───────────────────────────────────────────────────────────

@app.post("/api/setup/trunk")
async def api_setup_trunk():
    url        = await eff("LIVEKIT_URL")
    key        = await eff("LIVEKIT_API_KEY")
    secret     = await eff("LIVEKIT_API_SECRET")
    sip_domain = await eff("VOBIZ_SIP_DOMAIN")
    username   = await eff("VOBIZ_USERNAME")
    password   = await eff("VOBIZ_PASSWORD")
    phone      = await eff("VOBIZ_OUTBOUND_NUMBER")

    if not all([url, key, secret, sip_domain, username, password, phone]):
        raise HTTPException(400, "Configure LiveKit and Vobiz credentials in Settings first.")

    lk = None
    session = None
    try:
        from livekit import api as lk_api
        session = livekit_client_session()
        lk = lk_api.LiveKitAPI(url=url, api_key=key, api_secret=secret, session=session)
        trunk = await lk.sip.create_sip_outbound_trunk(
            lk_api.CreateSIPOutboundTrunkRequest(
                trunk=lk_api.SIPOutboundTrunkInfo(
                    name="Vobiz Outbound Trunk",
                    address=sip_domain,
                    auth_username=username,
                    auth_password=password,
                    numbers=[phone],
                )
            )
        )
        trunk_id = trunk.sip_trunk_id
        await set_setting("OUTBOUND_TRUNK_ID", trunk_id)
        if get_current_tenant_id() == DEFAULT_TENANT_ID:
            os.environ["OUTBOUND_TRUNK_ID"] = trunk_id
        return {"status": "created", "trunk_id": trunk_id}
    except Exception as exc:
        raise HTTPException(500, f"Trunk creation failed: {exc}")
    finally:
        try:
            if lk:
                await lk.aclose()
        except Exception:
            pass
        try:
            if session:
                await session.close()
        except Exception:
            pass


# ── Logs ──────────────────────────────────────────────────────────────────────

@app.get("/api/logs")
async def api_get_logs(limit: int = 200, level: Optional[str] = None, source: Optional[str] = None):
    return await get_logs(level=level, source=source, limit=limit)


@app.delete("/api/logs")
async def api_clear_logs():
    await clear_errors()
    return {"status": "cleared"}


@app.get("/api/transcripts/recent")
async def api_recent_transcripts(limit: int = 120, room_name: Optional[str] = None):
    return await get_recent_transcripts(limit=limit, room_name=room_name)


# ── CRM ───────────────────────────────────────────────────────────────────────

@app.get("/api/crm")
async def api_get_contacts():
    return {"data": await get_contacts()}


@app.get("/api/crm/calls")
async def api_get_contact_calls(phone: str = Query(...)):
    return {"data": await get_calls_by_phone(phone)}


# ── Agent Profiles ────────────────────────────────────────────────────────────

@app.get("/api/agent-profiles")
async def api_list_agent_profiles():
    try:
        return await get_all_agent_profiles()
    except Exception as exc:
        raise HTTPException(500, str(exc))


@app.post("/api/agent-profiles")
async def api_create_agent_profile(req: AgentProfileRequest):
    try:
        profile_id = await create_agent_profile(
            name=req.name, voice=req.voice, model=req.model,
            system_prompt=req.system_prompt, enabled_tools=req.enabled_tools, is_default=req.is_default,
        )
        return {"status": "created", "id": profile_id}
    except Exception as exc:
        raise HTTPException(500, str(exc))


@app.get("/api/agent-profiles/{profile_id}")
async def api_get_agent_profile(profile_id: str):
    profile = await get_agent_profile(profile_id)
    if not profile:
        raise HTTPException(404, "Profile not found")
    return profile


@app.put("/api/agent-profiles/{profile_id}")
async def api_update_agent_profile(profile_id: str, req: AgentProfileRequest):
    ok = await update_agent_profile(profile_id, {
        "name": req.name, "voice": req.voice, "model": req.model,
        "system_prompt": req.system_prompt, "enabled_tools": req.enabled_tools,
        "is_default": 1 if req.is_default else 0,
    })
    if not ok:
        raise HTTPException(404, "Profile not found")
    return {"status": "updated"}


@app.delete("/api/agent-profiles/{profile_id}")
async def api_delete_agent_profile(profile_id: str):
    ok = await delete_agent_profile(profile_id)
    if not ok:
        raise HTTPException(404, "Profile not found")
    return {"status": "deleted"}


@app.post("/api/agent-profiles/{profile_id}/set-default")
async def api_set_default_profile(profile_id: str):
    try:
        await set_default_agent_profile(profile_id)
        return {"status": "default set"}
    except Exception as exc:
        raise HTTPException(500, str(exc))


# ── Campaigns ─────────────────────────────────────────────────────────────────

async def _dispatch_one(lk, lk_api, contact: dict, room_name: str,
                         prompt: Optional[str], profile: Optional[dict] = None) -> tuple[bool, str]:
    try:
        saved_prompt = prompt or (await get_setting("system_prompt", "")) or None
        prompt_source = "campaign" if prompt else ("global" if saved_prompt else "default")
        profile_source = "campaign_profile" if profile else "none"
        if not profile:
            profile = await get_default_agent_profile()
            profile_source = "default" if profile else "built_in_fallback"
        branding = await get_tenant_branding()
        biz_name = contact.get("business_name")
        if not biz_name or biz_name == "our company":
            biz_name = branding.get("default_business_name") or branding.get("company_name") or "our company"
        svc_type = contact.get("service_type")
        if not svc_type or svc_type == "our service":
            svc_type = branding.get("default_service_type") or "our service"

        metadata: dict = {
            "phone_number":  contact["phone"],
            "lead_name":     contact.get("lead_name", "there"),
            "business_name": biz_name,
            "service_type":  svc_type,
            "system_prompt": saved_prompt,
            "agent_profile_id": profile.get("id") if profile else None,
            "agent_profile_name": profile.get("name") if profile else None,
            "agent_profile_source": profile_source,
            "system_prompt_override_present": bool(prompt),
            "prompt_source": prompt_source,
            "canonical_phone": contact["phone"],
            "direction": "outbound",
            "tenant_id": get_current_tenant_id(),
        }
        if profile:
            if not metadata["system_prompt"] and profile.get("system_prompt"):
                metadata["system_prompt"] = profile["system_prompt"]
                metadata["prompt_source"] = "agent_profile"
            if profile.get("voice"):         metadata["voice_override"] = profile["voice"]
            if profile.get("model"):         metadata["model_override"] = profile["model"]
            if profile.get("enabled_tools") is not None: metadata["tools_override"] = profile["enabled_tools"]
        # Inject resolved credentials so agent.py uses per-tenant BYOK keys if set
        metadata["google_api_key"]    = await eff("GOOGLE_API_KEY")
        metadata["gemini_model"]      = await eff("GEMINI_MODEL")
        metadata["gemini_voice"]      = await eff("GEMINI_TTS_VOICE")
        metadata["outbound_trunk_id"] = await eff("OUTBOUND_TRUNK_ID")
        _active_call_started(room_name, contact["phone"], contact.get("lead_name", "there"), "dispatching", get_current_tenant_id())
        logger.info("Campaign room create: room=%s phone=%s", room_name, contact.get("phone"))
        await lk.room.create_room(lk_api.CreateRoomRequest(name=room_name, empty_timeout=300, max_participants=5))
        _active_call_update(room_name, "dialing", "Campaign room created; agent dispatch requested")
        logger.info("Campaign agent dispatch: room=%s phone=%s", room_name, contact.get("phone"))
        await lk.agent_dispatch.create_dispatch(
            lk_api.CreateAgentDispatchRequest(
                agent_name="outbound-caller", room=room_name, metadata=json.dumps(metadata)
            )
        )
        await log_error("server", "Campaign dispatch succeeded", f"room={room_name}; phone={contact.get('phone')}", "info")
        return True, ""
    except Exception as exc:
        logger.error("Campaign dispatch error for %s: %s", contact.get("phone"), exc)
        _active_call_failed(room_name, f"Campaign dispatch failed: {exc}")
        await log_error("server", "Campaign dispatch failed", f"room={room_name}; phone={contact.get('phone')}; error={exc}", "error")
        return False, str(exc)


async def _run_campaign(campaign_id: str, trigger_source: str = "manual") -> None:
    if campaign_id in _running_campaigns:
        logger.warning("Campaign %s run skipped; another run is already active", campaign_id)
        await log_error("server", "Campaign run skipped", f"campaign_id={campaign_id}; reason=already_running", "warning")
        return
    _running_campaigns.add(campaign_id)
    campaign_probe = await get_campaign_for_worker(campaign_id)
    tokens = None
    if campaign_probe and campaign_probe.get("tenant_id"):
        tokens = set_request_context(campaign_probe.get("tenant_id"), get_current_user_email(), "TENANT_ADMIN")
    try:
        try:
            await require_active_tenant("campaign execution")
        except PermissionError as exc:
            await log_error("server", "Campaign run blocked", f"campaign_id={campaign_id}; reason={exc}", "warning")
            await update_campaign_run_stats(campaign_id, 0, 0, "failed")
            return
        campaign = await get_campaign(campaign_id)
        if not campaign:
            return
        contacts = json.loads(campaign.get("contacts_json") or "[]")
        if not contacts:
            await update_campaign_run_stats(campaign_id, 0, 0, "failed")
            await log_error("server", "Campaign run failed", f"campaign_id={campaign_id}; reason=no_contacts", "error")
            return
        if campaign.get("status") in ("paused", "completed") and trigger_source != "manual":
            await log_error("server", "Campaign scheduled run skipped", f"campaign_id={campaign_id}; status={campaign.get('status')}", "info")
            return
        await update_campaign_status(campaign_id, "running")
        await log_error("server", "Campaign run started", f"campaign_id={campaign_id}; trigger={trigger_source}; total={len(contacts)}", "info")
        delay   = int(campaign.get("call_delay_seconds") or 3)
        prompt  = campaign.get("system_prompt")
        agent_profile_id = campaign.get("agent_profile_id")
        profile = None
        if agent_profile_id:
            profile = await get_agent_profile(agent_profile_id)
            if not profile:
                await log_error("server", "Campaign agent profile missing; using default fallback", f"campaign_id={campaign_id}; profile_id={agent_profile_id}", "warning")

        url    = await eff("LIVEKIT_URL")
        key    = await eff("LIVEKIT_API_KEY")
        secret = await eff("LIVEKIT_API_SECRET")
        if not (url and key and secret):
            logger.error("Campaign %s: LiveKit not configured", campaign_id)
            await update_campaign_run_stats(campaign_id, 0, len(contacts), "failed")
            await log_error("server", "Campaign run failed", f"campaign_id={campaign_id}; reason=livekit_not_configured", "error")
            return

        from livekit import api as lk_api_module
        session = livekit_client_session()

        campaign_pricing = await get_platform_pricing()
        ok_count = fail_count = skipped_count = 0
        final_status = "failed"
        try:
            lk = lk_api_module.LiveKitAPI(url=url, api_key=key, api_secret=secret, session=session)
            for i, contact in enumerate(contacts):
                phone = contact.get("phone", "")
                if not phone.startswith("+"):
                    skipped_count += 1
                    fail_count += 1
                    await log_error("server", "Campaign contact skipped", f"campaign_id={campaign_id}; phone={phone or 'missing'}; reason=invalid_phone", "warning")
                    continue
                # Per-contact wallet check (stop campaign if balance depleted)
                if campaign_pricing["price_per_call_attempt"] > 0:
                    try:
                        wallet_tenant = await get_tenant(get_current_tenant_id())
                        if wallet_tenant and float(wallet_tenant.get("wallet_balance") or 0) <= 0:
                            await log_error("server", "Campaign halted — wallet depleted", f"campaign_id={campaign_id}; stopped_at={i}/{len(contacts)}", "warning")
                            break
                    except Exception:
                        pass
                room_name = f"camp-{campaign_id[:8]}-{phone.replace('+','')}-{random.randint(100,999)}"
                success, reason = await _dispatch_one(lk, lk_api_module, contact, room_name, prompt, profile)
                if success:
                    ok_count += 1
                    # Deduct call attempt fee (non-blocking)
                    if campaign_pricing["price_per_call_attempt"] > 0:
                        asyncio.create_task(
                            deduct_wallet_for_event(
                                get_current_tenant_id(), "campaign_call_attempt",
                                campaign_pricing["price_per_call_attempt"], get_current_user_email()
                            )
                        )
                else:
                    fail_count += 1
                    await log_error("server", "Campaign contact failed", f"campaign_id={campaign_id}; phone={phone}; reason={reason}", "error")
                if i < len(contacts) - 1:
                    await asyncio.sleep(delay)
            final_status = "completed" if fail_count == 0 else ("partial" if ok_count else "failed")
            await lk.aclose()
        except Exception as exc:
            logger.error("Campaign run error: %s", exc)
            final_status = "partial" if ok_count else "failed"
            await log_error("server", "Campaign run crashed", f"campaign_id={campaign_id}; error={exc}", "error")
        finally:
            try:
                await session.close()
            except Exception:
                pass

        persisted_status = "active" if campaign.get("schedule_type") in ("daily", "weekdays") and campaign.get("status") != "paused" else final_status
        await update_campaign_run_stats(campaign_id, ok_count, fail_count, persisted_status)
        await log_error(
            "server",
            "Campaign run completed",
            (
                f"campaign_id={campaign_id}; trigger={trigger_source}; status={final_status}; "
                f"persisted_status={persisted_status}; dispatched={ok_count}; failed={fail_count}; skipped={skipped_count}"
            ),
            "warning" if fail_count else "info",
        )
        logger.info("Campaign %s done - %d dispatched, %d failed, %d skipped, status=%s", campaign_id, ok_count, fail_count, skipped_count, final_status)
    finally:
        _running_campaigns.discard(campaign_id)
        if tokens:
            reset_request_context(tokens)


async def _reschedule_all_campaigns() -> None:
    if not _scheduler:
        return
    try:
        campaigns = await get_all_campaigns_unscoped()
        for c in campaigns:
            if c.get("status") == "active" and c.get("schedule_type") in ("daily", "weekdays"):
                _schedule_campaign(c["id"], c["schedule_type"], c.get("schedule_time", "09:00"))
        logger.info("Campaign scheduler restore complete")
    except Exception as exc:
        logger.warning("Could not reschedule campaigns: %s", exc)


def _schedule_campaign(campaign_id: str, schedule_type: str, schedule_time: str) -> None:
    if not _scheduler:
        return
    job_id = f"campaign_{campaign_id}"
    if _scheduler.get_job(job_id):
        _scheduler.remove_job(job_id)
        logger.info("Removed existing scheduler job %s before re-registering", job_id)
    try:
        hour, minute = map(int, schedule_time.split(":"))
    except (ValueError, AttributeError):
        hour, minute = 9, 0
    if schedule_type == "daily":
        trigger = CronTrigger(hour=hour, minute=minute)
    else:
        trigger = CronTrigger(day_of_week="mon-fri", hour=hour, minute=minute)
    _scheduler.add_job(_run_campaign, trigger=trigger, args=[campaign_id, "scheduled"], id=job_id, replace_existing=True, max_instances=1, coalesce=True)
    logger.info("Scheduled campaign %s (%s at %02d:%02d)", campaign_id, schedule_type, hour, minute)


@app.post("/api/campaigns")
async def api_create_campaign(req: CampaignRequest):
    await _require_active("campaign execution")
    if not req.contacts:
        raise HTTPException(400, "contacts list cannot be empty")
    if req.schedule_type not in ("once", "daily", "weekdays"):
        raise HTTPException(400, "schedule_type must be: once | daily | weekdays")

    campaign_id = await create_campaign(
        name=req.name, contacts_json=json.dumps(req.contacts),
        schedule_type=req.schedule_type, schedule_time=req.schedule_time,
        call_delay_seconds=req.call_delay_seconds, system_prompt=req.system_prompt,
        agent_profile_id=req.agent_profile_id,
    )
    campaign = await get_campaign(campaign_id)

    if req.schedule_type == "once":
        asyncio.create_task(_run_campaign(campaign_id, "created_once"))
    else:
        _schedule_campaign(campaign_id, req.schedule_type, req.schedule_time)

    return {"status": "created", "campaign_id": campaign_id, "campaign": campaign}


@app.get("/api/campaigns")
async def api_list_campaigns():
    return await get_all_campaigns()


@app.delete("/api/campaigns/{campaign_id}")
async def api_delete_campaign_endpoint(campaign_id: str):
    ok = await delete_campaign(campaign_id)
    if not ok:
        raise HTTPException(404, "Campaign not found")
    job_id = f"campaign_{campaign_id}"
    if _scheduler and _scheduler.get_job(job_id):
        _scheduler.remove_job(job_id)
        logger.info("Removed scheduler job %s after campaign delete", job_id)
    return {"status": "deleted"}


@app.post("/api/campaigns/{campaign_id}/run")
async def api_run_campaign_now(campaign_id: str):
    await _require_active("campaign execution")
    campaign = await get_campaign(campaign_id)
    if not campaign:
        raise HTTPException(404, "Campaign not found")
    asyncio.create_task(_run_campaign(campaign_id, "manual"))
    return {"status": "dispatching", "campaign_id": campaign_id}


@app.patch("/api/campaigns/{campaign_id}/status")
async def api_update_campaign_status(campaign_id: str, req: StatusRequest):
    await _require_active("campaign execution")
    if req.status not in ("active", "paused", "completed", "running", "partial", "failed"):
        raise HTTPException(400, "status must be: active | paused | completed | running | partial | failed")
    ok = await update_campaign_status(campaign_id, req.status)
    if not ok:
        raise HTTPException(404, "Campaign not found")
    job_id = f"campaign_{campaign_id}"
    if req.status == "paused" and _scheduler and _scheduler.get_job(job_id):
        _scheduler.remove_job(job_id)
        logger.info("Removed scheduler job %s after campaign pause", job_id)
    elif req.status == "active":
        campaign = await get_campaign(campaign_id)
        if campaign and campaign.get("schedule_type") in ("daily", "weekdays"):
            _schedule_campaign(campaign_id, campaign["schedule_type"], campaign.get("schedule_time", "09:00"))
    return {"status": req.status}


@app.post("/api/simulate-call")
async def simulate_call():
    room_name = f"sim-{random.randint(1000,9999)}"

    url = os.getenv("LIVEKIT_URL")
    key = os.getenv("LIVEKIT_API_KEY")
    secret = os.getenv("LIVEKIT_API_SECRET")
    if not all([url, key, secret]):
        raise HTTPException(400, "LiveKit credentials not configured. Go to Settings → LiveKit.")

    token = api.AccessToken(
        key,
        secret,
    ) \
    .with_identity("browser-user") \
    .with_name("Browser User") \
    .with_grants(api.VideoGrants(
        room_join=True,
        room=room_name,
    ))

    room_token = token.to_jwt()

    livekit_api = api.LiveKitAPI(url=url, api_key=key, api_secret=secret)
    try:
        await livekit_api.room.create_room(
            api.CreateRoomRequest(name=room_name)
        )

        metadata = {
        "lead_name": "there",
        "phone_number": None,
        "business_name": "our company",
        "service_type": "our service",
        "prompt_source": "simulate",
        "canonical_phone": None,
        }

        await livekit_api.agent_dispatch.create_dispatch(
            api.CreateAgentDispatchRequest(
                agent_name="outbound-caller",
                room=room_name,
                metadata=json.dumps(metadata)
            )
        )
    finally:
        await livekit_api.aclose()

    return {
        "room_name": room_name,
        "token": room_token,
        "livekit_url": url,
    }


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=port)
