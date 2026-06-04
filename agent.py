import asyncio
import json
import logging
import os
import re
import ssl
import certifi
import traceback
from typing import Optional

from dotenv import load_dotenv

# Patch SSL before any network import — reused from LIvekitAIVoice pattern
_orig_ssl = ssl.create_default_context
def _certifi_ssl(purpose=ssl.Purpose.SERVER_AUTH, **kwargs):
    if not kwargs.get("cafile") and not kwargs.get("capath") and not kwargs.get("cadata"):
        kwargs["cafile"] = certifi.where()
    return _orig_ssl(purpose, **kwargs)
ssl.create_default_context = _certifi_ssl

from livekit import agents, api, rtc
from livekit.agents import Agent, AgentSession, RoomInputOptions
try:
    from livekit.agents import RoomOptions as _RoomOptions
    _HAS_ROOM_OPTIONS = True
except ImportError:
    _HAS_ROOM_OPTIONS = False
from livekit.plugins import noise_cancellation

from db import init_db, log_error, get_enabled_tools, save_transcript
from prompts import build_prompt
from tools import AppointmentTools, DEFAULT_TOOL_NAMES, MANDATORY_BOOKING_TOOLS

load_dotenv(".env", override=False)  # VPS env vars always win — .env only for local dev
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("outbound-agent")

SIP_DOMAIN = os.getenv("VOBIZ_SIP_DOMAIN", "")

MANDATORY_TOOL_CONTRACT = """

MANDATORY TOOL CONTRACT:
- Always use the provided lead_name, business_name, service_type, phone, agent name, and organization metadata.
- Never invent company names, services, phone numbers, lead names, agent names, calendar details, SMS status, prices, or locations.
- Never invent a prior inquiry, existing appointment, previous date/time, insurance topic, booking, or contact history unless a tool or call metadata confirms it.
- Never say "Alex", "ABC Insurance", "insurance options", or "recent inquiry" unless those exact values are present in resolved call metadata or the user's prompt.
- If a value is missing, ask the user or use known call metadata.
- If the user says "use the number you called me on", "same number", or "this number", use the call metadata phone number from the dispatch payload.
- If phone is missing from metadata, ask the user to confirm their phone number.
- Never say a time slot is available unless check_availability returned available.
- Never say an appointment is booked, scheduled, or confirmed unless book_appointment returned a Booking ID.
- Never say SMS was sent until send_sms_confirmation succeeds.
- Never say a calendar event was created unless book_appointment reports Calendar sync success or a booking confirmation.
- If the user asks to book, first collect date, time, service, name, and phone if missing.
- Then call check_availability.
- If available, call book_appointment.
- Only after book_appointment succeeds may you say the booking is confirmed.
- Only after book_appointment succeeds may you call end_call(outcome="booked").
- If booking fails, say the team will follow up and use end_call(outcome="appointment_failed" or callback_requested).
- If the caller disconnects before booking, do not mark booked.
- When the caller declines, says no/thank you/goodbye, asks to talk later, says they will discuss with someone, or the objective is complete, say one short closing line and immediately call end_call with the correct outcome.
- For not interested: say "No problem, have a good day." then call end_call(outcome="not_interested").
- For callback/follow-up: say "Sure, we'll have someone follow up." then call end_call(outcome="callback_requested").
- For confirmed booking: say "Thanks, you're all set. Have a great day." then call end_call(outcome="booked") only if book_appointment succeeded.
- If the caller asks for a person, human, representative, female agent, or woman agent, ALWAYS call transfer_to_human first.
- Never say transfer is unavailable, impossible, or that you cannot connect them before transfer_to_human returns failure.
- If transfer_to_human fails or is unavailable, say the team will follow up and use callback_requested.
- Never just stop responding. Never leave the call open after the user clearly ends the conversation.
- Never roleplay tool success.
"""


FIRST_RESPONSE_IDENTITY_CONTRACT = """
CRITICAL FIRST RESPONSE IDENTITY RULE:
Your first spoken sentence must be exactly:
"Hi, am I speaking with {lead_name}? This is {agent_name} from {business_name}."
If the caller speaks first or says hello, immediately answer exactly:
"Hi, this is {agent_name} from {business_name}. Am I speaking with {lead_name}?"
Never use any other identity, company, business, clinic, doctor, provider, service, demo, prior inquiry, appointment, or medical/insurance context.
Never say Alex, Dr. Smith, ABC Insurance, therapy provider, insurance options, routine checkup, recent inquiry, or June 3rd unless those exact values are in the resolved call metadata or user-provided prompt.
Default language is English unless explicitly configured or the caller requests another language.
If the system has already played the opening greeting for you, do not repeat it or reintroduce yourself. Continue naturally from the caller's response.
"""


async def _log(level: str, msg: str, detail: str = "") -> None:
    if level == "info":      logger.info(msg)
    elif level == "warning": logger.warning(msg)
    else:                    logger.error(msg)
    try:
        asyncio.create_task(_persist_log_entry(level, msg, detail))
    except RuntimeError:
        logger.debug("No running loop; skipped async log persistence")


async def _persist_log_entry(level: str, msg: str, detail: str = "") -> None:
    try:
        await log_error("agent", msg, detail, level)
    except Exception:
        logger.exception("Failed to persist log entry")


async def _log_exception(msg: str, exc: Exception, level: str = "error") -> None:
    tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
    if level == "warning":
        logger.exception(msg)
    else:
        logger.exception(msg)
    try:
        asyncio.create_task(_persist_log_entry(level, msg, tb))
    except RuntimeError:
        logger.debug("No running loop; skipped async exception persistence")


def _safe_preview(text: Optional[str], limit: int = 200) -> str:
    if not text:
        return ""
    return " ".join(str(text).split())[:limit]


def _parse_tool_names(raw) -> tuple[Optional[list[str]], str]:
    if raw is None:
        return None, "not_supplied"
    if isinstance(raw, list):
        return [str(t).strip() for t in raw if str(t).strip()], "list"
    text = str(raw).strip()
    if text == "":
        return None, "blank"
    try:
        parsed = json.loads(text)
        if isinstance(parsed, list):
            return [str(t).strip() for t in parsed if str(t).strip()], "json"
    except Exception:
        pass
    return [t.strip() for t in text.split(",") if t.strip()], "csv"


def _dedupe_tool_names(names: list[str]) -> list[str]:
    seen = set()
    out = []
    for name in names:
        if name in DEFAULT_TOOL_NAMES and name not in seen:
            seen.add(name)
            out.append(name)
    return out


def _booking_workflow_active(system_prompt: str) -> bool:
    disabled_flag = os.getenv("DISABLE_BOOKING_TOOLS", "").lower() in ("1", "true", "yes")
    if disabled_flag:
        return False
    text = (system_prompt or "").lower()
    return any(word in text for word in ("book", "appointment", "schedule", "calendar", "slot"))


def load_db_settings_to_env() -> None:
    """Load Supabase settings table into os.environ before worker starts."""
    url = os.getenv("SUPABASE_URL", "")
    key = os.getenv("SUPABASE_SERVICE_KEY", "")
    if not url or not key:
        return
    try:
        from supabase import create_client
        client = create_client(url, key)
        result = client.table("settings").select("key, value").execute()
        for row in (result.data or []):
            if row.get("value"):
                os.environ[row["key"]] = row["value"]
    except Exception as exc:
        logger.exception("Could not load settings from Supabase")


# ── Import Google plugin paths ───────────────────────────────────────────────
_google_realtime = None
_google_beta_realtime = None
_google_llm = None
_google_tts = None

try:
    from livekit.plugins import google as _gp
    try:
        _google_realtime = _gp.realtime.RealtimeModel
        logger.info("Loaded google.realtime.RealtimeModel (stable path)")
    except AttributeError:
        logger.exception("google.realtime.RealtimeModel not available")
    try:
        _google_beta_realtime = _gp.beta.realtime.RealtimeModel
        logger.info("Loaded google.beta.realtime.RealtimeModel (beta path)")
    except AttributeError:
        logger.exception("google.beta.realtime.RealtimeModel not available")
    try:
        _google_llm = _gp.LLM
        _google_tts = _gp.TTS
    except AttributeError:
        logger.exception("google LLM/TTS not available")
except ImportError:
    logger.exception("livekit-plugins-google not installed")

# _deepgram_stt = None
# try:
#     from livekit.plugins import deepgram as _dg
#     _deepgram_stt = _dg.STT
# except ImportError:
#     logger.exception("livekit-plugins-deepgram not installed")


# ── Session factory ──────────────────────────────────────────────────────────

def _sync_setting_value(key: str) -> str:
    url = os.getenv("SUPABASE_URL", "").strip()
    service_key = os.getenv("SUPABASE_SERVICE_KEY", "").strip()
    if not url or not service_key:
        return ""
    try:
        from supabase import create_client
        client = create_client(url, service_key)
        result = client.table("settings").select("value").eq("key", key).maybe_single().execute()
        if result and result.data:
            return str(result.data.get("value") or "").strip()
    except Exception:
        logger.warning("Could not read %s from Supabase settings", key)
    return ""


def _credential_candidate_value(source: str, value: str) -> tuple[Optional[dict], Optional[str], str, str]:
    value = (value or "").strip()
    if not value:
        return None, None, "", ""
    if value.startswith("{"):
        try:
            parsed = json.loads(value)
            if isinstance(parsed, dict):
                return parsed, None, source, str(parsed.get("project_id") or "")
        except Exception:
            logger.warning("Google TTS credential source %s looked like JSON but could not be parsed", source)
            return None, None, "", ""
    return None, value, source, ""


def _resolve_google_tts_credentials() -> tuple[Optional[dict], Optional[str], str, str]:
    candidates = [
        ("env_tts_json", os.getenv("GOOGLE_TTS_SERVICE_ACCOUNT_JSON", "")),
        ("db_tts_json", _sync_setting_value("GOOGLE_TTS_SERVICE_ACCOUNT_JSON")),
        ("env_gcal_json", os.getenv("GOOGLE_CALENDAR_SERVICE_ACCOUNT_JSON", "")),
        ("db_gcal_json", _sync_setting_value("GOOGLE_CALENDAR_SERVICE_ACCOUNT_JSON")),
        ("application_credentials", os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")),
        ("tts_file", os.getenv("GOOGLE_TTS_CREDENTIALS_FILE", "")),
    ]
    for source, value in candidates:
        credentials_info, credentials_file, resolved_source, project_id = _credential_candidate_value(source, value)
        if credentials_info or credentials_file:
            return credentials_info, credentials_file, resolved_source, project_id
    return None, None, "", ""


def _build_opening_tts(voice_name: Optional[str] = None) -> tuple[Optional[object], str]:
    enabled = os.getenv("DETERMINISTIC_GREETING_TTS_ENABLED", "true").lower() not in ("0", "false", "no")
    if not enabled:
        return None, "disabled_by_env"
    if not _google_tts:
        return None, "google_tts_plugin_unavailable"

    credentials_info, credentials_file, credential_source, project_id = _resolve_google_tts_credentials()
    if not credentials_info and not credentials_file:
        logger.warning("Google opening TTS credentials missing")
        return None, "missing_google_cloud_credentials"

    try:
        kwargs = {
            "language": os.getenv("GOOGLE_TTS_LANGUAGE", "en-US"),
            "voice_name": os.getenv("GOOGLE_TTS_VOICE", voice_name or os.getenv("GEMINI_TTS_VOICE", "Aoede")),
            "model_name": os.getenv("GOOGLE_TTS_MODEL", "gemini-2.5-flash-tts"),
            "speaking_rate": float(os.getenv("GOOGLE_TTS_SPEAKING_RATE", "1.04")),
        }
        if credentials_info:
            kwargs["credentials_info"] = credentials_info
        else:
            kwargs["credentials_file"] = credentials_file
        tts_model = _google_tts(**kwargs)
        project_detail = f"; project_id={project_id}" if project_id else ""
        logger.info("Google opening TTS credentials source=%s%s", credential_source, project_detail)
        return tts_model, f"configured_google_tts_{credential_source}{project_detail}"
    except Exception as exc:
        logger.exception("Google opening TTS init failed")
        return None, f"init_failed={type(exc).__name__}"


def _build_session(
    tools: list,
    system_prompt: str,
    model_name: Optional[str] = None,
    voice_name: Optional[str] = None,
) -> tuple[AgentSession, str]:
    """
    Build AgentSession with Gemini Live or pipeline fallback.

    CRITICAL SILENCE-PREVENTION CONFIG — all 3 required:
    1. SessionResumptionConfig(transparent=True) — auto-reconnects on timeout instead of going silent
    2. ContextWindowCompressionConfig — prevents freeze when context fills up
    3. RealtimeInputConfig with END_SENSITIVITY_LOW + silence_duration_ms=2000

    Without all 3, calls will go silent within 30-90 seconds.
    """
    use_realtime = os.getenv("USE_GEMINI_REALTIME", "true").lower() in ("true", "1", "yes")
    model_name = model_name or os.getenv("GEMINI_MODEL", "gemini-3.1-flash-live-preview")
    voice = voice_name or os.getenv("GEMINI_TTS_VOICE", "Aoede")
    api_key = os.getenv("GOOGLE_API_KEY", "")

    if use_realtime and api_key:
        RealtimeModel = _google_realtime or _google_beta_realtime
        if RealtimeModel:
            try:
                from google.genai import types as _gt
                model = RealtimeModel(
                    model=model_name,
                    voice=voice,
                    api_key=api_key,
                    instructions=system_prompt,
                    # Rule 6 — all 3 silence-prevention configs are mandatory
                    session_resumption=_gt.SessionResumptionConfig(transparent=True),
                    context_window_compression=_gt.ContextWindowCompressionConfig(
                        trigger_tokens=25600,
                        sliding_window=_gt.SlidingWindow(target_tokens=12800),
                    ),
                    realtime_input_config=_gt.RealtimeInputConfig(
                        automatic_activity_detection=_gt.AutomaticActivityDetection(
                            # Rule 3 — must use full string END_SENSITIVITY_LOW, not LOW
                            end_of_speech_sensitivity=_gt.EndSensitivity.END_SENSITIVITY_LOW,
                            silence_duration_ms=2000,
                            prefix_padding_ms=200,
                        ),
                    ),
                )
                logger.info("✅ Gemini Live: model=%s voice=%s", model_name, voice)
                opening_tts, opening_tts_status = _build_opening_tts(voice_name=voice)
                if opening_tts:
                    return AgentSession(llm=model, tools=tools, tts=opening_tts), opening_tts_status
                return AgentSession(llm=model, tools=tools), opening_tts_status
            except Exception:
                logger.exception("Gemini Live init failed")
                raise

    raise RuntimeError("Gemini realtime not configured — set USE_GEMINI_REALTIME=true and GOOGLE_API_KEY")


class OutboundAssistant(Agent):
    """
    Outbound appointment booking agent.
    tools=[] in super().__init__ to avoid duplicate tool name error (Rule 13).
    Tools are passed only to AgentSession.
    """
    def __init__(self, instructions: str) -> None:
        super().__init__(instructions=instructions, tools=[])


async def entrypoint(ctx: agents.JobContext):
    """
    LiveKit agent entrypoint — dial-first pattern (Architecture Rule 1).

    Order:
    1. Connect to room
    2. Parse metadata (phone, lead info, profile overrides)
    3. Dial via SIP — wait_until_answered=True blocks until pickup
    4. THEN start Gemini Live session
    5. Keep alive via participant_disconnected event
    """
    await ctx.connect()

    # ── Parse metadata ───────────────────────────────────────────────────────
    phone_number:   Optional[str] = None
    lead_name      = "there"
    business_name  = "our company"
    service_type   = "our service"
    system_prompt: Optional[str] = None
    voice_override: Optional[str] = None
    model_override: Optional[str] = None
    tools_override = None
    agent_profile_id: Optional[str] = None
    agent_profile_name: Optional[str] = None
    agent_profile_source = "metadata"
    prompt_override_present = False
    prompt_source = "default"
    tools_override_supplied = False

    raw_meta = ctx.job.metadata or ""
    logger.error("SIM_TRACE_AGENT_JOB_METADATA_RAW room=%s metadata=%s", getattr(ctx.room, "name", ""), raw_meta[:5000])
    try:
        meta = json.loads(raw_meta) if raw_meta else {}
        phone_number   = meta.get("phone_number")
        lead_name      = meta.get("lead_name", "there")
        business_name  = meta.get("business_name", "our company")
        service_type   = meta.get("service_type", "our service")
        system_prompt  = meta.get("system_prompt")
        voice_override = meta.get("voice_override")
        model_override = meta.get("model_override")
        tools_override = meta.get("tools_override")
        tools_override_supplied = "tools_override" in meta
        agent_profile_id = meta.get("agent_profile_id")
        agent_profile_name = meta.get("agent_profile_name")
        agent_profile_source = meta.get("agent_profile_source") or "metadata"
        prompt_override_present = bool(meta.get("system_prompt_override_present"))
        prompt_source = meta.get("prompt_source") or ("per_call_override" if prompt_override_present else "metadata_or_global")
<<<<<<< Updated upstream
=======
        call_session_id = meta.get("call_session_id")
        direction = meta.get("direction") or "outbound"
        await _log(
            "warning",
            "SIM_TRACE_AGENT_PARSED_METADATA",
            (
                f"room={ctx.room.name}; lead_name={lead_name}; business_name={business_name}; "
                f"service_type={service_type}; agent_profile_id={agent_profile_id or ''}; "
                f"agent_profile_name={agent_profile_name or ''}; agent_profile_source={agent_profile_source}; "
                f"prompt_source={prompt_source}; system_prompt_present={bool(system_prompt)}; "
                f"tools_override_supplied={tools_override_supplied}"
            ),
        )
>>>>>>> Stashed changes
    except Exception as exc:
        await _log_exception("Metadata parse error", exc, "warning")

    # ── Apply agent profile overrides (Rule 10) ──────────────────────────────
    resolved_voice = voice_override or os.getenv("GEMINI_TTS_VOICE", "Aoede")
    resolved_model = model_override or os.getenv("GEMINI_MODEL", "gemini-3.1-flash-live-preview")

    # ── Resolve enabled tools ────────────────────────────────────────────────
    # enabled_tools: list = []
    # if tools_override:
    #     try:
    #         enabled_tools = json.loads(tools_override) if isinstance(tools_override, str) else list(tools_override)
    #     except Exception:
    #         enabled_tools = []
    # if not enabled_tools:
    #     enabled_tools = await get_enabled_tools()
    # ── Resolve system prompt (DB → metadata → default) ─────────────────────
    if system_prompt:
        prompt_source = prompt_source or "metadata"
    if not system_prompt:
        from db import get_setting as _gs
        system_prompt = await _gs("system_prompt", "") or None
        prompt_source = "global" if system_prompt else "default"

    agent_display_name = agent_profile_name or "Priya"
    raw_prompt_for_guard = system_prompt or ""

    system_prompt = build_prompt(
        lead_name=lead_name,
        business_name=business_name,
        service_type=service_type,
        phone=phone_number or "",
        agent_name=agent_display_name,
        custom_prompt=system_prompt,
    )
    mandatory_contract = MANDATORY_TOOL_CONTRACT.format(
        lead_name=lead_name,
        business_name=business_name,
        service_type=service_type,
        phone=phone_number or "unknown",
        agent_name=agent_display_name,
    )
    first_response_contract = FIRST_RESPONSE_IDENTITY_CONTRACT.format(
        lead_name=lead_name,
        business_name=business_name,
        service_type=service_type,
        phone=phone_number or "unknown",
        agent_name=agent_display_name,
    )
    system_prompt = f"{first_response_contract.rstrip()}\n\n{system_prompt.rstrip()}{mandatory_contract}"

    # Tool precedence: mandatory booking/transfer tools are injected after the selected source.
    # Source order is global defaults, then profile/campaign/per-call metadata overrides.
    global_tool_names = await get_enabled_tools()
    override_tool_names, override_parse_source = _parse_tool_names(tools_override) if tools_override_supplied else (None, "not_supplied")
    if tools_override_supplied:
        base_tool_names = override_tool_names or []
        tool_source = f"metadata_override:{override_parse_source}"
    elif global_tool_names is not None:
        base_tool_names = global_tool_names
        tool_source = "global_setting"
    else:
        base_tool_names = list(DEFAULT_TOOL_NAMES)
        tool_source = "built_in_default"

    booking_active = _booking_workflow_active(system_prompt)
    mandatory_injections = []
    if booking_active:
        for name in [*MANDATORY_BOOKING_TOOLS, "transfer_to_human"]:
            if name not in base_tool_names:
                base_tool_names.append(name)
                mandatory_injections.append(name)
    enabled_tools = _dedupe_tool_names(base_tool_names)
    if booking_active:
        missing_mandatory = [name for name in [*MANDATORY_BOOKING_TOOLS, "transfer_to_human"] if name not in enabled_tools]
        if missing_mandatory:
            await _log("warning", "Mandatory booking tools missing after resolution", ",".join(missing_mandatory))
    else:
        system_prompt = f"{system_prompt.rstrip()}\n\nBooking actions unavailable in this session. Do not claim availability or confirmed bookings.\n"

    tool_ctx = AppointmentTools(ctx, phone_number=phone_number, lead_name=lead_name)
    transcript_saved = False
    call_answered = False
    session_started = False
    call_answered_at: Optional[float] = None
    session_start_done_at: Optional[float] = None
    first_user_audio_at: Optional[float] = None
    first_agent_audio_at: Optional[float] = None
    first_gemini_audio_at: Optional[float] = None
    first_ai_transcript_seen = False
    first_user_before_ai = False
    identity_guard_terminated = False
    first_greeting_controlled = False
    controlled_greeting_text = ""

    resolved_identity_context = " ".join(
        str(v or "").lower()
        for v in (agent_display_name, business_name, service_type, lead_name, phone_number, raw_prompt_for_guard)
    )

    def _first_greeting_opening() -> str:
        return f"Hi, am I speaking with {lead_name}? This is {agent_display_name} from {business_name}."

    def _first_greeting_reply() -> str:
        return f"Hi, this is {agent_display_name} from {business_name}. Am I speaking with {lead_name}?"

    opening_greeting_text = _first_greeting_opening()
    system_prompt = (
        f"{system_prompt.rstrip()}\n\n"
        "CONTROLLED OPENING GREETING STATE:\n"
        f'The system will attempt to play this deterministic greeting before Gemini speaks: "{opening_greeting_text}"\n'
        "If the caller has heard that greeting, do not repeat it or reintroduce yourself. "
        "Continue from the caller's response. If the caller says yes, proceed naturally with the call objective. "
        f'If the caller asks who this is, answer: "This is {agent_display_name} from {business_name}."\n'
    )

    def _looks_like_controlled_greeting(text: str) -> bool:
        normalized = " ".join((text or "").lower().replace(",", "").replace(".", "").split())
        expected = " ".join(opening_greeting_text.lower().replace(",", "").replace(".", "").split())
        return bool(expected and normalized.startswith(expected[: min(len(expected), 80)]))

    def _identity_guard_violations(text: str) -> list[str]:
        lower = (text or "").lower()
        reasons = []
        for leaked in (
            "alex", "dr. smith", "abc insurance", "therapy provider",
            "insurance options", "routine checkup", "recent inquiry", "june 3rd",
            "sarah", "chris", "sparkle cleaners",
        ):
            if leaked in lower and leaked not in resolved_identity_context:
                reasons.append(f"forbidden_demo_leak={leaked}")
        intro_patterns = (
            r"\bmy name is\b",
            r"\bthis is\b",
            r"\bi am\b.{0,60}\bfrom\b",
            r"\bi'm\b.{0,60}\bfrom\b",
            r"\bcalling from\b",
            r"\bcalling on behalf of\b",
            r"\bon behalf of\b",
        )
        has_intro = any(re.search(pattern, lower) for pattern in intro_patterns)
        expected_agent = (agent_display_name or "").lower()
        expected_business = (business_name or "").lower()
        expected_service = (service_type or "").lower()
        has_expected_agent = bool(expected_agent and expected_agent in lower)
        has_expected_business = bool(expected_business and expected_business in lower)
        has_expected_service = bool(expected_service and expected_service in lower)
        if has_intro and not (has_expected_agent or has_expected_business or has_expected_service):
            reasons.append("intro_missing_resolved_identity")
        if "my name is" in lower and not has_expected_agent:
            reasons.append("agent_name_intro_mismatch")
        if ("calling from" in lower or "calling on behalf of" in lower or "on behalf of" in lower) and not (has_expected_business or has_expected_service):
            reasons.append("business_intro_mismatch")
        return reasons

    def _context_guard_violations(text: str) -> list[str]:
        lower = (text or "").lower()
        reasons = []
        if tool_ctx._booking_confirmed or tool_ctx._booking_id:
            return reasons

        existing_appointment_claims = (
            "you have an appointment scheduled",
            "your upcoming appointment",
            "your existing appointment",
            "we have you down for",
            "you are scheduled for",
        )
        if any(claim in lower for claim in existing_appointment_claims):
            reasons.append("fake_existing_appointment_claim")

        if "i see you have" in lower and any(anchor in lower for anchor in ("appointment", "scheduled", "audit", "checkup", "quote", "cleaning", "dental", "therapy")):
            reasons.append("fake_existing_appointment_claim")

        if any(time_claim in lower for time_claim in ("scheduled today", "today at", "tomorrow at")) and any(
            anchor in lower for anchor in ("appointment", "scheduled", "audit", "checkup", "quote", "cleaning", "dental", "therapy")
        ):
            reasons.append("fake_existing_appointment_time")

        for service_leak in (
            "routine checkup", "energy audit", "insurance quote", "carpet cleaning",
            "dental appointment", "therapy appointment",
        ):
            if service_leak in lower and service_leak not in resolved_identity_context:
                reasons.append(f"fake_service_context={service_leak}")

        return reasons

    async def _log_identity_guard(text: str) -> bool:
        identity_reasons = _identity_guard_violations(text)
        context_reasons = _context_guard_violations(text)
        identity_passed = not bool(identity_reasons)
        context_passed = not bool(context_reasons)
        await _log(
            "info" if identity_passed else "error",
            "IDENTITY_GUARD_CHECK",
            (
                f"text={text[:500]}\n"
                f"passed={str(identity_passed).lower()}\n"
                f"reasons={identity_reasons}"
            ),
        )
        if context_reasons:
            await _log(
                "error",
                "CONTEXT_GUARD_CHECK",
                (
                    f"text={text[:500]}\n"
                    f"passed={str(context_passed).lower()}\n"
                    f"reason={context_reasons}"
                ),
            )
        reasons = identity_reasons + context_reasons
        if reasons:
            await _log(
                "error",
                "IDENTITY_GUARD_TERMINATE",
                f"reason={reasons}; text={text[:240]}",
            )
            await _log(
                "error",
                "WRONG_IDENTITY_GUARD AI output mismatch",
                (
                    f"room={ctx.room.name}; expected_agent={agent_display_name}; "
                    f"expected_business={business_name}; expected_service={service_type}; "
                    f"reasons={reasons}; text={text[:240]}"
                ),
            )
            return True
        await _log("info", "identity_guard_passed", f"room={ctx.room.name}; text={text[:160]}")
        return False

    async def _guard_and_maybe_terminate(_text: str) -> None:
        if await _log_identity_guard(_text):
            await _terminate_wrong_identity(_text)

    async def _fallback_call_log(outcome: str, reason: str, detail: str = "") -> None:
        try:
            await tool_ctx.log_fallback_call_end(outcome=outcome, reason=reason, detail=detail)
        except Exception as exc:
            await _log_exception("Fallback call logging crashed", exc, "error")

    def _mark_connected_background() -> None:
        async def _run() -> None:
            try:
                await tool_ctx.mark_connected()
            except Exception as exc:
                await _log_exception("mark_connected background update failed", exc, "warning")
        asyncio.create_task(_run())

    await _log(
        "info",
        "Agent prompt/tools resolved",
        (
            f"room={ctx.room.name}; profile_id={agent_profile_id or ''}; "
            f"profile_name={agent_profile_name or ''}; profile_source={agent_profile_source}; prompt_source={prompt_source}; "
            f"agent_display_name={agent_display_name}; business_name={business_name}; "
            f"service_type={service_type}; canonical_phone={phone_number or ''}; "
            f"resolved_tools={enabled_tools}; tool_source={tool_source}; "
            f"tools_override_supplied={tools_override_supplied}; mandatory_injections={mandatory_injections}; "
            f"booking_workflow_active={booking_active}; "
            f"model={resolved_model}; "
            f"voice={resolved_voice}; "
            f"override_prompt_present={prompt_override_present}; "
            f"prompt_preview={_safe_preview(system_prompt)}"
        ),
    )
    await _log("info", "Mandatory tool contract appended", f"room={ctx.room.name}")

    # Phase 1 latency reduction: build tools, Gemini model/session, and opening TTS before dialing.
    gemini_model = resolved_model
    gemini_31_native_audio = "3.1" in gemini_model
    if gemini_31_native_audio:
        await _log(
            "warning",
            "Gemini 3.1 native audio cannot guarantee exact first utterance without TTS; using prompt anchoring + identity guard.",
            f"room={ctx.room.name}; model={gemini_model}",
        )
    prebuild_begin = time.perf_counter()
    await _log("info", "prebuild_begin", f"room={ctx.room.name}; at={prebuild_begin:.6f}; model={gemini_model}")
    await _log("info", "latency session_build_start", f"room={ctx.room.name}; at={prebuild_begin:.6f}; phase=pre_dial")
    await _log("info", f"Building AI session — model={gemini_model}")
    active_tools = tool_ctx.build_tool_list(enabled_tools)
    loaded_tool_names = [t.__name__ for t in active_tools]
    await _log("info", f"Tools loaded: {loaded_tool_names}")
    if loaded_tool_names != enabled_tools:
        await _log("warning", "Resolved tool names differ from loaded tools", f"resolved={enabled_tools}; loaded={loaded_tool_names}")
    try:
        session, opening_tts_status = _build_session(
            tools=active_tools,
            system_prompt=system_prompt,
            model_name=resolved_model,
            voice_name=resolved_voice,
        )
        prebuild_done = time.perf_counter()
        await _log(
            "info",
            "prebuild_done",
            f"room={ctx.room.name}; at={prebuild_done:.6f}; prebuild_ms={int((prebuild_done - prebuild_begin) * 1000)}",
        )
        await _log(
            "info",
            "latency session_build_done",
            f"room={ctx.room.name}; at={prebuild_done:.6f}; build_ms={int((prebuild_done - prebuild_begin) * 1000)}; phase=pre_dial",
        )
        await _log("info", "deterministic_tts_config", f"room={ctx.room.name}; status={opening_tts_status}")
    except Exception as exc:
        await _log_exception("AI session build failed", exc, "error")
        await _fallback_call_log(
            "no_answer",
            "session prebuild failed before dial",
            f"error={exc}",
        )
        ctx.shutdown()
        return

    async def _persist_transcript(speaker: str, text: str) -> None:
        nonlocal transcript_saved
        try:
            await save_transcript(ctx.room.name, speaker, text)
            transcript_saved = True
            await _log("info", "Transcript saved", f"{speaker}: {text[:160]}")
        except Exception as exc:
            await _log_exception("Transcript save failed", exc, "warning")

    def _is_local(participant: Optional[rtc.Participant]) -> bool:
        try:
            if participant is None:
                return False
            if getattr(participant, "is_local", False):
                return True
            return getattr(participant, "identity", None) == ctx.room.local_participant.identity
        except Exception:
            return False

    def _on_transcription_received(segments, participant=None, publication=None):
        nonlocal first_user_audio_at, first_agent_audio_at, first_gemini_audio_at, first_ai_transcript_seen, first_user_before_ai
        try:
            for seg in segments or []:
                text = getattr(seg, "text", "") or ""
                if not text.strip():
                    continue
                is_final = getattr(seg, "final", True)
                if not is_final:
                    continue
                speaker = "ai" if _is_local(participant) else "user"
                if speaker == "user":
                    if first_user_audio_at is None:
                        first_user_audio_at = time.perf_counter()
                        first_user_before_ai = first_agent_audio_at is None and not first_ai_transcript_seen
                        asyncio.create_task(_log(
                            "info",
                            "first_user_audio_timestamp",
                            (
                                f"room={ctx.room.name}; at={first_user_audio_at:.6f}; "
                                f"since_answer_ms={int((first_user_audio_at - call_answered_at) * 1000) if call_answered_at else ''}; "
                                f"since_session_start_ms={int((first_user_audio_at - session_start_done_at) * 1000) if session_start_done_at else ''}; "
                                f"first_user_before_ai={first_user_before_ai}"
                            ),
                        ))
                        asyncio.create_task(_log(
                            "info",
                            "first_user_audio",
                            (
                                f"room={ctx.room.name}; at={first_user_audio_at:.6f}; "
                                f"since_answer_ms={int((first_user_audio_at - call_answered_at) * 1000) if call_answered_at else ''}; "
                                f"since_session_start_ms={int((first_user_audio_at - session_start_done_at) * 1000) if session_start_done_at else ''}; "
                                f"first_user_before_ai={first_user_before_ai}"
                            ),
                        ))
                    asyncio.create_task(_log("info", "User speech detected", text[:160]))
                else:
                    if first_agent_audio_at is None:
                        first_agent_audio_at = time.perf_counter()
                        asyncio.create_task(_log(
                            "info",
                            "first_agent_audio_timestamp",
                            (
                                f"room={ctx.room.name}; at={first_agent_audio_at:.6f}; "
                                f"since_answer_ms={int((first_agent_audio_at - call_answered_at) * 1000) if call_answered_at else ''}; "
                                f"since_session_start_ms={int((first_agent_audio_at - session_start_done_at) * 1000) if session_start_done_at else ''}; "
                                f"first_user_before_ai={first_user_before_ai}"
                            ),
                        ))
                    if first_greeting_controlled and _looks_like_controlled_greeting(text):
                        asyncio.create_task(_log("info", "controlled_greeting_transcript", text[:160]))
                        asyncio.create_task(_persist_transcript(speaker, text))
                        continue
                    if first_gemini_audio_at is None:
                        first_gemini_audio_at = time.perf_counter()
                        asyncio.create_task(_log(
                            "info",
                            "first_gemini_audio",
                            (
                                f"room={ctx.room.name}; at={first_gemini_audio_at:.6f}; "
                                f"since_answer_ms={int((first_gemini_audio_at - call_answered_at) * 1000) if call_answered_at else ''}; "
                                f"since_session_start_ms={int((first_gemini_audio_at - session_start_done_at) * 1000) if session_start_done_at else ''}; "
                                f"first_user_before_ai={first_user_before_ai}"
                            ),
                        ))
                    if not first_ai_transcript_seen:
                        first_ai_transcript_seen = True
                        asyncio.create_task(_log("info", "first_ai_transcript_text", text[:240]))
                    asyncio.create_task(_guard_and_maybe_terminate(text))
                    asyncio.create_task(_log("info", "AI response generated", text[:160]))
                asyncio.create_task(_persist_transcript(speaker, text))
        except Exception as exc:
            asyncio.create_task(_log_exception("Transcription handler error", exc, "warning"))

    def _on_participant_connected(participant: rtc.RemoteParticipant):
        asyncio.create_task(_log("info", "Participant connected", participant.identity))

    def _on_track_subscribed(track, publication=None, participant=None):
        try:
            kind = getattr(track, "kind", "unknown")
            pid = getattr(participant, "identity", "unknown") if participant else "unknown"
            asyncio.create_task(_log("info", "Audio track subscribed", f"{kind} from {pid}"))
        except Exception as exc:
            asyncio.create_task(_log_exception("Track subscribed handler error", exc, "warning"))

    def _on_track_published(publication=None, participant=None):
        try:
            if participant and participant.identity != ctx.room.local_participant.identity:
                return
            kind = getattr(publication, "kind", "unknown")
            asyncio.create_task(_log("info", "AI audio published", str(kind)))
        except Exception as exc:
            asyncio.create_task(_log_exception("Track published handler error", exc, "warning"))

    def _on_room_disconnected():
        asyncio.create_task(_log("info", "Room disconnected"))

    try:
        ctx.room.on("transcription_received", _on_transcription_received)
        ctx.room.on("participant_connected", _on_participant_connected)
        ctx.room.on("track_subscribed", _on_track_subscribed)
        ctx.room.on("track_published", _on_track_published)
        ctx.room.on("disconnected", _on_room_disconnected)
    except Exception as exc:
        await _log_exception("Failed to register room event handlers", exc, "warning")

    await _log("info", f"AI agent connected: room={ctx.room.name}")

    # ── Dial — MUST come before session.start() (Rule 1) ────────────────────
    if phone_number:
        trunk_id = os.getenv("OUTBOUND_TRUNK_ID")

        if trunk_id:
            await _log("info", f"Dialing {phone_number} via SIP trunk {trunk_id}")

            try:
                await ctx.api.sip.create_sip_participant(
                    api.CreateSIPParticipantRequest(
                        room_name=ctx.room.name,
                        sip_trunk_id=trunk_id,
                        sip_call_to=phone_number,
                        participant_identity=f"sip_{phone_number}",
                        wait_until_answered=True,
                    )
                )

                await _log(
                    "info",
                    f"Call ANSWERED — {phone_number} picked up, starting AI session now"
                )
                call_answered = True
<<<<<<< Updated upstream
=======
                call_answered_at = time.perf_counter()
                await _log("info", "latency call_answered", f"room={ctx.room.name}; at={call_answered_at:.6f}; phone={phone_number}")
                _mark_connected_background()
>>>>>>> Stashed changes

            except Exception as exc:
                await _log("error", f"SIP dial FAILED for {phone_number}: {exc}")
                await _fallback_call_log(
                    "no_answer",
                    "sip dial failed before answer",
                    f"error={exc}",
                )
                ctx.shutdown()
                return

        else:
            await _log(
                "warning",
                "No SIP trunk configured — running in local/browser mode"
            )
<<<<<<< Updated upstream
    # ── Build and start Gemini Live ──────────────────────────────────────────
    gemini_model = os.getenv("GEMINI_MODEL", "gemini-3.1-flash-live-preview")
    await _log("info", f"Building AI session — model={gemini_model}")
    active_tools = tool_ctx.build_tool_list(enabled_tools)
    loaded_tool_names = [t.__name__ for t in active_tools]
    await _log("info", f"Tools loaded: {loaded_tool_names}")
    if loaded_tool_names != enabled_tools:
        await _log("warning", "Resolved tool names differ from loaded tools", f"resolved={enabled_tools}; loaded={loaded_tool_names}")
    try:
        session = _build_session(tools=active_tools, system_prompt=system_prompt)
    except Exception as exc:
        await _log_exception("AI session build failed", exc, "error")
        await _fallback_call_log(
            "disconnected" if call_answered else "no_answer",
            "session build failed before end_call",
            f"error={exc}",
        )
        ctx.shutdown()
        return

=======
            call_answered = True
            call_answered_at = time.perf_counter()
            await _log("info", "latency call_answered", f"room={ctx.room.name}; at={call_answered_at:.6f}; mode=local_browser")
            _mark_connected_background()
    # Start the prebuilt Gemini Live session only after the call is answered.
>>>>>>> Stashed changes
    # Never use close_on_disconnect=True with SIP (Rule 2)
    if _HAS_ROOM_OPTIONS:
        from livekit.agents import RoomOptions as _RO
        _session_kwargs = dict(
            room=ctx.room,
            agent=OutboundAssistant(instructions=""),
            room_options=_RO(input_options=RoomInputOptions(noise_cancellation=noise_cancellation.BVCTelephony())),
        )
    else:
        _session_kwargs = dict(
            room=ctx.room,
            agent=OutboundAssistant(instructions=""),
            room_input_options=RoomInputOptions(noise_cancellation=noise_cancellation.BVCTelephony()),
        )

    try:
        session_start_begin = time.perf_counter()
        await _log(
            "info",
            "latency session_start_begin",
            (
                f"room={ctx.room.name}; at={session_start_begin:.6f}; "
                f"since_answer_ms={int((session_start_begin - call_answered_at) * 1000) if call_answered_at else ''}"
            ),
        )
        await _log(
            "info",
            "session_start_begin",
            (
                f"room={ctx.room.name}; at={session_start_begin:.6f}; "
                f"since_answer_ms={int((session_start_begin - call_answered_at) * 1000) if call_answered_at else ''}"
            ),
        )
        await _log(
            "info",
            "gemini_session_start_begin",
            (
                f"room={ctx.room.name}; at={session_start_begin:.6f}; "
                f"since_answer_ms={int((session_start_begin - call_answered_at) * 1000) if call_answered_at else ''}"
            ),
        )
        await session.start(**_session_kwargs)
        session_started = True
        session_start_done = time.perf_counter()
        session_start_done_at = session_start_done
        await _log(
            "info",
            "latency session_start_done",
            (
                f"room={ctx.room.name}; at={session_start_done:.6f}; start_ms={int((session_start_done - session_start_begin) * 1000)}; "
                f"since_answer_ms={int((session_start_done - call_answered_at) * 1000) if call_answered_at else ''}"
            ),
        )
        await _log(
            "info",
            "session_start_done",
            (
                f"room={ctx.room.name}; at={session_start_done:.6f}; start_ms={int((session_start_done - session_start_begin) * 1000)}; "
                f"since_answer_ms={int((session_start_done - call_answered_at) * 1000) if call_answered_at else ''}"
            ),
        )
        await _log(
            "info",
            "gemini_session_start_done",
            (
                f"room={ctx.room.name}; at={session_start_done:.6f}; start_ms={int((session_start_done - session_start_begin) * 1000)}; "
                f"since_answer_ms={int((session_start_done - call_answered_at) * 1000) if call_answered_at else ''}"
            ),
        )
    except Exception as exc:
        await _log_exception("AI session start failed", exc, "error")
        await _fallback_call_log(
            "disconnected" if call_answered else "no_answer",
            "session start failed before end_call",
            f"error={exc}",
        )
        ctx.shutdown()
        return
    await _log("info", "Gemini session started")

    greeting = opening_greeting_text
    say = getattr(session, "say", None)
    generate_reply = getattr(session, "generate_reply", None)
    available_first_response_methods = [
        name for name, method in (("say", say), ("generate_reply", generate_reply)) if callable(method)
    ]
    await _log(
        "info",
        "first_response_methods_available",
        f"room={ctx.room.name}; methods={available_first_response_methods}",
    )
    await _log("info", "latency first_response_trigger_attempted", f"room={ctx.room.name}; method=session.say")

    first_greeting_triggered = False
    first_greeting_requested = False
    say_failed_known_native_audio = False
    if callable(say):
        try:
            greeting_start = time.perf_counter()
            await _log(
                "info",
                "deterministic_greeting_start",
                (
                    f"room={ctx.room.name}; since_answer_ms="
                    f"{int((greeting_start - call_answered_at) * 1000) if call_answered_at else ''}"
                ),
            )
            try:
                handle = say(greeting, allow_interruptions=True, add_to_chat_ctx=False)
            except TypeError:
                handle = say(greeting)
            controlled_greeting_text = greeting
            first_greeting_controlled = True
            wait_for_playout = getattr(handle, "wait_for_playout", None)
            if callable(wait_for_playout):
                await wait_for_playout()
            first_greeting_triggered = True
            greeting_done = time.perf_counter()
            await _log(
                "info",
                "deterministic_greeting_done",
                (
                    f"room={ctx.room.name}; duration_ms={int((greeting_done - greeting_start) * 1000)}; "
                    f"since_answer_ms={int((greeting_done - call_answered_at) * 1000) if call_answered_at else ''}"
                ),
            )
            await _log("info", "first_greeting_controlled", f"room={ctx.room.name}; method=google_tts_session_say")
        except RuntimeError as exc:
            first_greeting_controlled = False
            controlled_greeting_text = ""
            msg = str(exc)
            say_failed_known_native_audio = "without a TTS model" in msg or "supports say" in msg
            await _log(
                "warning",
                "first_greeting_skipped_reason: session.say failed",
                f"known_native_audio_failure={say_failed_known_native_audio}; error={msg}",
            )
        except Exception as exc:
            first_greeting_controlled = False
            controlled_greeting_text = ""
            await _log_exception("first_greeting_skipped_reason: session.say failed", exc, "warning")
    else:
        await _log("warning", "first_greeting_skipped_reason", "session.say unavailable for this LiveKit/Gemini session")

    if not first_greeting_triggered and gemini_31_native_audio:
        await _log(
            "warning",
            "first_greeting_generate_reply_skipped",
            "reason=gemini_3_1_mutable_chat_context_disabled",
        )
    elif not first_greeting_triggered and callable(generate_reply) and (say_failed_known_native_audio or not callable(say)):
        instruction = (
            f'The next assistant response must say exactly: "{greeting}" '
            f"Do not use any other identity, company, service, prior inquiry, medical, insurance, or appointment context."
        )
        synthetic_user_input = (
            f"Start the call now. Your first spoken sentence must be exactly: {greeting}"
        )
        await _log("info", "latency first_response_trigger_attempted", f"room={ctx.room.name}; method=session.generate_reply")
        try:
            handle = generate_reply(
                user_input=synthetic_user_input,
                instructions=instruction,
                tools=[],
                allow_interruptions=True,
                input_modality="text",
            )
            first_greeting_requested = True
            await _log(
                "info",
                "first_greeting_generate_reply_requested",
                f"room={ctx.room.name}; handle_id={getattr(handle, 'id', '')}; awaiting transcript guard confirmation",
            )
        except Exception as exc:
            await _log_exception("first_greeting_skipped_reason: generate_reply failed", exc, "warning")

    if not first_greeting_triggered and not first_greeting_requested:
        await _log(
            "warning",
            "first_greeting_forced_unavailable",
            f"room={ctx.room.name}; relying_on_prompt_anchoring=true; greeting={greeting}",
        )
    elif first_greeting_requested and not first_greeting_triggered:
        await _log(
            "info",
            "first_greeting_pending_transcript_confirmation",
            f"room={ctx.room.name}; greeting={greeting}",
        )

    async def _terminate_wrong_identity(text: str) -> None:
        nonlocal identity_guard_terminated
        if identity_guard_terminated:
            return
        identity_guard_terminated = True
        await _log(
            "error",
            "CRITICAL wrong first AI identity; terminating session",
            (
                f"room={ctx.room.name}; expected_agent={agent_display_name}; "
                f"expected_business={business_name}; expected_service={service_type}; text={text[:240]}"
            ),
        )
        try:
            await _fallback_call_log(
                "abandoned",
                "wrong first AI identity guard terminated session",
                f"first_ai_text={text[:240]}",
            )
        except Exception as exc:
            await _log_exception("Wrong identity fallback logging failed", exc, "error")
        try:
            interrupt = getattr(session, "interrupt", None)
            if callable(interrupt):
                fut = interrupt(force=True)
                if fut is not None:
                    await fut
        except Exception:
            pass
        try:
            await session.aclose()
        except Exception:
            pass
        try:
            ctx.shutdown()
        except Exception:
            pass

    def _on_user_input_transcribed(ev) -> None:
        nonlocal first_user_audio_at, first_user_before_ai
        try:
            if not getattr(ev, "is_final", False):
                return
            text = getattr(ev, "transcript", "") or ""
            if not text.strip():
                return
            if first_user_audio_at is None:
                first_user_audio_at = time.perf_counter()
                first_user_before_ai = first_agent_audio_at is None and not first_ai_transcript_seen
                asyncio.create_task(_log(
                    "info",
                    "first_user_audio_timestamp",
                    (
                        f"room={ctx.room.name}; at={first_user_audio_at:.6f}; "
                        f"since_answer_ms={int((first_user_audio_at - call_answered_at) * 1000) if call_answered_at else ''}; "
                        f"since_session_start_ms={int((first_user_audio_at - session_start_done_at) * 1000) if session_start_done_at else ''}; "
                        f"first_user_before_ai={first_user_before_ai}"
                    ),
                ))
                asyncio.create_task(_log(
                    "info",
                    "first_user_audio",
                    (
                        f"room={ctx.room.name}; at={first_user_audio_at:.6f}; "
                        f"since_answer_ms={int((first_user_audio_at - call_answered_at) * 1000) if call_answered_at else ''}; "
                        f"since_session_start_ms={int((first_user_audio_at - session_start_done_at) * 1000) if session_start_done_at else ''}; "
                        f"first_user_before_ai={first_user_before_ai}"
                    ),
                ))
            asyncio.create_task(_log("info", "User speech detected", text[:160]))
            asyncio.create_task(_persist_transcript("user", text))
        except Exception as exc:
            asyncio.create_task(_log_exception("User transcript handler error", exc, "warning"))

    def _on_conversation_item_added(ev) -> None:
        nonlocal first_agent_audio_at, first_gemini_audio_at, first_ai_transcript_seen
        try:
            item = getattr(ev, "item", None)
            role = getattr(item, "role", None)
            if role != "assistant":
                return
            text = getattr(item, "text_content", None) or ""
            if not text.strip():
                return
            if first_agent_audio_at is None:
                first_agent_audio_at = time.perf_counter()
                asyncio.create_task(_log(
                    "info",
                    "first_agent_audio_timestamp",
                    (
                        f"room={ctx.room.name}; at={first_agent_audio_at:.6f}; "
                        f"since_answer_ms={int((first_agent_audio_at - call_answered_at) * 1000) if call_answered_at else ''}; "
                        f"since_session_start_ms={int((first_agent_audio_at - session_start_done_at) * 1000) if session_start_done_at else ''}; "
                        f"first_user_before_ai={first_user_before_ai}"
                    ),
                ))
            if first_greeting_controlled and _looks_like_controlled_greeting(text):
                asyncio.create_task(_log("info", "controlled_greeting_transcript", text[:160]))
                asyncio.create_task(_persist_transcript("ai", text))
                return
            if first_gemini_audio_at is None:
                first_gemini_audio_at = time.perf_counter()
                asyncio.create_task(_log(
                    "info",
                    "first_gemini_audio",
                    (
                        f"room={ctx.room.name}; at={first_gemini_audio_at:.6f}; "
                        f"since_answer_ms={int((first_gemini_audio_at - call_answered_at) * 1000) if call_answered_at else ''}; "
                        f"since_session_start_ms={int((first_gemini_audio_at - session_start_done_at) * 1000) if session_start_done_at else ''}; "
                        f"first_user_before_ai={first_user_before_ai}"
                    ),
                ))
            if not first_ai_transcript_seen:
                first_ai_transcript_seen = True
                asyncio.create_task(_log("info", "first_ai_transcript_text", text[:240]))
            asyncio.create_task(_guard_and_maybe_terminate(text))
            asyncio.create_task(_log("info", "AI response generated", text[:160]))
            asyncio.create_task(_persist_transcript("ai", text))
        except Exception as exc:
            asyncio.create_task(_log_exception("AI transcript handler error", exc, "warning"))

    try:
        session.on("user_input_transcribed", _on_user_input_transcribed)
        session.on("conversation_item_added", _on_conversation_item_added)
    except Exception as exc:
        await _log_exception("Failed to register session handlers", exc, "warning")

    # track_published handled via room event (local participant lacks .on in rtc 1.1.8)

    # ── Optional S3 recording via LiveKit Egress ─────────────────────────────
    # if phone_number:
    #     _aws_key     = os.getenv("S3_ACCESS_KEY_ID") or os.getenv("AWS_ACCESS_KEY_ID", "")
    #     _aws_secret  = os.getenv("S3_SECRET_ACCESS_KEY") or os.getenv("AWS_SECRET_ACCESS_KEY", "")
    #     _aws_bucket  = os.getenv("S3_BUCKET") or os.getenv("AWS_BUCKET_NAME", "")
    #     _s3_endpoint = os.getenv("S3_ENDPOINT_URL") or os.getenv("S3_ENDPOINT", "")
    #     _s3_region   = os.getenv("S3_REGION") or os.getenv("AWS_REGION", "ap-northeast-1")
    #     if _aws_key and _aws_secret and _aws_bucket:
    #         try:
    #             _recording_path = f"recordings/{ctx.room.name}.ogg"
    #             _egress_req = api.RoomCompositeEgressRequest(
    #                 room_name=ctx.room.name, audio_only=True,
    #                 file_outputs=[api.EncodedFileOutput(
    #                     file_type=api.EncodedFileType.OGG, filepath=_recording_path,
    #                     s3=api.S3Upload(
    #                         access_key=_aws_key, secret=_aws_secret,
    #                         bucket=_aws_bucket, region=_s3_region, endpoint=_s3_endpoint,
    #                     ),
    #                 )],
    #             )
    #             _egress = await ctx.api.egress.start_room_composite_egress(_egress_req)
    #             _s3_ep = _s3_endpoint.rstrip("/")
    #             tool_ctx.recording_url = (
    #                 f"{_s3_ep}/{_aws_bucket}/{_recording_path}"
    #                 if _s3_ep else f"s3://{_aws_bucket}/{_recording_path}"
    #             )
    #             await _log("info", f"Recording started: egress={_egress.egress_id}")
    #         except Exception as _exc:
    #             await _log_exception("Recording start failed (non-fatal)", _exc, "warning")

    # ── Greeting (Rule 4) ────────────────────────────────────────────────────
    # gemini-3.1 and gemini-2.5 native-audio speak autonomously from system prompt.
    # generate_reply() is blocked by the plugin for these models — skip entirely.
    _active_model = os.getenv("GEMINI_MODEL", "")
    
    # ── Initial Greeting ────────────────────────────────────

    # greeting = (
    #     f"Hi, am I speaking with {lead_name}?"
    #     if phone_number else
    #     "Hello, how can I help you today?"
    # )

    # await asyncio.sleep(0.8)

    # try:
    #     await session.say(
    #         greeting,
    #         allow_interruptions=True
    #     )

    #     await _log("info", f"Greeting spoken: {greeting}")

    # except Exception as exc:
    #     await _log_exception("Initial greeting failed", exc, "warning")

    # try:
    #     await session.generate_reply(instructions=greeting)
    #     await _log("info", "Greeting generated")
    # except Exception as _gr_exc:
    #     await _log_exception("generate_reply failed", _gr_exc, "warning")

    # ── Keep session alive until SIP participant actually leaves (Rule 2) ────
    # Watch participant_disconnected for the specific SIP identity.
    # Never use close_on_disconnect=True — kills session on any SIP audio dropout.
    if phone_number:
        _sip_identity = f"sip_{phone_number}"
        _disconnect_event = asyncio.Event()

        async def _delayed_disconnect():
            await asyncio.sleep(8)

            still_exists = any(
                p.identity == _sip_identity
                for p in ctx.room.remote_participants.values()
            )

            if not still_exists:
                _disconnect_event.set()

        def _on_participant_disconnected(participant: rtc.RemoteParticipant):
            if participant.identity == _sip_identity:
                asyncio.create_task(_delayed_disconnect())

        def _on_disconnected():
            _disconnect_event.set()

        ctx.room.on("participant_disconnected", _on_participant_disconnected)
        ctx.room.on("disconnected", _on_disconnected)

        try:
            await asyncio.wait_for(_disconnect_event.wait(), timeout=3600)
        except asyncio.TimeoutError:
            await _log("warning", "Call reached 1-hour safety timeout — shutting down")

        await _log("info", f"SIP participant disconnected — ending session for {phone_number}")

        fallback_outcome = "abandoned" if call_answered else "no_answer"
        fallback_reason = "sip disconnect before end_call" if call_answered else "sip session ended before answer confirmation"
        await _fallback_call_log(
            fallback_outcome,
            fallback_reason,
            f"session_started={session_started}; transcript_saved={transcript_saved}",
        )

        await _log(
            "info",
            "Session disconnect summary",
            (
                f"room={ctx.room.name}; duration_seconds={int(time.time() - tool_ctx._call_start_time)}; "
                f"end_call_called={tool_ctx._end_call_called}; "
                f"final_log_written={tool_ctx._final_log_written}; "
                f"booking_confirmed={tool_ctx._booking_confirmed}; "
                f"booking_id={tool_ctx._booking_id or ''}; "
                f"transfer_invoked={tool_ctx._transfer_invoked}; "
                f"transfer_succeeded={tool_ctx._transfer_succeeded}; "
                f"transfer_failure_reason={tool_ctx._transfer_failure_reason or ''}; "
                f"transcript_saved={transcript_saved}"
            ),
        )

        try:
            await asyncio.sleep(2)
            await session.aclose()
        except:
            pass

    else:
        _done = asyncio.Event()

        ctx.room.on("disconnected", lambda: _done.set())

        try:
            await asyncio.wait_for(_done.wait(), timeout=3600)
        except asyncio.TimeoutError:
            pass
        await _fallback_call_log(
            "disconnected",
            "session ended before end_call",
            f"session_started={session_started}; transcript_saved={transcript_saved}",
        )
        await _log(
            "info",
            "Session disconnect summary",
            (
                f"room={ctx.room.name}; duration_seconds={int(time.time() - tool_ctx._call_start_time)}; "
                f"end_call_called={tool_ctx._end_call_called}; "
                f"final_log_written={tool_ctx._final_log_written}; "
                f"booking_confirmed={tool_ctx._booking_confirmed}; "
                f"booking_id={tool_ctx._booking_id or ''}; "
                f"transfer_invoked={tool_ctx._transfer_invoked}; "
                f"transfer_succeeded={tool_ctx._transfer_succeeded}; "
                f"transfer_failure_reason={tool_ctx._transfer_failure_reason or ''}; "
                f"transcript_saved={transcript_saved}"
            ),
        )


if __name__ == "__main__":
    init_db()
    load_db_settings_to_env()
    agents.cli.run_app(
        agents.WorkerOptions(entrypoint_fnc=entrypoint, agent_name="outbound-caller")
    )
