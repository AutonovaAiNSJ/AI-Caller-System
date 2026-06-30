import asyncio
import json
import logging
import os
import ssl
import certifi
import time
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

from livekit import agents, rtc
from livekit.agents import Agent, AgentSession, RoomInputOptions
try:
    from livekit.agents import RoomOptions as _RoomOptions
    _HAS_ROOM_OPTIONS = True
except ImportError:
    _HAS_ROOM_OPTIONS = False
from livekit.plugins import noise_cancellation

from agent import (
    _build_session,
    _booking_workflow_active,
    _dedupe_tool_names,
    _parse_tool_names,
    _safe_preview,
    MANDATORY_TOOL_CONTRACT,
    load_db_settings_to_env,
)
from db import init_db, log_error, get_enabled_tools, save_transcript, set_request_context
from prompts import build_prompt
from tools import AppointmentTools, DEFAULT_TOOL_NAMES, MANDATORY_BOOKING_TOOLS

load_dotenv(".env", override=False)  # VPS env vars always win — .env only for local dev
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("inbound-agent")


async def _log(level: str, msg: str, detail: str = "") -> None:
    if level == "info":
        logger.info(msg)
    elif level == "warning":
        logger.warning(msg)
    else:
        logger.error(msg)
    try:
        await log_error("inbound-agent", msg, detail, level)
    except Exception:
        logger.exception("Failed to persist log entry")


async def _log_exception(msg: str, exc: Exception, level: str = "error") -> None:
    tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
    if level == "warning":
        logger.exception(msg)
    else:
        logger.exception(msg)
    try:
        await log_error("inbound-agent", msg, tb, level)
    except Exception:
        logger.exception("Failed to persist exception log")


def _normalize_phone_identity(identity: Optional[str]) -> str:
    if not identity:
        return ""
    clean = identity.replace("sip_", "").replace("sip:", "").replace("tel:", "")
    if "@" in clean:
        clean = clean.split("@", 1)[0]
    return clean.strip()


class InboundAssistant(Agent):
    """
    Inbound appointment booking agent.
    tools=[] in super().__init__ to avoid duplicate tool name error (Rule 13).
    Tools are passed only to AgentSession.
    """

    def __init__(self, instructions: str) -> None:
        super().__init__(instructions=instructions, tools=[])


async def entrypoint(ctx: agents.JobContext):
    """
    LiveKit agent entrypoint — inbound pattern.

    Order:
    1. Connect to room
    2. Parse metadata (phone, lead info, profile overrides)
    3. Start Gemini Live session (caller is already connected)
    4. Keep alive via participant_disconnected event
    """
    await ctx.connect()

    phone_number: Optional[str] = None
    lead_name = "there"
    business_name = "our company"
    service_type = "our service"
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
    call_session_id: Optional[str] = None
    direction = "inbound"

    tenant_id: Optional[str] = None
    raw_meta = ctx.job.metadata or ""
    try:
        meta = json.loads(raw_meta) if raw_meta else {}
        tenant_id = meta.get("tenant_id")
        phone_number = meta.get("phone_number")
        lead_name = meta.get("lead_name", "there")
        business_name = meta.get("business_name", "our company")
        service_type = meta.get("service_type", "our service")
        system_prompt = meta.get("system_prompt")
        voice_override = meta.get("voice_override")
        model_override = meta.get("model_override")
        tools_override = meta.get("tools_override")
        tools_override_supplied = "tools_override" in meta
        agent_profile_id = meta.get("agent_profile_id")
        agent_profile_name = meta.get("agent_profile_name")
        agent_profile_source = meta.get("agent_profile_source") or "metadata"
        prompt_override_present = bool(meta.get("system_prompt_override_present"))
        prompt_source = meta.get("prompt_source") or (
            "per_call_override" if prompt_override_present else "metadata_or_global"
        )
        call_session_id = meta.get("call_session_id")
        direction = meta.get("direction") or "inbound"
    except Exception as exc:
        await _log_exception("Metadata parse error", exc, "warning")

    set_request_context(tenant_id=tenant_id, role="TENANT_ADMIN")

    for participant in ctx.room.remote_participants.values():
        if not phone_number:
            phone_number = _normalize_phone_identity(participant.identity)

    if voice_override:
        os.environ["GEMINI_TTS_VOICE"] = voice_override
    if model_override:
        os.environ["GEMINI_MODEL"] = model_override

    # ── Resolve system prompt (DB → metadata → default) ─────────────────────
    from db import get_setting as _gs
    
    # 1. Resolve Global Prompt
    global_prompt_raw = await _gs("system_prompt", "") or None
    global_prompt_str = build_prompt(
        lead_name=lead_name,
        business_name=business_name,
        service_type=service_type,
        phone=phone_number or "",
        agent_name=agent_profile_name or "Priya",
        custom_prompt=global_prompt_raw
    )
    
    # 2. Resolve Agent-Specific Prompt (if any)
    agent_prompt_str = ""
    meta_prompt = meta.get("system_prompt")
    if meta_prompt and prompt_source in ("agent_profile", "default_agent_profile", "per_call_override"):
        agent_prompt_str = build_prompt(
            lead_name=lead_name,
            business_name=business_name,
            service_type=service_type,
            phone=phone_number or "",
            agent_name=agent_profile_name or "Priya",
            custom_prompt=meta_prompt
        )

    # Merge Global + Agent
    if agent_prompt_str:
        system_prompt = f"{global_prompt_str.rstrip()}\n\n{agent_prompt_str.strip()}"
    else:
        system_prompt = global_prompt_str
        
    system_prompt = f"{system_prompt.rstrip()}{MANDATORY_TOOL_CONTRACT}"

    # 3. Resolve Company Knowledge (if documents exist)
    from db import get_active_company_knowledge
    kb_docs = await get_active_company_knowledge(tenant_id)
    if kb_docs:
        merged_knowledge = "\n\n".join([doc["content"] for doc in kb_docs])
        MAX_KB_SIZE = 40000
        if len(merged_knowledge) > MAX_KB_SIZE:
            merged_knowledge = merged_knowledge[:MAX_KB_SIZE] + "\n[Content truncated due to size limit]"
            
        kb_prompt = f"\n\n-----------------\nCOMPANY KNOWLEDGE\n{merged_knowledge}\n-----------------\n"
        
        ai_rules = (
            "You must answer company-related questions ONLY using the company knowledge below.\n"
            "Never invent services.\n"
            "Never invent pricing.\n"
            "Never invent policies.\n"
            "Never invent products.\n"
            "Never guess.\n"
            "If the answer does not exist inside the uploaded company knowledge, reply exactly:\n"
            "\"I don't have that information right now.\"\n"
        )
        
        system_prompt = f"{system_prompt.rstrip()}\n\n{ai_rules}\n{kb_prompt}"


    global_tool_names = await get_enabled_tools()
    override_tool_names, override_parse_source = _parse_tool_names(tools_override) if tools_override_supplied else (
        None,
        "not_supplied",
    )
    if tools_override_supplied:
        base_tool_names = override_tool_names or []
        tool_source = f"metadata_override:{override_parse_source}"
    elif global_tool_names is not None:
        base_tool_names = global_tool_names
        tool_source = "global_setting"
    else:
        base_tool_names = list(DEFAULT_TOOL_NAMES)
        tool_source = "built_in_default"

    booking_active = _booking_workflow_active(system_prompt, base_tool_names)
    mandatory_injections = []
    if booking_active:
        for name in MANDATORY_BOOKING_TOOLS:
            if name not in base_tool_names:
                base_tool_names.append(name)
                mandatory_injections.append(name)
    enabled_tools = _dedupe_tool_names(base_tool_names)
    if booking_active:
        missing_mandatory = [name for name in MANDATORY_BOOKING_TOOLS if name not in enabled_tools]
        if missing_mandatory:
            await _log("warning", "Mandatory booking tools missing after resolution", ",".join(missing_mandatory))
    else:
        system_prompt = (
            f"{system_prompt.rstrip()}\n\nBooking actions unavailable in this session. "
            "Do not claim availability or confirmed bookings.\n"
        )

    tool_ctx = AppointmentTools(
        ctx,
        phone_number=phone_number,
        lead_name=lead_name,
        direction=direction,
        call_session_id=call_session_id,
    )
    await tool_ctx.mark_connected()
    transcript_saved = False
    session_started = False

    async def _fallback_call_log(outcome: str, reason: str, detail: str = "") -> None:
        try:
            await tool_ctx.log_fallback_call_end(outcome=outcome, reason=reason, detail=detail)
        except Exception as exc:
            await _log_exception("Fallback call logging crashed", exc, "error")

    await _log(
        "info",
        "Inbound agent prompt/tools resolved",
        (
            f"room={ctx.room.name}; profile_id={agent_profile_id or ''}; "
            f"profile_name={agent_profile_name or ''}; profile_source={agent_profile_source}; prompt_source={prompt_source}; "
            f"agent_display_name={agent_display_name}; business_name={business_name}; "
            f"service_type={service_type}; canonical_phone={phone_number or ''}; "
            f"resolved_tools={enabled_tools}; tool_source={tool_source}; "
            f"tools_override_supplied={tools_override_supplied}; mandatory_injections={mandatory_injections}; "
            f"booking_workflow_active={booking_active}; "
            f"model={os.getenv('GEMINI_MODEL', 'gemini-3.1-flash-live-preview')}; "
            f"voice={os.getenv('GEMINI_TTS_VOICE', 'Aoede')}; "
            f"override_prompt_present={prompt_override_present}; "
            f"prompt_preview={_safe_preview(system_prompt)}"
        ),
    )

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
                    asyncio.create_task(_log("info", "User speech detected", text[:160]))
                else:
                    asyncio.create_task(_log("info", "AI response generated", text[:160]))
                asyncio.create_task(_persist_transcript(speaker, text))
        except Exception as exc:
            asyncio.create_task(_log_exception("Transcription handler error", exc, "warning"))

    def _on_participant_connected(participant: rtc.RemoteParticipant):
        nonlocal phone_number
        asyncio.create_task(_log("info", "Participant connected", participant.identity))
        if not phone_number:
            resolved = _normalize_phone_identity(participant.identity)
            if resolved:
                phone_number = resolved
                tool_ctx.phone_number = resolved

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

    await _log("info", f"Inbound AI agent connected: room={ctx.room.name}")

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
            "disconnected",
            "session build failed before end_call",
            f"error={exc}",
        )
        ctx.shutdown()
        return

    if _HAS_ROOM_OPTIONS:
        from livekit.agents import RoomOptions as _RO

        _session_kwargs = dict(
            room=ctx.room,
            agent=InboundAssistant(instructions=system_prompt),
            room_options=_RO(input_options=RoomInputOptions(noise_cancellation=noise_cancellation.BVCTelephony())),
        )
    else:
        _session_kwargs = dict(
            room=ctx.room,
            agent=InboundAssistant(instructions=system_prompt),
            room_input_options=RoomInputOptions(noise_cancellation=noise_cancellation.BVCTelephony()),
        )

    try:
        await session.start(**_session_kwargs)
        session_started = True
    except Exception as exc:
        await _log_exception("AI session start failed", exc, "error")
        await _fallback_call_log(
            "disconnected",
            "session start failed before end_call",
            f"error={exc}",
        )
        ctx.shutdown()
        return
    await asyncio.sleep(2)

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
            f"booking_id={tool_ctx._booking_id or ''}; transcript_saved={transcript_saved}"
        ),
    )


if __name__ == "__main__":
    init_db()
    load_db_settings_to_env()
    agents.cli.run_app(
        agents.WorkerOptions(entrypoint_fnc=entrypoint, agent_name="inbound-caller")
    )
