import asyncio
import json
import logging
import os
import ssl
import certifi
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
from livekit.plugins import noise_cancellation, silero

from db import init_db, log_error, get_enabled_tools
from prompts import build_prompt
from tools import AppointmentTools

load_dotenv(".env", override=False)  # VPS env vars always win — .env only for local dev
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("outbound-agent")

SIP_DOMAIN = os.getenv("VOBIZ_SIP_DOMAIN", "")


async def _log(level: str, msg: str, detail: str = "") -> None:
    if level == "info":      logger.info(msg)
    elif level == "warning": logger.warning(msg)
    else:                    logger.error(msg)
    try:
        await log_error("agent", msg, detail, level)
    except Exception:
        pass


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
        logger.warning("Could not load settings from Supabase: %s", exc)


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
        pass
    try:
        _google_beta_realtime = _gp.beta.realtime.RealtimeModel
        logger.info("Loaded google.beta.realtime.RealtimeModel (beta path)")
    except AttributeError:
        pass
    try:
        _google_llm = _gp.LLM
        _google_tts = _gp.TTS
    except AttributeError:
        pass
except ImportError:
    logger.warning("livekit-plugins-google not installed")

_deepgram_stt = None
try:
    from livekit.plugins import deepgram as _dg
    _deepgram_stt = _dg.STT
except ImportError:
    pass


# ── Session factory ──────────────────────────────────────────────────────────

def _build_session(tools: list, system_prompt: str) -> AgentSession:
    """
    Build AgentSession with Gemini Live or pipeline fallback.

    CRITICAL SILENCE-PREVENTION CONFIG — all 3 required:
    1. SessionResumptionConfig(transparent=True) — auto-reconnects on timeout instead of going silent
    2. ContextWindowCompressionConfig — prevents freeze when context fills up
    3. RealtimeInputConfig with END_SENSITIVITY_LOW + silence_duration_ms=2000

    Without all 3, calls will go silent within 30-90 seconds.
    """
    use_realtime = os.getenv("USE_GEMINI_REALTIME", "true").lower() in ("true", "1", "yes")
    model_name = os.getenv("GEMINI_MODEL", "gemini-3.1-flash-live-preview")
    voice = os.getenv("GEMINI_TTS_VOICE", "Aoede")
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
                return AgentSession(model=model, tools=tools)
            except Exception as exc:
                logger.warning("Gemini Live init failed (%s) — falling back to pipeline", exc)

    # ── Pipeline fallback (Deepgram STT + Gemini LLM) ───────────────────────
    logger.warning("Using pipeline fallback (STT + LLM)")
    _vad = silero.VAD.load()
    _stt = None
    if _deepgram_stt:
        _dgk = os.getenv("DEEPGRAM_API_KEY", "")
        if _dgk:
            _stt = _deepgram_stt(api_key=_dgk)
    _llm = None
    if _google_llm and api_key:
        _llm = _google_llm(model="gemini-2.0-flash", api_key=api_key)
    if _stt and _llm:
        return AgentSession(vad=_vad, stt=_stt, llm=_llm, tools=tools)
    raise RuntimeError(
        "Cannot build AgentSession — check GOOGLE_API_KEY and DEEPGRAM_API_KEY env vars."
    )


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

    raw_meta = ctx.job.metadata or ""
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
    except Exception as exc:
        await _log("warning", f"Metadata parse error: {exc}")

    # ── Apply agent profile overrides (Rule 10) ──────────────────────────────
    if voice_override:
        os.environ["GEMINI_TTS_VOICE"] = voice_override
    if model_override:
        os.environ["GEMINI_MODEL"] = model_override

    # ── Resolve enabled tools ────────────────────────────────────────────────
    enabled_tools: list = []
    if tools_override:
        try:
            enabled_tools = json.loads(tools_override) if isinstance(tools_override, str) else list(tools_override)
        except Exception:
            enabled_tools = []
    if not enabled_tools:
        enabled_tools = await get_enabled_tools()

    # ── Resolve system prompt (DB → metadata → default) ─────────────────────
    if not system_prompt:
        from db import get_setting as _gs
        system_prompt = await _gs("system_prompt", "") or None

    system_prompt = build_prompt(
        lead_name=lead_name,
        business_name=business_name,
        service_type=service_type,
        custom_prompt=system_prompt,
    )

    tool_ctx = AppointmentTools(ctx, phone_number=phone_number, lead_name=lead_name)

    await _log("info", f"Connected to LiveKit room: {ctx.room.name}")

    # ── Dial — MUST come before session.start() (Rule 1) ────────────────────
    if phone_number:
        trunk_id = os.getenv("OUTBOUND_TRUNK_ID")
        if not trunk_id:
            await _log("error", "OUTBOUND_TRUNK_ID not set — cannot place outbound call")
            ctx.shutdown()
            return
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
        except Exception as exc:
            await _log("error", f"SIP dial FAILED for {phone_number}: {exc}")
            ctx.shutdown()
            return
        await _log("info", f"Call ANSWERED — {phone_number} picked up, starting AI session now")

    # ── Build and start Gemini Live ──────────────────────────────────────────
    gemini_model = os.getenv("GEMINI_MODEL", "gemini-3.1-flash-live-preview")
    await _log("info", f"Building AI session — model={gemini_model}")
    active_tools = tool_ctx.build_tool_list(enabled_tools)
    await _log("info", f"Tools loaded: {[t.__name__ for t in active_tools]}")
    session = _build_session(tools=active_tools, system_prompt=system_prompt)

    # Never use close_on_disconnect=True with SIP (Rule 2)
    if _HAS_ROOM_OPTIONS:
        from livekit.agents import RoomOptions as _RO
        _session_kwargs = dict(
            room=ctx.room,
            agent=OutboundAssistant(instructions=system_prompt),
            room_options=_RO(input_options=RoomInputOptions(noise_cancellation=noise_cancellation.BVCTelephony())),
        )
    else:
        _session_kwargs = dict(
            room=ctx.room,
            agent=OutboundAssistant(instructions=system_prompt),
            room_input_options=RoomInputOptions(noise_cancellation=noise_cancellation.BVCTelephony()),
        )

    await session.start(**_session_kwargs)
    await _log("info", "Agent session started — AI ready, generating greeting")

    # ── Optional S3 recording via LiveKit Egress ─────────────────────────────
    if phone_number:
        _aws_key     = os.getenv("S3_ACCESS_KEY_ID") or os.getenv("AWS_ACCESS_KEY_ID", "")
        _aws_secret  = os.getenv("S3_SECRET_ACCESS_KEY") or os.getenv("AWS_SECRET_ACCESS_KEY", "")
        _aws_bucket  = os.getenv("S3_BUCKET") or os.getenv("AWS_BUCKET_NAME", "")
        _s3_endpoint = os.getenv("S3_ENDPOINT_URL") or os.getenv("S3_ENDPOINT", "")
        _s3_region   = os.getenv("S3_REGION") or os.getenv("AWS_REGION", "ap-northeast-1")
        if _aws_key and _aws_secret and _aws_bucket:
            try:
                _recording_path = f"recordings/{ctx.room.name}.ogg"
                _egress_req = api.RoomCompositeEgressRequest(
                    room_name=ctx.room.name, audio_only=True,
                    file_outputs=[api.EncodedFileOutput(
                        file_type=api.EncodedFileType.OGG, filepath=_recording_path,
                        s3=api.S3Upload(
                            access_key=_aws_key, secret=_aws_secret,
                            bucket=_aws_bucket, region=_s3_region, endpoint=_s3_endpoint,
                        ),
                    )],
                )
                _egress = await ctx.api.egress.start_room_composite_egress(_egress_req)
                _s3_ep = _s3_endpoint.rstrip("/")
                tool_ctx.recording_url = (
                    f"{_s3_ep}/{_aws_bucket}/{_recording_path}"
                    if _s3_ep else f"s3://{_aws_bucket}/{_recording_path}"
                )
                await _log("info", f"Recording started: egress={_egress.egress_id}")
            except Exception as _exc:
                await _log("warning", f"Recording start failed (non-fatal): {_exc}")

    # ── Greeting (Rule 4) ────────────────────────────────────────────────────
    # gemini-3.1 and gemini-2.5 native-audio speak autonomously from system prompt.
    # generate_reply() is blocked by the plugin for these models — skip entirely.
    _active_model = os.getenv("GEMINI_MODEL", "")
    if "3.1" in _active_model or "2.5" in _active_model:
        await _log("info", "Gemini native-audio: model will greet autonomously from system prompt")
    else:
        greeting = (
            f"The call just connected. Greet the lead and ask if you're speaking with {lead_name}."
            if phone_number else "Greet the caller warmly."
        )
        try:
            await session.generate_reply(instructions=greeting)
        except Exception as _gr_exc:
            await _log("warning", f"generate_reply failed: {_gr_exc}")

    # ── Keep session alive until SIP participant actually leaves (Rule 2) ────
    # Watch participant_disconnected for the specific SIP identity.
    # Never use close_on_disconnect=True — kills session on any SIP audio dropout.
    if phone_number:
        _sip_identity = f"sip_{phone_number}"
        _disconnect_event = asyncio.Event()

        def _on_participant_disconnected(participant: rtc.RemoteParticipant):
            if participant.identity == _sip_identity:
                _disconnect_event.set()

        def _on_disconnected():
            _disconnect_event.set()

        ctx.room.on("participant_disconnected", _on_participant_disconnected)
        ctx.room.on("disconnected", _on_disconnected)

        try:
            await asyncio.wait_for(_disconnect_event.wait(), timeout=3600)
        except asyncio.TimeoutError:
            await _log("warning", "Call reached 1-hour safety timeout — shutting down")

        await _log("info", f"SIP participant disconnected — ending session for {phone_number}")
        await session.aclose()
    else:
        _done = asyncio.Event()
        ctx.room.on("disconnected", lambda: _done.set())
        try:
            await asyncio.wait_for(_done.wait(), timeout=3600)
        except asyncio.TimeoutError:
            pass


if __name__ == "__main__":
    init_db()
    load_db_settings_to_env()
    agents.cli.run_app(
        agents.WorkerOptions(entrypoint_fnc=entrypoint, agent_name="outbound-caller")
    )
