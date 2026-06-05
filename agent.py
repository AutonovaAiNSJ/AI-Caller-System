import asyncio
import json
import logging
import os
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
        logger.exception("Failed to persist log entry")


async def _log_exception(msg: str, exc: Exception, level: str = "error") -> None:
    tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
    if level == "warning":
        logger.exception(msg)
    else:
        logger.exception(msg)
    try:
        await log_error("agent", msg, tb, level)
    except Exception:
        logger.exception("Failed to persist exception log")


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
                return AgentSession(llm=model, tools=tools)
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
        await _log_exception("Metadata parse error", exc, "warning")

    # ── Apply agent profile overrides (Rule 10) ──────────────────────────────
    if voice_override:
        os.environ["GEMINI_TTS_VOICE"] = voice_override
    if model_override:
        os.environ["GEMINI_MODEL"] = model_override

    # ── Resolve enabled tools ────────────────────────────────────────────────
    # enabled_tools: list = []
    # if tools_override:
    #     try:
    #         enabled_tools = json.loads(tools_override) if isinstance(tools_override, str) else list(tools_override)
    #     except Exception:
    #         enabled_tools = []
    # if not enabled_tools:
    #     enabled_tools = await get_enabled_tools()
    enabled_tools = []

    if tools_override:
        try:
            enabled_tools = (
                json.loads(tools_override)
                if isinstance(tools_override, str)
                else list(tools_override)
            )
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

    tool_ctx = AppointmentTools(ctx, phone_number=phone_number, lead_name=lead_name, business_name=business_name)

    async def _persist_transcript(speaker: str, text: str) -> None:
        try:
            await save_transcript(ctx.room.name, speaker, text)
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

            except Exception as exc:
                await _log("error", f"SIP dial FAILED for {phone_number}: {exc}")
                ctx.shutdown()
                return

        else:
            await _log(
                "warning",
                "No SIP trunk configured — running in local/browser mode"
            )
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
            agent=OutboundAssistant(instructions=""),
            room_options=_RO(input_options=RoomInputOptions(noise_cancellation=noise_cancellation.BVCTelephony())),
        )
    else:
        _session_kwargs = dict(
            room=ctx.room,
            agent=OutboundAssistant(instructions=""),
            room_input_options=RoomInputOptions(noise_cancellation=noise_cancellation.BVCTelephony()),
        )

    await session.start(**_session_kwargs)
    await asyncio.sleep(2)
    # await asyncio.sleep(2)

    # await session.say(
    #     greeting,
    #     allow_interruptions=True
    # )
    await _log("info", "Gemini session started")

    def _on_user_input_transcribed(ev) -> None:
        try:
            if not getattr(ev, "is_final", False):
                return
            text = getattr(ev, "transcript", "") or ""
            if not text.strip():
                return
            asyncio.create_task(_log("info", "User speech detected", text[:160]))
            asyncio.create_task(_persist_transcript("user", text))
        except Exception as exc:
            asyncio.create_task(_log_exception("User transcript handler error", exc, "warning"))

    def _on_conversation_item_added(ev) -> None:
        try:
            item = getattr(ev, "item", None)
            role = getattr(item, "role", None)
            if role != "assistant":
                return
            text = getattr(item, "text_content", None) or ""
            if not text.strip():
                return
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


if __name__ == "__main__":
    init_db()
    load_db_settings_to_env()
    agents.cli.run_app(
        agents.WorkerOptions(entrypoint_fnc=entrypoint, agent_name="outbound-caller")
    )
