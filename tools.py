import asyncio
import logging
import os
import time as time_module
import functools
import traceback
from datetime import datetime
from typing import Optional

from livekit import agents, api
from livekit.agents import llm

from db import (
    check_slot, get_next_available, insert_appointment, log_call, log_error,
    get_calls_by_phone, get_appointments_by_phone,
    add_contact_memory, get_contact_memory, compress_contact_memory,
    get_setting, update_appointment_gcal, get_enabled_tools,
)
from gcal import GoogleCalendarManager

logger = logging.getLogger("appointment-tools")
DEBUG_TOOL_LOGS = os.getenv("DEBUG_TOOL_LOGS", "").lower() in ("1", "true", "yes")
GCAL_AVAILABILITY_TIMEOUT_SECONDS = float(os.getenv("GCAL_AVAILABILITY_TIMEOUT_SECONDS", "2.5"))
GCAL_EVENT_TIMEOUT_SECONDS = float(os.getenv("GCAL_EVENT_TIMEOUT_SECONDS", "4.0"))
AVAILABILITY_CACHE_TTL_SECONDS = float(os.getenv("AVAILABILITY_CACHE_TTL_SECONDS", "180"))


MANDATORY_BOOKING_TOOLS = ["check_availability", "book_appointment", "end_call"]
DEFAULT_TOOL_NAMES = [
    "check_availability", "book_appointment", "end_call",
    "transfer_to_human", "send_sms_confirmation", "lookup_contact",
    "remember_details", "book_calcom", "cancel_calcom",
    "send_email",
]


def log_tool_execution(func):
    @functools.wraps(func)
    async def wrapper(self, *args, **kwargs):
        tool_name = func.__name__
        start_time = time_module.time()
        try:
            result = await func(self, *args, **kwargs)
            duration = time_module.time() - start_time
            summary = f"tool={tool_name} status=success duration={duration:.3f}s"
            if tool_name == "book_appointment" and getattr(self, "_booking_id", None):
                summary += f" booking_id={self._booking_id}"
            logger.info(summary)
            asyncio.create_task(log_error(
                source="tools",
                message="Tool execution completed",
                detail=summary,
                level="info"
            ))
            if DEBUG_TOOL_LOGS:
                asyncio.create_task(log_error(
                    source="tools",
                    message=f"Tool '{tool_name}' debug payload",
                    detail=f"Duration: {duration:.3f}s\nArguments: args={args}, kwargs={kwargs}\nResult: {result}",
                    level="info"
                ))
            return result
        except Exception as exc:
            duration = time_module.time() - start_time
            tb = traceback.format_exc()
            await log_error(
                source="tools",
                message=f"Tool '{tool_name}' execution failed: {exc}",
                detail=f"Duration: {duration:.3f}s\nError: {exc}\nTraceback:\n{tb}",
                level="error"
            )
            raise exc
    return wrapper


async def terminate_call_room(room_name: str, delay: float = 3.0):
    await asyncio.sleep(delay)
    try:
        from db import get_setting
        import livekit.api as lk_api
        url = await get_setting("LIVEKIT_URL", "") or os.getenv("LIVEKIT_URL", "")
        key = await get_setting("LIVEKIT_API_KEY", "") or os.getenv("LIVEKIT_API_KEY", "")
        secret = await get_setting("LIVEKIT_API_SECRET", "") or os.getenv("LIVEKIT_API_SECRET", "")
        if url and key and secret:
            async with lk_api.LiveKitAPI(url=url, api_key=key, api_secret=secret) as api_client:
                from livekit.api import DeleteRoomRequest
                await api_client.room.delete_room(DeleteRoomRequest(room=room_name))
                logger.info(f"Successfully deleted room {room_name} after delay")
    except Exception as exc:
        logger.error(f"Failed to delete room {room_name} after delay: {exc}")


async def _log(msg: str, detail: str = "", level: str = "info") -> None:
    try:
        if level == "info":
            logger.info("%s %s", msg, detail[:160] if detail else "")
            asyncio.create_task(log_error("agent", msg, detail, level))
        else:
            await log_error("agent", msg, detail, level)
    except Exception:
        pass


class AppointmentTools(llm.ToolContext):
    """All function tools available to the appointment-booking agent."""

    def __init__(
        self,
        ctx: agents.JobContext,
        phone_number: Optional[str] = None,
        lead_name: Optional[str] = None,
        direction: str = "outbound",
        call_session_id: Optional[str] = None,
        business_name: Optional[str] = None,
        service_type: Optional[str] = None,
    ):
        self.ctx = ctx
        self.phone_number = phone_number
        self.lead_name = lead_name
        self.direction = direction or "outbound"
        self.call_session_id = call_session_id
        self.business_name = business_name
        self.service_type = service_type
        self._call_start_time = time_module.time()
        self._sip_domain = os.getenv("VOBIZ_SIP_DOMAIN", "")
        self.recording_url: Optional[str] = None
        self._booking_id: Optional[str] = None
        self._booking_confirmed = False
        self._booking_error: Optional[str] = None
        self._end_call_called = False
        self._final_log_written = False
        self._fallback_log_attempted = False
        self._settings_cache: dict[str, str] = {}
        self._gcal_manager: Optional[GoogleCalendarManager] = None
        self._gcal_manager_key: Optional[tuple[str, str]] = None
        self._enabled_tools_cache: Optional[list] = None
        self._availability_cache: dict[tuple[str, str, str, str], dict] = {}
        super().__init__(tools=[])

    async def mark_connected(self) -> None:
        try:
            if self.call_session_id:
                from db import update_call_session
                await update_call_session(
                    call_session_id=self.call_session_id,
                    status="connected",
                    connected_at=datetime.now().isoformat(),
                )
        except Exception:
            pass

    async def _get_setting_cached(self, key: str, default: str = "") -> str:
        if key not in self._settings_cache:
            self._settings_cache[key] = await get_setting(key, default)
        return self._settings_cache.get(key, default)

    async def _load_calendar_settings(self) -> tuple[str, str, int]:
        gcal_json, gcal_id, duration_raw = await asyncio.gather(
            self._get_setting_cached("GOOGLE_CALENDAR_SERVICE_ACCOUNT_JSON", ""),
            self._get_setting_cached("GOOGLE_CALENDAR_ID", "primary"),
            self._get_setting_cached("GOOGLE_CALENDAR_SLOT_DURATION", "30"),
        )
        try:
            duration = int(duration_raw or "30")
        except ValueError:
            duration = 30
        return gcal_json, gcal_id or "primary", duration

    async def _get_enabled_tools_cached(self) -> Optional[list]:
        if self._enabled_tools_cache is None:
            self._enabled_tools_cache = await get_enabled_tools()
        return self._enabled_tools_cache

    def _get_gcal_manager(self, gcal_json: str, gcal_id: str) -> Optional[GoogleCalendarManager]:
        if not gcal_json:
            return None
        key = (gcal_json, gcal_id or "primary")
        if self._gcal_manager is None or self._gcal_manager_key != key:
            self._gcal_manager = GoogleCalendarManager(gcal_json, gcal_id)
            self._gcal_manager_key = key
        return self._gcal_manager

    async def _gcal_call(self, timeout: float, func, *args):
        return await asyncio.wait_for(asyncio.to_thread(func, *args), timeout=timeout)

    def _availability_cache_key(
        self,
        date: str,
        slot_time: str,
        business_name: str,
        service_type: str,
    ) -> tuple[str, str, str, str]:
        return (
            (date or "").strip(),
            (slot_time or "").strip(),
            (business_name or "").strip().lower(),
            (service_type or "").strip().lower(),
        )

    def _get_cached_availability(
        self,
        date: str,
        slot_time: str,
        business_name: str,
        service_type: str,
    ) -> Optional[dict]:
        key = self._availability_cache_key(date, slot_time, business_name, service_type)
        item = self._availability_cache.get(key)
        if not item:
            return None
        age = time_module.time() - item.get("cached_at", 0)
        if age > AVAILABILITY_CACHE_TTL_SECONDS:
            self._availability_cache.pop(key, None)
            return None
        return item

    def _set_cached_availability(
        self,
        date: str,
        slot_time: str,
        business_name: str,
        service_type: str,
        available: bool,
        result: str,
        source: str,
    ) -> None:
        key = self._availability_cache_key(date, slot_time, business_name, service_type)
        self._availability_cache[key] = {
            "available": available,
            "result": result,
            "source": source,
            "cached_at": time_module.time(),
        }

    def build_tool_list(self, enabled: list) -> list:
        """Return tool methods for the final resolved tool-name list."""
        all_methods = [
            self.check_availability, self.book_appointment, self.end_call,
            self.transfer_to_human, self.send_sms_confirmation, self.lookup_contact,
            self.remember_details, self.book_calcom, self.cancel_calcom,
            self.send_email,
        ]
        name_map = {m.__name__: m for m in all_methods}
        return [name_map[n] for n in enabled if n in name_map]

    @llm.function_tool
    @log_tool_execution
    async def check_availability(
        self,
        date: str,
        time: str,
        expected_business_name: str,
        expected_service_type: str
    ) -> str:
        """
        Check whether a date/time slot is available for booking.
        Call this BEFORE attempting to book whenever the lead proposes a date/time.
        date format: YYYY-MM-DD  |  time format: HH:MM (24-hour)
        expected_business_name: the business context name that the AI represents
        expected_service_type: the service context that the AI is discussing
        Returns 'available' or 'unavailable: next available slot is <slot>'.
        """
        total_start = time_module.perf_counter()
        spans: dict[str, float] = {}
        try:
            await _log("Availability check started", f"date={date} time={time}", "info")
            
            # Verify call metadata context matches parameters (Issue 5)
            if (expected_business_name.strip().lower() != (self.business_name or "").strip().lower() or
                expected_service_type.strip().lower() != (self.service_type or "").strip().lower()):
                err_msg = f"Context validation failed: expected business '{self.business_name}' and service '{self.service_type}', but tool received '{expected_business_name}' and '{expected_service_type}'."
                await log_error("tools", "Context validation failed in check_availability", err_msg, "error")
                return f"Error: Context mismatch. This tool can only be used for {self.business_name} and {self.service_type}."
            cached = self._get_cached_availability(date, time, expected_business_name, expected_service_type)
            if cached:
                spans["cache_hit"] = 1
                spans["total_ms"] = round((time_module.perf_counter() - total_start) * 1000, 1)
                await _log(
                    "Availability cache hit",
                    f"date={date} time={time} available={cached.get('available')} source={cached.get('source')}; timing={spans}",
                    "info",
                )
                return cached["result"]

            t0 = time_module.perf_counter()
            gcal_json, gcal_id, duration = await self._load_calendar_settings()
            spans["settings_ms"] = round((time_module.perf_counter() - t0) * 1000, 1)

            t0 = time_module.perf_counter()
            local_task = asyncio.create_task(check_slot(date, time))
            manager = self._get_gcal_manager(gcal_json, gcal_id)
            calendar_task = (
                asyncio.create_task(self._gcal_call(
                    GCAL_AVAILABILITY_TIMEOUT_SECONDS,
                    manager.is_slot_available,
                    date,
                    time,
                    duration,
                ))
                if manager else None
            )
            local_available = await local_task
            spans["supabase_slot_ms"] = round((time_module.perf_counter() - t0) * 1000, 1)
            if not local_available:
                if calendar_task:
                    if calendar_task.done():
                        try:
                            calendar_task.result()
                        except Exception:
                            pass
                    else:
                        calendar_task.cancel()
                spans["total_ms"] = round((time_module.perf_counter() - total_start) * 1000, 1)
                result = "unavailable: that slot is already booked. Please suggest another time."
                self._set_cached_availability(date, time, expected_business_name, expected_service_type, False, result, "supabase")
                await _log("Availability result", f"unavailable via Supabase; timing={spans}", "info")
                return result

            if gcal_json:
                g0 = time_module.perf_counter()
                try:
                    calendar_available = await calendar_task
                    spans["gcal_availability_ms"] = round((time_module.perf_counter() - g0) * 1000, 1)
                except asyncio.TimeoutError:
                    spans["gcal_availability_ms"] = round((time_module.perf_counter() - g0) * 1000, 1)
                    await _log("Availability check timed out", f"calendar_id={gcal_id}; timing={spans}", "warning")
                    return "Unable to confirm calendar availability quickly right now. Please offer another time or I can have the team follow up."
                except Exception as exc:
                    spans["gcal_availability_ms"] = round((time_module.perf_counter() - g0) * 1000, 1)
                    await _log("Availability check failed in Google Calendar", str(exc), "error")
                    return "Unable to confirm calendar availability right now. Please offer another time or I can have the team follow up."
                if local_available and calendar_available:
                    spans["total_ms"] = round((time_module.perf_counter() - total_start) * 1000, 1)
                    self._set_cached_availability(date, time, expected_business_name, expected_service_type, True, "available", "google_calendar")
                    await _log("Availability result", f"available via Google Calendar; timing={spans}", "info")
                    return "available"
                spans["total_ms"] = round((time_module.perf_counter() - total_start) * 1000, 1)
                result = "unavailable: that calendar slot is busy. Please suggest another time."
                self._set_cached_availability(date, time, expected_business_name, expected_service_type, False, result, "google_calendar")
                await _log("Availability result", f"unavailable via Google Calendar; timing={spans}", "info")
                return result

            if local_available:
                spans["total_ms"] = round((time_module.perf_counter() - total_start) * 1000, 1)
                self._set_cached_availability(date, time, expected_business_name, expected_service_type, True, "available", "supabase")
                await _log("Availability result", f"available via Supabase; timing={spans}", "info")
                return "available"
            spans["total_ms"] = round((time_module.perf_counter() - total_start) * 1000, 1)
            result = "unavailable: that slot is already booked. Please suggest another time."
            self._set_cached_availability(date, time, expected_business_name, expected_service_type, False, result, "supabase")
            await _log("Availability result", f"unavailable via Supabase; timing={spans}", "info")
            return result
        except Exception as exc:
            await _log("Availability check failed", str(exc), "error")
            return "Unable to check availability right now — please suggest a date and I will confirm."

    @llm.function_tool
    @log_tool_execution
    async def book_appointment(
        self,
        name: str,
        phone: str,
        email: str,
        date: str,
        time: str,
        service: str,
        expected_business_name: str,
        expected_service_type: str
    ) -> str:
        """
        Book an appointment after the lead has verbally confirmed date, time, email, and service.
        Call ONLY after the lead confirms all details.
        name: lead's full name | phone: with country code | email: lead's email address | date: YYYY-MM-DD | time: HH:MM | service: type
        expected_business_name: the business context name that the AI represents
        expected_service_type: the service context that the AI is discussing
        """
        total_start = time_module.perf_counter()
        spans: dict[str, float] = {}
        try:
            self._booking_id = None
            self._booking_confirmed = False
            self._booking_error = None
            await _log(
                "Appointment booking started",
                f"date={date} time={time} service={service}",
                "info",
            )
            
            # Service validation: reject services not matching current call context (Issue 1)
            if service.strip().lower() != (self.service_type or "").strip().lower():
                err_msg = f"Service validation failed: requested service '{service}' does not match call service context '{self.service_type}'."
                await log_error("tools", "Service validation failed in book_appointment", err_msg, "error")
                return f"Error: Cannot book appointment for '{service}'. The only service being discussed is '{self.service_type}'."

            # Context validation (Issue 5)
            if (expected_business_name.strip().lower() != (self.business_name or "").strip().lower() or
                expected_service_type.strip().lower() != (self.service_type or "").strip().lower()):
                err_msg = f"Context validation failed in book_appointment: expected business '{self.business_name}' and service '{self.service_type}', but tool received '{expected_business_name}' and '{expected_service_type}'."
                await log_error("tools", "Context validation failed in book_appointment", err_msg, "error")
                return f"Error: Context mismatch. This tool can only be used for {self.business_name} and {self.service_type}."

            cached = self._get_cached_availability(date, time, expected_business_name, expected_service_type)
            if cached and not cached.get("available"):
                self._booking_error = f"cached slot unavailable via {cached.get('source')}"
                await _log("Appointment booking blocked", self._booking_error, "warning")
                return "That slot is unavailable. Please suggest another time."

            t0 = time_module.perf_counter()
            local_available = await check_slot(date, time)
            spans["supabase_slot_ms"] = round((time_module.perf_counter() - t0) * 1000, 1)
            if not local_available:
                self._booking_error = "slot unavailable"
                self._set_cached_availability(date, time, expected_business_name, expected_service_type, False, "unavailable: that slot is already booked. Please suggest another time.", "supabase")
                await _log("Appointment booking blocked", self._booking_error, "warning")
                return "That slot was just booked. Please suggest another time."

            t0 = time_module.perf_counter()
            gcal_json, gcal_id, duration = await self._load_calendar_settings()
            spans["settings_ms"] = round((time_module.perf_counter() - t0) * 1000, 1)

            manager = self._get_gcal_manager(gcal_json, gcal_id)
            if manager:
                if cached and cached.get("available"):
                    spans["availability_cache_hit"] = 1
                    spans["gcal_availability_ms"] = 0.0
                    calendar_available = True
                else:
                    g0 = time_module.perf_counter()
                    try:
                        calendar_available = await self._gcal_call(
                            GCAL_AVAILABILITY_TIMEOUT_SECONDS,
                            manager.is_slot_available,
                            date,
                            time,
                            duration,
                        )
                        spans["gcal_availability_ms"] = round((time_module.perf_counter() - g0) * 1000, 1)
                    except asyncio.TimeoutError:
                        spans["gcal_availability_ms"] = round((time_module.perf_counter() - g0) * 1000, 1)
                        self._booking_error = "Google Calendar availability check timed out"
                        await _log("Appointment booking blocked", f"{self._booking_error}; timing={spans}", "warning")
                        return "I could not confirm that calendar slot quickly enough. Please suggest another time or I can have the team follow up."
                    except Exception as exc:
                        spans["gcal_availability_ms"] = round((time_module.perf_counter() - g0) * 1000, 1)
                        self._booking_error = f"Google Calendar availability failed: {exc}"
                        await _log("Appointment booking blocked", self._booking_error, "warning")
                        return "I could not confirm that calendar slot right now. Please suggest another time or I can have the team follow up."
                    if calendar_available:
                        self._set_cached_availability(date, time, expected_business_name, expected_service_type, True, "available", "google_calendar")
                if not calendar_available:
                    self._booking_error = "Google Calendar slot unavailable"
                    self._set_cached_availability(date, time, expected_business_name, expected_service_type, False, "unavailable: that calendar slot is busy. Please suggest another time.", "google_calendar")
                    await _log("Appointment booking blocked", f"{self._booking_error}; timing={spans}", "warning")
                    return "That calendar slot is unavailable. Please suggest another time."

            try:
                t0 = time_module.perf_counter()
                booking_id = await insert_appointment(name, phone, email, date, time, service)
                spans["supabase_insert_ms"] = round((time_module.perf_counter() - t0) * 1000, 1)
            except Exception as exc:
                self._booking_error = str(exc)
                await _log("Supabase appointment insert failed", self._booking_error, "error")
                raise

            self._booking_id = booking_id
            self._booking_confirmed = True
            self._booking_error = None
            await _log("Supabase appointment insert succeeded", f"booking_id={booking_id}; timing={spans}", "info")

            # Deduct appointment platform fee (non-blocking — does not affect booking confirmation)
            try:
                from db import get_platform_pricing, deduct_wallet_for_event, get_current_tenant_id, get_current_user_email
                t0 = time_module.perf_counter()
                appt_pricing = await get_platform_pricing()
                spans["wallet_pricing_ms"] = round((time_module.perf_counter() - t0) * 1000, 1)
                tenant_id = get_current_tenant_id()
                if appt_pricing["price_per_appointment"] > 0 and tenant_id:
                    asyncio.create_task(
                        deduct_wallet_for_event(
                            tenant_id, "appointment_booked",
                            appt_pricing["price_per_appointment"], get_current_user_email()
                        )
                    )
            except Exception as billing_exc:
                await _log("Appointment billing deduction failed (non-fatal)", str(billing_exc), "warning")

            
            # Send confirmation email if tool is enabled
            t0 = time_module.perf_counter()
            enabled = await self._get_enabled_tools_cached()
            spans["email_tool_check_ms"] = round((time_module.perf_counter() - t0) * 1000, 1)
            if enabled and "send_email" in enabled:
                subject = f"Appointment Confirmed: {service} on {date} at {time}"
                body = (
                    f"Hello {name},\n\n"
                    f"Your appointment for {service} has been successfully scheduled.\n\n"
                    f"Details:\n"
                    f"- Date: {date}\n"
                    f"- Time: {time}\n"
                    f"- Booking ID: {booking_id}\n\n"
                    f"If you need to reschedule or cancel, please contact us.\n\n"
                    f"Best regards,\n"
                    f"AI Assistant"
                )
                from email_manager import send_email_async
                asyncio.create_task(send_email_async(email, subject, body))

            # Force end_call execution (Issue 3)
            async def force_end_call_safety():
                await asyncio.sleep(15.0)  # Wait for confirmation and goodbye to be spoken
                if not self._end_call_called:
                    await _log("Forcing end_call safety fallback", "Booking succeeded but end_call was not called", "warning")
                    for attempt in range(3):
                        try:
                            await self.end_call(outcome="booked", reason="forced fallback after successful booking")
                            break
                        except Exception as exc:
                            await _log(f"Forced end_call retry {attempt+1}/3 failed", str(exc), "error")
                            await asyncio.sleep(2.0)
            asyncio.create_task(force_end_call_safety())

            if manager:
                try:
                    await _log("Google Calendar sync started", f"booking_id={booking_id} calendar_id={gcal_id}", "info")
                    t0 = time_module.perf_counter()
                    event_id, event_link = await self._gcal_call(
                        GCAL_EVENT_TIMEOUT_SECONDS,
                        manager.book_event,
                        name,
                        phone,
                        date,
                        time,
                        service,
                        duration,
                    )
                    spans["gcal_event_ms"] = round((time_module.perf_counter() - t0) * 1000, 1)
                    if event_id or event_link:
                        t0 = time_module.perf_counter()
                        await update_appointment_gcal(booking_id, event_id or "", event_link or "")
                        spans["supabase_gcal_update_ms"] = round((time_module.perf_counter() - t0) * 1000, 1)
                    await _log(
                        "Google Calendar sync succeeded",
                        f"booking_id={booking_id} event_id={event_id or ''}; timing={spans}",
                        "info",
                    )
                except asyncio.TimeoutError:
                    self._booking_error = "Google Calendar sync timed out"
                    spans["total_ms"] = round((time_module.perf_counter() - total_start) * 1000, 1)
                    await _log(
                        "Google Calendar sync timed out after local appointment insert",
                        f"booking_id={booking_id}; timing={spans}",
                        "warning",
                    )
                    return (
                        f"Confirmed! Booking ID: {booking_id}. See you on {date} at {time} "
                        f"for {service}. Calendar sync is pending."
                    )
                except Exception as exc:
                    self._booking_error = f"Google Calendar sync failed: {exc}"
                    spans["total_ms"] = round((time_module.perf_counter() - total_start) * 1000, 1)
                    await _log(
                        "Google Calendar sync failed after local appointment insert",
                        f"booking_id={booking_id} error={exc}; timing={spans}",
                        "error",
                    )
                    return (
                        f"Confirmed! Booking ID: {booking_id}. See you on {date} at {time} "
                        f"for {service}. Calendar sync is pending."
                    )
            else:
                await _log("Google Calendar sync skipped", "Google Calendar is not configured; Supabase booking only", "info")
            spans["total_ms"] = round((time_module.perf_counter() - total_start) * 1000, 1)
            await _log("Appointment booking completed", f"booking_id={booking_id}; timing={spans}", "info")
            return f"Confirmed! Booking ID: {booking_id}. See you on {date} at {time} for {service}."
        except Exception as exc:
            self._booking_confirmed = False
            await _log("Appointment booking failed", str(exc), "error")
            return "Technical issue saving the booking. Our team will confirm shortly."

    @llm.function_tool
    @log_tool_execution
    async def end_call(self, outcome: str, reason: str = "") -> str:
        """
        End the call and log the outcome. ALWAYS call this before the call ends.
        outcome: 'booked' | 'not_interested' | 'wrong_number' | 'voicemail' | 'no_answer' | 'callback_requested' | 'appointment_failed'
        reason: brief description
        """
        self._end_call_called = True
        duration = int(time_module.time() - self._call_start_time)
        await _log(
            "End call invoked",
            (
                f"outcome={outcome} reason={reason} "
                f"booking_confirmed={self._booking_confirmed} booking_id={self._booking_id or ''}"
            ),
            "info",
        )
        notes = None
        if outcome == "booked" and not self._booking_confirmed:
            guard_note = "Booked outcome blocked: no appointment booking tool success."
            await _log(
                "Booked outcome blocked because no successful book_appointment",
                self._booking_error or guard_note,
                "warning",
            )
            outcome = "appointment_failed"
            reason = f"{reason}; {guard_note}" if reason else guard_note
            notes = self._booking_error or guard_note
        elif outcome == "booked" and self._booking_id:
            notes = f"Booking ID: {self._booking_id}"
            reason = f"{reason}; booking_id={self._booking_id}" if reason else f"booking_id={self._booking_id}"
        
        # Retry loop for call logging database persistence (Issue 3)
        for attempt in range(3):
            try:
                await log_call(
                    phone_number=self.phone_number or "unknown",
                    lead_name=self.lead_name, outcome=outcome, reason=reason,
                    duration_seconds=duration, recording_url=self.recording_url, notes=notes,
                    direction=self.direction, call_session_id=self.call_session_id,
                )
                self._final_log_written = True
                break
            except Exception as exc:
                logger.error(f"Failed to log call (attempt {attempt + 1}/3): {exc}")
                if attempt < 2:
                    await asyncio.sleep(1.5)
                else:
                    logger.error("Call logging failed completely after 3 attempts.")

        try:
            # Schedule delayed room deletion (4 seconds)
            asyncio.create_task(terminate_call_room(self.ctx.room.name, delay=4.0))
            # Schedule graceful job shutdown (2 seconds)
            async def delayed_shutdown():
                await asyncio.sleep(2.0)
                try:
                    await self.ctx.shutdown()
                except Exception:
                    pass
            asyncio.create_task(delayed_shutdown())
        except Exception as exc:
            logger.error("Failed during end_call shutdown scheduling: %s", exc)
        return "Call ended."

    async def log_fallback_call_end(self, outcome: str, reason: str, detail: str = "") -> bool:
        """Persist a final call log if the model/session never completed end_call."""
        if self._final_log_written:
            await _log(
                "Fallback call log skipped",
                f"reason=final log already written; requested_outcome={outcome}; detail={detail}",
                "info",
            )
            return False
        if self._fallback_log_attempted:
            await _log(
                "Fallback call log skipped",
                f"reason=fallback already attempted; requested_outcome={outcome}; detail={detail}",
                "warning",
            )
            return False

        self._fallback_log_attempted = True
        duration = int(time_module.time() - self._call_start_time)
        notes = None
        
        # Correct fallback outcome to booked if booking succeeded (Issue 2)
        if self._booking_confirmed:
            outcome = "booked"
            reason = f"{reason} (auto-corrected to booked because booking succeeded)"
            if self._booking_id:
                notes = f"Booking ID: {self._booking_id}"

        try:
            await log_call(
                phone_number=self.phone_number or "unknown",
                lead_name=self.lead_name,
                outcome=outcome,
                reason=reason,
                duration_seconds=duration,
                recording_url=self.recording_url,
                notes=notes,
                direction=self.direction,
                call_session_id=self.call_session_id,
            )
            self._final_log_written = True
            await _log(
                "Fallback call log created",
                (
                    f"outcome={outcome}; reason={reason}; duration_seconds={duration}; "
                    f"end_call_called={self._end_call_called}; booking_confirmed={self._booking_confirmed}; "
                    f"booking_id={self._booking_id or ''}; detail={detail}"
                ),
                "warning",
            )
            return True
        except Exception as exc:
            await _log(
                "Fallback call log failed",
                f"outcome={outcome}; reason={reason}; error={exc}",
                "error",
            )
            return False

    @llm.function_tool
    @log_tool_execution
    async def transfer_to_human(self, reason: str) -> str:
        """
        Transfer the call to a human agent via SIP REFER.
        Call when lead requests a human, is angry, or has a complex issue.
        reason: why you're transferring
        """
        destination = await get_setting("DEFAULT_TRANSFER_NUMBER", "")
        if not destination:
            await log_error(
                "transfer",
                "Transfer unavailable: no destination configured",
                f"reason={reason}; room={self.ctx.room.name}; phone={self.phone_number or ''}",
                "warning",
            )
            return "Transfer unavailable: no fallback number configured."
        if "@" not in destination:
            clean = destination.replace("tel:", "").replace("sip:", "")
            destination = f"sip:{clean}@{self._sip_domain}" if self._sip_domain else f"tel:{clean}"
        elif not destination.startswith("sip:"):
            destination = f"sip:{destination}"
        
        participant_identity = None
        # Prioritize remote participants whose identity starts with "sip_"
        for p in self.ctx.room.remote_participants.values():
            if p.identity and p.identity.startswith("sip_"):
                participant_identity = p.identity
                break
        if not participant_identity and self.phone_number:
            target = f"sip_{self.phone_number}"
            for p in self.ctx.room.remote_participants.values():
                if p.identity == target:
                    participant_identity = p.identity
                    break
        if not participant_identity:
            for p in self.ctx.room.remote_participants.values():
                participant_identity = p.identity
                break
                
        if not participant_identity:
            await log_error(
                "transfer",
                "Transfer failed: no participant identity",
                f"reason={reason}; room={self.ctx.room.name}; destination={destination}",
                "warning",
            )
            return "Transfer failed: could not identify caller."
        try:
            await log_error(
                "transfer",
                "Transfer attempt",
                f"reason={reason}; room={self.ctx.room.name}; destination={destination}; participant={participant_identity}",
                "info",
            )
            await self.ctx.api.sip.transfer_sip_participant(
                api.TransferSIPParticipantRequest(
                    room_name=self.ctx.room.name,
                    participant_identity=participant_identity,
                    transfer_to=destination, play_dialtone=False,
                )
            )
            await log_error(
                "transfer",
                "Transfer requested",
                f"room={self.ctx.room.name}; destination={destination}; participant={participant_identity}",
                "info",
            )
            return "Transferring you to a human agent now. Please hold."
        except Exception as exc:
            tb = traceback.format_exc()
            await log_error(
                "transfer",
                f"Transfer failed: {exc}",
                f"room={self.ctx.room.name}; destination={destination}; participant={participant_identity}\nTraceback:\n{tb}",
                "error",
            )
            return "Transfer failed. Please call us back directly."

    @llm.function_tool
    @log_tool_execution
    async def send_sms_confirmation(self, phone: str, message: str) -> str:
        """
        Send SMS confirmation after a successful booking. Skips silently if Twilio not configured.
        phone: lead's phone | message: text to send
        """
        if not phone or not message:
            return "SMS skipped: missing phone or message."
        sid = os.getenv("TWILIO_ACCOUNT_SID", "")
        token = os.getenv("TWILIO_AUTH_TOKEN", "")
        from_num = os.getenv("TWILIO_FROM_NUMBER", "")
        if not (sid and token and from_num):
            return "SMS skipped: Twilio not configured."
        try:
            from twilio.rest import Client
            loop = asyncio.get_event_loop()
            client = Client(sid, token)
            await loop.run_in_executor(None, lambda: client.messages.create(body=message, from_=from_num, to=phone))
            return f"SMS sent to {phone}."
        except Exception:
            return "SMS delivery failed, but booking is confirmed."

    @llm.function_tool
    @log_tool_execution
    async def lookup_contact(self, phone: str) -> str:
        """
        Look up a contact's full history after the opening identity greeting.
        Do not delay the first greeting or first reply for this lookup.
        phone: the lead's phone number with country code
        Returns call history, appointments, and remembered details.
        """
        if not phone:
            return "Phone number missing; ask the caller to confirm their number."
        try:
            logger.debug("TIMING LOG: lookup_contact started")
            if DEBUG_TOOL_LOGS:
                asyncio.create_task(log_error("tools", "TIMING LOG: lookup_contact started", "", "info"))
            
            calls, appointments, memories = await asyncio.gather(
                get_calls_by_phone(phone),
                get_appointments_by_phone(phone),
                get_contact_memory(phone)
            )
            
            logger.debug("TIMING LOG: lookup_contact finished")
            if DEBUG_TOOL_LOGS:
                asyncio.create_task(log_error("tools", "TIMING LOG: lookup_contact finished", "", "info"))
            
            if not calls and not appointments and not memories:
                return f"No history for {phone}. First-time contact."
            lines = [f"Contact history for {phone}:"]
            if memories:
                lines.append(f"\nREMEMBERED ({len(memories)} notes):")
                for m in memories[:10]:
                    lines.append(f"  • {m['insight']}")
            if calls:
                lines.append(f"\nCALL HISTORY ({len(calls)} calls):")
                for c in calls[:5]:
                    ts = (c.get("timestamp") or "")[:16]
                    lines.append(f"  • {ts} — {c.get('outcome','?')}: {c.get('reason','')}")
            if appointments:
                lines.append(f"\nAPPOINTMENTS ({len(appointments)}):")
                for a in appointments[:3]:
                    lines.append(f"  • {a.get('date')} {a.get('time')} — {a.get('service')} [{a.get('status')}]")
            return "\n".join(lines)
        except Exception:
            logger.debug("TIMING LOG: lookup_contact finished (with error)")
            if DEBUG_TOOL_LOGS:
                asyncio.create_task(log_error("tools", "TIMING LOG: lookup_contact finished (with error)", "", "info"))
            return "Unable to retrieve contact history."

    @llm.function_tool
    @log_tool_execution
    async def remember_details(self, insight: str) -> str:
        """
        Store a key insight about this lead for future calls.
        Use whenever you learn something useful: preferences, objections, timing, family info.
        Examples: "Prefers morning calls", "Has 2 kids, interested in family plan", "Callback in 2 weeks"
        insight: the detail to remember
        """
        if not self.phone_number:
            return "Cannot remember — no phone number for this call."
        try:
            await add_contact_memory(self.phone_number, insight)
            memories = await get_contact_memory(self.phone_number)
            if len(memories) >= 5:
                asyncio.create_task(self._compress_memories())
            return f"Remembered: {insight}"
        except Exception:
            return "Could not save detail."

    async def _compress_memories(self) -> None:
        try:
            memories = await get_contact_memory(self.phone_number)
            if len(memories) < 5:
                return
            import google.generativeai as genai
            api_key = os.getenv("GOOGLE_API_KEY", "")
            if not api_key:
                return
            genai.configure(api_key=api_key)
            model = genai.GenerativeModel("gemini-2.0-flash")
            bullet_list = "\n".join(f"- {m['insight']}" for m in memories)
            prompt = f"Compress these notes about a sales contact into 3-5 concise bullets. Keep all key facts.\n\n{bullet_list}"
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(None, lambda: model.generate_content(prompt))
            if response.text.strip():
                await compress_contact_memory(self.phone_number, response.text.strip())
        except Exception as exc:
            logger.warning("Memory compression failed: %s", exc)

    @llm.function_tool
    @log_tool_execution
    async def book_calcom(self, name: str, email: str, date: str, start_time: str, notes: str = "") -> str:
        """
        Book in Cal.com calendar after book_appointment succeeds.
        name: full name | email: lead's email | date: YYYY-MM-DD | start_time: HH:MM | notes: optional
        """
        api_key = os.getenv("CALCOM_API_KEY", "")
        event_type_id = os.getenv("CALCOM_EVENT_TYPE_ID", "")
        timezone = os.getenv("CALCOM_TIMEZONE", "Asia/Kolkata")
        if not api_key or not event_type_id:
            return "Cal.com not configured — skipping. Add CALCOM_API_KEY and CALCOM_EVENT_TYPE_ID."
        try:
            from datetime import datetime as _dt
            start_dt = _dt.strptime(f"{date} {start_time}", "%Y-%m-%d %H:%M")
            start_iso = start_dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")
            import httpx
            async with httpx.AsyncClient(timeout=15) as client:
                resp = await client.post(
                    "https://api.cal.com/v1/bookings",
                    headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
                    json={"eventTypeId": int(event_type_id), "start": start_iso, "timeZone": timezone,
                          "responses": {"name": name, "email": email, "notes": notes},
                          "metadata": {"source": "OutboundAI"}, "language": "en"},
                )
            data = resp.json()
            if resp.status_code not in (200, 201):
                raise ValueError(data.get("message") or str(data))
            uid = data.get("uid", "")
            return f"Cal.com booked. UID: {uid}"
        except Exception as exc:
            return f"Cal.com booking failed: {exc}"

    @llm.function_tool
    @log_tool_execution
    async def cancel_calcom(self, booking_uid: str, reason: str = "") -> str:
        """
        Cancel a Cal.com booking by UID.
        booking_uid: from book_calcom | reason: optional
        """
        api_key = os.getenv("CALCOM_API_KEY", "")
        if not api_key:
            return "Cal.com not configured."
        try:
            import httpx
            async with httpx.AsyncClient(timeout=15) as client:
                resp = await client.delete(
                    f"https://api.cal.com/v1/bookings/{booking_uid}",
                    headers={"Authorization": f"Bearer {api_key}"},
                    params={"reason": reason} if reason else {},
                )
            if resp.status_code not in (200, 204):
                raise ValueError(f"HTTP {resp.status_code}")
            return f"Cancelled Cal.com booking {booking_uid}."
        except Exception as exc:
            return f"Cancellation failed: {exc}"

    @llm.function_tool
    @log_tool_execution
    async def send_email(self, recipient_email: str, subject: str, body: str) -> str:
        """
        Send a white-labeled email to a recipient.
        recipient_email: lead's email address | subject: email subject | body: email body text
        """
        enabled = await get_enabled_tools()
        if not enabled or "send_email" not in enabled:
            return "Email skipped: send_email tool is currently disabled in system settings."
        from email_manager import send_email_async
        success = await send_email_async(recipient_email, subject, body)
        if success:
            return f"Email sent successfully to {recipient_email}."
        else:
            return "Email failed to send. Please check SMTP configuration."
