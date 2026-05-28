import asyncio
import logging
import os
import time
from typing import Optional

from livekit import agents, api
from livekit.agents import llm

from db import (
    check_slot, get_next_available, insert_appointment, log_call, log_error,
    get_calls_by_phone, get_appointments_by_phone,
    add_contact_memory, get_contact_memory, compress_contact_memory,
    get_setting, update_appointment_gcal,
)
from gcal import GoogleCalendarManager

logger = logging.getLogger("appointment-tools")


MANDATORY_BOOKING_TOOLS = ["check_availability", "book_appointment", "end_call"]
DEFAULT_TOOL_NAMES = [
    "check_availability", "book_appointment", "end_call",
    "transfer_to_human", "send_sms_confirmation", "lookup_contact",
    "remember_details", "book_calcom", "cancel_calcom",
]


async def _log(msg: str, detail: str = "", level: str = "info") -> None:
    try:
        await log_error("agent", msg, detail, level)
    except Exception:
        pass


class AppointmentTools(llm.ToolContext):
    """All function tools available to the appointment-booking agent."""

    def __init__(self, ctx: agents.JobContext, phone_number: Optional[str] = None, lead_name: Optional[str] = None):
        self.ctx = ctx
        self.phone_number = phone_number
        self.lead_name = lead_name
        self._call_start_time = time.time()
        self._sip_domain = os.getenv("VOBIZ_SIP_DOMAIN", "")
        self.recording_url: Optional[str] = None
        self._booking_id: Optional[str] = None
        self._booking_confirmed = False
        self._booking_error: Optional[str] = None
        self._end_call_called = False
        self._final_log_written = False
        self._fallback_log_attempted = False
        super().__init__(tools=[])

    def build_tool_list(self, enabled: list) -> list:
        """Return tool methods for the final resolved tool-name list."""
        all_methods = [
            self.check_availability, self.book_appointment, self.end_call,
            self.transfer_to_human, self.send_sms_confirmation, self.lookup_contact,
            self.remember_details, self.book_calcom, self.cancel_calcom,
        ]
        name_map = {m.__name__: m for m in all_methods}
        return [name_map[n] for n in enabled if n in name_map]

    @llm.function_tool
    async def check_availability(self, date: str, time: str) -> str:
        """
        Check whether a date/time slot is available for booking.
        Call this BEFORE attempting to book whenever the lead proposes a date/time.
        date format: YYYY-MM-DD  |  time format: HH:MM (24-hour)
        Returns 'available' or 'unavailable: next available slot is <slot>'.
        """
        try:
            await _log("Availability check started", f"date={date} time={time}", "info")
            gcal_json = await get_setting("GOOGLE_CALENDAR_SERVICE_ACCOUNT_JSON", "")
            gcal_id = await get_setting("GOOGLE_CALENDAR_ID", "primary")
            duration_raw = await get_setting("GOOGLE_CALENDAR_SLOT_DURATION", "30")
            try:
                duration = int(duration_raw or "30")
            except ValueError:
                duration = 30

            local_available = await check_slot(date, time)
            if gcal_json:
                await _log("Availability check using Google Calendar", f"calendar_id={gcal_id}", "info")
                manager = GoogleCalendarManager(gcal_json, gcal_id)
                calendar_available = manager.is_slot_available(date, time, duration)
                if local_available and calendar_available:
                    await _log("Availability result", f"available via Google Calendar date={date} time={time}", "info")
                    return "available"
                next_slot = manager.get_next_available(date, time, duration)
                await _log(
                    "Availability result",
                    f"unavailable via Google Calendar date={date} time={time}; next={next_slot}",
                    "info",
                )
                return f"unavailable: next available slot is {next_slot}"

            await _log("Availability check using Supabase fallback", f"date={date} time={time}", "info")
            if local_available:
                await _log("Availability result", f"available via Supabase date={date} time={time}", "info")
                return "available"
            next_slot = await get_next_available(date, time)
            await _log(
                "Availability result",
                f"unavailable via Supabase date={date} time={time}; next={next_slot}",
                "info",
            )
            return f"unavailable: next available slot is {next_slot}"
        except Exception as exc:
            await _log("Availability check failed", str(exc), "error")
            return "Unable to check availability right now — please suggest a date and I will confirm."

    @llm.function_tool
    async def book_appointment(self, name: str, phone: str, date: str, time: str, service: str) -> str:
        """
        Book an appointment after the lead has verbally confirmed date, time, and service.
        Call ONLY after the lead confirms all details.
        name: lead's full name | phone: with country code | date: YYYY-MM-DD | time: HH:MM | service: type
        """
        try:
            self._booking_id = None
            self._booking_confirmed = False
            self._booking_error = None
            await _log(
                "Appointment booking started",
                f"name={name} phone={phone} date={date} time={time} service={service}",
                "info",
            )
            if not await check_slot(date, time):
                next_slot = await get_next_available(date, time)
                self._booking_error = f"slot unavailable; next={next_slot}"
                await _log("Appointment booking blocked", self._booking_error, "warning")
                return f"That slot was just booked. The next available slot is {next_slot}."

            gcal_json = await get_setting("GOOGLE_CALENDAR_SERVICE_ACCOUNT_JSON", "")
            gcal_id = await get_setting("GOOGLE_CALENDAR_ID", "primary")
            duration_raw = await get_setting("GOOGLE_CALENDAR_SLOT_DURATION", "30")
            try:
                duration = int(duration_raw or "30")
            except ValueError:
                duration = 30

            manager = GoogleCalendarManager(gcal_json, gcal_id) if gcal_json else None
            if manager and not manager.is_slot_available(date, time, duration):
                next_slot = manager.get_next_available(date, time, duration)
                self._booking_error = f"Google Calendar slot unavailable; next={next_slot}"
                await _log("Appointment booking blocked", self._booking_error, "warning")
                return f"That slot is unavailable. The next available slot is {next_slot}."

            try:
                booking_id = await insert_appointment(name, phone, date, time, service)
            except Exception as exc:
                self._booking_error = str(exc)
                await _log("Supabase appointment insert failed", self._booking_error, "error")
                raise

            self._booking_id = booking_id
            self._booking_confirmed = True
            self._booking_error = None
            await _log("Supabase appointment insert succeeded", f"booking_id={booking_id}", "info")
            if manager:
                try:
                    await _log("Google Calendar sync started", f"booking_id={booking_id} calendar_id={gcal_id}", "info")
                    event_id, event_link = manager.book_event(name, phone, date, time, service, duration)
                    if event_id or event_link:
                        await update_appointment_gcal(booking_id, event_id or "", event_link or "")
                    await _log(
                        "Google Calendar sync succeeded",
                        f"booking_id={booking_id} event_id={event_id or ''} event_link={event_link or ''}",
                        "info",
                    )
                except Exception as exc:
                    self._booking_error = f"Google Calendar sync failed: {exc}"
                    await _log(
                        "Google Calendar sync failed after local appointment insert",
                        f"booking_id={booking_id} error={exc}",
                        "error",
                    )
                    return (
                        f"Confirmed! Booking ID: {booking_id}. See you on {date} at {time} "
                        f"for {service}. Calendar sync is pending."
                    )
            else:
                await _log("Google Calendar sync skipped", "Google Calendar is not configured; Supabase booking only", "info")
            return f"Confirmed! Booking ID: {booking_id}. See you on {date} at {time} for {service}."
        except Exception as exc:
            self._booking_confirmed = False
            await _log("Appointment booking failed", str(exc), "error")
            return "Technical issue saving the booking. Our team will confirm shortly."

    @llm.function_tool
    async def end_call(self, outcome: str, reason: str = "") -> str:
        """
        End the call and log the outcome. ALWAYS call this before the call ends.
        outcome: 'booked' | 'not_interested' | 'wrong_number' | 'voicemail' | 'no_answer' | 'callback_requested' | 'appointment_failed'
        reason: brief description
        """
        self._end_call_called = True
        duration = int(time.time() - self._call_start_time)
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
        try:
            await log_call(
                phone_number=self.phone_number or "unknown",
                lead_name=self.lead_name, outcome=outcome, reason=reason,
                duration_seconds=duration, recording_url=self.recording_url, notes=notes,
            )
            self._final_log_written = True
        except Exception as exc:
            logger.error("Failed to log call: %s", exc)
        try:
            await asyncio.sleep(1)
            await self.ctx.shutdown()   
        except Exception:
            pass
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
        duration = int(time.time() - self._call_start_time)
        notes = None
        if self._booking_confirmed and self._booking_id:
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
    async def transfer_to_human(self, reason: str) -> str:
        """
        Transfer the call to a human agent via SIP REFER.
        Call when lead requests a human, is angry, or has a complex issue.
        reason: why you're transferring
        """
        destination = os.getenv("DEFAULT_TRANSFER_NUMBER", "")
        if not destination:
            return "Transfer unavailable: no fallback number configured."
        if "@" not in destination:
            clean = destination.replace("tel:", "").replace("sip:", "")
            destination = f"sip:{clean}@{self._sip_domain}" if self._sip_domain else f"tel:{clean}"
        elif not destination.startswith("sip:"):
            destination = f"sip:{destination}"
        participant_identity = f"sip_{self.phone_number}" if self.phone_number else None
        if not participant_identity:
            for p in self.ctx.room.remote_participants.values():
                participant_identity = p.identity
                break
        if not participant_identity:
            return "Transfer failed: could not identify caller."
        try:
            await self.ctx.api.sip.transfer_sip_participant(
                api.TransferSIPParticipantRequest(
                    room_name=self.ctx.room.name,
                    participant_identity=participant_identity,
                    transfer_to=destination, play_dialtone=False,
                )
            )
            return "Transferring you to a human agent now. Please hold."
        except Exception:
            return "Transfer failed. Please call us back directly."

    @llm.function_tool
    async def send_sms_confirmation(self, phone: str, message: str) -> str:
        """
        Send SMS confirmation after a successful booking. Skips silently if Twilio not configured.
        phone: lead's phone | message: text to send
        """
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
    async def lookup_contact(self, phone: str) -> str:
        """
        Look up a contact's full history. Call at the START of every call before engaging.
        phone: the lead's phone number with country code
        Returns call history, appointments, and remembered details.
        """
        try:
            calls = await get_calls_by_phone(phone)
            appointments = await get_appointments_by_phone(phone)
            memories = await get_contact_memory(phone)
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
            return "Unable to retrieve contact history."

    @llm.function_tool
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
