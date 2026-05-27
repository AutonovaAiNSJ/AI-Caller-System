import os
import json
import logging
from datetime import datetime, timedelta
from typing import Optional, Tuple
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from db import get_setting

logger = logging.getLogger("google-calendar")


class GoogleCalendarManager:
    """Manages Google Calendar interactions using a Service Account."""

    def __init__(self, service_account_json: str, calendar_id: str = "primary"):
        self.service_account_json = service_account_json
        self.calendar_id = calendar_id or "primary"
        self._service = None

    def get_service(self):
        """Instantiate Google Calendar service v3 dynamically."""
        if self._service is None:
            try:
                # Load credentials from service account JSON string
                info = json.loads(self.service_account_json)
                scopes = ["https://www.googleapis.com/auth/calendar"]
                credentials = service_account.Credentials.from_service_account_info(
                    info, scopes=scopes
                )
                self._service = build("calendar", "v3", credentials=credentials)
            except Exception as exc:
                logger.error("Failed to initialize Google Calendar API service: %s", exc)
                raise
        return self._service

    def is_slot_available(self, date_str: str, time_str: str, duration_mins: int = 30) -> bool:
        """
        Check if a slot is available on the calendar (no overlapping events).
        date_str format: YYYY-MM-DD
        time_str format: HH:MM
        """
        try:
            service = self.get_service()
            
            # Formulate naive datetimes for proposed range
            start_dt = datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M")
            end_dt = start_dt + timedelta(minutes=duration_mins)

            # Query list of events for the target day
            # Format boundaries in ISO-8601 strings
            time_min = f"{date_str}T00:00:00Z"
            time_max = f"{date_str}T23:59:59Z"

            logger.info("Listing events for calendar %s on %s to verify availability", self.calendar_id, date_str)
            
            events_result = service.events().list(
                calendarId=self.calendar_id,
                timeMin=time_min,
                timeMax=time_max,
                singleEvents=True,
                orderBy="startTime"
            ).execute()

            events = events_result.get("items", [])

            # Check overlaps using clock-face naive timezone comparisons
            for event in events:
                start_raw = event.get("start", {})
                end_raw = event.get("end", {})

                # Check for all-day event
                if "date" in start_raw:
                    ev_date_str = start_raw["date"]
                    if ev_date_str == date_str:
                        logger.info("Calendar is busy: All-day event '%s' found", event.get("summary"))
                        return False
                    continue

                ev_start_str = start_raw.get("dateTime")
                ev_end_str = end_raw.get("dateTime")
                if not ev_start_str or not ev_end_str:
                    continue

                # Strip timezone offsets to compare identical wall-clock face time values
                # e.g., "2026-05-27T16:30:00+05:30"[:19] -> "2026-05-27T16:30:00"
                ev_start_naive = datetime.strptime(ev_start_str[:19], "%Y-%m-%dT%H:%M:%S")
                ev_end_naive = datetime.strptime(ev_end_str[:19], "%Y-%m-%dT%H:%M:%S")

                # Intervals overlap if: max(Start1, Start2) < min(End1, End2)
                if max(start_dt, ev_start_naive) < min(end_dt, ev_end_naive):
                    logger.info(
                        "Overlap detected: Proposed [%s, %s] overlaps existing '%s' [%s, %s]",
                        start_dt, end_dt, event.get("summary"), ev_start_naive, ev_end_naive
                    )
                    return False

            logger.info("Slot %s at %s is available on Google Calendar", date_str, time_str)
            return True
        except Exception as exc:
            logger.error("Failed to check Google Calendar availability: %s", exc)
            # Default to False in case of API/auth error to prevent double booking
            return False

    def get_next_available(self, date_str: str, time_str: str, duration_mins: int = 30) -> str:
        """
        Find the next free slot in business hours (9 AM to 6 PM) over the next 7 days.
        """
        try:
            dt = datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M")
        except ValueError:
            dt = datetime.now().replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
        
        for _ in range(7 * 24):
            dt += timedelta(hours=1)
            if 9 <= dt.hour < 18:
                d = dt.strftime("%Y-%m-%d")
                t = dt.strftime("%H:%M")
                if self.is_slot_available(d, t, duration_mins):
                    return f"{d} at {t}"
        return "no open slots found in the next 7 days"

    def book_event(
        self,
        name: str,
        phone: str,
        date_str: str,
        time_str: str,
        service_name: str,
        duration_mins: int = 30
    ) -> Tuple[Optional[str], Optional[str]]:
        """
        Book an event in the Google Calendar.
        Returns a tuple: (event_id, html_link)
        """
        try:
            service = self.get_service()

            # Retrieve calendar timezone dynamically to book in local terms
            try:
                cal_meta = service.calendars().get(calendarId=self.calendar_id).execute()
                tz = cal_meta.get("timeZone", "Asia/Kolkata")
            except Exception:
                tz = "Asia/Kolkata"

            start_dt = datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M")
            end_dt = start_dt + timedelta(minutes=duration_mins)

            event_body = {
                "summary": f"{service_name.capitalize()} — {name}",
                "description": f"Lead Phone: {phone}\nBooked automatically by OutboundAI voice assistant.",
                "start": {
                    "dateTime": start_dt.strftime("%Y-%m-%dT%H:%M:%S"),
                    "timeZone": tz,
                },
                "end": {
                    "dateTime": end_dt.strftime("%Y-%m-%dT%H:%M:%S"),
                    "timeZone": tz,
                },
                "reminders": {
                    "useDefault": True,
                }
            }

            event = service.events().insert(calendarId=self.calendar_id, body=event_body).execute()
            logger.info("Google Calendar Event booked successfully: %s", event.get("htmlLink"))
            return event.get("id"), event.get("htmlLink")
        except Exception as exc:
            logger.error("Failed to create Google Calendar event: %s", exc)
            raise
