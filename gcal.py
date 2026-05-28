import json
import logging
from datetime import datetime, timedelta
from typing import Optional, Tuple

from google.oauth2 import service_account
from googleapiclient.discovery import build

logger = logging.getLogger("google-calendar")


class GoogleCalendarManager:
    """Manages Google Calendar interactions using a service account."""

    def __init__(self, service_account_json: str, calendar_id: str = "primary"):
        self.service_account_json = service_account_json
        self.calendar_id = calendar_id or "primary"
        self._service = None

    def get_service(self):
        """Instantiate the Google Calendar service lazily."""
        if self._service is None:
            try:
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
        Check whether a slot is available on the calendar.
        date_str format: YYYY-MM-DD
        time_str format: HH:MM
        """
        try:
            service = self.get_service()
            start_dt = datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M")
            end_dt = start_dt + timedelta(minutes=duration_mins)

            events_result = service.events().list(
                calendarId=self.calendar_id,
                timeMin=f"{date_str}T00:00:00Z",
                timeMax=f"{date_str}T23:59:59Z",
                singleEvents=True,
                orderBy="startTime",
            ).execute()

            for event in events_result.get("items", []):
                start_raw = event.get("start", {})
                end_raw = event.get("end", {})

                if "date" in start_raw:
                    if start_raw["date"] == date_str:
                        logger.info("Calendar is busy: all-day event '%s'", event.get("summary"))
                        return False
                    continue

                ev_start_str = start_raw.get("dateTime")
                ev_end_str = end_raw.get("dateTime")
                if not ev_start_str or not ev_end_str:
                    continue

                ev_start_naive = datetime.strptime(ev_start_str[:19], "%Y-%m-%dT%H:%M:%S")
                ev_end_naive = datetime.strptime(ev_end_str[:19], "%Y-%m-%dT%H:%M:%S")

                if max(start_dt, ev_start_naive) < min(end_dt, ev_end_naive):
                    logger.info(
                        "Calendar overlap: proposed [%s, %s] overlaps '%s' [%s, %s]",
                        start_dt,
                        end_dt,
                        event.get("summary"),
                        ev_start_naive,
                        ev_end_naive,
                    )
                    return False

            return True
        except Exception as exc:
            logger.error("Failed to check Google Calendar availability: %s", exc)
            return False

    def get_next_available(self, date_str: str, time_str: str, duration_mins: int = 30) -> str:
        """Find the next free slot in business hours over the next 7 days."""
        try:
            dt = datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M")
        except ValueError:
            dt = datetime.now().replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)

        for _ in range(7 * 24):
            dt += timedelta(hours=1)
            if 9 <= dt.hour < 18:
                date_candidate = dt.strftime("%Y-%m-%d")
                time_candidate = dt.strftime("%H:%M")
                if self.is_slot_available(date_candidate, time_candidate, duration_mins):
                    return f"{date_candidate} at {time_candidate}"
        return "no open slots found in the next 7 days"

    def book_event(
        self,
        name: str,
        phone: str,
        date_str: str,
        time_str: str,
        service_name: str,
        duration_mins: int = 30,
    ) -> Tuple[Optional[str], Optional[str]]:
        """Book an event in Google Calendar and return (event_id, html_link)."""
        try:
            service = self.get_service()
            try:
                cal_meta = service.calendars().get(calendarId=self.calendar_id).execute()
                tz = cal_meta.get("timeZone", "Asia/Kolkata")
            except Exception:
                tz = "Asia/Kolkata"

            start_dt = datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M")
            end_dt = start_dt + timedelta(minutes=duration_mins)

            event_body = {
                "summary": f"{service_name.capitalize()} - {name}",
                "description": f"Lead Phone: {phone}\nBooked automatically by OutboundAI voice assistant.",
                "start": {
                    "dateTime": start_dt.strftime("%Y-%m-%dT%H:%M:%S"),
                    "timeZone": tz,
                },
                "end": {
                    "dateTime": end_dt.strftime("%Y-%m-%dT%H:%M:%S"),
                    "timeZone": tz,
                },
                "reminders": {"useDefault": True},
            }

            event = service.events().insert(calendarId=self.calendar_id, body=event_body).execute()
            logger.info("Google Calendar event booked successfully: %s", event.get("htmlLink"))
            return event.get("id"), event.get("htmlLink")
        except Exception as exc:
            logger.error("Failed to create Google Calendar event: %s", exc)
            raise
