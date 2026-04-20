from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import requests


class RamClientError(RuntimeError):
    pass


@dataclass(frozen=True)
class RamEvent:
    event_date: datetime
    event_name: str | None
    speed_kph: float | None
    speed_limit_kph: float | None
    odometer: float | None
    latitude: float | None
    longitude: float | None
    post_code: str | None
    raw: dict[str, Any]


def _parse_dt(s: str) -> datetime:
    # RAM typically returns ISO timestamps; normalize to timezone-aware UTC.
    # Accept both Z and offset forms.
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception as e:
        raise RamClientError(f"Unparseable event_date: {s!r}") from e
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _as_float(v: Any) -> float | None:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


class RamClient:
    def __init__(
        self,
        *,
        api_base_url: str,
        token_url: str,
        oauth_basic_user: str,
        oauth_basic_password: str,
        oauth_username: str,
        oauth_password: str,
        timeout_seconds: float = 45.0,
        max_retries: int = 5,
    ):
        self._api_base_url = api_base_url.rstrip("/")
        self._token_url = token_url
        self._oauth_basic_user = oauth_basic_user
        self._oauth_basic_password = oauth_basic_password
        self._oauth_username = oauth_username
        self._oauth_password = oauth_password
        self._timeout = timeout_seconds
        self._max_retries = max_retries

        self._cached_token: str | None = None
        self._cached_token_expiry_epoch: float | None = None
        self._cooldown_until_epoch: float | None = None

    def _get_token(self) -> str:
        now = time.time()
        if self._cached_token and self._cached_token_expiry_epoch and now < self._cached_token_expiry_epoch - 30:
            return self._cached_token

        try:
            resp = requests.post(
                self._token_url,
                auth=(self._oauth_basic_user, self._oauth_basic_password),
                data={
                    "grant_type": "password",
                    "username": self._oauth_username,
                    "password": self._oauth_password,
                },
                timeout=self._timeout,
            )
        except requests.RequestException as e:
            raise RamClientError(f"Token request failed: {e}") from e

        if resp.status_code >= 400:
            raise RamClientError(f"Token request failed: HTTP {resp.status_code}: {resp.text[:500]}")

        try:
            data = resp.json()
        except ValueError as e:
            raise RamClientError("Token response was not JSON.") from e

        token = data.get("access_token")
        if not token or not isinstance(token, str):
            raise RamClientError("Token response missing access_token.")

        expires_in = data.get("expires_in")
        expiry = None
        if isinstance(expires_in, (int, float)):
            expiry = now + float(expires_in)
        else:
            # If the server doesn't provide expires_in, cache briefly.
            expiry = now + 60.0

        self._cached_token = token
        self._cached_token_expiry_epoch = expiry
        return token

    def fetch_history(self, *, vehicle_id: int, date_from: str, date_to: str) -> list[RamEvent]:
        url = f"{self._api_base_url}/api/v1/history/{vehicle_id}/{date_from}T00:00:00/{date_to}T00:00:00"
        last_err: str | None = None

        # Retry transient failures and rate limits.
        for attempt in range(1, max(1, self._max_retries) + 1):
            # Global cooldown (shared within this client instance) when we hit a rate limit.
            if self._cooldown_until_epoch:
                now = time.time()
                if now < self._cooldown_until_epoch:
                    time.sleep(self._cooldown_until_epoch - now)

            token = self._get_token()
            try:
                resp = requests.get(
                    url,
                    headers={"Authorization": f"Bearer {token}"},
                    timeout=self._timeout,
                )
            except requests.RequestException as e:
                last_err = f"History request failed: {e}"
                sleep_s = min(2**attempt, 20)
                time.sleep(sleep_s)
                continue

            if resp.status_code == 429:
                retry_after = resp.headers.get("Retry-After")
                try:
                    wait_s = float(retry_after) if retry_after is not None else min(2**attempt, 30)
                except ValueError:
                    wait_s = min(2**attempt, 30)
                last_err = f"History rate-limited (HTTP 429). Waiting {wait_s:.0f}s then retrying."
                self._cooldown_until_epoch = time.time() + max(0.0, wait_s)
                time.sleep(wait_s)
                continue

            if 500 <= resp.status_code <= 599:
                last_err = f"History server error: HTTP {resp.status_code}: {resp.text[:300]}"
                time.sleep(min(2**attempt, 20))
                continue

            if resp.status_code >= 400:
                # Non-retriable (auth/permission/bad request).
                raise RamClientError(
                    f"History request failed: HTTP {resp.status_code}: {resp.text[:500]}"
                )

            break
        else:
            raise RamClientError(last_err or "History request failed after retries.")

        try:
            data = resp.json()
        except ValueError as e:
            raise RamClientError("History response was not JSON.") from e

        history = data.get("history") or []
        if not isinstance(history, list):
            raise RamClientError("History response missing 'history' list.")

        events: list[RamEvent] = []
        for item in history:
            if not isinstance(item, dict):
                continue
            ev_date = item.get("event_date")
            if not isinstance(ev_date, str):
                continue
            dt = _parse_dt(ev_date)

            events.append(
                RamEvent(
                    event_date=dt,
                    event_name=item.get("event_name") if isinstance(item.get("event_name"), str) else None,
                    speed_kph=_as_float(item.get("speedKph")),
                    speed_limit_kph=_as_float(item.get("speedLimitKph")),
                    odometer=_as_float(item.get("odometer")),
                    latitude=_as_float(item.get("latitude")),
                    longitude=_as_float(item.get("longitude")),
                    post_code=item.get("postCode") if isinstance(item.get("postCode"), str) else None,
                    raw=item,
                )
            )

        events.sort(key=lambda e: e.event_date)
        return events

