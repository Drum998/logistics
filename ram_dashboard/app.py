from __future__ import annotations

import time as time_module
from datetime import date, datetime, time, timedelta, timezone

from flask import Flask, jsonify, render_template, request
from werkzeug.exceptions import HTTPException

from .config import ConfigError, load_settings
from .metrics import compute_metrics_for_shift, extract_overspeed_events
from .ram_client import RamClient, RamClientError


app = Flask(__name__)

@app.errorhandler(Exception)
def handle_exception(e: Exception):
    # Ensure API callers always get JSON, not an HTML error page.
    if request.path.startswith("/api/"):
        if isinstance(e, HTTPException):
            return jsonify({"error": e.name, "details": e.description}), e.code
        return jsonify({"error": "Internal Server Error", "details": str(e)}), 500
    raise e


def _parse_date_yyyy_mm_dd(s: str) -> date:
    try:
        return date.fromisoformat(s)
    except ValueError as e:
        raise ValueError("date must be YYYY-MM-DD") from e


def _shift_window_utc(d: date) -> tuple[datetime, datetime]:
    shift_start = datetime.combine(d, time(18, 0, 0), tzinfo=timezone.utc)
    shift_end = datetime.combine(d + timedelta(days=1), time(6, 0, 0), tzinfo=timezone.utc)
    return shift_start, shift_end


@app.get("/")
def index():
    # Load without requiring secrets so the page can load and show a nice error
    # when the user tries to query metrics.
    try:
        settings = load_settings(require_secrets=False)
        van_count = len(settings.van_map)
    except Exception:
        van_count = 0
    return render_template("index.html", van_count=van_count)


@app.get("/api/vans")
def api_vans():
    settings = load_settings(require_secrets=False)
    vans = [{"vrn": vrn, "vehicleId": vid} for vrn, vid in sorted(settings.van_map.items())]
    return jsonify({"vans": vans})


@app.post("/api/metrics")
def api_metrics():
    try:
        settings = load_settings(require_secrets=True)
    except ConfigError as e:
        return jsonify({"error": str(e)}), 400

    payload = request.get_json(silent=True) or {}
    date_str = payload.get("date")

    if not isinstance(date_str, str):
        return jsonify({"error": "Missing or invalid 'date' (YYYY-MM-DD)."}), 400

    try:
        d = _parse_date_yyyy_mm_dd(date_str)
    except ValueError as e:
        return jsonify({"error": str(e)}), 400

    shift_start, shift_end = _shift_window_utc(d)

    # Query a slightly wider range (by date) then filter precisely to shift window.
    query_from = (d - timedelta(days=1)).isoformat()
    query_to = (d + timedelta(days=2)).isoformat()

    client = RamClient(
        api_base_url=settings.api_base_url,
        token_url=settings.token_url,
        oauth_basic_user=settings.oauth_basic_user,
        oauth_basic_password=settings.oauth_basic_password,
        oauth_username=settings.oauth_username,
        oauth_password=settings.oauth_password,
        max_retries=settings.max_retries,
    )

    results: dict[str, object] = {}
    errors: dict[str, str] = {}

    for vrn in sorted(settings.van_map.keys()):
        if settings.request_delay_ms > 0:
            time_module.sleep(settings.request_delay_ms / 1000.0)
        vehicle_id = settings.van_map.get(vrn)
        if vehicle_id is None:
            continue
        try:
            events = client.fetch_history(vehicle_id=vehicle_id, date_from=query_from, date_to=query_to)
            metrics = compute_metrics_for_shift(
                vrn=vrn,
                events=events,
                shift_start=shift_start,
                shift_end=shift_end,
                overspeed_kph_tolerance=settings.overspeed_kph_tolerance,
            )
            results[vrn] = metrics
        except (RamClientError, ValueError) as e:
            errors[vrn] = str(e)

    return jsonify(
        {
            "date": d.isoformat(),
            "shift": {"startUtc": shift_start.isoformat(), "endUtc": shift_end.isoformat()},
            "results": results,
            "errors": errors,
        }
    )


@app.post("/api/metrics/retry")
def api_metrics_retry():
    try:
        settings = load_settings(require_secrets=True)
    except ConfigError as e:
        return jsonify({"error": str(e)}), 400

    payload = request.get_json(silent=True) or {}
    date_str = payload.get("date")
    vrns = payload.get("vrns")

    if not isinstance(date_str, str):
        return jsonify({"error": "Missing or invalid 'date' (YYYY-MM-DD)."}), 400
    if not isinstance(vrns, list) or not vrns or not all(isinstance(v, str) for v in vrns):
        return jsonify({"error": "Missing or invalid 'vrns' (non-empty list of strings)."}), 400

    try:
        d = _parse_date_yyyy_mm_dd(date_str)
    except ValueError as e:
        return jsonify({"error": str(e)}), 400

    shift_start, shift_end = _shift_window_utc(d)
    query_from = (d - timedelta(days=1)).isoformat()
    query_to = (d + timedelta(days=2)).isoformat()

    client = RamClient(
        api_base_url=settings.api_base_url,
        token_url=settings.token_url,
        oauth_basic_user=settings.oauth_basic_user,
        oauth_basic_password=settings.oauth_basic_password,
        oauth_username=settings.oauth_username,
        oauth_password=settings.oauth_password,
        max_retries=settings.max_retries,
    )

    results: dict[str, object] = {}
    errors: dict[str, str] = {}
    seen: set[str] = set()
    for vrn in vrns:
        if vrn in seen:
            continue
        seen.add(vrn)
        if settings.request_delay_ms > 0:
            time_module.sleep(settings.request_delay_ms / 1000.0)
        vehicle_id = settings.van_map.get(vrn)
        if vehicle_id is None:
            errors[vrn] = "Unknown VRN (not in VAN_MAP)."
            continue
        try:
            events = client.fetch_history(vehicle_id=vehicle_id, date_from=query_from, date_to=query_to)
            metrics = compute_metrics_for_shift(
                vrn=vrn,
                events=events,
                shift_start=shift_start,
                shift_end=shift_end,
                overspeed_kph_tolerance=settings.overspeed_kph_tolerance,
            )
            results[vrn] = metrics
        except (RamClientError, ValueError) as e:
            errors[vrn] = str(e)

    return jsonify(
        {
            "date": d.isoformat(),
            "shift": {"startUtc": shift_start.isoformat(), "endUtc": shift_end.isoformat()},
            "results": results,
            "errors": errors,
        }
    )


def _fmt_seconds(secs: float | int | None) -> str:
    if secs is None:
        return "-"
    s = int(round(float(secs)))
    if s < 0:
        s = 0
    h = s // 3600
    m = (s % 3600) // 60
    r = s % 60
    if h > 0:
        return f"{h}h {m}m {r}s"
    if m > 0:
        return f"{m}m {r}s"
    return f"{r}s"


@app.get("/van/<vrn>")
def van_details(vrn: str):
    date_str = request.args.get("date", "")
    if not date_str:
        return render_template(
            "van.html",
            vrn=vrn,
            date="",
            error="Missing ?date=YYYY-MM-DD in URL.",
            status="ERROR",
            shift_start="",
            shift_end="",
            vehicle_id="",
            overspeed_segments="-",
            overspeed_time="-",
            idling_time="-",
            journey_count="-",
            overspeed_events=[],
            idling_events=[],
            journeys=[],
        ), 400

    try:
        d = _parse_date_yyyy_mm_dd(date_str)
    except ValueError as e:
        return render_template(
            "van.html",
            vrn=vrn,
            date=date_str,
            error=str(e),
            status="ERROR",
            shift_start="",
            shift_end="",
            vehicle_id="",
            overspeed_segments="-",
            overspeed_time="-",
            idling_time="-",
            journey_count="-",
            overspeed_events=[],
            idling_events=[],
            journeys=[],
        ), 400

    try:
        settings = load_settings(require_secrets=True)
    except ConfigError as e:
        return render_template(
            "van.html",
            vrn=vrn,
            date=d.isoformat(),
            error=str(e),
            status="ERROR",
            shift_start="",
            shift_end="",
            vehicle_id="",
            overspeed_segments="-",
            overspeed_time="-",
            idling_time="-",
            journey_count="-",
            overspeed_events=[],
            idling_events=[],
            journeys=[],
        ), 400

    vehicle_id = settings.van_map.get(vrn)
    if vehicle_id is None:
        return render_template(
            "van.html",
            vrn=vrn,
            date=d.isoformat(),
            error="Unknown VRN (not in VAN_MAP).",
            status="ERROR",
            shift_start="",
            shift_end="",
            vehicle_id="",
            overspeed_segments="-",
            overspeed_time="-",
            idling_time="-",
            journey_count="-",
            overspeed_events=[],
            idling_events=[],
            journeys=[],
        ), 404

    shift_start, shift_end = _shift_window_utc(d)
    query_from = (d - timedelta(days=1)).isoformat()
    query_to = (d + timedelta(days=2)).isoformat()

    client = RamClient(
        api_base_url=settings.api_base_url,
        token_url=settings.token_url,
        oauth_basic_user=settings.oauth_basic_user,
        oauth_basic_password=settings.oauth_basic_password,
        oauth_username=settings.oauth_username,
        oauth_password=settings.oauth_password,
        max_retries=settings.max_retries,
    )

    try:
        events = client.fetch_history(vehicle_id=vehicle_id, date_from=query_from, date_to=query_to)
        metrics = compute_metrics_for_shift(
            vrn=vrn,
            events=events,
            shift_start=shift_start,
            shift_end=shift_end,
            overspeed_kph_tolerance=settings.overspeed_kph_tolerance,
        )
    except RamClientError as e:
        return render_template(
            "van.html",
            vrn=vrn,
            date=d.isoformat(),
            error=str(e),
            status="ERROR",
            shift_start=shift_start.isoformat(),
            shift_end=shift_end.isoformat(),
            vehicle_id=vehicle_id,
            overspeed_segments="-",
            overspeed_time="-",
            idling_time="-",
            journey_count="-",
            overspeed_events=[],
            idling_events=[],
            journeys=[],
        ), 502

    status = metrics.get("status")
    if status == "NO_DATA":
        return render_template(
            "van.html",
            vrn=vrn,
            date=d.isoformat(),
            error="",
            status="NO_DATA",
            shift_start=shift_start.isoformat(),
            shift_end=shift_end.isoformat(),
            vehicle_id=vehicle_id,
            overspeed_segments="-",
            overspeed_time="-",
            idling_time="-",
            journey_count="-",
            overspeed_events=[],
            idling_events=[],
            journeys=[],
        )

    overspeed = metrics.get("overspeed") or {}
    idling = metrics.get("idling") or {}
    journeys = (metrics.get("journeys") or {}).get("items") or []

    overspeed_events = overspeed.get("events") or extract_overspeed_events(
        events=sorted(events, key=lambda e: e.event_date),
        shift_start=shift_start,
        shift_end=shift_end,
        overspeed_kph_tolerance=settings.overspeed_kph_tolerance,
    )
    idling_events = idling.get("events") or []

    return render_template(
        "van.html",
        vrn=vrn,
        date=d.isoformat(),
        error="",
        status="OK",
        shift_start=shift_start.isoformat(),
        shift_end=shift_end.isoformat(),
        vehicle_id=vehicle_id,
        overspeed_segments=overspeed.get("segmentCount", 0),
        overspeed_time=_fmt_seconds(overspeed.get("totalSeconds")),
        idling_time=_fmt_seconds(idling.get("totalSeconds")),
        journey_count=(metrics.get("journeys") or {}).get("count", 0),
        overspeed_events=overspeed_events,
        idling_events=idling_events,
        journeys=journeys,
    )

