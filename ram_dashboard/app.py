from __future__ import annotations

import time as time_module
from datetime import date, datetime, time, timedelta, timezone

from flask import Flask, jsonify, render_template, request
from werkzeug.exceptions import HTTPException

from .config import ConfigError, load_settings
from .metrics import compute_geofence_inside_seconds, compute_metrics_for_shift, extract_overspeed_events
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


def _parse_iso_dt(value: str | None) -> datetime | None:
    if not value or not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _fmt_date_heading(dt: datetime | None) -> str:
    if dt is None:
        return "Unknown date"
    return dt.strftime("%a %d %b %Y")


def _fmt_time_hh_mm_24(dt: datetime | None) -> str:
    if dt is None:
        return "-"
    return dt.strftime("%H:%M")


def _fmt_seconds_hh_mm(secs: float | int | None) -> str:
    if secs is None:
        return "-"
    s = int(round(float(secs)))
    if s < 0:
        s = 0
    h = s // 3600
    m = (s % 3600) // 60
    return f"{h:02d}:{m:02d}"


def _fmt_seconds_hh_mm_ss(secs: float | int | None) -> str:
    if secs is None:
        return "-"
    s = int(round(float(secs)))
    if s < 0:
        s = 0
    h = s // 3600
    m = (s % 3600) // 60
    r = s % 60
    return f"{h:02d}:{m:02d}:{r:02d}"


def _group_point_events_by_date(events: list[dict], *, time_key: str) -> list[dict]:
    groups: dict[str, dict] = {}
    order: list[str] = []
    for ev in events:
        dt = _parse_iso_dt(ev.get(time_key))
        date_key = dt.date().isoformat() if dt else "unknown"
        if date_key not in groups:
            groups[date_key] = {"dateLabel": _fmt_date_heading(dt), "rows": []}
            order.append(date_key)
        item = dict(ev)
        item["timeText"] = _fmt_time_hh_mm_24(dt)
        groups[date_key]["rows"].append(item)
    return [groups[k] for k in order]


def _group_span_events_by_date(events: list[dict], *, start_key: str, end_key: str) -> list[dict]:
    groups: dict[str, dict] = {}
    order: list[str] = []
    for ev in events:
        start_dt = _parse_iso_dt(ev.get(start_key))
        end_dt = _parse_iso_dt(ev.get(end_key))
        date_key = start_dt.date().isoformat() if start_dt else "unknown"
        if date_key not in groups:
            groups[date_key] = {"dateLabel": _fmt_date_heading(start_dt), "rows": []}
            order.append(date_key)
        item = dict(ev)
        item["startTimeText"] = _fmt_time_hh_mm_24(start_dt)
        item["endTimeText"] = _fmt_time_hh_mm_24(end_dt)
        item["durationHm"] = _fmt_seconds_hh_mm(item.get("durationSeconds"))
        item["durationHms"] = _fmt_seconds_hh_mm_ss(item.get("durationSeconds"))
        groups[date_key]["rows"].append(item)
    return [groups[k] for k in order]


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
    idling_depot = metrics.get("idlingDepot") or {}
    idling_on_round = metrics.get("idlingOnRound") or {}
    stationary_ign_off_depot = metrics.get("stationaryIgnOffDepot") or {}
    stationary_ign_off_on_round = metrics.get("stationaryIgnOffOnRound") or {}
    journeys = (metrics.get("journeys") or {}).get("items") or []

    overspeed_events = overspeed.get("events") or extract_overspeed_events(
        events=sorted(events, key=lambda e: e.event_date),
        shift_start=shift_start,
        shift_end=shift_end,
        overspeed_kph_tolerance=settings.overspeed_kph_tolerance,
    )
    idling_events = idling.get("events") or []
    idling_depot_events = idling_depot.get("events") or []
    idling_on_round_events = idling_on_round.get("events") or []
    stationary_ign_off_depot_events = stationary_ign_off_depot.get("events") or []
    stationary_ign_off_on_round_events = stationary_ign_off_on_round.get("events") or []
    geofence_inside_seconds = compute_geofence_inside_seconds(
        events=events,
        shift_start=shift_start,
        shift_end=shift_end,
    )

    overspeed_groups = _group_point_events_by_date(overspeed_events, time_key="eventUtc")
    idling_on_round_groups = _group_span_events_by_date(idling_on_round_events, start_key="startUtc", end_key="endUtc")
    idling_depot_groups = _group_span_events_by_date(idling_depot_events, start_key="startUtc", end_key="endUtc")
    stationary_ign_off_on_round_groups = _group_span_events_by_date(
        stationary_ign_off_on_round_events,
        start_key="startUtc",
        end_key="endUtc",
    )
    stationary_ign_off_depot_groups = _group_span_events_by_date(
        stationary_ign_off_depot_events,
        start_key="startUtc",
        end_key="endUtc",
    )
    journey_groups = _group_span_events_by_date(journeys, start_key="startUtc", end_key="endUtc")

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
        idling_depot_time=_fmt_seconds(idling_depot.get("totalSeconds")),
        idling_on_round_time=_fmt_seconds(idling_on_round.get("totalSeconds")),
        stationary_ign_off_depot_time=_fmt_seconds(stationary_ign_off_depot.get("totalSeconds")),
        stationary_ign_off_on_round_time=_fmt_seconds(stationary_ign_off_on_round.get("totalSeconds")),
        geofence_inside_time=_fmt_seconds(geofence_inside_seconds),
        journey_count=(metrics.get("journeys") or {}).get("count", 0),
        overspeed_events=overspeed_events,
        overspeed_groups=overspeed_groups,
        idling_events=idling_events,
        idling_depot_events=idling_depot_events,
        idling_on_round_events=idling_on_round_events,
        idling_on_round_groups=idling_on_round_groups,
        idling_depot_groups=idling_depot_groups,
        stationary_ign_off_depot_events=stationary_ign_off_depot_events,
        stationary_ign_off_on_round_events=stationary_ign_off_on_round_events,
        stationary_ign_off_on_round_groups=stationary_ign_off_on_round_groups,
        stationary_ign_off_depot_groups=stationary_ign_off_depot_groups,
        journeys=journeys,
        journey_groups=journey_groups,
    )


@app.get("/geofence")
def geofence_times():
    date_str = request.args.get("date", "")
    today_str = datetime.now(timezone.utc).date().isoformat()

    if not date_str:
        return render_template(
            "geofence.html",
            date=today_str,
            rows=[],
            shift_start="",
            shift_end="",
            error="",
            has_results=False,
        )

    try:
        d = _parse_date_yyyy_mm_dd(date_str)
    except ValueError as e:
        return render_template(
            "geofence.html",
            date=date_str,
            rows=[],
            shift_start="",
            shift_end="",
            error=str(e),
            has_results=False,
        ), 400

    try:
        settings = load_settings(require_secrets=True)
    except ConfigError as e:
        return render_template(
            "geofence.html",
            date=d.isoformat(),
            rows=[],
            shift_start="",
            shift_end="",
            error=str(e),
            has_results=False,
        ), 400

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

    rows: list[dict[str, object]] = []
    for vrn in sorted(settings.van_map.keys()):
        if settings.request_delay_ms > 0:
            time_module.sleep(settings.request_delay_ms / 1000.0)
        vehicle_id = settings.van_map.get(vrn)
        if vehicle_id is None:
            rows.append({"vrn": vrn, "vehicle_id": "", "inside_seconds": None, "inside_text": "-", "status": "UNKNOWN_VRN"})
            continue
        try:
            events = client.fetch_history(vehicle_id=vehicle_id, date_from=query_from, date_to=query_to)
            inside_seconds = compute_geofence_inside_seconds(
                events=events,
                shift_start=shift_start,
                shift_end=shift_end,
            )
            rows.append(
                {
                    "vrn": vrn,
                    "vehicle_id": vehicle_id,
                    "inside_seconds": inside_seconds,
                    "inside_text": _fmt_seconds(inside_seconds),
                    "status": "OK",
                }
            )
        except (RamClientError, ValueError) as e:
            rows.append(
                {
                    "vrn": vrn,
                    "vehicle_id": vehicle_id,
                    "inside_seconds": None,
                    "inside_text": "-",
                    "status": f"ERROR: {e}",
                }
            )

    rows.sort(
        key=lambda r: (
            0 if r.get("inside_seconds") is not None else 1,
            -float(r.get("inside_seconds") or 0),
            str(r.get("vrn") or ""),
        )
    )

    return render_template(
        "geofence.html",
        date=d.isoformat(),
        rows=rows,
        shift_start=shift_start.isoformat(),
        shift_end=shift_end.isoformat(),
        error="",
        has_results=True,
    )

