from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timedelta

from .ram_client import RamEvent


def _google_maps_url(lat: float, lon: float) -> str:
    return f"https://www.google.com/maps?q={lat},{lon}"

def _kph_to_mph(kph: float) -> float:
    return kph * 0.621371


def extract_overspeed_events(
    *,
    events: list[RamEvent],
    shift_start: datetime,
    shift_end: datetime,
    overspeed_kph_tolerance: float,
    max_items: int = 500,
) -> list[dict]:
    def _fmt_hh_mm_from_seconds(secs: float | int | None) -> str:
        if secs is None:
            return "00:00"
        s = int(round(float(secs)))
        if s < 0:
            s = 0
        h = s // 3600
        m = (s % 3600) // 60
        return f"{h:02d}:{m:02d}"

    out: list[dict] = []
    run_id = 0
    in_run = False
    for idx, ev in enumerate(events):
        if ev.event_date < shift_start:
            continue
        if ev.event_date > shift_end:
            break
        if ev.speed_kph is None or ev.speed_limit_kph is None:
            in_run = False
            continue
        if ev.speed_limit_kph <= 0:
            # Ignore invalid/unset speed limits.
            in_run = False
            continue
        if ev.speed_kph <= (ev.speed_limit_kph + overspeed_kph_tolerance):
            in_run = False
            continue

        if not in_run:
            run_id += 1
            in_run = True

        next_event_dt = events[idx + 1].event_date if (idx + 1) < len(events) else shift_end
        event_end = min(next_event_dt, shift_end)
        event_duration_seconds = max(0.0, (event_end - ev.event_date).total_seconds())

        item = {
            "eventUtc": ev.event_date.isoformat(),
            "speedKph": ev.speed_kph,
            "speedLimitKph": ev.speed_limit_kph,
            "speedMph": round(_kph_to_mph(ev.speed_kph), 1),
            "speedLimitMph": int(math.ceil(_kph_to_mph(ev.speed_limit_kph))),
            "overspeedMph": round(_kph_to_mph(ev.speed_kph - ev.speed_limit_kph), 1),
            "postCode": ev.post_code,
            "latitude": ev.latitude,
            "longitude": ev.longitude,
            "runId": run_id,
            "eventDurationSeconds": event_duration_seconds,
        }
        if ev.latitude is not None and ev.longitude is not None:
            item["mapsUrl"] = _google_maps_url(ev.latitude, ev.longitude)
        out.append(item)
        if len(out) >= max_items:
            break

    # Add consecutive run position metadata for table display.
    run_totals: dict[int, int] = {}
    run_durations: dict[int, float] = {}
    for item in out:
        rid = int(item.get("runId") or 0)
        run_totals[rid] = run_totals.get(rid, 0) + 1
        run_durations[rid] = run_durations.get(rid, 0.0) + float(item.get("eventDurationSeconds") or 0.0)

    run_seen: dict[int, int] = {}
    for item in out:
        rid = int(item.get("runId") or 0)
        run_seen[rid] = run_seen.get(rid, 0) + 1
        item["runEventIndex"] = run_seen[rid]
        item["runLength"] = run_totals.get(rid, 1)
        item["runText"] = f'{item["runEventIndex"]}/{item["runLength"]}'
        item["runDurationSeconds"] = run_durations.get(rid, 0.0)
        item["runDurationText"] = _fmt_hh_mm_from_seconds(item["runDurationSeconds"])
    return out


def _clip_interval(start: datetime, end: datetime, clip_start: datetime, clip_end: datetime) -> tuple[datetime, datetime] | None:
    s = max(start, clip_start)
    e = min(end, clip_end)
    if e <= s:
        return None
    return s, e


def _sum_interval_seconds(intervals: list[tuple[datetime, datetime]]) -> float:
    return float(sum((e - s).total_seconds() for s, e in intervals))


def _merge_intervals(intervals: list[tuple[datetime, datetime]]) -> list[tuple[datetime, datetime]]:
    if not intervals:
        return []
    intervals = sorted(intervals, key=lambda x: x[0])
    merged: list[tuple[datetime, datetime]] = [intervals[0]]
    for s, e in intervals[1:]:
        ps, pe = merged[-1]
        if s <= pe:
            merged[-1] = (ps, max(pe, e))
        else:
            merged.append((s, e))
    return merged


def _paired_windows(events: list[RamEvent], start_name: str, end_name: str, *, shift_start: datetime, shift_end: datetime) -> list[tuple[datetime, datetime]]:
    windows: list[tuple[datetime, datetime]] = []
    open_start: datetime | None = None
    for ev in events:
        name = ev.event_name
        if name == start_name and open_start is None:
            open_start = ev.event_date
        elif name == end_name and open_start is not None:
            clipped = _clip_interval(open_start, ev.event_date, shift_start, shift_end)
            if clipped:
                windows.append(clipped)
            open_start = None
    # If a window starts but never ends within the query, clip it to shift_end.
    if open_start is not None:
        clipped = _clip_interval(open_start, shift_end, shift_start, shift_end)
        if clipped:
            windows.append(clipped)
    return windows


def _build_idling_windows_union(events: list[RamEvent], *, shift_start: datetime, shift_end: datetime) -> list[tuple[datetime, datetime]]:
    # Union of IGNITION_OFF..IGNITION_ON and IDLE_START..IDLE_END.
    w1 = _paired_windows(events, "IGNITION_OFF", "IGNITION_ON", shift_start=shift_start, shift_end=shift_end)
    w2 = _paired_windows(events, "IDLE_START", "IDLE_END", shift_start=shift_start, shift_end=shift_end)
    return _merge_intervals(w1 + w2)


def _stationary_segments(events: list[RamEvent], *, shift_start: datetime, shift_end: datetime) -> list[tuple[datetime, datetime]]:
    segs: list[tuple[datetime, datetime]] = []
    in_stationary = False
    seg_start: datetime | None = None

    for ev in events:
        if ev.event_date < shift_start:
            continue
        if ev.event_date > shift_end:
            break

        if ev.event_name == "STATIONARY" and not in_stationary:
            in_stationary = True
            seg_start = ev.event_date
            continue
        if ev.event_name == "DRIVING" and in_stationary and seg_start is not None:
            clipped = _clip_interval(seg_start, ev.event_date, shift_start, shift_end)
            if clipped:
                segs.append(clipped)
            in_stationary = False
            seg_start = None

    if in_stationary and seg_start is not None:
        clipped = _clip_interval(seg_start, shift_end, shift_start, shift_end)
        if clipped:
            segs.append(clipped)
    return segs


def _intersect_intervals(a: list[tuple[datetime, datetime]], b: list[tuple[datetime, datetime]]) -> list[tuple[datetime, datetime]]:
    if not a or not b:
        return []
    a = sorted(a, key=lambda x: x[0])
    b = sorted(b, key=lambda x: x[0])
    out: list[tuple[datetime, datetime]] = []
    i = 0
    j = 0
    while i < len(a) and j < len(b):
        as_, ae = a[i]
        bs, be = b[j]
        s = max(as_, bs)
        e = min(ae, be)
        if e > s:
            out.append((s, e))
        if ae <= be:
            i += 1
        else:
            j += 1
    return out


def _subtract_intervals(
    intervals: list[tuple[datetime, datetime]],
    subtractors: list[tuple[datetime, datetime]],
) -> list[tuple[datetime, datetime]]:
    if not intervals:
        return []
    if not subtractors:
        return list(intervals)

    out: list[tuple[datetime, datetime]] = []
    subtractors_sorted = _merge_intervals(subtractors)
    for start, end in _merge_intervals(intervals):
        cursor = start
        for ss, se in subtractors_sorted:
            if se <= cursor:
                continue
            if ss >= end:
                break
            if ss > cursor:
                out.append((cursor, min(ss, end)))
            cursor = max(cursor, se)
            if cursor >= end:
                break
        if cursor < end:
            out.append((cursor, end))
    return out


def _format_iso(dt: datetime) -> str:
    return dt.isoformat()


def _segment_location(events: list[RamEvent], segment_start: datetime) -> tuple[str | None, float | None, float | None]:
    for ev in events:
        if ev.event_date != segment_start:
            continue
        return ev.post_code, ev.latitude, ev.longitude
    return None, None, None


def extract_idling_events(*, events: list[RamEvent], segments: list[tuple[datetime, datetime]], max_items: int = 500) -> list[dict]:
    out: list[dict] = []
    for start, end in segments[:max_items]:
        post_code, latitude, longitude = _segment_location(events, start)
        item = {
            "startUtc": _format_iso(start),
            "endUtc": _format_iso(end),
            "durationSeconds": float((end - start).total_seconds()),
            "durationText": str(timedelta(seconds=int(round((end - start).total_seconds())))),
            "postCode": post_code,
            "latitude": latitude,
            "longitude": longitude,
        }
        if latitude is not None and longitude is not None:
            item["mapsUrl"] = _google_maps_url(latitude, longitude)
        out.append(item)
    return out


def _nearest_odometer_at_or_before(events: list[RamEvent], idx: int) -> float | None:
    for k in range(idx, -1, -1):
        if events[k].odometer is not None:
            return events[k].odometer
    return None


def _nearest_odometer_at_or_after(events: list[RamEvent], idx: int) -> float | None:
    for k in range(idx, len(events)):
        if events[k].odometer is not None:
            return events[k].odometer
    return None


def _compute_geofence_journeys(
    events: list[RamEvent],
    *,
    shift_start: datetime,
    shift_end: datetime,
) -> list[dict]:
    journeys: list[dict] = []

    # Track private mileage windows to exclude.
    private_windows = _paired_windows(events, "PRIVATE_MILEAGE_START", "PRIVATE_MILEAGE_END", shift_start=shift_start, shift_end=shift_end)
    private_windows = _merge_intervals(private_windows)

    open_out_idx: int | None = None
    open_out_time: datetime | None = None

    for idx, ev in enumerate(events):
        if ev.event_date < shift_start:
            continue
        if ev.event_date > shift_end:
            break

        if ev.event_name == "GEOFENCE_OUT" and open_out_idx is None:
            open_out_idx = idx
            open_out_time = ev.event_date
            continue

        if ev.event_name == "GEOFENCE_IN" and open_out_idx is not None and open_out_time is not None:
            start = open_out_time
            end = ev.event_date
            clipped = _clip_interval(start, end, shift_start, shift_end)
            if not clipped:
                open_out_idx = None
                open_out_time = None
                continue
            start_c, end_c = clipped

            # Compute kept intervals = journey interval minus private windows
            kept = [(start_c, end_c)]
            if private_windows:
                # subtract private windows
                kept2: list[tuple[datetime, datetime]] = []
                for ks, ke in kept:
                    cursor = ks
                    for ps, pe in _intersect_intervals([(ks, ke)], private_windows):
                        if ps > cursor:
                            kept2.append((cursor, ps))
                        cursor = max(cursor, pe)
                    if cursor < ke:
                        kept2.append((cursor, ke))
                kept = kept2

            duration_seconds = _sum_interval_seconds(kept)

            # Distance: approximate using odometer at OUT and IN; if missing, try nearby.
            # Note: this ignores private-mileage subtraction for distance when odometer is sparse.
            odo_start = events[open_out_idx].odometer
            odo_end = ev.odometer
            if odo_start is None:
                odo_start = _nearest_odometer_at_or_after(events, open_out_idx)
            if odo_end is None:
                odo_end = _nearest_odometer_at_or_before(events, idx)

            distance_km = None
            if odo_start is not None and odo_end is not None:
                # RAM odometer units are assumed km (as in VBA "odometer"). If it's meters, this will be obvious in output.
                distance_km = float(odo_end - odo_start)
                if distance_km < 0:
                    distance_km = None

            journeys.append(
                {
                    "startUtc": _format_iso(start_c),
                    "endUtc": _format_iso(end_c),
                    "durationSeconds": duration_seconds,
                    "distanceKm": distance_km,
                }
            )

            open_out_idx = None
            open_out_time = None

    return journeys


def compute_geofence_inside_seconds(
    *,
    events: list[RamEvent],
    shift_start: datetime,
    shift_end: datetime,
) -> float:
    inside_windows = _paired_windows(
        events,
        "GEOFENCE_IN",
        "GEOFENCE_OUT",
        shift_start=shift_start,
        shift_end=shift_end,
    )
    inside_windows = _merge_intervals(inside_windows)
    return _sum_interval_seconds(inside_windows)


def compute_metrics_for_shift(
    *,
    vrn: str,
    events: list[RamEvent],
    shift_start: datetime,
    shift_end: datetime,
    overspeed_kph_tolerance: float,
) -> dict:
    if shift_end <= shift_start:
        raise ValueError("Invalid shift window.")

    # Clip events to a bit wider range for duration calculations (need next event timestamp).
    events_sorted = sorted(events, key=lambda e: e.event_date)

    # Determine whether the van operated during the shift window.
    operated = any((e.event_date >= shift_start and e.event_date <= shift_end) for e in events_sorted)
    if not operated:
        return {
            "vrn": vrn,
            "status": "NO_DATA",
            "shift": {"startUtc": shift_start.isoformat(), "endUtc": shift_end.isoformat()},
        }

    overspeed_events = extract_overspeed_events(
        events=events_sorted,
        shift_start=shift_start,
        shift_end=shift_end,
        overspeed_kph_tolerance=overspeed_kph_tolerance,
    )

    # Overspeed: sum per-event durations where overspeed condition holds.
    overspeed_seconds = 0.0
    overspeed_segments = 0
    in_overspeed = False

    for i in range(len(events_sorted) - 1):
        cur = events_sorted[i]
        nxt = events_sorted[i + 1]

        seg = _clip_interval(cur.event_date, nxt.event_date, shift_start, shift_end)
        if not seg:
            continue
        s, e = seg

        if cur.speed_kph is None or cur.speed_limit_kph is None or cur.speed_limit_kph <= 0:
            is_over = False
        else:
            is_over = cur.speed_kph > (cur.speed_limit_kph + overspeed_kph_tolerance)

        if is_over:
            overspeed_seconds += (e - s).total_seconds()
            if not in_overspeed:
                overspeed_segments += 1
                in_overspeed = True
        else:
            in_overspeed = False

    # Idling: STATIONARY->DRIVING segments, intersected with union windows.
    idling_windows = _build_idling_windows_union(events_sorted, shift_start=shift_start, shift_end=shift_end)
    stationary_segs = _stationary_segments(events_sorted, shift_start=shift_start, shift_end=shift_end)
    idling_segs = _intersect_intervals(stationary_segs, idling_windows) if idling_windows else stationary_segs
    depot_windows = _paired_windows(events_sorted, "GEOFENCE_IN", "GEOFENCE_OUT", shift_start=shift_start, shift_end=shift_end)
    depot_windows = _merge_intervals(depot_windows)

    idling_depot_segs = _intersect_intervals(idling_segs, depot_windows) if depot_windows else []
    idling_on_round_segs = _subtract_intervals(idling_segs, depot_windows) if depot_windows else idling_segs

    idling_depot_seconds = _sum_interval_seconds(idling_depot_segs)
    idling_on_round_seconds = _sum_interval_seconds(idling_on_round_segs)
    idling_depot_events = extract_idling_events(events=events_sorted, segments=idling_depot_segs)
    idling_on_round_events = extract_idling_events(events=events_sorted, segments=idling_on_round_segs)

    ignition_off_windows = _paired_windows(events_sorted, "IGNITION_OFF", "IGNITION_ON", shift_start=shift_start, shift_end=shift_end)
    ignition_off_windows = _merge_intervals(ignition_off_windows)
    stationary_ign_off_segs = _intersect_intervals(stationary_segs, ignition_off_windows) if ignition_off_windows else []
    stationary_ign_off_depot_segs = _intersect_intervals(stationary_ign_off_segs, depot_windows) if depot_windows else []
    stationary_ign_off_on_round_segs = (
        _subtract_intervals(stationary_ign_off_segs, depot_windows) if depot_windows else stationary_ign_off_segs
    )

    journeys = _compute_geofence_journeys(events_sorted, shift_start=shift_start, shift_end=shift_end)
    journey_total_seconds = float(sum(j.get("durationSeconds") or 0 for j in journeys))
    journey_total_km = float(sum((j.get("distanceKm") or 0) for j in journeys))

    return {
        "vrn": vrn,
        "shift": {"startUtc": shift_start.isoformat(), "endUtc": shift_end.isoformat()},
        "overspeed": {
            "kphTolerance": overspeed_kph_tolerance,
            "totalSeconds": overspeed_seconds,
            "segmentCount": overspeed_segments,
            "events": overspeed_events,
        },
        "idling": {
            # Backward-compatible key now represents on-round only.
            "totalSeconds": idling_on_round_seconds,
            "stationarySegmentsCount": len(stationary_segs),
            "countedSegmentsCount": len(idling_on_round_segs),
            "windows": [{"startUtc": s.isoformat(), "endUtc": e.isoformat()} for s, e in idling_windows],
            "events": idling_on_round_events,
        },
        "idlingDepot": {
            "totalSeconds": idling_depot_seconds,
            "countedSegmentsCount": len(idling_depot_segs),
            "events": idling_depot_events,
        },
        "idlingOnRound": {
            "totalSeconds": idling_on_round_seconds,
            "countedSegmentsCount": len(idling_on_round_segs),
            "events": idling_on_round_events,
        },
        "stationaryIgnOffDepot": {
            "totalSeconds": _sum_interval_seconds(stationary_ign_off_depot_segs),
            "countedSegmentsCount": len(stationary_ign_off_depot_segs),
            "events": extract_idling_events(events=events_sorted, segments=stationary_ign_off_depot_segs),
        },
        "stationaryIgnOffOnRound": {
            "totalSeconds": _sum_interval_seconds(stationary_ign_off_on_round_segs),
            "countedSegmentsCount": len(stationary_ign_off_on_round_segs),
            "events": extract_idling_events(events=events_sorted, segments=stationary_ign_off_on_round_segs),
        },
        "journeys": {
            "count": len(journeys),
            "totalSeconds": journey_total_seconds,
            "totalKm": journey_total_km,
            "items": journeys,
        },
    }

