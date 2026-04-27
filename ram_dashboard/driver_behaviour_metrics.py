from __future__ import annotations

from decimal import Decimal
from typing import Any


LOW_DISTANCE_MILES = 1.0


def as_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return float(value)
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def as_int(value: Any) -> int:
    if value is None:
        return 0
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def rate_per_100(value: Any, miles: Any) -> float | None:
    miles_float = as_float(miles)
    if miles_float is None or miles_float <= 0:
        return None
    return round((as_float(value) or 0.0) * 100.0 / miles_float, 2)


def per_mile(value: Any, miles: Any) -> float | None:
    miles_float = as_float(miles)
    if miles_float is None or miles_float <= 0:
        return None
    return round((as_float(value) or 0.0) / miles_float, 3)


def risk_band(
    *,
    score: Any,
    miles: Any,
    speeding_per_100: float | None,
    harsh_per_100: float | None,
    points_per_mile: Any,
) -> dict[str, str]:
    score_float = as_float(score) or 0.0
    miles_float = as_float(miles) or 0.0
    points_float = as_float(points_per_mile) or 0.0

    if miles_float < LOW_DISTANCE_MILES and (score_float > 0 or points_float > 0):
        return {"band": "anomaly", "label": "Low-mileage anomaly"}

    if score_float >= 60 or points_float >= 5 or (speeding_per_100 or 0) >= 50 or (harsh_per_100 or 0) >= 10:
        return {"band": "high", "label": "High"}

    if score_float >= 10 or points_float >= 0.5 or (speeding_per_100 or 0) >= 10 or (harsh_per_100 or 0) >= 2:
        return {"band": "medium", "label": "Medium"}

    return {"band": "low", "label": "Low"}


def normalize_driver_row(row: dict[str, Any]) -> dict[str, Any]:
    speeding = as_int(row.get("speeding"))
    harsh_braking = as_int(row.get("harshBraking"))
    harsh_acceleration = as_int(row.get("harshAcceleration"))
    harsh_events = harsh_braking + harsh_acceleration
    behaviour_events = speeding + as_int(row.get("idling")) + harsh_events
    miles = as_float(row.get("miles")) or 0.0
    co2_output = as_float(row.get("co2Output"))

    speeding_rate = rate_per_100(speeding, miles)
    harsh_rate = rate_per_100(harsh_events, miles)
    row["miles"] = round(miles, 1)
    row["score"] = round(as_float(row.get("score")) or 0.0, 2)
    row["speeding"] = speeding
    row["idling"] = as_int(row.get("idling"))
    row["harshBraking"] = harsh_braking
    row["harshAcceleration"] = harsh_acceleration
    row["harshEvents"] = harsh_events
    row["behaviourEvents"] = behaviour_events
    row["eventsPer100Miles"] = rate_per_100(behaviour_events, miles)
    row["speedingPer100Miles"] = speeding_rate
    row["harshPer100Miles"] = harsh_rate
    row["pointsPerMile"] = as_float(row.get("pointsPerMile"))
    row["pointsAccrued"] = as_int(row.get("pointsAccrued"))
    row["totalBreaches"] = as_int(row.get("totalBreaches"))
    row["maxSpeed"] = as_int(row.get("maxSpeed"))
    row["co2Output"] = co2_output
    row["co2PerMile"] = round(co2_output / miles, 3) if co2_output is not None and miles > 0 else None
    row["isLowDistance"] = miles < LOW_DISTANCE_MILES
    row["risk"] = risk_band(
        score=row["score"],
        miles=miles,
        speeding_per_100=speeding_rate,
        harsh_per_100=harsh_rate,
        points_per_mile=row["pointsPerMile"],
    )
    return row
