from __future__ import annotations

from contextlib import contextmanager
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Iterator

import mysql.connector

from .config import DbSettings
from .driver_behaviour_metrics import as_float, as_int, normalize_driver_row


def _json_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return value


def _json_row(row: dict[str, Any]) -> dict[str, Any]:
    return {k: _json_value(v) for k, v in row.items()}


class DriverBehaviourStore:
    def __init__(self, settings: DbSettings):
        self.settings = settings

    @contextmanager
    def _cursor(self) -> Iterator[Any]:
        conn = mysql.connector.connect(
            host=self.settings.host,
            port=self.settings.port,
            user=self.settings.user,
            password=self.settings.password,
            database=self.settings.database,
        )
        try:
            cur = conn.cursor(dictionary=True)
            try:
                yield cur
            finally:
                cur.close()
        finally:
            conn.close()

    def get_dashboard(
        self,
        *,
        date_from: str | None = None,
        date_to: str | None = None,
        query: str | None = None,
        trend_days: int = 14,
    ) -> dict[str, Any]:
        with self._cursor() as cur:
            periods = self._list_periods(cur)
            period = self._resolve_period(cur, date_from=date_from, date_to=date_to, periods=periods)
            leaderboard = self._leaderboard(cur, period=period, query=query)
            summary = self._summary(cur, period=period, leaderboard=leaderboard)
            trends = self._trends(cur, limit=trend_days)
            warnings = self._data_quality_warnings(cur, period=period, leaderboard=leaderboard)
            visuals = self._visuals(summary=summary, leaderboard=leaderboard, trends=trends)

        return {
            "period": period,
            "periods": periods,
            "summary": summary,
            "leaderboard": leaderboard,
            "trends": trends,
            "visuals": visuals,
            "dataQualityWarnings": warnings,
            "errors": [],
        }

    def _list_periods(self, cur: Any, *, limit: int = 90) -> list[dict[str, Any]]:
        cur.execute(
            """
            SELECT Date_From AS dateFrom, Date_To AS dateTo, COUNT(*) AS rowCount
            FROM flyingfish_aux.driver_behaviour
            GROUP BY Date_From, Date_To
            ORDER BY Date_From DESC, Date_To DESC
            LIMIT %s
            """,
            (limit,),
        )
        return [_json_row(r) for r in cur.fetchall()]

    def _resolve_period(
        self,
        cur: Any,
        *,
        date_from: str | None,
        date_to: str | None,
        periods: list[dict[str, Any]],
    ) -> dict[str, Any]:
        if date_from:
            if not date_to:
                cur.execute(
                    """
                    SELECT Date_To AS dateTo, COUNT(*) AS rowCount
                    FROM flyingfish_aux.driver_behaviour
                    WHERE Date_From = %s
                    GROUP BY Date_To
                    ORDER BY Date_To DESC
                    LIMIT 1
                    """,
                    (date_from,),
                )
                row = cur.fetchone()
                date_to = str(_json_value(row["dateTo"])) if row else date_from
                row_count = as_int(row.get("rowCount") if row else 0)
            else:
                cur.execute(
                    """
                    SELECT COUNT(*) AS rowCount
                    FROM flyingfish_aux.driver_behaviour
                    WHERE Date_From = %s AND Date_To = %s
                    """,
                    (date_from, date_to),
                )
                row_count = as_int(cur.fetchone().get("rowCount"))
            return {"dateFrom": date_from, "dateTo": date_to, "rowCount": row_count}

        if periods:
            return periods[0]

        today = date.today().isoformat()
        return {"dateFrom": today, "dateTo": today, "rowCount": 0}

    def _summary(self, cur: Any, *, period: dict[str, Any], leaderboard: list[dict[str, Any]]) -> dict[str, Any]:
        params = (period["dateFrom"], period["dateTo"])
        cur.execute(
            """
            SELECT
                COUNT(*) AS behaviourRows,
                COUNT(DISTINCT Registration) AS vehicles,
                COUNT(DISTINCT Driver) AS drivers,
                ROUND(SUM(COALESCE(Miles, 0)), 1) AS miles,
                ROUND(AVG(Score), 2) AS avgScore,
                SUM(Speeding) AS speeding,
                SUM(Idling) AS idling,
                SUM(Harsh_Braking) AS harshBraking,
                SUM(Harsh_Acceleration) AS harshAcceleration
            FROM flyingfish_aux.driver_behaviour
            WHERE Date_From = %s AND Date_To = %s
            """,
            params,
        )
        behaviour = _json_row(cur.fetchone() or {})

        cur.execute(
            """
            SELECT
                COUNT(*) AS speedRows,
                SUM(Total_Breaches) AS totalBreaches,
                SUM(Points_Accrued) AS pointsAccrued,
                MAX(Max_Speed) AS maxSpeed,
                ROUND(AVG(Points_Mile), 3) AS avgPointsPerMile,
                ROUND(SUM(COALESCE(Distance_Miles, 0)), 1) AS speedMiles
            FROM flyingfish_aux.speed_league
            WHERE Date_From = %s AND Date_To = %s
            """,
            params,
        )
        speed = _json_row(cur.fetchone() or {})

        cur.execute(
            """
            SELECT
                COUNT(*) AS carbonRows,
                ROUND(SUM(COALESCE(Total_Distance, 0)), 1) AS carbonDistance,
                ROUND(SUM(COALESCE(CO2_Emissions, 0)), 1) AS co2Emissions,
                ROUND(SUM(COALESCE(CO2_Output, 0)), 1) AS co2Output
            FROM flyingfish_aux.carbon_report
            WHERE Date_from = %s AND Date_to = %s
            """,
            params,
        )
        carbon = _json_row(cur.fetchone() or {})

        high_risk = sum(1 for row in leaderboard if (row.get("risk") or {}).get("band") == "high")
        anomaly = sum(1 for row in leaderboard if (row.get("risk") or {}).get("band") == "anomaly")
        return {
            **behaviour,
            **speed,
            **carbon,
            "highRiskDrivers": high_risk,
            "anomalyDrivers": anomaly,
            "speedingPer100Miles": self._rate(behaviour.get("speeding"), behaviour.get("miles")),
            "harshPer100Miles": self._rate(
                (as_int(behaviour.get("harshBraking")) + as_int(behaviour.get("harshAcceleration"))),
                behaviour.get("miles"),
            ),
            "co2PerMile": self._per_mile(carbon.get("co2Output"), behaviour.get("miles")),
        }

    def _leaderboard(self, cur: Any, *, period: dict[str, Any], query: str | None) -> list[dict[str, Any]]:
        params: list[Any] = [period["dateFrom"], period["dateTo"]]
        filter_sql = ""
        if query:
            filter_sql = "AND (db.Registration LIKE %s OR db.Driver LIKE %s)"
            like = f"%{query}%"
            params.extend([like, like])

        cur.execute(
            f"""
            SELECT
                db.Registration AS registration,
                db.Driver AS driver,
                db.Speeding AS speeding,
                db.Idling AS idling,
                db.Harsh_Braking AS harshBraking,
                db.Harsh_Acceleration AS harshAcceleration,
                db.Miles AS miles,
                db.Score AS score,
                sl.Total_Breaches AS totalBreaches,
                sl.Points_Accrued AS pointsAccrued,
                sl.Points_Mile AS pointsPerMile,
                sl.Max_Speed AS maxSpeed,
                sl.`20_Mph_Zone` AS zone20,
                sl.`30_Mph_Zone` AS zone30,
                sl.`40_Mph_Zone` AS zone40,
                sl.`50_Mph_Zone` AS zone50,
                sl.`60_Mph_Zone` AS zone60,
                sl.`70_Mph_Zone` AS zone70,
                cr.CO2_Output AS co2Output
            FROM flyingfish_aux.driver_behaviour db
            LEFT JOIN flyingfish_aux.speed_league sl
                ON sl.Registration = db.Registration
                AND sl.Driver = db.Driver
                AND sl.Date_From = db.Date_From
                AND sl.Date_To = db.Date_To
            LEFT JOIN flyingfish_aux.carbon_report cr
                ON cr.Vehicle_Registration = db.Registration
                AND cr.Date_from = db.Date_From
                AND cr.Date_to = db.Date_To
            WHERE db.Date_From = %s AND db.Date_To = %s
            {filter_sql}
            ORDER BY db.Score DESC, sl.Points_Mile DESC, db.Speeding DESC, db.Registration
            LIMIT 250
            """,
            tuple(params),
        )
        rows = [_json_row(r) for r in cur.fetchall()]
        return [normalize_driver_row(r) for r in rows]

    def _trends(self, cur: Any, *, limit: int) -> list[dict[str, Any]]:
        limit = max(1, min(int(limit or 14), 60))
        cur.execute(
            """
            SELECT Date_From AS dateFrom, Date_To AS dateTo
            FROM flyingfish_aux.driver_behaviour
            GROUP BY Date_From, Date_To
            ORDER BY Date_From DESC, Date_To DESC
            LIMIT %s
            """,
            (limit,),
        )
        periods = [_json_row(r) for r in cur.fetchall()]
        if not periods:
            return []

        out: list[dict[str, Any]] = []
        for p in reversed(periods):
            params = (p["dateFrom"], p["dateTo"])
            cur.execute(
                """
                SELECT
                    COUNT(*) AS drivers,
                    ROUND(SUM(COALESCE(Miles, 0)), 1) AS miles,
                    ROUND(AVG(Score), 2) AS avgScore,
                    SUM(Speeding) AS speeding,
                    SUM(Idling) AS idling,
                    SUM(Harsh_Braking) AS harshBraking,
                    SUM(Harsh_Acceleration) AS harshAcceleration
                FROM flyingfish_aux.driver_behaviour
                WHERE Date_From = %s AND Date_To = %s
                """,
                params,
            )
            behaviour = _json_row(cur.fetchone() or {})
            cur.execute(
                """
                SELECT
                    SUM(Total_Breaches) AS totalBreaches,
                    SUM(Points_Accrued) AS pointsAccrued,
                    ROUND(AVG(Points_Mile), 3) AS avgPointsPerMile,
                    MAX(Max_Speed) AS maxSpeed
                FROM flyingfish_aux.speed_league
                WHERE Date_From = %s AND Date_To = %s
                """,
                params,
            )
            speed = _json_row(cur.fetchone() or {})
            out.append({**p, **behaviour, **speed})
        return out

    def _data_quality_warnings(
        self,
        cur: Any,
        *,
        period: dict[str, Any],
        leaderboard: list[dict[str, Any]],
    ) -> list[str]:
        params = (period["dateFrom"], period["dateTo"])
        warnings: list[str] = []

        cur.execute(
            """
            SELECT COUNT(*) AS rowCount
            FROM flyingfish_aux.driver_behaviour
            WHERE Date_From = %s AND Date_To = %s AND Date_To < Date_From
            """,
            params,
        )
        invalid_period_rows = as_int(cur.fetchone().get("rowCount"))
        if invalid_period_rows:
            warnings.append(f"{invalid_period_rows} behaviour row(s) have Date_To earlier than Date_From in this period.")

        low_distance = [r for r in leaderboard if r.get("isLowDistance") and ((r.get("score") or 0) > 0 or (r.get("pointsPerMile") or 0) > 0)]
        if low_distance:
            warnings.append(f"{len(low_distance)} row(s) have under 1 mile but non-zero risk metrics; rate-based rankings may be inflated.")

        cur.execute(
            """
            SELECT COUNT(*) AS rowCount
            FROM flyingfish_aux.driver_behaviour db
            LEFT JOIN flyingfish_aux.speed_league sl
                ON sl.Registration = db.Registration
                AND sl.Driver = db.Driver
                AND sl.Date_From = db.Date_From
                AND sl.Date_To = db.Date_To
            WHERE db.Date_From = %s AND db.Date_To = %s AND sl.id IS NULL
            """,
            params,
        )
        missing_speed = as_int(cur.fetchone().get("rowCount"))
        if missing_speed:
            warnings.append(f"{missing_speed} behaviour row(s) have no matching speed-league row for this period.")

        cur.execute(
            """
            SELECT COUNT(*) AS rowCount
            FROM flyingfish_aux.driver_behaviour db
            LEFT JOIN flyingfish_aux.carbon_report cr
                ON cr.Vehicle_Registration = db.Registration
                AND cr.Date_from = db.Date_From
                AND cr.Date_to = db.Date_To
            WHERE db.Date_From = %s AND db.Date_To = %s AND cr.id IS NULL
            """,
            params,
        )
        missing_carbon = as_int(cur.fetchone().get("rowCount"))
        if missing_carbon:
            warnings.append(f"{missing_carbon} behaviour row(s) have no matching carbon row for this period.")

        return warnings

    def _visuals(
        self,
        *,
        summary: dict[str, Any],
        leaderboard: list[dict[str, Any]],
        trends: list[dict[str, Any]],
    ) -> dict[str, Any]:
        risk_counts = {"low": 0, "medium": 0, "high": 0, "anomaly": 0}
        for row in leaderboard:
            band = (row.get("risk") or {}).get("band")
            if band in risk_counts:
                risk_counts[band] += 1
        risk_total = max(sum(risk_counts.values()), 1)
        risk_distribution = [
            {"band": "low", "label": "Low", "count": risk_counts["low"], "width": self._percent(risk_counts["low"], risk_total)},
            {"band": "medium", "label": "Medium", "count": risk_counts["medium"], "width": self._percent(risk_counts["medium"], risk_total)},
            {"band": "high", "label": "High", "count": risk_counts["high"], "width": self._percent(risk_counts["high"], risk_total)},
            {"band": "anomaly", "label": "Low-mileage anomalies", "count": risk_counts["anomaly"], "width": self._percent(risk_counts["anomaly"], risk_total)},
        ]

        behaviour_mix_values = [
            ("speeding", "Speeding", as_int(summary.get("speeding"))),
            ("idling", "Idling", as_int(summary.get("idling"))),
            ("braking", "Harsh braking", as_int(summary.get("harshBraking"))),
            ("acceleration", "Harsh acceleration", as_int(summary.get("harshAcceleration"))),
        ]
        behaviour_total = max(sum(v for _, _, v in behaviour_mix_values), 1)
        behaviour_mix = [
            {"key": key, "label": label, "value": value, "width": self._percent(value, behaviour_total)}
            for key, label, value in behaviour_mix_values
        ]

        trend_max = {
            "speeding": max([as_int(r.get("speeding")) for r in trends] or [0]),
            "breaches": max([as_int(r.get("totalBreaches")) for r in trends] or [0]),
            "score": max([as_float(r.get("avgScore")) or 0.0 for r in trends] or [0.0]),
        }
        trend_rows = []
        for row in trends:
            speeding = as_int(row.get("speeding"))
            breaches = as_int(row.get("totalBreaches"))
            avg_score = as_float(row.get("avgScore")) or 0.0
            trend_rows.append(
                {
                    **row,
                    "label": str(row.get("dateFrom") or "")[5:],
                    "speedingWidth": self._percent(speeding, trend_max["speeding"]),
                    "breachesWidth": self._percent(breaches, trend_max["breaches"]),
                    "scoreWidth": self._percent(avg_score, trend_max["score"]),
                    "harshEvents": as_int(row.get("harshBraking")) + as_int(row.get("harshAcceleration")),
                }
            )

        attention_cards = sorted(
            leaderboard,
            key=lambda r: (
                self._risk_weight((r.get("risk") or {}).get("band")),
                as_float(r.get("score")) or 0.0,
                as_float(r.get("eventsPer100Miles")) or 0.0,
                as_float(r.get("pointsPerMile")) or 0.0,
            ),
            reverse=True,
        )[:10]
        max_score = max([as_float(r.get("score")) or 0.0 for r in attention_cards] or [0.0])
        max_events = max([as_float(r.get("eventsPer100Miles")) or 0.0 for r in attention_cards] or [0.0])
        priority_cards = [
            {
                **row,
                "reasons": self._attention_reasons(row),
                "scoreWidth": self._percent(as_float(row.get("score")) or 0.0, max_score),
                "eventsWidth": self._percent(as_float(row.get("eventsPer100Miles")) or 0.0, max_events),
            }
            for row in attention_cards
        ]

        return {
            "riskDistribution": risk_distribution,
            "behaviourMix": behaviour_mix,
            "trendRows": trend_rows,
            "priorityCards": priority_cards,
            "topDriverCards": priority_cards,
        }

    @staticmethod
    def _risk_weight(band: Any) -> int:
        return {"high": 4, "anomaly": 3, "medium": 2, "low": 1}.get(str(band or ""), 0)

    @staticmethod
    def _attention_reasons(row: dict[str, Any]) -> list[str]:
        reasons: list[str] = []
        if (row.get("risk") or {}).get("band") == "anomaly":
            reasons.append("Low mileage anomaly")
        if (as_float(row.get("score")) or 0.0) >= 60:
            reasons.append("High score")
        if (as_float(row.get("speedingPer100Miles")) or 0.0) >= 50:
            reasons.append("High speeding rate")
        elif as_int(row.get("speeding")) > 0:
            reasons.append(f"{as_int(row.get('speeding'))} speeding event(s)")
        if (as_float(row.get("harshPer100Miles")) or 0.0) >= 10:
            reasons.append("High harsh-event rate")
        elif as_int(row.get("harshEvents")) > 0:
            reasons.append(f"{as_int(row.get('harshEvents'))} harsh event(s)")
        if (as_float(row.get("pointsPerMile")) or 0.0) >= 5:
            reasons.append("High speed points per mile")
        elif as_int(row.get("totalBreaches")) > 0:
            reasons.append(f"{as_int(row.get('totalBreaches'))} speed breach(es)")
        if not reasons:
            reasons.append("No major issues")
        return reasons[:4]

    @staticmethod
    def _rate(value: Any, miles: Any) -> float | None:
        miles_float = as_float(miles)
        if miles_float is None or miles_float <= 0:
            return None
        return round((as_float(value) or 0.0) * 100.0 / miles_float, 2)

    @staticmethod
    def _per_mile(value: Any, miles: Any) -> float | None:
        miles_float = as_float(miles)
        if miles_float is None or miles_float <= 0:
            return None
        return round((as_float(value) or 0.0) / miles_float, 3)

    @staticmethod
    def _percent(value: Any, maximum: Any) -> int:
        maximum_float = as_float(maximum) or 0.0
        value_float = as_float(value) or 0.0
        if maximum_float <= 0 or value_float <= 0:
            return 0
        return max(2, min(100, int(round(value_float * 100.0 / maximum_float))))
