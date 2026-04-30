from __future__ import annotations

from contextlib import contextmanager
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Iterator

import mysql.connector

from .config import DbSettings
from .driver_behaviour_metrics import LOW_DISTANCE_MILES, as_float, as_int, normalize_driver_row


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
            use_pure=True,
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
            source_data = self._source_data_checks(
                cur,
                date_from=str(period["dateFrom"]),
                date_to=str(period["dateTo"]),
                mode="period",
            )
            leaderboard = self._leaderboard(cur, period=period, query=query)
            summary = self._summary(cur, period=period, leaderboard=leaderboard)
            trends = self._trends(cur, limit=trend_days)
            warnings = self._data_quality_warnings(cur, period=period, leaderboard=leaderboard)
            warnings = source_data["warnings"] + warnings
            visuals = self._visuals(summary=summary, leaderboard=leaderboard, trends=trends)

        return {
            "period": period,
            "periods": periods,
            "summary": summary,
            "leaderboard": leaderboard,
            "trends": trends,
            "visuals": visuals,
            "sourceDataChecks": source_data["checks"],
            "dataQualityWarnings": warnings,
            "errors": [],
        }

    def get_speeding_offenders_report(
        self,
        *,
        date_from: str | None = None,
        date_to: str | None = None,
    ) -> dict[str, Any]:
        with self._cursor() as cur:
            periods = self._list_periods(cur)
            report_range = self._resolve_report_range(cur, date_from=date_from, date_to=date_to, periods=periods)
            source_data = self._source_data_checks(
                cur,
                date_from=str(report_range["dateFrom"]),
                date_to=str(report_range["dateTo"]),
                mode="range",
            )
            rows = self._speeding_report_rows(cur, report_range=report_range)
            assignments_by_vrn = self._route_assignments_by_registration(
                cur,
                date_from=str(report_range["dateFrom"]),
                date_to=str(report_range["dateTo"]),
            )

        evidence_rows: list[dict[str, Any]] = []
        rollups: dict[str, dict[str, Any]] = {}
        unmatched_rows = 0
        shared_rows = 0
        low_mileage_rows = 0
        missing_speed_rows = 0
        total_speeding = 0
        total_breaches = 0
        total_points = 0
        vehicle_keys: set[str] = set()

        for raw_row in rows:
            vehicle_code = raw_row.get("driver")
            row = normalize_driver_row(raw_row)
            row["vehicleCode"] = vehicle_code
            row["periodLabel"] = f"{row.get('dateFrom')} to {row.get('dateTo')}"
            row["zoneText"] = self._zone_text(row)
            row["vanDetailDate"] = row.get("dateFrom")
            row["speedingPer100Km"] = self._rate_per_100km(row.get("speeding"), row.get("miles"))

            total_speeding += as_int(row.get("speeding"))
            total_breaches += as_int(row.get("totalBreaches"))
            total_points += as_int(row.get("pointsAccrued"))
            vehicle_key = self._normalise_registration(row.get("registration"))
            if vehicle_key:
                vehicle_keys.add(vehicle_key)
            if row.get("isLowDistance"):
                low_mileage_rows += 1
            if not row.get("speedRowId"):
                missing_speed_rows += 1

            assignments = [
                assignment
                for assignment in assignments_by_vrn.get(vehicle_key, [])
                if self._date_in_range(
                    assignment.get("despatchDate"),
                    row.get("dateFrom"),
                    row.get("dateTo"),
                )
            ]
            driver_identities = self._assignment_driver_identities(assignments)
            if not driver_identities:
                unmatched_rows += 1
            if len(driver_identities) > 1:
                shared_rows += 1

            row["assignments"] = assignments
            row["driverIdentities"] = driver_identities
            row["attribution"] = "Shared" if len(driver_identities) > 1 else "Direct" if driver_identities else "Unmatched"
            row["realDriverText"] = (
                ", ".join(driver["label"] for driver in driver_identities)
                if driver_identities
                else "No route driver found"
            )
            evidence_rows.append(row)

            for driver in driver_identities:
                rollup = rollups.setdefault(
                    driver["key"],
                    {
                        "driverCode": driver["code"],
                        "driverName": driver["name"],
                        "driverLabel": driver["label"],
                        "vanPeriods": 0,
                        "sharedAttributions": 0,
                        "speeding": 0,
                        "totalBreaches": 0,
                        "pointsAccrued": 0,
                        "maxSpeed": 0,
                        "miles": 0.0,
                        "vehicles": set(),
                        "periods": set(),
                        "routes": set(),
                        "evidence": [],
                    },
                )
                rollup["vanPeriods"] += 1
                if len(driver_identities) > 1:
                    rollup["sharedAttributions"] += 1
                rollup["speeding"] += as_int(row.get("speeding"))
                rollup["totalBreaches"] += as_int(row.get("totalBreaches"))
                rollup["pointsAccrued"] += as_int(row.get("pointsAccrued"))
                rollup["maxSpeed"] = max(as_int(rollup.get("maxSpeed")), as_int(row.get("maxSpeed")))
                rollup["miles"] += as_float(row.get("miles")) or 0.0
                rollup["vehicles"].add(row.get("registration"))
                rollup["periods"].add(row.get("periodLabel"))
                for assignment in assignments:
                    if driver["code"] in assignment.get("driverCodes", []) or driver["name"] in assignment.get("driverNames", []):
                        rollup["routes"].add(assignment.get("routeName") or assignment.get("routeCode") or "Unknown route")
                rollup["evidence"].append(row)

        offender_rows = [self._finalise_offender_rollup(rollup) for rollup in rollups.values()]
        offender_rows.sort(
            key=lambda r: (
                as_int(r.get("vanPeriods")),
                as_int(r.get("pointsAccrued")),
                as_int(r.get("totalBreaches")),
                as_int(r.get("speeding")),
                as_int(r.get("maxSpeed")),
                r.get("driverLabel") or "",
            ),
            reverse=True,
        )
        evidence_rows.sort(
            key=lambda r: (
                as_int(r.get("pointsAccrued")),
                as_int(r.get("totalBreaches")),
                as_int(r.get("speeding")),
                as_int(r.get("maxSpeed")),
                r.get("dateFrom") or "",
            ),
            reverse=True,
        )

        warnings: list[str] = []
        if unmatched_rows:
            warnings.append(f"{unmatched_rows} speeding row(s) had no matching route assignment driver.")
        if shared_rows:
            warnings.append(f"{shared_rows} speeding row(s) matched multiple route drivers and are marked as shared.")
        if missing_speed_rows:
            warnings.append(f"{missing_speed_rows} behaviour row(s) had no matching speed-league row.")
        if low_mileage_rows:
            warnings.append(f"{low_mileage_rows} speeding row(s) were under {LOW_DISTANCE_MILES:g} mile; rate metrics may be inflated.")
        warnings = source_data["warnings"] + warnings

        return {
            "range": report_range,
            "periods": periods,
            "summary": {
                "speedingRows": len(evidence_rows),
                "offenders": len(offender_rows),
                "vehicles": len(vehicle_keys),
                "assignedDrivers": len(offender_rows),
                "unmatchedRows": unmatched_rows,
                "sharedRows": shared_rows,
                "speeding": total_speeding,
                "totalBreaches": total_breaches,
                "pointsAccrued": total_points,
            },
            "offenders": offender_rows,
            "evidenceRows": evidence_rows[:500],
            "sourceDataChecks": source_data["checks"],
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

    def _resolve_report_range(
        self,
        cur: Any,
        *,
        date_from: str | None,
        date_to: str | None,
        periods: list[dict[str, Any]],
    ) -> dict[str, Any]:
        if not date_from and not date_to and periods:
            date_from = str(periods[0]["dateFrom"])
            date_to = str(periods[0]["dateTo"])
        elif date_from and not date_to:
            date_to = date_from
        elif date_to and not date_from:
            date_from = date_to

        if not date_from or not date_to:
            today = date.today().isoformat()
            date_from = date_to = today

        if date_to < date_from:
            date_from, date_to = date_to, date_from

        cur.execute(
            """
            SELECT COUNT(*) AS rowCount
            FROM flyingfish_aux.driver_behaviour
            WHERE Date_From <= %s AND Date_To >= %s
            """,
            (date_to, date_from),
        )
        row_count = as_int((cur.fetchone() or {}).get("rowCount"))
        return {"dateFrom": date_from, "dateTo": date_to, "rowCount": row_count}

    def _source_data_checks(self, cur: Any, *, date_from: str, date_to: str, mode: str) -> dict[str, list[Any]]:
        if mode == "period":
            sources = [
                {
                    "key": "driverBehaviour",
                    "label": "Driver behaviour",
                    "table": "flyingfish_aux.driver_behaviour",
                    "sql": """
                        SELECT COUNT(*) AS rowCount
                        FROM flyingfish_aux.driver_behaviour
                        WHERE Date_From = %s AND Date_To = %s
                    """,
                },
                {
                    "key": "speedLeague",
                    "label": "Speed league",
                    "table": "flyingfish_aux.speed_league",
                    "sql": """
                        SELECT COUNT(*) AS rowCount
                        FROM flyingfish_aux.speed_league
                        WHERE Date_From = %s AND Date_To = %s
                    """,
                },
                {
                    "key": "carbonReport",
                    "label": "Carbon report",
                    "table": "flyingfish_aux.carbon_report",
                    "sql": """
                        SELECT COUNT(*) AS rowCount
                        FROM flyingfish_aux.carbon_report
                        WHERE Date_from = %s AND Date_to = %s
                    """,
                },
                {
                    "key": "routeDetails",
                    "label": "Route assignments",
                    "table": "flyingfish_aux.route_details_export",
                    "sql": """
                        SELECT COUNT(*) AS rowCount
                        FROM flyingfish_aux.route_details_export
                        WHERE despatchdate BETWEEN %s AND %s
                    """,
                },
            ]
            date_label = f"{date_from} to {date_to}"
        else:
            sources = [
                {
                    "key": "driverBehaviour",
                    "label": "Driver behaviour",
                    "table": "flyingfish_aux.driver_behaviour",
                    "sql": """
                        SELECT COUNT(*) AS rowCount
                        FROM flyingfish_aux.driver_behaviour
                        WHERE Date_From <= %s AND Date_To >= %s
                    """,
                },
                {
                    "key": "speedLeague",
                    "label": "Speed league",
                    "table": "flyingfish_aux.speed_league",
                    "sql": """
                        SELECT COUNT(*) AS rowCount
                        FROM flyingfish_aux.speed_league
                        WHERE Date_From <= %s AND Date_To >= %s
                    """,
                },
                {
                    "key": "routeDetails",
                    "label": "Route assignments",
                    "table": "flyingfish_aux.route_details_export",
                    "sql": """
                        SELECT COUNT(*) AS rowCount
                        FROM flyingfish_aux.route_details_export
                        WHERE despatchdate BETWEEN %s AND %s
                    """,
                },
            ]
            date_label = f"{date_from} to {date_to}"

        checks: list[dict[str, Any]] = []
        warnings: list[str] = []
        for source in sources:
            params = (date_to, date_from) if mode == "range" and source["key"] != "routeDetails" else (date_from, date_to)
            cur.execute(source["sql"], params)
            row_count = as_int((cur.fetchone() or {}).get("rowCount"))
            check = {
                "key": source["key"],
                "label": source["label"],
                "table": source["table"],
                "dateFrom": date_from,
                "dateTo": date_to,
                "rowCount": row_count,
                "status": "available" if row_count else "missing",
            }
            checks.append(check)
            if not row_count:
                warnings.append(
                    f"No {source['label'].lower()} data found in {source['table']} "
                    f"for {date_label}; report results may be incomplete."
                )
        return {"checks": checks, "warnings": warnings}

    def _speeding_report_rows(self, cur: Any, *, report_range: dict[str, Any]) -> list[dict[str, Any]]:
        cur.execute(
            """
            SELECT
                db.Date_From AS dateFrom,
                db.Date_To AS dateTo,
                db.Registration AS registration,
                db.Driver AS driver,
                db.Speeding AS speeding,
                db.Idling AS idling,
                db.Harsh_Braking AS harshBraking,
                db.Harsh_Acceleration AS harshAcceleration,
                db.Miles AS miles,
                db.Score AS score,
                sl.id AS speedRowId,
                sl.Total_Breaches AS totalBreaches,
                sl.Points_Accrued AS pointsAccrued,
                sl.Points_Mile AS pointsPerMile,
                sl.Max_Speed AS maxSpeed,
                sl.`20_Mph_Zone` AS zone20,
                sl.`30_Mph_Zone` AS zone30,
                sl.`40_Mph_Zone` AS zone40,
                sl.`50_Mph_Zone` AS zone50,
                sl.`60_Mph_Zone` AS zone60,
                sl.`70_Mph_Zone` AS zone70
            FROM flyingfish_aux.driver_behaviour db
            LEFT JOIN flyingfish_aux.speed_league sl
                ON sl.Registration = db.Registration
                AND sl.Driver = db.Driver
                AND sl.Date_From = db.Date_From
                AND sl.Date_To = db.Date_To
            WHERE db.Date_From <= %s AND db.Date_To >= %s
                AND (
                    COALESCE(db.Speeding, 0) > 0
                    OR COALESCE(sl.Total_Breaches, 0) > 0
                    OR COALESCE(sl.Points_Accrued, 0) > 0
                )
            ORDER BY sl.Points_Accrued DESC, sl.Total_Breaches DESC, db.Speeding DESC, db.Registration
            LIMIT 1000
            """,
            (report_range["dateTo"], report_range["dateFrom"]),
        )
        return [_json_row(r) for r in cur.fetchall()]

    def _summary(self, cur: Any, *, period: dict[str, Any], leaderboard: list[dict[str, Any]]) -> dict[str, Any]:
        params = (period["dateFrom"], period["dateTo"])
        behaviour_params = (period["dateFrom"], period["dateTo"], LOW_DISTANCE_MILES)
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
                AND COALESCE(Miles, 0) >= %s
            """,
            behaviour_params,
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
        params: list[Any] = [period["dateFrom"], period["dateTo"], LOW_DISTANCE_MILES]
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
                AND COALESCE(db.Miles, 0) >= %s
            {filter_sql}
            ORDER BY db.Score DESC, sl.Points_Mile DESC, db.Speeding DESC, db.Registration
            LIMIT 250
            """,
            tuple(params),
        )
        rows = [normalize_driver_row(_json_row(r)) for r in cur.fetchall()]
        assignments_by_vrn = self._route_assignments_by_registration(
            cur,
            date_from=str(period["dateFrom"]),
            date_to=str(period["dateTo"]),
        )
        for row in rows:
            assignments = assignments_by_vrn.get(self._normalise_registration(row.get("registration")), [])
            row["assignments"] = assignments
            assignment_driver_names = self._compact(
                [
                    driver_name
                    for assignment in assignments
                    for driver_name in assignment.get("driverNames", [])
                ]
            )
            if assignment_driver_names:
                row["driver"] = ", ".join(assignment_driver_names)
        return rows

    def _route_assignments_by_registration(
        self,
        cur: Any,
        *,
        date_from: str,
        date_to: str,
    ) -> dict[str, list[dict[str, Any]]]:
        cur.execute(
            """
            SELECT
                rde.despatchdate AS despatchDate,
                rde.transportroutecode AS routeCode,
                COALESCE(NULLIF(TRIM(rl.Delivery_Route), ''), rde.transportroutecode) AS routeName,
                rde.registrationnumberdriver AS mainRegistration,
                rde.registrationnumbershunt AS shuntRegistration1,
                rde.registrationnumbershunt2 AS shuntRegistration2,
                rde.drivercode1 AS driverCode1,
                COALESCE(d1.Name, rde.drivercode1) AS driverName1,
                rde.drivercode2 AS driverCode2,
                COALESCE(d2.Name, rde.drivercode2) AS driverName2,
                rde.shuntdrivercode1 AS shuntDriverCode1,
                COALESCE(s1.Name, rde.shuntdrivercode1) AS shuntDriverName1,
                rde.shuntdrivercode2 AS shuntDriverCode2,
                COALESCE(s2.Name, rde.shuntdrivercode2) AS shuntDriverName2,
                rde.shuntdrivercode3 AS shuntDriverCode3,
                COALESCE(s3.Name, rde.shuntdrivercode3) AS shuntDriverName3,
                rde.shuntdrivercode4 AS shuntDriverCode4,
                COALESCE(s4.Name, rde.shuntdrivercode4) AS shuntDriverName4,
                rde.driverswaplocation AS driverSwapLocation,
                rde.shuntdriverswaplocation AS shuntSwapLocation
            FROM flyingfish_aux.route_details_export rde
            LEFT JOIN flyingfish_aux.route_lookup rl ON UPPER(TRIM(rl.Route_Code)) = UPPER(TRIM(rde.transportroutecode))
            LEFT JOIN flyingfish_aux.driver_lookup d1 ON UPPER(TRIM(d1.Driver_Code)) = UPPER(TRIM(rde.drivercode1))
            LEFT JOIN flyingfish_aux.driver_lookup d2 ON UPPER(TRIM(d2.Driver_Code)) = UPPER(TRIM(rde.drivercode2))
            LEFT JOIN flyingfish_aux.driver_lookup s1 ON UPPER(TRIM(s1.Driver_Code)) = UPPER(TRIM(rde.shuntdrivercode1))
            LEFT JOIN flyingfish_aux.driver_lookup s2 ON UPPER(TRIM(s2.Driver_Code)) = UPPER(TRIM(rde.shuntdrivercode2))
            LEFT JOIN flyingfish_aux.driver_lookup s3 ON UPPER(TRIM(s3.Driver_Code)) = UPPER(TRIM(rde.shuntdrivercode3))
            LEFT JOIN flyingfish_aux.driver_lookup s4 ON UPPER(TRIM(s4.Driver_Code)) = UPPER(TRIM(rde.shuntdrivercode4))
            WHERE rde.despatchdate BETWEEN %s AND %s
            ORDER BY rde.despatchdate, rde.transportroutecode
            """,
            (date_from, date_to),
        )
        out: dict[str, list[dict[str, Any]]] = {}
        for raw_row in cur.fetchall():
            row = _json_row(raw_row)
            self._append_assignment(
                out,
                registration=row.get("mainRegistration"),
                assignment={
                    "despatchDate": row.get("despatchDate"),
                    "role": "Main route",
                    "routeCode": row.get("routeCode"),
                    "routeName": row.get("routeName") or row.get("routeCode") or "Unknown route",
                    "driverCodes": self._compact([row.get("driverCode1"), row.get("driverCode2")]),
                    "driverNames": self._compact([row.get("driverName1"), row.get("driverName2")]),
                    "swapLocation": row.get("driverSwapLocation"),
                    "sourceRegistrationField": "registrationnumberdriver",
                },
            )
            self._append_assignment(
                out,
                registration=row.get("shuntRegistration1"),
                assignment={
                    "despatchDate": row.get("despatchDate"),
                    "role": "Shunt route",
                    "routeCode": row.get("routeCode"),
                    "routeName": row.get("routeName") or row.get("routeCode") or "Unknown route",
                    "driverCodes": self._compact([row.get("shuntDriverCode1"), row.get("shuntDriverCode2")]),
                    "driverNames": self._compact([row.get("shuntDriverName1"), row.get("shuntDriverName2")]),
                    "swapLocation": row.get("shuntSwapLocation"),
                    "sourceRegistrationField": "registrationnumbershunt",
                },
            )
            self._append_assignment(
                out,
                registration=row.get("shuntRegistration2"),
                assignment={
                    "despatchDate": row.get("despatchDate"),
                    "role": "Shunt route",
                    "routeCode": row.get("routeCode"),
                    "routeName": row.get("routeName") or row.get("routeCode") or "Unknown route",
                    "driverCodes": self._compact([row.get("shuntDriverCode3"), row.get("shuntDriverCode4")]),
                    "driverNames": self._compact([row.get("shuntDriverName3"), row.get("shuntDriverName4")]),
                    "swapLocation": row.get("shuntSwapLocation"),
                    "sourceRegistrationField": "registrationnumbershunt2",
                },
            )
        return out

    def _append_assignment(
        self,
        out: dict[str, list[dict[str, Any]]],
        *,
        registration: Any,
        assignment: dict[str, Any],
    ) -> None:
        normalised = self._normalise_registration(registration)
        if not normalised:
            return
        assignment["registration"] = str(registration).strip()
        assignment["driverText"] = ", ".join(assignment["driverNames"]) if assignment["driverNames"] else "Driver not assigned"
        out.setdefault(normalised, []).append(assignment)

    def _assignment_driver_identities(self, assignments: list[dict[str, Any]]) -> list[dict[str, str]]:
        out: list[dict[str, str]] = []
        seen: set[str] = set()
        for assignment in assignments:
            codes = assignment.get("driverCodes", [])
            names = assignment.get("driverNames", [])
            max_len = max(len(codes), len(names))
            for index in range(max_len):
                code = str(codes[index] if index < len(codes) else "").strip()
                name = str(names[index] if index < len(names) else "").strip()
                if not code and not name:
                    continue
                key = (code or name).upper()
                if key in seen:
                    continue
                seen.add(key)
                label = f"{name} ({code})" if name and code and name != code else name or code
                out.append({"key": key, "code": code, "name": name or code, "label": label})
        return out

    def _finalise_offender_rollup(self, rollup: dict[str, Any]) -> dict[str, Any]:
        evidence = sorted(
            rollup.get("evidence", []),
            key=lambda r: (
                as_int(r.get("pointsAccrued")),
                as_int(r.get("totalBreaches")),
                as_int(r.get("speeding")),
                as_int(r.get("maxSpeed")),
            ),
            reverse=True,
        )
        miles = round(as_float(rollup.get("miles")) or 0.0, 1)
        speeding = as_int(rollup.get("speeding"))
        return {
            "driverCode": rollup.get("driverCode") or "",
            "driverName": rollup.get("driverName") or "",
            "driverLabel": rollup.get("driverLabel") or "",
            "vanPeriods": as_int(rollup.get("vanPeriods")),
            "sharedAttributions": as_int(rollup.get("sharedAttributions")),
            "speeding": speeding,
            "totalBreaches": as_int(rollup.get("totalBreaches")),
            "pointsAccrued": as_int(rollup.get("pointsAccrued")),
            "maxSpeed": as_int(rollup.get("maxSpeed")),
            "miles": miles,
            "speedingPer100Miles": self._rate(speeding, miles),
            "speedingPer100Km": self._rate_per_100km(speeding, miles),
            "vehicles": self._compact(sorted(v for v in rollup.get("vehicles", []) if v)),
            "periods": self._compact(sorted(p for p in rollup.get("periods", []) if p)),
            "routes": self._compact(sorted(r for r in rollup.get("routes", []) if r)),
            "topEvidence": evidence[:5],
        }

    @staticmethod
    def _rate_per_100km(value: Any, miles: Any) -> float | None:
        miles_float = as_float(miles)
        if miles_float is None or miles_float <= 0:
            return None
        kilometres = miles_float * 1.609344
        return round((as_float(value) or 0.0) * 100.0 / kilometres, 2)

    @staticmethod
    def _date_in_range(value: Any, date_from: Any, date_to: Any) -> bool:
        text = str(value or "")
        return bool(text) and str(date_from or "") <= text <= str(date_to or "")

    @staticmethod
    def _zone_text(row: dict[str, Any]) -> str:
        zones = []
        for label, key in (
            ("20", "zone20"),
            ("30", "zone30"),
            ("40", "zone40"),
            ("50", "zone50"),
            ("60", "zone60"),
            ("70", "zone70"),
        ):
            value = as_int(row.get(key))
            if value:
                zones.append(f"{label}: {value}")
        return ", ".join(zones) if zones else "-"

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
    def _normalise_registration(value: Any) -> str:
        return "".join(str(value or "").upper().strip().split())

    @staticmethod
    def _compact(values: list[Any]) -> list[str]:
        out: list[str] = []
        seen: set[str] = set()
        for value in values:
            text = str(value or "").strip()
            if not text or text in seen:
                continue
            seen.add(text)
            out.append(text)
        return out

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
