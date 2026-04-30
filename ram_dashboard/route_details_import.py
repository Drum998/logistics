from __future__ import annotations

import csv
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Iterator, TextIO

import mysql.connector

from .config import DbSettings


ROUTE_DETAILS_COLUMNS = [
    "despatchdate",
    "transportroutecode",
    "registrationnumbershunt",
    "registrationnumberdriver",
    "shuntdrivercode1",
    "shuntdrivercode2",
    "drivercode1",
    "drivercode2",
    "shuntdriverswaplocation",
    "driverswaplocation",
    "registrationnumbershunt2",
    "shuntdrivercode3",
    "shuntdrivercode4",
]


@dataclass(frozen=True)
class RouteDetailsRow:
    line_number: int
    values: dict[str, Any]


@dataclass(frozen=True)
class RouteDetailsError:
    line_number: int
    message: str


@dataclass
class RouteDetailsParseResult:
    rows: list[RouteDetailsRow] = field(default_factory=list)
    errors: list[RouteDetailsError] = field(default_factory=list)

    @property
    def invalid_count(self) -> int:
        return len(self.errors)


@dataclass
class RouteDetailsImportSummary:
    uploaded_rows: int = 0
    inserted_rows: int = 0
    skipped_duplicate_rows: int = 0
    invalid_rows: int = 0
    errors: list[RouteDetailsError] = field(default_factory=list)
    preview_rows: list[dict[str, Any]] = field(default_factory=list)


class RouteDetailsImportError(ValueError):
    pass


def parse_route_details_csv(csv_file: TextIO) -> RouteDetailsParseResult:
    reader = csv.DictReader(csv_file)
    if reader.fieldnames is None:
        raise RouteDetailsImportError("The uploaded CSV is empty.")

    missing_columns = [col for col in ROUTE_DETAILS_COLUMNS if col not in reader.fieldnames]
    if missing_columns:
        raise RouteDetailsImportError(
            "The uploaded CSV is missing required column(s): " + ", ".join(missing_columns)
        )

    result = RouteDetailsParseResult()
    for row in reader:
        line_number = reader.line_num
        try:
            values = _transform_row(row)
        except ValueError as e:
            result.errors.append(RouteDetailsError(line_number=line_number, message=str(e)))
            continue
        result.rows.append(RouteDetailsRow(line_number=line_number, values=values))

    return result


def _transform_row(row: dict[str, str | None]) -> dict[str, Any]:
    values: dict[str, Any] = {}
    values["despatchdate"] = _parse_despatch_date(_clean(row.get("despatchdate")))
    for column in ROUTE_DETAILS_COLUMNS[1:]:
        values[column] = _clean(row.get(column))
    return values


def _parse_despatch_date(value: str) -> date:
    if not value:
        raise ValueError("despatchdate is required.")
    for date_format in ("%d/%m/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(value, date_format).date()
        except ValueError:
            pass
    raise ValueError(f"Invalid despatchdate {value!r}; expected DD/MM/YYYY.")


def _clean(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def display_value(value: Any) -> str:
    if isinstance(value, date):
        return value.strftime("%d/%m/%Y")
    return str(value or "")


class RouteDetailsImporter:
    def __init__(self, settings: DbSettings):
        self.settings = settings

    @contextmanager
    def _connection(self) -> Iterator[Any]:
        conn = mysql.connector.connect(
            host=self.settings.host,
            port=self.settings.port,
            user=self.settings.user,
            password=self.settings.password,
            database=self.settings.database,
            use_pure=True,
        )
        try:
            yield conn
        finally:
            conn.close()

    def import_rows(self, parse_result: RouteDetailsParseResult) -> RouteDetailsImportSummary:
        summary = RouteDetailsImportSummary(
            uploaded_rows=len(parse_result.rows) + parse_result.invalid_count,
            invalid_rows=parse_result.invalid_count,
            errors=parse_result.errors[:20],
            preview_rows=[_display_row(row.values) for row in parse_result.rows[:10]],
        )

        if not parse_result.rows:
            return summary

        with self._connection() as conn:
            cur = conn.cursor()
            try:
                for row in parse_result.rows:
                    if self._row_exists(cur, row.values):
                        summary.skipped_duplicate_rows += 1
                        continue
                    self._insert_row(cur, row.values)
                    summary.inserted_rows += 1
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                cur.close()

        return summary

    def _row_exists(self, cur: Any, values: dict[str, Any]) -> bool:
        where_clause = " AND ".join(
            f"COALESCE(`{column}`, '') = %s" if column != "despatchdate" else f"`{column}` = %s"
            for column in ROUTE_DETAILS_COLUMNS
        )
        cur.execute(
            f"""
            SELECT 1
            FROM flyingfish_aux.route_details_export
            WHERE {where_clause}
            LIMIT 1
            """,
            tuple(values[column] for column in ROUTE_DETAILS_COLUMNS),
        )
        return cur.fetchone() is not None

    def _insert_row(self, cur: Any, values: dict[str, Any]) -> None:
        columns_sql = ", ".join(f"`{column}`" for column in ROUTE_DETAILS_COLUMNS)
        placeholders = ", ".join(["%s"] * len(ROUTE_DETAILS_COLUMNS))
        cur.execute(
            f"""
            INSERT INTO flyingfish_aux.route_details_export ({columns_sql})
            VALUES ({placeholders})
            """,
            tuple(values[column] for column in ROUTE_DETAILS_COLUMNS),
        )


def _display_row(values: dict[str, Any]) -> dict[str, str]:
    return {column: display_value(values[column]) for column in ROUTE_DETAILS_COLUMNS}
