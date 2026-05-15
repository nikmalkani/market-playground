from __future__ import annotations

import argparse
import json
import os
import secrets
import sqlite3
import threading
from datetime import date, datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from time import monotonic
from typing import Any
from urllib.parse import parse_qs, urlparse
from zoneinfo import ZoneInfo

from .config import Settings
from .tracking import (
    METRICS_HTML,
    build_common_legs_payload,
    build_overview_payload,
    build_recent_runs_payload,
    build_timeseries_payload,
    ensure_tracking_db,
    insert_tracking_event,
    parse_metrics_date_range,
)

_PROD_SHARE_DB_URL_ENV = "STRATEGY_SHARE_DB_URL"
_PROD_SHARE_DB_DEFAULT_URL = "sqlite:///strategy_shares.db"
_MAX_STRATEGY_SHARE_BODY_BYTES = 1_500_000
_MAX_CONTRACT_LIMIT = 2_000
_MAX_STRATEGY_PLAN_DATES = 750
_METADATA_CACHE_TTL_SECONDS = 300

_OPTION_DB_PERFORMANCE_INDEXES = (
    """
    CREATE INDEX IF NOT EXISTS ix_spx_option_snapshots_symbol_streamer_ts
    ON spx_option_snapshots (symbol, streamer_symbol, snapshot_ts)
    """,
    """
    CREATE INDEX IF NOT EXISTS ix_spx_option_snapshots_symbol_type_dte_ts
    ON spx_option_snapshots (symbol, option_type, dte, snapshot_ts)
    """,
    """
    CREATE INDEX IF NOT EXISTS ix_spx_option_snapshots_symbol_snapshot_date
    ON spx_option_snapshots (symbol, date(snapshot_ts))
    """,
    """
    CREATE INDEX IF NOT EXISTS ix_spx_option_snapshots_symbol_option_type
    ON spx_option_snapshots (symbol, option_type)
    """,
)

_METADATA_CACHE_LOCK = threading.Lock()
_METADATA_CACHE: dict[tuple[str, str, int], tuple[float, dict[str, Any]]] = {}


def _resolve_sqlite_path(db_url: str) -> Path:
    prefix = "sqlite:///"
    if not db_url.startswith(prefix):
        raise ValueError(
            f"SQL UI currently supports sqlite only. DB_URL was: {db_url!r}"
        )
    raw_path = db_url[len(prefix) :]
    return Path(raw_path).expanduser().resolve()


def _assert_env_file_permissions(env_file: str = ".env") -> None:
    env_path = Path(env_file).expanduser()
    if not env_path.exists():
        return
    if not env_path.is_file():
        raise PermissionError(f"{env_path} exists but is not a regular file.")
    if env_path.stat().st_mode & 0o077:
        raise PermissionError(
            f"Insecure permissions on {env_path}: expected 600, got "
            f"{oct(env_path.stat().st_mode & 0o777)}"
        )


def _json_response(
    handler: BaseHTTPRequestHandler, payload: dict[str, Any], status: int = 200
) -> None:
    data = json.dumps(payload, default=str).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(data)))
    handler.end_headers()
    handler.wfile.write(data)


def _html_response(handler: BaseHTTPRequestHandler, html: str) -> None:
    data = html.encode("utf-8")
    handler.send_response(200)
    handler.send_header("Content-Type", "text/html; charset=utf-8")
    handler.send_header("Content-Length", str(len(data)))
    handler.end_headers()
    handler.wfile.write(data)


def _render_app_html(*, tracking_enabled: bool) -> str:
    return _HTML.replace("__TRACKING_ENABLED__", "true" if tracking_enabled else "false")


def _connect_options_db(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = 30000")
    conn.execute("PRAGMA temp_store = MEMORY")
    conn.execute("PRAGMA cache_size = -64000")
    conn.execute("PRAGMA query_only = ON")
    return conn


def ensure_option_query_performance(db_path: Path) -> None:
    with sqlite3.connect(db_path, timeout=30) as conn:
        conn.execute("PRAGMA busy_timeout = 30000")
        for statement in _OPTION_DB_PERFORMANCE_INDEXES:
            conn.execute(statement)
        conn.execute("PRAGMA optimize")
        conn.commit()


def _sqlite_cache_version(db_path: Path) -> int:
    versions = [db_path.stat().st_mtime_ns]
    for suffix in ("-wal", "-shm"):
        sidecar = Path(f"{db_path}{suffix}")
        try:
            versions.append(sidecar.stat().st_mtime_ns)
        except FileNotFoundError:
            pass
    return max(versions)


def _cached_metadata_payload(
    db_path: Path,
    cache_key: str,
    builder: Any,
) -> dict[str, Any]:
    version = _sqlite_cache_version(db_path)
    key = (str(db_path), cache_key, version)
    now = monotonic()
    with _METADATA_CACHE_LOCK:
        cached = _METADATA_CACHE.get(key)
        if cached is not None and now - cached[0] <= _METADATA_CACHE_TTL_SECONDS:
            return cached[1]

    with _connect_options_db(db_path) as conn:
        payload = builder(conn)

    with _METADATA_CACHE_LOCK:
        stale_keys = [
            existing_key
            for existing_key in _METADATA_CACHE
            if existing_key[0] == key[0] and existing_key[1] == key[1]
        ]
        for stale_key in stale_keys:
            _METADATA_CACHE.pop(stale_key, None)
        _METADATA_CACHE[key] = (now, payload)
    return payload


def _schema_payload(conn: sqlite3.Connection) -> dict[str, Any]:
    tables = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()
    table_names = [str(t[0]) for t in tables]
    schema: dict[str, list[dict[str, Any]]] = {}
    for table in table_names:
        rows = conn.execute(f"PRAGMA table_info('{table}')").fetchall()
        schema[table] = [
            {
                "cid": r[0],
                "name": r[1],
                "type": r[2],
                "notnull": r[3],
                "default": r[4],
                "pk": r[5],
            }
            for r in rows
        ]
    return {"tables": table_names, "schema": schema}


def _parse_datetime(value: str | None, label: str) -> datetime | None:
    if not value:
        return None
    s = value.strip()
    if not s:
        return None
    if s.endswith("Z"):
        s = f"{s[:-1]}+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except ValueError as exc:
        raise ValueError(f"Invalid {label}. Expected ISO datetime.") from exc

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=ZoneInfo("America/New_York"))
    return dt.astimezone(timezone.utc)


def _sqlite_timestamp(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _parse_float(value: str | None, label: str) -> float | None:
    if value is None:
        return None
    s = value.strip()
    if not s:
        return None
    try:
        return float(s)
    except ValueError as exc:
        raise ValueError(f"Invalid {label}.") from exc


def _parse_int(value: str | None, label: str, fallback: int) -> int:
    if value is None:
        return fallback
    s = value.strip()
    if not s:
        return fallback
    try:
        return int(s)
    except ValueError as exc:
        raise ValueError(f"Invalid {label}.") from exc


def _parse_int_required(value: str | None, label: str) -> int:
    if value is None:
        raise ValueError(f"Missing required {label}.")
    s = value.strip()
    if not s:
        raise ValueError(f"Missing required {label}.")
    try:
        return int(s)
    except ValueError as exc:
        raise ValueError(f"Invalid {label}.") from exc


def _parse_float_required(value: str | None, label: str) -> float:
    if value is None:
        raise ValueError(f"Missing required {label}.")
    s = value.strip()
    if not s:
        raise ValueError(f"Missing required {label}.")
    try:
        return float(s)
    except ValueError as exc:
        raise ValueError(f"Invalid {label}.") from exc


def _parse_date(value: str | None, label: str) -> date | None:
    if value is None:
        return None
    s = value.strip()
    if not s:
        return None
    try:
        return date.fromisoformat(s)
    except ValueError as exc:
        raise ValueError(f"Invalid {label}. Expected YYYY-MM-DD.") from exc


def _parse_est_hhmm(value: str | None, label: str) -> tuple[int, int]:
    if value is None:
        raise ValueError(f"Missing required {label}.")
    s = value.strip()
    if not s:
        raise ValueError(f"Missing required {label}.")
    try:
        hour_min = datetime.strptime(s, "%H:%M").time()
    except ValueError as exc:
        raise ValueError(f"Invalid {label}. Expected HH:MM.") from exc
    return hour_min.hour, hour_min.minute


def _resolve_latest_option_date(conn: sqlite3.Connection, symbol: str) -> date | None:
    row = conn.execute(
        "SELECT date(MAX(snapshot_ts)) FROM spx_option_snapshots WHERE symbol = ?",
        [symbol],
    ).fetchone()
    if not row or not row[0]:
        return None
    try:
        return date.fromisoformat(str(row[0]))
    except ValueError:
        return None


def _run_snapshot_dates_payload(conn: sqlite3.Connection, *, symbol: str) -> dict[str, Any]:
    rows = conn.execute(
        "SELECT DISTINCT date(snapshot_ts) AS snapshot_date FROM spx_option_snapshots WHERE symbol = ? ORDER BY snapshot_date ASC",
        [symbol],
    ).fetchall()
    return {"dates": [str(row[0]) for row in rows if row[0]]}


def _run_option_types_payload(conn: sqlite3.Connection, *, symbol: str) -> dict[str, Any]:
    rows = conn.execute(
        """
        SELECT DISTINCT option_type
        FROM spx_option_snapshots
        WHERE symbol = ?
          AND option_type IS NOT NULL
          AND TRIM(option_type) <> ''
        ORDER BY option_type ASC
        """,
        [symbol],
    ).fetchall()
    return {"option_types": [str(row[0]).upper() for row in rows if row[0]]}


def _run_resolve_leg_payload(
    conn: sqlite3.Connection,
    *,
    symbol: str,
    option_type: str,
    dte: int,
    target_delta: float,
    entry_time: str,
    entry_date: date | None = None,
    target_side: str | None = None,
    snapshot_from: datetime | None = None,
    snapshot_to: datetime | None = None,
    window_minutes: int = 5,
    strict_dte: bool = False,
    best_only: bool = False,
) -> dict[str, Any]:
    opt_type = option_type.upper()
    if opt_type not in {"PUT", "CALL"}:
        raise ValueError("option_type must be PUT or CALL.")

    normalized_delta = abs(target_delta)
    if normalized_delta > 1:
        normalized_delta /= 100

    effective_side = target_side.upper() if target_side else None
    if effective_side and effective_side not in {"BUY", "SELL"}:
        raise ValueError("target_side must be BUY or SELL.")

    if entry_date is None:
        latest_date = _resolve_latest_option_date(conn, symbol)
        if latest_date is None:
            raise ValueError("No option snapshots found for this symbol.")
        entry_date = latest_date

    hh, mm = _parse_est_hhmm(entry_time, "entry_time")
    entry_local = datetime(
        year=entry_date.year,
        month=entry_date.month,
        day=entry_date.day,
        hour=hh,
        minute=mm,
        second=0,
        tzinfo=ZoneInfo("America/New_York"),
    )
    entry_utc = entry_local.astimezone(timezone.utc)
    entry_epoch = int(entry_utc.timestamp())

    default_from = entry_utc - timedelta(minutes=window_minutes)
    default_to = entry_utc + timedelta(minutes=window_minutes)
    window_from = _sqlite_timestamp(snapshot_from.astimezone(timezone.utc)) if snapshot_from else _sqlite_timestamp(default_from)
    window_to = _sqlite_timestamp(snapshot_to.astimezone(timezone.utc)) if snapshot_to else _sqlite_timestamp(default_to)

    def query_candidates(dte_min: int, dte_max: int) -> list[Any]:
        rows = conn.execute(
            f"""
        SELECT
            streamer_symbol,
            option_type,
            strike_price,
            expiration_date,
            dte,
            delta,
            snapshot_ts,
            mid_price,
            bid_price,
            ask_price,
            ABS(ABS(delta) - ?) AS delta_diff,
            ABS(CAST(strftime('%s', snapshot_ts) AS INTEGER) - ?) AS time_diff,
            ABS(dte - ?) AS dte_diff,
            CASE WHEN CAST(strftime('%s', snapshot_ts) AS INTEGER) <= ? THEN 0 ELSE 1 END AS is_after
        FROM spx_option_snapshots
        WHERE symbol = ?
          AND option_type = ?
          AND dte BETWEEN ? AND ?
          AND delta IS NOT NULL
          AND snapshot_ts BETWEEN ? AND ?
        ORDER BY time_diff ASC, is_after ASC, delta_diff ASC, dte_diff ASC, strike_price ASC
        {"LIMIT 1" if best_only else ""}
        """,
            [
            normalized_delta,
            entry_epoch,
            dte,
            entry_epoch,
            symbol,
            opt_type,
            dte_min,
            dte_max,
            window_from,
            window_to,
            ],
        ).fetchall()

        if best_only:
            return rows

        by_streamer: dict[str, Any] = {}
        for row in rows:
            streamer = str(row[0])
            if streamer not in by_streamer:
                by_streamer[streamer] = row
        return list(by_streamer.values())

    rows = query_candidates(dte, dte)
    if not rows and not strict_dte:
        rows = query_candidates(max(0, dte - 1), dte + 1)

    if not rows:
        if strict_dte:
            raise ValueError(
                f"No exact DTE={dte} contract found for this leg within {window_minutes} minutes of the requested entry time."
            )
        raise ValueError(
            f"No matching contract for this leg within {window_minutes} minutes of the requested entry time."
        )

    contracts: list[dict[str, Any]] = []
    for row in rows:
        value = row[7]
        if value is None and row[8] is not None and row[9] is not None:
            value = (row[8] + row[9]) / 2.0

        delta_diff = float(row[10]) if row[10] is not None else None
        time_diff = float(row[11]) if row[11] is not None else None
        score = None
        if delta_diff is not None and time_diff is not None:
            score = round(delta_diff * 100 + (time_diff / 60.0), 4)

        contracts.append(
            {
                "symbol": symbol,
                "streamer_symbol": row[0],
                "option_type": row[1],
                "strike_price": row[2],
                "expiration_date": row[3],
                "dte": row[4],
                "delta": row[5],
                "snapshot_ts": row[6],
                "value": value,
                "target_dte": dte,
                "target_delta": normalized_delta,
                "delta_diff": delta_diff,
                "time_diff_seconds": time_diff,
                "entry_snapshot_ts": row[6],
                "entry_date": str(entry_date),
                "entry_time": entry_time,
                "entry_timezone": "America/New_York",
                "score": score,
                "window_minutes": window_minutes,
                "target_side": effective_side,
                "label": f"{row[1]} {row[2]} {row[3]}",
            }
        )

    # Keep top-level contract keys for backward compatibility while adding full match set.
    best = contracts[0]
    if best_only:
        return {
            **best,
            "count": len(contracts),
        }
    return {
        **best,
        "count": len(contracts),
        "contracts": contracts,
    }


def _run_contracts_payload(
    conn: sqlite3.Connection,
    *,
    symbol: str,
    start_dt: datetime | None = None,
    end_dt: datetime | None = None,
    option_type: str | None = None,
    min_strike: float | None = None,
    max_strike: float | None = None,
    limit: int = 400,
) -> dict[str, Any]:
    limit = max(1, min(limit, _MAX_CONTRACT_LIMIT))
    clauses = ["symbol = ?"]
    params: list[Any] = [symbol]

    if start_dt is not None:
        clauses.append("snapshot_ts >= ?")
        params.append(_sqlite_timestamp(start_dt))
    if end_dt is not None:
        clauses.append("snapshot_ts <= ?")
        params.append(_sqlite_timestamp(end_dt))
    if option_type:
        clauses.append("option_type = ?")
        params.append(option_type.upper())
    if min_strike is not None:
        clauses.append("strike_price >= ?")
        params.append(min_strike)
    if max_strike is not None:
        clauses.append("strike_price <= ?")
        params.append(max_strike)

    where = " AND ".join(clauses)
    rows = conn.execute(
        f"""
        SELECT
            streamer_symbol,
            option_type,
            strike_price,
            expiration_date,
            MIN(snapshot_ts) AS first_ts,
            MAX(snapshot_ts) AS last_ts,
            COUNT(*) AS points
        FROM spx_option_snapshots
        WHERE {where}
        GROUP BY streamer_symbol, option_type, strike_price, expiration_date
        ORDER BY last_ts DESC, option_type, strike_price, expiration_date
        LIMIT ?
        """,
        [*params, limit],
    ).fetchall()

    return {
        "count": len(rows),
        "contracts": [
            {
                "streamer_symbol": str(row[0]),
                "option_type": row[1],
                "strike_price": row[2],
                "expiration_date": row[3],
                "first_ts": row[4],
                "last_ts": row[5],
                "points": row[6],
            }
            for row in rows
        ],
    }


def _run_series_payload(
    conn: sqlite3.Connection,
    *,
    symbol: str,
    streamers: list[str],
    start_dt: datetime | None = None,
    end_dt: datetime | None = None,
    field: str = "mid_price",
) -> dict[str, Any]:
    if not streamers:
        return {"rows": [], "count": 0}
    if len(streamers) > 120:
        raise ValueError("At most 120 streamers supported per call.")

    allowed_fields = {
        "mid_price": "mid_price",
        "bid_price": "bid_price",
        "ask_price": "ask_price",
    }
    if field not in allowed_fields:
        raise ValueError("Invalid field.")

    value_expr = f"COALESCE({allowed_fields[field]}, (bid_price + ask_price) / 2.0)"
    placeholders = ",".join(["?"] * len(streamers))
    clauses = [
        "symbol = ?",
        f"streamer_symbol IN ({placeholders})",
        "(mid_price IS NOT NULL OR (bid_price IS NOT NULL AND ask_price IS NOT NULL))",
    ]
    params: list[Any] = [symbol, *streamers]

    if start_dt is not None:
        clauses.append("snapshot_ts >= ?")
        params.append(_sqlite_timestamp(start_dt))
    if end_dt is not None:
        clauses.append("snapshot_ts <= ?")
        params.append(_sqlite_timestamp(end_dt))

    where = " AND ".join(clauses)
    rows = conn.execute(
        f"""
        SELECT
            snapshot_ts,
            streamer_symbol,
            mid_price,
            bid_price,
            ask_price,
            strike_price,
            option_type,
            expiration_date,
            delta,
            gamma,
            theta,
            vega,
            volatility,
            {value_expr} AS value
        FROM spx_option_snapshots
        WHERE {where}
        ORDER BY streamer_symbol, snapshot_ts
        """,
        params,
    ).fetchall()

    return {"count": len(rows), "rows": [dict(row) for row in rows]}


def _run_summary_payload(
    conn: sqlite3.Connection,
    *,
    symbol: str,
    start_dt: datetime | None = None,
    end_dt: datetime | None = None,
) -> dict[str, Any]:
    option_clauses = ["symbol = ?"]
    option_params: list[Any] = [symbol]

    if start_dt is not None:
        option_clauses.append("snapshot_ts >= ?")
        option_params.append(_sqlite_timestamp(start_dt))
    if end_dt is not None:
        option_clauses.append("snapshot_ts <= ?")
        option_params.append(_sqlite_timestamp(end_dt))

    option_where = " AND ".join(option_clauses)
    option_stats = conn.execute(
        f"""
        SELECT
            COUNT(*) AS option_rows,
            COUNT(DISTINCT streamer_symbol) AS contract_count,
            MIN(snapshot_ts) AS first_ts,
            MAX(snapshot_ts) AS last_ts
        FROM spx_option_snapshots
        WHERE {option_where}
        """,
        option_params,
    ).fetchone()

    market_clauses = ["symbol = ?"]
    market_params: list[Any] = [symbol]
    if start_dt is not None:
        market_clauses.append("snapshot_ts >= ?")
        market_params.append(_sqlite_timestamp(start_dt))
    if end_dt is not None:
        market_clauses.append("snapshot_ts <= ?")
        market_params.append(_sqlite_timestamp(end_dt))
    market_where = " AND ".join(market_clauses)

    market_rows = conn.execute(
        f"""
        SELECT snapshot_ts, spot_price, implied_volatility_index
        FROM spx_market_snapshots
        WHERE {market_where}
        ORDER BY snapshot_ts
        """,
        market_params,
    ).fetchall()

    return {
        "option_rows": option_stats[0] if option_stats else 0,
        "contract_count": option_stats[1] if option_stats else 0,
        "first_ts": option_stats[2] if option_stats else None,
        "last_ts": option_stats[3] if option_stats else None,
        "market_series": [dict(row) for row in market_rows],
    }


def _parse_strategy_leg_payload(value: Any, index: int) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"leg[{index}] must be an object.")

    side = str(value.get("side", "")).upper()
    if side not in {"BUY", "SELL"}:
        raise ValueError(f"leg[{index}] must specify side BUY or SELL.")

    option_type = str(value.get("option_type", "PUT")).upper()
    if option_type not in {"PUT", "CALL"}:
        raise ValueError(f"leg[{index}] option_type must be PUT or CALL.")

    dte = _parse_int_required(str(value.get("dte")), "leg.dte")
    if dte < 0:
        raise ValueError(f"leg[{index}] dte must be >= 0.")

    target_delta = _parse_float_required(str(value.get("target_delta")), "leg.target_delta")
    entry_time = str(value.get("entry_time", "")).strip()
    if not entry_time:
        raise ValueError(f"leg[{index}] entry_time is required.")
    _parse_est_hhmm(entry_time, f"leg[{index}].entry_time")

    quantity = _parse_int(str(value.get("quantity", "1")), "leg.quantity", 1)
    if quantity <= 0:
        raise ValueError(f"leg[{index}] quantity must be > 0.")

    return {
        "side": side,
        "option_type": option_type,
        "dte": dte,
        "target_delta": target_delta,
        "entry_time": entry_time,
        "quantity": quantity,
    }


def _parse_strategy_plan_leg_payload(value: Any, index: int) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"leg[{index}] must be an object.")
    normalized = dict(value)
    if "dte" not in normalized and "target_dte" in normalized:
        normalized["dte"] = normalized["target_dte"]
    return _parse_strategy_leg_payload(normalized, index)


def _parse_strategy_plan_dates(value: Any) -> list[date]:
    if not isinstance(value, list) or not value:
        raise ValueError("trade_dates must be a non-empty array.")
    if len(value) > _MAX_STRATEGY_PLAN_DATES:
        raise ValueError(f"At most {_MAX_STRATEGY_PLAN_DATES} trade dates are supported.")

    dates: list[date] = []
    seen: set[date] = set()
    for index, item in enumerate(value):
        parsed = _parse_date(str(item), f"trade_dates[{index}]")
        if parsed is None:
            raise ValueError(f"trade_dates[{index}] is required.")
        if parsed in seen:
            continue
        seen.add(parsed)
        dates.append(parsed)
    dates.sort()
    return dates


def _run_strategy_plan_payload(
    conn: sqlite3.Connection,
    *,
    symbol: str,
    legs: list[dict[str, Any]],
    trade_dates: list[date],
    window_minutes: int = 5,
) -> dict[str, Any]:
    if not legs:
        raise ValueError("At least one strategy leg is required.")
    if not trade_dates:
        raise ValueError("At least one trade date is required.")
    if window_minutes <= 0:
        raise ValueError("window_minutes must be > 0.")

    trade_plans: list[dict[str, Any]] = []
    skipped_dates = 0
    for trade_date in trade_dates:
        plan_legs: list[dict[str, Any]] = []
        for leg in legs:
            try:
                resolved = _run_resolve_leg_payload(
                    conn,
                    symbol=symbol,
                    option_type=leg["option_type"],
                    dte=leg["dte"],
                    target_delta=leg["target_delta"],
                    entry_time=leg["entry_time"],
                    entry_date=trade_date,
                    target_side=leg["side"],
                    window_minutes=window_minutes,
                    strict_dte=True,
                    best_only=True,
                )
            except ValueError:
                plan_legs = []
                break

            streamer = resolved.get("streamer_symbol")
            if not streamer:
                plan_legs = []
                break

            plan_legs.append(
                {
                    "leg_def": {
                        "side": leg["side"],
                        "quantity": leg["quantity"],
                        "option_type": leg["option_type"],
                        "target_delta": leg["target_delta"],
                        "target_dte": leg["dte"],
                        "entry_time": leg["entry_time"],
                    },
                    "sign": -1 if leg["side"] == "SELL" else 1,
                    "quantity": leg["quantity"],
                    "streamer_symbol": streamer,
                    "entry_snapshot_ts": resolved.get("snapshot_ts"),
                    "contract": {
                        "streamer_symbol": streamer,
                        "option_type": resolved.get("option_type"),
                        "strike_price": resolved.get("strike_price"),
                        "expiration_date": resolved.get("expiration_date"),
                    },
                }
            )

        if len(plan_legs) != len(legs):
            skipped_dates += 1
            continue

        trade_plans.append(
            {
                "trade_index": len(trade_plans) + 1,
                "trade_date": str(trade_date),
                "legs": plan_legs,
            }
        )

    return {
        "trade_plans": trade_plans,
        "skipped_dates": skipped_dates,
        "trade_dates_count": len(trade_dates),
    }


def _run_strategy_history_payload(
    conn: sqlite3.Connection,
    *,
    symbol: str,
    legs: list[dict[str, Any]],
    start_date: date | None,
    end_date: date | None,
    window_minutes: int = 5,
) -> dict[str, Any]:
    if not legs:
        raise ValueError("At least one strategy leg is required.")

    if start_date is None or end_date is None:
        latest_date = _resolve_latest_option_date(conn, symbol)
        if latest_date is None:
            raise ValueError("No option snapshots found for this symbol.")
        if end_date is None:
            end_date = latest_date
        if start_date is None:
            start_candidate = latest_date - timedelta(days=30)
            start_date = start_candidate if end_date is None or start_candidate <= end_date else end_date

    if start_date > end_date:
        raise ValueError("from cannot be after to.")

    if window_minutes <= 0:
        raise ValueError("window_minutes must be > 0.")

    trades: list[dict[str, Any]] = []
    total_pnl = 0.0
    total_indexed = []
    completed_count = 0
    win_count = 0

    current = start_date
    while current <= end_date:
        trade_result: dict[str, Any] = {
            "trade_date": str(current),
            "status": "ok",
            "legs": [],
            "strategy_entry": None,
            "strategy_exit": None,
            "strategy_pnl": None,
            "strategy_indexed": None,
            "strategy_contracts": 0,
        }

        leg_rows: list[dict[str, Any]] = []
        valid = True

        for index, leg in enumerate(legs):
            resolved_payload = _run_resolve_leg_payload(
                conn,
                symbol=symbol,
                option_type=leg["option_type"],
                dte=leg["dte"],
                target_delta=leg["target_delta"],
                entry_time=leg["entry_time"],
                entry_date=current,
                target_side=leg["side"],
                window_minutes=window_minutes,
            )
            resolved_contracts = list(resolved_payload.get("contracts") or [])
            if not resolved_contracts:
                valid = False
                trade_result["status"] = "missing_entry"
                break

            next_day_local = datetime(
                year=current.year,
                month=current.month,
                day=current.day,
                hour=23,
                minute=59,
                second=59,
                tzinfo=ZoneInfo("America/New_York"),
            ) + timedelta(seconds=1)
            exit_window_end = _sqlite_timestamp(next_day_local.astimezone(timezone.utc))
            missing_exit_count = 0
            for resolved in resolved_contracts:
                exit_row = conn.execute(
                    """
                    SELECT
                        snapshot_ts,
                        COALESCE(mid_price, (bid_price + ask_price) / 2.0) AS value
                    FROM spx_option_snapshots INDEXED BY ix_spx_option_snapshots_symbol_streamer_ts
                    WHERE symbol = ?
                      AND streamer_symbol = ?
                      AND snapshot_ts >= ?
                      AND snapshot_ts < ?
                      AND (mid_price IS NOT NULL OR (bid_price IS NOT NULL AND ask_price IS NOT NULL))
                    ORDER BY snapshot_ts DESC
                    LIMIT 1;
                    """,
                    [
                        symbol,
                        resolved["streamer_symbol"],
                        resolved["snapshot_ts"],
                        exit_window_end,
                    ],
                ).fetchone()
                if exit_row is None or exit_row[1] is None:
                    missing_exit_count += 1
                    continue

                exit_value = float(exit_row[1])
                entry_value = resolved.get("value")
                if entry_value is None:
                    continue

                sign = 1 if leg["side"] == "BUY" else -1
                qty = leg["quantity"]
                leg_entry_cash = sign * qty * float(entry_value)
                leg_exit_cash = sign * qty * exit_value
                leg_rows.append({
                    "streamer_symbol": resolved["streamer_symbol"],
                    "option_type": resolved["option_type"],
                    "strike_price": resolved["strike_price"],
                    "expiration_date": resolved["expiration_date"],
                    "entry_snapshot_ts": resolved["snapshot_ts"],
                    "exit_snapshot_ts": exit_row[0],
                    "entry_value": entry_value,
                    "exit_value": exit_value,
                    "qty": qty,
                    "side": leg["side"],
                    "target_delta": leg["target_delta"],
                    "target_dte": leg["dte"],
                    "resolved_delta": resolved["delta"],
                    "delta_diff": resolved["delta_diff"],
                    "time_diff_seconds": resolved["time_diff_seconds"],
                    "leg_entry_cash": leg_entry_cash,
                    "leg_exit_cash": leg_exit_cash,
                    "leg_pnl": leg_exit_cash - leg_entry_cash,
                })

            if missing_exit_count and not leg_rows:
                valid = False
                trade_result["status"] = "missing_exit"
                break
            if missing_exit_count:
                trade_result["status"] = "partial_missing_exit"

        if valid and leg_rows:
            strategy_entry = sum(leg["leg_entry_cash"] for leg in leg_rows)
            strategy_exit = sum(leg["leg_exit_cash"] for leg in leg_rows)
            strategy_pnl = strategy_exit - strategy_entry
            strategy_indexed = (strategy_exit / strategy_entry * 100) if strategy_entry else None

            completed_count += 1
            total_pnl += strategy_pnl
            if strategy_indexed is not None:
                total_indexed.append(strategy_indexed)
            if strategy_pnl > 0:
                win_count += 1

            trade_result.update(
                {
                    "status": "ok",
                    "legs": leg_rows,
                    "strategy_contracts": len(leg_rows),
                    "strategy_entry": strategy_entry,
                    "strategy_exit": strategy_exit,
                    "strategy_pnl": strategy_pnl,
                    "strategy_indexed": strategy_indexed,
                }
            )
        else:
            trade_result["legs"] = leg_rows
            trade_result["strategy_contracts"] = len(leg_rows)

        trades.append(trade_result)
        current += timedelta(days=1)

    completed = completed_count
    avg_indexed = sum(total_indexed) / len(total_indexed) if total_indexed else None
    win_rate = (win_count / completed * 100.0) if completed > 0 else None

    return {
        "summary": {
            "trade_count": len(trades),
            "completed_count": completed,
            "overall_pnl": total_pnl,
            "avg_indexed": avg_indexed,
            "win_rate": win_rate,
        },
        "trades": trades,
    }


def _share_db_url() -> str:
    return os.getenv(_PROD_SHARE_DB_URL_ENV, _PROD_SHARE_DB_DEFAULT_URL)


def ensure_strategy_share_db(db_url: str) -> Path:
    db_path = _resolve_sqlite_path(db_url)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS strategy_shares (
                share_token TEXT PRIMARY KEY,
                created_at_utc TEXT NOT NULL,
                strategy_json TEXT NOT NULL,
                results_json TEXT NOT NULL,
                meta_json TEXT NOT NULL
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS ix_strategy_shares_created_at ON strategy_shares (created_at_utc)"
        )
        conn.commit()
    return db_path


def _normalize_share_payload(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError("Share payload must be a JSON object.")
    strategy = value.get("strategy")
    results = value.get("results")
    meta = value.get("meta")
    if not isinstance(strategy, dict):
        raise ValueError("strategy must be an object.")
    if not isinstance(results, dict):
        raise ValueError("results must be an object.")
    if meta is None:
        meta = {}
    if not isinstance(meta, dict):
        raise ValueError("meta must be an object.")
    legs = strategy.get("legs")
    rows = results.get("rows")
    if not isinstance(legs, list) or not legs:
        raise ValueError("strategy.legs must be a non-empty array.")
    if not isinstance(rows, list) or not rows:
        raise ValueError("results.rows must be a non-empty array.")
    return {
        "strategy": strategy,
        "results": results,
        "meta": meta,
    }


def _generate_share_token() -> str:
    return secrets.token_urlsafe(9).rstrip("=")


def _create_strategy_share(db_path: Path, payload: Any) -> dict[str, Any]:
    normalized = _normalize_share_payload(payload)
    created_at_utc = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    meta = dict(normalized["meta"])
    token = _generate_share_token()
    meta["share_token"] = token
    meta["created_at_utc"] = created_at_utc
    strategy_json = json.dumps(normalized["strategy"], separators=(",", ":"), sort_keys=True)
    results_json = json.dumps(normalized["results"], separators=(",", ":"), sort_keys=True)
    meta_json = json.dumps(meta, separators=(",", ":"), sort_keys=True)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO strategy_shares (
                share_token,
                created_at_utc,
                strategy_json,
                results_json,
                meta_json
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (token, created_at_utc, strategy_json, results_json, meta_json),
        )
        conn.commit()
    return {
        "share_token": token,
        "created_at_utc": created_at_utc,
        "strategy": normalized["strategy"],
        "results": normalized["results"],
        "meta": meta,
    }


def _load_strategy_share(db_path: Path, share_token: str) -> dict[str, Any] | None:
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            """
            SELECT share_token, created_at_utc, strategy_json, results_json, meta_json
            FROM strategy_shares
            WHERE share_token = ?
            LIMIT 1
            """,
            [share_token],
        ).fetchone()
    if row is None:
        return None
    strategy = json.loads(str(row["strategy_json"]))
    results = json.loads(str(row["results_json"]))
    meta = json.loads(str(row["meta_json"]))
    if not isinstance(strategy, dict) or not isinstance(results, dict) or not isinstance(meta, dict):
        raise ValueError("Stored share payload is invalid.")
    return {
        "share_token": str(row["share_token"]),
        "created_at_utc": str(row["created_at_utc"]),
        "strategy": strategy,
        "results": results,
        "meta": meta,
    }


def _build_strategy_share_url(handler: BaseHTTPRequestHandler, share_token: str) -> str:
    forwarded_host = str(handler.headers.get("X-Forwarded-Host") or "").strip()
    host = forwarded_host or str(handler.headers.get("Host") or "").strip()
    if not host:
        server_host, server_port = handler.server.server_address[:2]
        host = f"{server_host}:{server_port}"
    proto = str(handler.headers.get("X-Forwarded-Proto") or "http").split(",")[0].strip() or "http"
    return f"{proto}://{host}/?share={share_token}"


def _error_response(handler: BaseHTTPRequestHandler, message: str, status: int = 400) -> None:
    _json_response(handler, {"error": message}, status=status)


def _get_query_params(path: str) -> tuple[str, dict[str, list[str]]]:
    parsed = urlparse(path)
    return parsed.path, {k: v for k, v in parse_qs(parsed.query).items()}


def _get_qs(params: dict[str, list[str]], key: str, default: str | None = None) -> str | None:
    values = params.get(key)
    if not values:
        return default
    return values[0]


class SqlUiHandler(BaseHTTPRequestHandler):
    db_path: Path
    share_db_path: Path | None = None
    tracking_db_path: Path | None = None
    tracking_enabled: bool = False
    tracking_metrics_enabled: bool = False

    def do_GET(self) -> None:  # noqa: N802
        path, qs = _get_query_params(self.path)

        if path == "/":
            _html_response(self, _render_app_html(tracking_enabled=self.tracking_enabled))
            return
        if path == "/ops/metrics":
            if not self.tracking_metrics_enabled:
                _json_response(self, {"error": "not_found"}, status=404)
                return
            _html_response(self, METRICS_HTML)
            return
        if path == "/api/health":
            _json_response(self, {"ok": True})
            return
        if path.startswith("/api/strategy-shares/"):
            if self.share_db_path is None:
                _json_response(self, {"error": "not_found"}, status=404)
                return
            share_token = path[len("/api/strategy-shares/") :].strip()
            if not share_token:
                _json_response(self, {"error": "not_found"}, status=404)
                return
            try:
                payload = _load_strategy_share(self.share_db_path, share_token)
                if payload is None:
                    _json_response(self, {"error": "not_found"}, status=404)
                    return
                payload["share_url"] = _build_strategy_share_url(
                    self, payload["share_token"]
                )
                _json_response(self, payload)
            except Exception as exc:
                _error_response(self, str(exc))
            return
        if path == "/api/ops/metrics/overview":
            if not self.tracking_metrics_enabled or self.tracking_db_path is None:
                _json_response(self, {"error": "not_found"}, status=404)
                return
            try:
                from_date, to_date = parse_metrics_date_range(
                    _get_qs(qs, "from"), _get_qs(qs, "to")
                )
                payload = build_overview_payload(
                    self.tracking_db_path, from_date=from_date, to_date=to_date
                )
                _json_response(self, payload)
            except Exception as exc:
                _error_response(self, str(exc))
            return
        if path == "/api/ops/metrics/timeseries":
            if not self.tracking_metrics_enabled or self.tracking_db_path is None:
                _json_response(self, {"error": "not_found"}, status=404)
                return
            try:
                from_date, to_date = parse_metrics_date_range(
                    _get_qs(qs, "from"), _get_qs(qs, "to")
                )
                payload = build_timeseries_payload(
                    self.tracking_db_path, from_date=from_date, to_date=to_date
                )
                _json_response(self, payload)
            except Exception as exc:
                _error_response(self, str(exc))
            return
        if path == "/api/ops/metrics/runs":
            if not self.tracking_metrics_enabled or self.tracking_db_path is None:
                _json_response(self, {"error": "not_found"}, status=404)
                return
            try:
                from_date, to_date = parse_metrics_date_range(
                    _get_qs(qs, "from"), _get_qs(qs, "to")
                )
                payload = build_recent_runs_payload(
                    self.tracking_db_path, from_date=from_date, to_date=to_date
                )
                _json_response(self, payload)
            except Exception as exc:
                _error_response(self, str(exc))
            return
        if path == "/api/ops/metrics/common-legs":
            if not self.tracking_metrics_enabled or self.tracking_db_path is None:
                _json_response(self, {"error": "not_found"}, status=404)
                return
            try:
                from_date, to_date = parse_metrics_date_range(
                    _get_qs(qs, "from"), _get_qs(qs, "to")
                )
                payload = build_common_legs_payload(
                    self.tracking_db_path, from_date=from_date, to_date=to_date
                )
                _json_response(self, payload)
            except Exception as exc:
                _error_response(self, str(exc))
            return
        if path == "/api/schema":
            with _connect_options_db(self.db_path) as conn:
                payload = _schema_payload(conn)
            _json_response(self, payload)
            return
        if path == "/api/options/contracts":
            try:
                symbol = _get_qs(qs, "symbol", "SPX")
                start_dt = _parse_datetime(_get_qs(qs, "from"), "from")
                end_dt = _parse_datetime(_get_qs(qs, "to"), "to")
                option_type = _get_qs(qs, "type")
                min_strike = _parse_float(_get_qs(qs, "min_strike"), "min_strike")
                max_strike = _parse_float(_get_qs(qs, "max_strike"), "max_strike")
                limit = _parse_int(_get_qs(qs, "limit"), "limit", 400)
                with _connect_options_db(self.db_path) as conn:
                    payload = _run_contracts_payload(
                        conn,
                        symbol=symbol or "SPX",
                        start_dt=start_dt,
                        end_dt=end_dt,
                        option_type=option_type,
                        min_strike=min_strike,
                        max_strike=max_strike,
                        limit=limit,
                    )
                _json_response(self, payload)
            except Exception as exc:
                _error_response(self, str(exc))
            return
        if path == "/api/options/series":
            try:
                symbol = _get_qs(qs, "symbol", "SPX")
                raw_streamers = _get_qs(qs, "streamers", "")
                streamers = [s.strip() for s in (raw_streamers or "").split(",") if s.strip()]
                if not streamers:
                    raise ValueError("streamers parameter is required.")
                start_dt = _parse_datetime(_get_qs(qs, "from"), "from")
                end_dt = _parse_datetime(_get_qs(qs, "to"), "to")
                field = _get_qs(qs, "field", "mid_price")
                with _connect_options_db(self.db_path) as conn:
                    payload = _run_series_payload(
                        conn,
                        symbol=symbol or "SPX",
                        streamers=streamers,
                        start_dt=start_dt,
                        end_dt=end_dt,
                        field=field,
                    )
                _json_response(self, payload)
            except Exception as exc:
                _error_response(self, str(exc))
            return
        if path == "/api/options/summary":
            try:
                symbol = _get_qs(qs, "symbol", "SPX")
                start_dt = _parse_datetime(_get_qs(qs, "from"), "from")
                end_dt = _parse_datetime(_get_qs(qs, "to"), "to")
                with _connect_options_db(self.db_path) as conn:
                    payload = _run_summary_payload(
                        conn, symbol=symbol or "SPX", start_dt=start_dt, end_dt=end_dt
                    )
                _json_response(self, payload)
            except Exception as exc:
                _error_response(self, str(exc))
            return
        if path == "/api/options/snapshot-dates":
            try:
                symbol = _get_qs(qs, "symbol", "SPX")
                effective_symbol = symbol or "SPX"
                payload = _cached_metadata_payload(
                    self.db_path,
                    f"snapshot-dates:{effective_symbol}",
                    lambda conn: _run_snapshot_dates_payload(
                        conn, symbol=effective_symbol
                    ),
                )
                _json_response(self, payload)
            except Exception as exc:
                _error_response(self, str(exc))
            return
        if path == "/api/options/option-types":
            try:
                symbol = _get_qs(qs, "symbol", "SPX")
                effective_symbol = symbol or "SPX"
                payload = _cached_metadata_payload(
                    self.db_path,
                    f"option-types:{effective_symbol}",
                    lambda conn: _run_option_types_payload(
                        conn, symbol=effective_symbol
                    ),
                )
                _json_response(self, payload)
            except Exception as exc:
                _error_response(self, str(exc))
            return
        if path == "/api/options/resolve-leg":
            try:
                symbol = _get_qs(qs, "symbol", "SPX")
                option_type = _get_qs(qs, "option_type", "PUT")
                dte = _parse_int_required(_get_qs(qs, "dte"), "dte")
                target_delta = _parse_float_required(
                    _get_qs(qs, "target_delta"), "target_delta"
                )
                entry_time = _get_qs(qs, "entry_time")
                entry_date = _parse_date(_get_qs(qs, "entry_date"), "entry_date")
                target_side = _get_qs(qs, "target_side")
                snapshot_from = _parse_datetime(_get_qs(qs, "snapshot_from"), "snapshot_from")
                snapshot_to = _parse_datetime(_get_qs(qs, "snapshot_to"), "snapshot_to")
                window_minutes = _parse_int(_get_qs(qs, "window_minutes"), "window_minutes", 5)
                strict_dte_raw = (_get_qs(qs, "strict_dte") or "").strip().lower()
                strict_dte = strict_dte_raw in {"1", "true", "yes", "on"}
                best_only_raw = (_get_qs(qs, "best_only") or "").strip().lower()
                best_only = best_only_raw in {"1", "true", "yes", "on"}
                with _connect_options_db(self.db_path) as conn:
                    payload = _run_resolve_leg_payload(
                        conn,
                        symbol=symbol or "SPX",
                        option_type=option_type,
                        dte=dte,
                        target_delta=target_delta,
                        entry_time=entry_time or "",
                        entry_date=entry_date,
                        target_side=target_side,
                        snapshot_from=snapshot_from,
                        snapshot_to=snapshot_to,
                        window_minutes=window_minutes,
                        strict_dte=strict_dte,
                        best_only=best_only,
                    )
                _json_response(self, payload)
            except Exception as exc:
                _error_response(self, str(exc))
            return

        _json_response(self, {"error": "not_found"}, status=404)

    def do_POST(self) -> None:  # noqa: N802
        path, _ = _get_query_params(self.path)
        if path not in {
            "/api/options/strategy-history",
            "/api/options/strategy-plan",
            "/api/track",
            "/api/strategy-shares",
        }:
            _json_response(self, {"error": "not_found"}, status=404)
            return

        try:
            raw_len = int(self.headers.get("Content-Length", "0"))
            max_body = (
                _MAX_STRATEGY_SHARE_BODY_BYTES
                if path == "/api/strategy-shares"
                else 32_768
            )
            if raw_len > max_body:
                raise ValueError("Request body too large.")
            body = self.rfile.read(raw_len).decode("utf-8")
            parsed = json.loads(body)

            if path == "/api/track":
                if not self.tracking_enabled or self.tracking_db_path is None:
                    _json_response(self, {"error": "tracking_disabled"}, status=404)
                    return
                payload = insert_tracking_event(self.tracking_db_path, parsed)
                _json_response(self, payload, status=202)
                return
            if path == "/api/strategy-shares":
                if self.share_db_path is None:
                    _json_response(self, {"error": "not_found"}, status=404)
                    return
                payload = _create_strategy_share(self.share_db_path, parsed)
                payload["share_url"] = _build_strategy_share_url(
                    self, payload["share_token"]
                )
                _json_response(self, payload, status=201)
                return

            if path == "/api/options/strategy-plan":
                payload_legs_raw = parsed.get("legs")
                if not isinstance(payload_legs_raw, list):
                    raise ValueError("legs must be an array.")
                legs = [
                    _parse_strategy_plan_leg_payload(leg, i)
                    for i, leg in enumerate(payload_legs_raw)
                ]
                trade_dates = _parse_strategy_plan_dates(parsed.get("trade_dates"))
                symbol = str(parsed.get("symbol", "SPX"))
                window_minutes = _parse_int(
                    str(parsed.get("window_minutes", "5")), "window_minutes", 5
                )
                with _connect_options_db(self.db_path) as conn:
                    payload = _run_strategy_plan_payload(
                        conn,
                        symbol=symbol or "SPX",
                        legs=legs,
                        trade_dates=trade_dates,
                        window_minutes=window_minutes,
                    )
                _json_response(self, payload)
                return

            payload_legs_raw = parsed.get("legs")
            if not isinstance(payload_legs_raw, list):
                raise ValueError("legs must be an array.")
            legs = [_parse_strategy_leg_payload(leg, i) for i, leg in enumerate(payload_legs_raw)]

            start = _parse_date(parsed.get("from"), "from")
            end = _parse_date(parsed.get("to"), "to")
            symbol = str(parsed.get("symbol", "SPX"))
            window_minutes = _parse_int(
                str(parsed.get("window_minutes", "5")), "window_minutes", 5
            )

            with _connect_options_db(self.db_path) as conn:
                payload = _run_strategy_history_payload(
                    conn,
                    symbol=symbol or "SPX",
                    legs=legs,
                    start_date=start,
                    end_date=end,
                    window_minutes=window_minutes,
                )
            _json_response(self, payload)
        except Exception as exc:
            _json_response(self, {"error": str(exc)}, status=400)

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
        return


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="spx-backtest-ui",
        description="Run the public SPX backtest UI against the collector database.",
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8789)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    _assert_env_file_permissions()
    settings = Settings()
    db_path = _resolve_sqlite_path(settings.db_url)
    if not db_path.exists():
        raise FileNotFoundError(f"SQLite DB not found at {db_path}")
    ensure_option_query_performance(db_path)
    share_db_path = ensure_strategy_share_db(_share_db_url())
    tracking_db_path = None
    if settings.tracking_enabled or settings.tracking_metrics_enabled:
        tracking_db_path = ensure_tracking_db(settings.tracking_db_url)

    SqlUiHandler.db_path = db_path
    SqlUiHandler.share_db_path = share_db_path
    SqlUiHandler.tracking_db_path = tracking_db_path
    SqlUiHandler.tracking_enabled = bool(settings.tracking_enabled and tracking_db_path)
    SqlUiHandler.tracking_metrics_enabled = bool(
        settings.tracking_metrics_enabled and tracking_db_path
    )
    server = ThreadingHTTPServer((args.host, args.port), SqlUiHandler)
    print(f"Backtest prod UI running at http://{args.host}:{args.port} using {db_path}")
    server.serve_forever()


_HTML = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <meta name="color-scheme" content="light only" />
  <meta name="supported-color-schemes" content="light" />
  <meta name="theme-color" content="#f8f7f5" />
  <title>Market Playground</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;500;600;700&display=swap" rel="stylesheet">
  <style>
    :root {
      color-scheme: light;
      --ink: #1c1917;
      --ink-soft: #44403c;
      --bg: #f8f7f5;
      --panel: rgba(255, 255, 255, 0.92);
      --panel-strong: #ffffff;
      --panel-muted: #f4f2ef;
      --line: rgba(28, 25, 23, 0.1);
      --accent: rgba(255, 71, 43, 1);
      --accent-strong: rgba(217, 54, 29, 1);
      --accent-soft: rgba(255, 227, 221, 1);
      --muted: #6b625a;
      --success: #166534;
      --danger: #b91c1c;
      --shadow-card: 0 1px 2px rgba(28, 25, 23, 0.04), 0 18px 40px rgba(28, 25, 23, 0.06);
      --shadow-float: 0 24px 60px rgba(28, 25, 23, 0.12);
    }

    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: "Plus Jakarta Sans", sans-serif;
      color: var(--ink);
      background:
        radial-gradient(80rem 32rem at 0% 0%, rgba(251, 146, 60, 0.18) 0%, transparent 55%),
        radial-gradient(64rem 28rem at 100% 0%, rgba(120, 113, 108, 0.14) 0%, transparent 50%),
        var(--bg);
      min-height: 100vh;
    }
    .wrap {
      max-width: 1440px;
      margin: 0 auto;
      padding: 24px;
    }
    h1, h2, h3 {
      margin-top: 0;
      margin-bottom: 0;
    }
    h1 {
      font-size: clamp(2.1rem, 3vw, 3.35rem);
      line-height: 1.02;
      letter-spacing: -0.035em;
      word-spacing: 0.04em;
      max-width: none;
      white-space: nowrap;
    }
    h2 { font-size: 1.15rem; letter-spacing: -0.02em; }
    .app-shell { position: relative; z-index: 1; }
    .hero {
      position: relative;
      overflow: hidden;
      border: 1px solid rgba(255, 255, 255, 0.08);
      border-radius: 24px;
      padding: 14px 22px 16px;
      background:
        linear-gradient(135deg, rgba(28, 25, 23, 0.96), rgba(68, 64, 60, 0.88)),
        radial-gradient(32rem 18rem at 100% 0%, rgba(251, 146, 60, 0.22), transparent 60%);
      color: #fafaf9;
      box-shadow: var(--shadow-float);
    }
    .topbar {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
      margin-bottom: 12px;
    }
    .brand {
      display: inline-flex;
      align-items: center;
      gap: 10px;
    }
    .brand-mark {
      width: 42px;
      height: 42px;
      border-radius: 14px;
      display: inline-flex;
      align-items: center;
      justify-content: center;
      background: linear-gradient(135deg, var(--accent), var(--accent-strong));
      color: #fff7ed;
      font-size: 0.92rem;
      font-family: inherit;
      letter-spacing: -0.04em;
      font-weight: 700;
      box-shadow: 0 12px 24px rgba(234, 88, 12, 0.24);
    }
    .brand-copy {
      display: flex;
      flex-direction: column;
      justify-content: center;
    }
    .eyebrow {
      font-size: 0.72rem;
      letter-spacing: 0.16em;
      text-transform: uppercase;
      color: rgba(255, 237, 213, 0.82);
    }
    .brand-title {
      font-size: 1.7rem;
      line-height: 1;
      letter-spacing: -0.04em;
      font-weight: 700;
      color: #fafaf9;
    }
    .hero-grid {
      display: block;
    }
    .hero-copy {
      display: flex;
      flex-direction: column;
      gap: 8px;
      max-width: none;
      align-items: center;
      text-align: center;
    }
    .sub {
      margin-top: 0;
      max-width: 54ch;
      color: rgba(245, 245, 244, 0.72);
      line-height: 1.7;
      font-size: 1rem;
    }
    .hero-note {
      font-size: 0.88rem;
      color: rgba(245, 245, 244, 0.62);
      max-width: 48ch;
      line-height: 1.6;
    }
    .surface {
      margin-top: 22px;
      padding: 18px;
      border: 1px solid var(--line);
      border-radius: 30px;
      background: rgba(255, 255, 255, 0.56);
      backdrop-filter: blur(12px);
    }
    .grid {
      display: grid;
      grid-template-columns: 1.4fr 1fr;
      gap: 18px;
      margin-top: 18px;
    }
    .grid.full {
      grid-template-columns: 1fr;
    }
    .card {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 24px;
      padding: 20px;
      box-shadow: var(--shadow-card);
      overflow: hidden;
      backdrop-filter: blur(10px);
    }
    textarea {
      width: 100%;
      min-height: 150px;
      border: 1px solid var(--line);
      border-radius: 16px;
      padding: 14px 16px;
      font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
      font-size: 0.92rem;
      resize: vertical;
      background: var(--panel-strong);
      color: var(--ink);
    }
    .row {
      display: flex;
      gap: 12px;
      margin-top: 12px;
      flex-wrap: wrap;
      align-items: center;
    }
    button, select {
      border: 0;
      border-radius: 16px;
      padding: 11px 16px;
      font-weight: 600;
      color: #fff;
      background: linear-gradient(135deg, var(--accent), var(--accent-strong));
      cursor: pointer;
      font-family: inherit;
      transition: transform 160ms ease, box-shadow 160ms ease, opacity 160ms ease, background 160ms ease;
      box-shadow: 0 10px 24px rgba(234, 88, 12, 0.2);
    }
    button:hover, select:hover { transform: translateY(-1px); }
    select.input {
      box-shadow: 0 4px 12px rgba(234, 88, 12, 0.08);
    }
    button:focus-visible, select:focus-visible, .input:focus-visible, textarea:focus-visible {
      outline: 2px solid rgba(251, 146, 60, 0.45);
      outline-offset: 2px;
    }
    .secondary { background: linear-gradient(135deg, #57534e, #292524); box-shadow: 0 10px 24px rgba(41, 37, 36, 0.16); }
    .input {
      border: 1px solid var(--line);
      border-radius: 16px;
      padding: 11px 14px;
      background: var(--panel-strong);
      color: var(--ink);
      font-size: 0.9rem;
      font-family: inherit;
      display: block;
      width: 100%;
      min-width: 0;
      max-width: 100%;
    }
    .mobile-native-field {
      position: relative;
      width: 100%;
    }
    .mobile-native-display {
      display: none;
    }
    select.input {
      appearance: none;
      -webkit-appearance: none;
      -moz-appearance: none;
      padding-right: 44px;
      background-image: url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='14' height='14' viewBox='0 0 14 14' fill='none'%3E%3Cpath d='M3.25 5.5L7 9.25L10.75 5.5' stroke='%236b625a' stroke-width='1.5' stroke-linecap='round' stroke-linejoin='round'/%3E%3C/svg%3E");
      background-repeat: no-repeat;
      background-position: right 16px center;
      background-size: 14px 14px;
    }
    label {
      display: inline-block;
      margin-bottom: 8px;
      font-size: 0.78rem;
      letter-spacing: 0.08em;
      text-transform: uppercase;
      color: var(--muted);
    }
    .meta {
      color: var(--muted);
      font-size: 0.92rem;
      line-height: 1.6;
    }
    .result-wrap {
      margin-top: 14px;
      max-height: 520px;
      overflow: auto;
      border: 1px solid var(--line);
      border-radius: 20px;
      background: rgba(255,255,255,0.94);
      padding-bottom: 10px;
      scrollbar-gutter: stable both-edges;
    }
    .chart-wrap {
      margin-top: 12px;
      border: 1px solid var(--line);
      border-radius: 22px;
      background: rgba(255,255,255,0.94);
      overflow: hidden;
      position: relative;
      padding: 14px;
    }
    .chart-svg {
      width: 100%;
      height: 320px;
      display: block;
      background: linear-gradient(180deg, rgba(255,255,255,0.96), rgba(244,242,239,0.92));
      cursor: crosshair;
      border-radius: 16px;
      touch-action: none;
    }
    .chart-tooltip {
      position: absolute;
      min-width: 120px;
      max-width: 160px;
      padding: 12px 14px;
      border-radius: 16px;
      background: rgba(15, 23, 42, 0.94);
      color: #f8fafc;
      box-shadow: 0 10px 24px rgba(15, 23, 42, 0.18);
      font-size: 0.88rem;
      line-height: 1.25;
      pointer-events: none;
      opacity: 0;
      transform: translate(12px, -12px);
      transition: opacity 120ms ease;
      z-index: 2;
      white-space: normal;
      text-align: center;
    }
    .chart-tooltip.visible {
      opacity: 1;
    }
    .chart-tooltip-debug {
      color: #f8fafc;
      font-size: 0.88rem;
      font-weight: 600;
      margin-top: 2px;
    }
    .chart-tooltip-value {
      font-weight: 700;
      text-align: center;
      font-size: 0.88rem;
      margin-top: 2px;
    }
    .chart-legend {
      display: flex;
      gap: 12px;
      flex-wrap: wrap;
      margin-top: 14px;
      font-size: 0.82rem;
      color: var(--ink-soft);
    }
    .chart-legend-item {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      padding: 8px 10px;
      border-radius: 999px;
      background: var(--panel-muted);
    }
    .chart-card-header {
      display: flex;
      justify-content: space-between;
      align-items: flex-start;
      gap: 12px;
      flex-wrap: wrap;
    }
    .chart-share-group {
      margin-left: auto;
      display: flex;
      flex-direction: column;
      align-items: flex-end;
      gap: 6px;
    }
    .share-strategy-btn {
      width: auto;
      min-width: 168px;
      padding: 10px 16px;
      border-radius: 999px;
      background: linear-gradient(135deg, #2563eb, #1d4ed8);
      box-shadow: 0 12px 24px rgba(37, 99, 235, 0.2);
    }
    .share-strategy-btn:disabled {
      cursor: not-allowed;
      opacity: 0.55;
      box-shadow: none;
    }
    .share-feedback {
      max-width: 360px;
      text-align: right;
      font-size: 0.82rem;
      line-height: 1.4;
    }
    .share-feedback a {
      color: inherit;
      font-weight: 700;
      text-decoration: underline;
      word-break: break-all;
    }
    .share-snapshot-note {
      margin-top: 8px;
    }
    .chart-toggle-group {
      display: inline-flex;
      gap: 10px;
      align-items: center;
      justify-content: flex-end;
      margin-left: auto;
      font-size: 0.82rem;
      color: var(--ink-soft);
      padding: 6px;
      border-radius: 999px;
      background: var(--panel-muted);
    }
    .chart-toggle-option {
      display: inline-flex;
      align-items: center;
      gap: 5px;
      cursor: pointer;
    }
    .chart-toggle-option input {
      margin: 0;
    }
    .stats-grid {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 14px;
      margin-top: 16px;
    }
    .stat-tile {
      border: 1px solid var(--line);
      border-radius: 18px;
      background: linear-gradient(180deg, rgba(255,255,255,0.96), rgba(244,242,239,0.92));
      padding: 16px;
    }
    .stat-label {
      font-size: 0.72rem;
      text-transform: uppercase;
      letter-spacing: 0.12em;
      color: var(--muted);
    }
    .stat-value {
      margin-top: 10px;
      font-size: 1.5rem;
      font-weight: 600;
      color: var(--ink);
    }
    .meta-emphasis {
      display: inline-flex;
      align-items: center;
      margin-left: 8px;
      padding: 4px 10px;
      border-radius: 999px;
      background: var(--panel-muted);
      color: var(--ink-soft);
      font-size: 0.8rem;
      font-weight: 600;
      letter-spacing: 0.02em;
      vertical-align: middle;
    }
    .section-heading {
      grid-column: 1 / -1;
      margin: 10px 0 0;
      font-size: 0.72rem;
      font-weight: 800;
      letter-spacing: 0.14em;
      text-transform: uppercase;
      color: var(--muted);
      padding-top: 8px;
      border-top: 1px solid var(--line);
    }
    .field-disabled {
      opacity: 0.45;
    }
    .checkbox-row {
      display: flex;
      align-items: center;
      gap: 8px;
      min-height: 42px;
    }
    .checkbox-row input[type="checkbox"] {
      width: 16px;
      height: 16px;
      accent-color: var(--accent);
    }
    .chart-legend-swatch {
      width: 14px;
      height: 8px;
      border-radius: 4px;
      display: inline-block;
    }
    table {
      border-collapse: collapse;
      width: 100%;
      font-size: 0.9rem;
    }
    th, td {
      border-bottom: 1px solid var(--line);
      padding: 11px 12px;
      text-align: left;
      white-space: nowrap;
    }
    th {
      position: sticky;
      top: 0;
      background: rgba(244, 242, 239, 0.96);
      z-index: 1;
      font-size: 0.74rem;
      letter-spacing: 0.08em;
      text-transform: uppercase;
      color: var(--muted);
    }
    .strategy-summary {
      background: var(--accent-soft);
      font-weight: 600;
    }
    .schema-block {
      margin-top: 10px;
      padding-top: 10px;
      border-top: 1px dashed var(--line);
    }
    .controls-card {
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 14px;
    }
    .controls-card > div {
      min-width: 0;
      overflow: hidden;
    }
    .analyzer-filter-row {
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 14px;
      grid-column: 1 / -1;
    }
    .controls-card .full { grid-column: 1 / -1; }
    .controls-card select[multiple] {
      appearance: auto;
      -webkit-appearance: auto;
      -moz-appearance: auto;
      height: 260px;
      padding: 10px;
      font-family: inherit;
      border-radius: 16px;
      border: 1px solid var(--line);
      background: var(--panel-strong);
      background-image: none;
      color: var(--ink);
    }
    select option { padding: 4px; }
    .small {
      font-size: 0.85rem;
      color: var(--ink-soft);
    }
    .status {
      font-size: 0.9rem;
      color: var(--muted);
      min-height: 1.25rem;
    }
    .success { color: #0369a1; }
    .danger { color: var(--danger); }
    .run-analysis-wide {
      width: 100%;
      margin-top: 12px;
      background: linear-gradient(135deg, var(--accent), var(--accent-strong));
    }
    .remove-leg {
      border: 0;
      background: transparent;
      color: var(--muted);
      font-weight: 700;
      cursor: pointer;
      padding: 0 4px;
      font-size: 1rem;
      line-height: 1;
      box-shadow: none;
    }
    .remove-leg:hover { color: var(--ink); transform: none; }
    .side-group {
      display: inline-flex;
      gap: 6px;
    }
    .side-btn {
      min-width: 58px;
      border: 0;
      border-radius: 8px;
      padding: 6px 10px;
      color: #fff;
      font-weight: 700;
      cursor: pointer;
      opacity: 1;
    }
    .side-btn.active { opacity: 1; }
    .buy-btn { background: #94bba0; }
    .sell-btn { background: #e0a3a3; }
    .buy-btn.active { background: var(--success); }
    .sell-btn.active { background: var(--danger); }
    .qty-input {
      width: 100px;
    }
    .strategy-leg-mobile-label {
      display: none;
    }
    .tab-nav {
      display: block;
      margin: 0 0 8px;
    }
    .tab-heading {
      display: inline-block;
      font-size: 1rem;
      font-weight: 700;
      color: var(--ink);
      letter-spacing: -0.02em;
      padding: 6px 2px 2px;
    }
    .tab-button {
      border: 1px solid var(--line);
      background: transparent;
      color: var(--ink);
      font-weight: 500;
      opacity: 0.8;
      box-shadow: none;
      min-width: 172px;
    }
    .tab-button.active {
      color: #fff;
      background: linear-gradient(135deg, var(--accent), var(--accent-strong));
      opacity: 1;
      box-shadow: 0 12px 24px rgba(234, 88, 12, 0.18);
    }
    .tab-panel {
      display: none;
      margin-top: 24px;
    }
    .tab-panel.active {
      display: block;
      animation: fade-in 180ms ease;
    }
    .is-hidden {
      display: none !important;
    }
    @keyframes fade-in {
      from { opacity: 0; transform: translateY(3px); }
      to { opacity: 1; transform: translateY(0); }
    }
    @media (max-width: 1100px) {
      .grid { grid-template-columns: 1fr; }
      .controls-card { grid-template-columns: 1fr; }
      .controls-card .full { grid-column: auto; }
      .stats-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .tab-nav { width: 100%; }
      .tab-button { width: 100%; min-width: 0; }
      .hero { padding: 14px 18px; }
    }
    @media (max-width: 720px) {
      .wrap { padding: 16px; }
      .hero { border-radius: 20px; }
      .surface { padding: 14px; border-radius: 24px; }
      .card { padding: 16px; border-radius: 20px; }
      .brand-mark {
        width: 36px;
        height: 36px;
        font-size: 0.82rem;
      }
      .brand-title { font-size: 1.35rem; }
      h1 {
        white-space: normal;
        max-width: 10ch;
      }
      .stats-grid {
        grid-template-columns: repeat(2, minmax(0, 1fr));
        gap: 10px;
      }
      .chart-wrap {
        overflow: hidden;
        padding: 10px;
      }
      .chart-share-group {
        width: 100%;
        align-items: stretch;
      }
      .share-strategy-btn {
        width: 100%;
      }
      .share-feedback {
        max-width: none;
        text-align: left;
      }
      .chart-svg {
        height: min(62vw, 260px);
      }
      .chart-legend {
        gap: 8px;
      }
      .chart-legend-item {
        padding: 7px 9px;
        font-size: 0.76rem;
      }
      .controls-card {
        gap: 12px;
      }
      .controls-card > div {
        min-width: 0;
        overflow: hidden;
      }
      .controls-card .input,
      .controls-card input[type="date"],
      .controls-card input[type="time"] {
        width: 100%;
        min-width: 0;
        max-width: 100%;
      }
      @supports (-webkit-touch-callout: none) {
        .mobile-native-field {
          position: relative;
        }
        .mobile-native-display {
          display: flex;
          align-items: center;
          width: 100%;
          min-height: 56px;
          padding: 11px 14px;
          border: 1px solid var(--line);
          border-radius: 16px;
          background: var(--panel-strong);
          color: var(--ink);
          font-size: 0.9rem;
          font-family: inherit;
          overflow: hidden;
          white-space: nowrap;
          text-overflow: ellipsis;
        }
        .mobile-native-field > input[type="date"],
        .mobile-native-field > input[type="time"] {
          position: absolute;
          inset: 0;
          width: 100%;
          height: 100%;
          opacity: 0;
          z-index: 2;
          cursor: pointer;
        }
      }
      .stat-tile {
        padding: 12px;
        border-radius: 16px;
      }
      .stat-label {
        font-size: 0.66rem;
        letter-spacing: 0.1em;
      }
      .stat-value {
        margin-top: 8px;
        font-size: 1rem;
        line-height: 1.15;
        overflow-wrap: anywhere;
      }
      .meta-emphasis {
        margin-left: 0;
        margin-top: 6px;
      }
      #strategyLegsWrap {
        overflow-x: hidden;
      }
      #strategyLegsTable thead {
        display: none;
      }
      #strategyLegsTable,
      #strategyLegsTable tbody {
        display: block;
        width: 100%;
      }
      #strategyLegsTable tr {
        display: grid;
        grid-template-columns: auto minmax(0, 1fr);
        grid-template-areas:
          "remove name"
          "side side"
          "qty qty";
        gap: 10px 12px;
        padding: 14px 12px;
      }
      #strategyLegsTable td {
        display: block;
        white-space: normal;
        border-bottom: 0;
        padding: 0;
      }
      #strategyLegsTable tr + tr {
        border-top: 1px solid var(--line);
      }
      .strategy-leg-remove {
        grid-area: remove;
        display: flex;
        align-items: flex-start;
      }
      .strategy-leg-name {
        grid-area: name;
        font-weight: 500;
        line-height: 1.45;
        overflow-wrap: anywhere;
      }
      .strategy-leg-side {
        grid-area: side;
      }
      .strategy-leg-qty {
        grid-area: qty;
      }
      .strategy-leg-mobile-label {
        display: block;
        margin-bottom: 6px;
        font-size: 0.72rem;
        font-weight: 700;
        letter-spacing: 0.08em;
        text-transform: uppercase;
        color: var(--muted);
      }
      .strategy-leg-side .side-group {
        display: flex;
        width: 100%;
      }
      .strategy-leg-side .side-btn {
        flex: 1 1 0;
        min-width: 0;
      }
      .strategy-leg-qty .qty-input {
        width: 100%;
      }
    }
  </style>
</head>
<body>
  <div class="wrap app-shell">
    <section class="hero">
      <div class="topbar">
        <div class="brand">
          <div class="brand-mark">MP</div>
          <div class="brand-copy">
            <span class="brand-title">Market&nbsp;&nbsp;Playground</span>
          </div>
        </div>
      </div>
      <div class="hero-grid">
        <div class="hero-copy">
          <h1>Explore, interact, and discover</h1>
        </div>
      </div>
    </section>
    <div class="surface">
      <div class="tab-nav">
      <div class="tab-heading">Strategy Replay</div>
      <div class="meta" style="margin-top:10px;">Build a strategy by setting your entry rules and clicking 'Add Leg'. Repeat to build a spread or multi-leg strategy, then choose your exit criteria and run the replay.</div>
    </div>

    <section id="tab-strategy" class="tab-panel active" data-tab="strategy">
      <div class="grid full">
        <div class="card">
          <h2 style="margin-bottom: 6px;">Strategy</h2>
          <div class="controls-card" style="margin-top:10px;">
            <div>
              <label for="strategySymbol">Symbol</label><br/>
              <select id="strategySymbol" class="input">
                <option value="SPX">SPX</option>
              </select>
            </div>
            <div>
              <label for="strategySide">Side</label><br/>
              <select id="strategySide" class="input">
                <option value="BUY">BUY</option>
                <option value="SELL">SELL</option>
              </select>
            </div>
            <div>
              <label for="strategyOptionType">Option Type</label><br/>
              <select id="strategyOptionType" class="input">
                <option value="PUT">PUT</option>
                <option value="CALL">CALL</option>
              </select>
            </div>
            <div class="section-heading">Entry Criteria</div>
            <div>
              <label for="strategyDte">DTE</label><br/>
              <input id="strategyDte" class="input" type="number" min="0" step="1" value="1" />
            </div>
            <div>
              <label for="strategyDelta">Delta</label><br/>
              <input id="strategyDelta" class="input" type="number" min="0" step="1" value="35" />
            </div>
            <div>
              <label for="strategyEntryTime">Entry Time (ET)</label><br/>
              <div class="mobile-native-field">
                <div id="strategyEntryTimeDisplay" class="mobile-native-display" aria-hidden="true"></div>
                <input id="strategyEntryTime" class="input" type="time" value="10:30" />
              </div>
            </div>
            <div>
              <label for="strategySnapshotFromDate">Snapshot From</label><br/>
              <div class="mobile-native-field">
                <div id="strategySnapshotFromDateDisplay" class="mobile-native-display" aria-hidden="true"></div>
                <input id="strategySnapshotFromDate" class="input" type="date" list="strategySnapshotFromDateList" />
              </div>
              <datalist id="strategySnapshotFromDateList"></datalist>
            </div>
            <div>
              <label for="strategySnapshotToDate">Snapshot To</label><br/>
              <div class="mobile-native-field">
                <div id="strategySnapshotToDateDisplay" class="mobile-native-display" aria-hidden="true"></div>
                <input id="strategySnapshotToDate" class="input" type="date" list="strategySnapshotToDateList" />
              </div>
              <datalist id="strategySnapshotToDateList"></datalist>
            </div>
            <div>
              <label>&nbsp;</label><br/>
              <button id="strategyResolveBtn" class="run-analysis-wide">Add leg</button>
            </div>
          </div>
          <div id="strategyBuilderMeta" class="meta" style="margin-top:4px;">Resolve a leg to add it to the strategy.</div>
          <div id="strategyLegsNote" class="meta is-hidden" style="margin-top:12px;">Review your legs here. Update quantities, toggle buy/sell, or remove any leg before running the strategy.</div>
          <div id="strategyLegsWrap" class="result-wrap is-hidden" style="margin-top:12px;">
            <table id="strategyLegsTable">
              <thead>
                <tr>
                  <th></th><th>Leg</th><th>Side</th><th>Quantity</th>
                </tr>
              </thead>
              <tbody></tbody>
            </table>
          </div>
          <div class="controls-card" style="margin-top:12px;">
            <div class="section-heading">Exit Criteria</div>
            <div class="meta" style="grid-column: 1 / -1; margin-top:-4px;">Defaults to 'Hold till expiry', toggle to set exit criteria</div>
            <div class="checkbox-row">
              <input id="strategyHoldToExpiry" type="checkbox" checked />
              <label for="strategyHoldToExpiry">Hold till expiry</label>
            </div>
            <div>
              <label for="strategyExitDays">Exit After (days)</label><br/>
              <input id="strategyExitDays" class="input" type="number" min="0" step="1" value="0" />
            </div>
            <div>
              <label for="strategyExitTime">Time (ET)</label><br/>
              <div class="mobile-native-field">
                <div id="strategyExitTimeDisplay" class="mobile-native-display" aria-hidden="true"></div>
                <input id="strategyExitTime" class="input" type="time" value="15:30" />
              </div>
            </div>
          </div>
          <button id="strategyRunBtn" class="run-analysis-wide" style="display:none; margin-top:12px;">Run Strategy</button>
          <div id="strategyAnalysisMeta" class="meta" style="margin-top:4px;">Run the replay across all dates in your selected range to analyze</div>
          <div id="strategySharedState" class="meta share-snapshot-note is-hidden"></div>
        </div>
      </div>

      <div class="grid full">
        <div id="strategyStatsCard" class="card is-hidden">
          <h2 style="margin-bottom: 6px;">Strategy Stats</h2>
          <div id="strategyStatsMeta" class="meta">Run analysis to compute trade-level summary stats.</div>
          <div id="strategyStatsGrid" class="stats-grid"></div>
        </div>
        <div id="strategyTimeSeriesCard" class="card is-hidden">
          <h2 style="margin-bottom: 6px;">Strategy Time Series</h2>
          <div id="strategySeriesMeta" class="meta">Resolved legs only.</div>
          <div class="result-wrap" style="margin-top:12px;">
            <table id="strategySeriesTable">
              <thead>
                <tr>
                  <th>Snapshot</th><th>Trade</th><th>Contract</th><th>Leg</th><th>Side</th><th>Spot</th><th>Price</th><th>Indexed</th><th>Delta</th><th>Gamma</th><th>Theta</th><th>Vega</th><th>Vol</th><th>Spread</th><th>Leg Contribution</th><th>Strategy</th><th>Strategy Cost</th><th>Strategy P&L</th><th>Strategy Indexed</th>
                </tr>
              </thead>
              <tbody></tbody>
            </table>
          </div>
        </div>
        <div id="strategyIndexChartCard" class="card is-hidden">
          <div class="chart-card-header">
            <h2 style="margin-bottom: 6px;">Strategy Index Chart</h2>
            <div class="chart-share-group">
              <button id="strategyShareBtn" class="share-strategy-btn is-hidden" type="button">Share strategy</button>
              <div id="strategyShareFeedback" class="meta share-feedback is-hidden"></div>
            </div>
          </div>
          <div id="strategyIndexChartMeta" class="meta">Aligned at entry (T+0). 15-minute ET interpolation with blended average.</div>
          <div class="chart-wrap">
            <svg id="strategyIndexChartSvg" class="chart-svg" viewBox="0 0 1200 320" preserveAspectRatio="none"></svg>
            <div id="strategyIndexChartTooltip" class="chart-tooltip" aria-hidden="true"></div>
          </div>
          <div class="chart-legend">
            <span class="chart-legend-item"><span class="chart-legend-swatch" style="background:#0f172a;"></span>Blended avg</span>
            <span class="chart-legend-item"><span class="chart-legend-swatch" style="background:#94a3b8;"></span>Strategy cost</span>
            <span class="chart-legend-item"><span class="chart-legend-swatch" style="background:rgba(22,163,74,0.24);"></span>Profit zone</span>
            <span class="chart-legend-item"><span class="chart-legend-swatch" style="background:rgba(220,38,38,0.22);"></span>Loss zone</span>
          </div>
        </div>
      </div>
    </section>

    </div>
  </div>

  <script>
    const MAX_ANALYZER_SELECTED_CONTRACTS = 4;
    const MAX_STRATEGY_RESOLVED_CONTRACTS = 50;
    const MAX_STRATEGY_ANALYSIS_STREAMERS = 120;
    const MAX_TABLE_RENDER_ROWS = 1000;
    const MINUTE_DIFF_LABEL = 60;
    const TRACKING_ENABLED = __TRACKING_ENABLED__;
    const TRACKING_ANON_KEY = "marketplayground_tracking_anonymous_id";
    const TRACKING_SESSION_KEY = "marketplayground_tracking_session_id";
    const TRACKING_LAST_SEEN_KEY = "marketplayground_tracking_last_seen_at";
    const TRACKING_IDLE_MS = 30 * 60 * 1000;
    const trackingFallbackStore = {};

    const strategyState = {
      symbol: "SPX",
      legs: [],
      nextLegId: 1,
      snapshotDates: [],
      optionTypes: [],
      tableRows: [],
      historyRows: [],
      lastMeta: "",
      hasCompletedResults: false,
      resultsOrigin: "",
      loadedShareToken: "",
      loadedShareUrl: "",
      loadedShareCreatedAt: "",
      isDirtySinceShareLoad: false,
    };

    const tabInitState = {
      strategy: false,
      analyzer: false,
    };

    const analyzerState = {
      symbol: "SPX",
      loadedContracts: [],
      selectedStreamers: new Set(),
      legs: new Map(),
      contractByStreamer: new Map(),
      tableRows: [],
      lastMeta: "",
    };

    function trackingRead(key) {
      try {
        return window.localStorage.getItem(key) || trackingFallbackStore[key] || "";
      } catch {
        return trackingFallbackStore[key] || "";
      }
    }

    function trackingWrite(key, value) {
      trackingFallbackStore[key] = value;
      try {
        window.localStorage.setItem(key, value);
      } catch {}
    }

    function randomTrackingId() {
      if (window.crypto && typeof window.crypto.randomUUID === "function") {
        return window.crypto.randomUUID();
      }
      return `mp-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 10)}`;
    }

    function getAnonymousTrackingId() {
      let current = trackingRead(TRACKING_ANON_KEY);
      if (current) return current;
      current = randomTrackingId();
      trackingWrite(TRACKING_ANON_KEY, current);
      return current;
    }

    function getTrackingSessionId() {
      const now = Date.now();
      const current = trackingRead(TRACKING_SESSION_KEY);
      const lastSeenRaw = trackingRead(TRACKING_LAST_SEEN_KEY);
      const lastSeen = parseInt(lastSeenRaw || "0", 10);
      const needsRefresh = !current || !Number.isFinite(lastSeen) || now - lastSeen > TRACKING_IDLE_MS;
      const next = needsRefresh ? randomTrackingId() : current;
      trackingWrite(TRACKING_SESSION_KEY, next);
      trackingWrite(TRACKING_LAST_SEEN_KEY, String(now));
      return next;
    }

    function getTrackingReferrerHost() {
      try {
        if (!document.referrer) return "";
        return new URL(document.referrer).host || "";
      } catch {
        return "";
      }
    }

    function sanitizeTrackingLeg(leg) {
      return {
        side: String(leg.side || "").toUpperCase(),
        option_type: String(leg.option_type || "").toUpperCase(),
        target_delta: leg.target_delta == null ? null : Number(leg.target_delta),
        target_dte: leg.target_dte == null ? null : Number(leg.target_dte),
        quantity: leg.quantity == null ? 1 : Number(leg.quantity),
        entry_time: String(leg.entry_time || ""),
        snapshot_from_date: String(leg.snapshot_from_date || ""),
        snapshot_to_date: String(leg.snapshot_to_date || ""),
      };
    }

    function currentStrategyLegsForTracking() {
      return strategyState.legs.map((leg) => sanitizeTrackingLeg(leg));
    }

    function currentStrategyRunTrackingPayload() {
      const symbol = document.getElementById("strategySymbol")?.value || "SPX";
      const snapshotDates = Array.isArray(strategyState.snapshotDates) ? strategyState.snapshotDates : [];
      const snapshotFromDate = document.getElementById("strategySnapshotFromDate")?.value || "";
      const snapshotToDate = document.getElementById("strategySnapshotToDate")?.value || "";
      return {
        symbol,
        legs: currentStrategyLegsForTracking(),
        leg_count: strategyState.legs.length,
        snapshot_from_date: snapshotFromDate || snapshotDates[0] || "",
        snapshot_to_date: snapshotToDate || snapshotDates[snapshotDates.length - 1] || "",
        hold_till_expiry: Boolean(document.getElementById("strategyHoldToExpiry")?.checked),
        exit_days: parseInt(document.getElementById("strategyExitDays")?.value || "0", 10),
        exit_time: document.getElementById("strategyExitTime")?.value || "",
      };
    }

    function cloneJsonSafe(value) {
      return JSON.parse(JSON.stringify(value));
    }

    function currentStrategyDefinitionPayload() {
      const base = currentStrategyRunTrackingPayload();
      return {
        symbol: base.symbol,
        snapshot_from_date: base.snapshot_from_date,
        snapshot_to_date: base.snapshot_to_date,
        hold_till_expiry: base.hold_till_expiry,
        exit_days: base.exit_days,
        exit_time: base.exit_time,
        legs: strategyState.legs.map((leg) => ({
          side: String(leg.side || "").toUpperCase(),
          quantity: leg.quantity == null ? 1 : Number(leg.quantity),
          option_type: String(leg.option_type || "").toUpperCase(),
          target_delta: leg.target_delta == null ? null : Number(leg.target_delta),
          target_dte: leg.target_dte == null ? null : Number(leg.target_dte),
          entry_time: String(leg.entry_time || ""),
          snapshot_from_date: String(leg.snapshot_from_date || ""),
          snapshot_to_date: String(leg.snapshot_to_date || ""),
          isResolved: Boolean(leg.isResolved),
          matched_count: leg.matched_count == null ? null : Number(leg.matched_count),
          entry_snapshot_ts: String(leg.entry_snapshot_ts || ""),
          resolved_contracts: Array.isArray(leg.resolved_contracts) ? cloneJsonSafe(leg.resolved_contracts) : [],
        })),
      };
    }

    function currentStrategyShareTrackingPayload(extra) {
      const stats = summarizeStrategyTrades(strategyState.historyRows || []);
      return Object.assign({}, currentStrategyRunTrackingPayload(), {
        result_origin: strategyState.resultsOrigin || "live",
        source_share_token: strategyState.loadedShareToken || "",
        is_dirty_since_share_load: Boolean(strategyState.isDirtySinceShareLoad),
        completed_trade_count: stats.tradeCount || 0,
        history_row_count: Array.isArray(strategyState.historyRows) ? strategyState.historyRows.length : 0,
      }, extra || {});
    }

    function buildStrategySharePayload() {
      const stats = summarizeStrategyTrades(strategyState.historyRows || []);
      return {
        strategy: currentStrategyDefinitionPayload(),
        results: {
          rows: cloneJsonSafe(strategyState.historyRows || []),
        },
        meta: {
          source: "backtest_prod",
          share_version: 1,
          summary: {
            trade_count: stats.tradeCount || 0,
            win_count: stats.winCount || 0,
            loss_count: stats.lossCount || 0,
            overall_pnl: stats.overallPnl == null ? null : Number(stats.overallPnl),
          },
          source_share_token: strategyState.loadedShareToken || "",
        },
      };
    }

    function trackEvent(eventName, data, outcome) {
      if (!TRACKING_ENABLED) return Promise.resolve(null);
      const payload = {
        event_name: eventName,
        event_version: 1,
        anonymous_id: getAnonymousTrackingId(),
        session_id: getTrackingSessionId(),
        page_path: window.location.pathname || "/",
        referrer_host: getTrackingReferrerHost(),
        occurred_at: new Date().toISOString(),
        data: data && typeof data === "object" ? data : {},
      };
      if (outcome) payload.outcome = outcome;
      const body = JSON.stringify(payload);
      try {
        if (navigator.sendBeacon) {
          const blob = new Blob([body], { type: "application/json" });
          if (navigator.sendBeacon("/api/track", blob)) {
            return Promise.resolve(true);
          }
        }
      } catch {}
      return fetch("/api/track", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body,
        keepalive: true,
      }).catch(() => null);
    }

    function trackPageView() {
      trackEvent("page_view", { title: document.title });
    }

    function trackStrategyShareResult(outcome, extra) {
      return trackEvent("strategy_share_result", currentStrategyShareTrackingPayload(extra), outcome);
    }

    function escapeHtml(v) {
      const s = v === null || v === undefined ? "" : String(v);
      return s.split("&").join("&amp;").split("<").join("&lt;").split(">").join("&gt;");
    }

    function parseTimestamp(value) {
      if (!value) return null;
      const raw = String(value).trim();
      if (!raw) return null;
      const hasZone = /Z$|[+-]\\d\\d:\\d\\d$/.test(raw);
      const normalized = hasZone ? raw : raw.replace(" ", "T") + "Z";
      const d = new Date(normalized);
      return Number.isNaN(d.getTime()) ? null : d;
    }

    function toCsv(columns, rows) {
      const esc = (v) => {
        const s = v === null || v === undefined ? "" : String(v);
        if (s.includes('"') || s.includes(",") || s.includes("\\n")) {
          return '"' + s.split('"').join('""') + '"';
        }
        return s;
      };
      const lines = [columns.map(esc).join(",")];
      rows.forEach((r) => lines.push(r.map(esc).join(",")));
      return lines.join("\\n");
    }

    function formatTimeDiff(seconds) {
      if (seconds === null || seconds === undefined || Number.isNaN(seconds)) return "";
      const secs = Number(seconds);
      if (!Number.isFinite(secs)) return "";
      const sign = secs < 0 ? "-" : "";
      const abs = Math.abs(secs);
      if (abs >= MINUTE_DIFF_LABEL) {
        const mins = Math.round(abs / 60);
        return `${sign}${mins}m`;
      }
      return `${sign}${Math.round(abs)}s`;
    }

    function formatDeltaTarget(value) {
      if (value === null || value === undefined) return "";
      const numeric = Number(value);
      if (!Number.isFinite(numeric)) return "";
      const normalized = Math.abs(numeric) <= 1 ? numeric * 100 : numeric;
      return String(Math.round(normalized));
    }

    function formatStrategyIndexAxisLabel(value) {
      const numeric = Number(value);
      if (!Number.isFinite(numeric)) return "";
      return `${numeric.toFixed(1)}%`;
    }

    function formatStatAmount(value) {
      const numeric = Number(value);
      if (!Number.isFinite(numeric)) return "";
      return `$${(numeric * 100).toLocaleString("en-US", {
        minimumFractionDigits: 2,
        maximumFractionDigits: 2,
      })}`;
    }

    function formatStatPercent(value) {
      const numeric = Number(value);
      if (!Number.isFinite(numeric)) return "";
      return `${numeric.toFixed(1)}%`;
    }

    function formatMobileStrategyDateDisplay(value) {
      if (!value || !/^\\d{4}-\\d{2}-\\d{2}$/.test(String(value))) return "";
      const [year, month, day] = String(value).split("-").map((part) => Number(part));
      const date = new Date(Date.UTC(year, month - 1, day));
      return new Intl.DateTimeFormat("en-US", {
        timeZone: "UTC",
        month: "short",
        day: "numeric",
        year: "numeric",
      }).format(date);
    }

    function formatMobileStrategyTimeDisplay(value) {
      if (!value || !/^\\d{2}:\\d{2}$/.test(String(value))) return "";
      const [hours, minutes] = String(value).split(":").map((part) => Number(part));
      const date = new Date(Date.UTC(2000, 0, 1, hours, minutes));
      return new Intl.DateTimeFormat("en-US", {
        timeZone: "UTC",
        hour: "numeric",
        minute: "2-digit",
        hour12: true,
      }).format(date);
    }

    function syncStrategyMobileNativeField(inputId, displayId, formatter) {
      const input = document.getElementById(inputId);
      const display = document.getElementById(displayId);
      if (!input || !display) return;
      display.textContent = formatter(input.value) || "";
    }

    function syncStrategyMobileNativeDisplays() {
      syncStrategyMobileNativeField("strategyEntryTime", "strategyEntryTimeDisplay", formatMobileStrategyTimeDisplay);
      syncStrategyMobileNativeField("strategySnapshotFromDate", "strategySnapshotFromDateDisplay", formatMobileStrategyDateDisplay);
      syncStrategyMobileNativeField("strategySnapshotToDate", "strategySnapshotToDateDisplay", formatMobileStrategyDateDisplay);
      syncStrategyMobileNativeField("strategyExitTime", "strategyExitTimeDisplay", formatMobileStrategyTimeDisplay);
    }

    function parseHmToMinutes(value) {
      const raw = String(value || "").trim();
      const match = raw.match(/^(\\d{2}):(\\d{2})$/);
      if (!match) return null;
      const hours = Number(match[1]);
      const minutes = Number(match[2]);
      if (!Number.isFinite(hours) || !Number.isFinite(minutes)) return null;
      return hours * 60 + minutes;
    }

    function formatLocalDateTime(value) {
      const d = parseTimestamp(value);
      if (!d) return "";
      return new Intl.DateTimeFormat("en-US", {
        timeZone: "America/New_York",
        year: "numeric",
        month: "2-digit",
        day: "2-digit",
        hour: "2-digit",
        minute: "2-digit",
        second: "2-digit",
        hour12: true,
      }).format(d) + " ET";
    }

    function contractLabel(contract) {
      const t = String(contract.option_type || "").toLowerCase();
      const typeLabel = t ? `${t[0].toUpperCase()}${t.slice(1)}` : "";
      return `${typeLabel} ${contract.strike_price} ${contract.expiration_date}`.trim();
    }

    function buildMarketLookup(series, fieldName) {
      const normalized = (series || [])
        .filter((row) => row.snapshot_ts && row[fieldName] !== null && row[fieldName] !== undefined)
        .map((row) => ({ ts: row.snapshot_ts, value: Number(row[fieldName]) }))
        .filter((row) => Number.isFinite(row.value))
        .sort((a, b) => parseTimestamp(a.ts) - parseTimestamp(b.ts));
      const times = normalized.map((row) => row.ts);

      return function nearestValue(ts) {
        if (!times.length) return null;
        const target = parseTimestamp(ts);
        if (!target) return null;
        let lo = 0;
        let hi = times.length - 1;
        let best = 0;
        while (lo <= hi) {
          const mid = Math.floor((lo + hi) / 2);
          const midTs = parseTimestamp(times[mid]);
          if (midTs <= target) {
            best = mid;
            lo = mid + 1;
          } else {
            hi = mid - 1;
          }
        }
        return normalized[best] ? normalized[best].value : null;
      };
    }

    function buildSpotLookup(series) {
      return buildMarketLookup(series, "spot_price");
    }

    function buildVixLookup(series) {
      return buildMarketLookup(series, "implied_volatility_index");
    }

    function renderSimpleTable(selector, columns, rows) {
      const table = document.getElementById(selector);
      const thead = table.querySelector("thead");
      const tbody = table.querySelector("tbody");
      thead.innerHTML = "";
      tbody.innerHTML = "";
      if (!columns.length) return;
      const trHead = document.createElement("tr");
      columns.forEach((c) => {
        const th = document.createElement("th");
        th.textContent = c;
        trHead.appendChild(th);
      });
      thead.appendChild(trHead);
      rows.forEach((r) => {
        const tr = document.createElement("tr");
        r.forEach((v) => {
          const td = document.createElement("td");
          td.textContent = v == null ? "" : v;
          tr.appendChild(td);
        });
        tbody.appendChild(tr);
      });
    }

    function initTabs() {
      const buttons = document.querySelectorAll(".tab-button");
      const panels = document.querySelectorAll(".tab-panel");

      function showTab(name) {
        buttons.forEach((btn) => {
          btn.classList.toggle("active", btn.getAttribute("data-tab") === name);
        });
        panels.forEach((panel) => {
          panel.classList.toggle("active", panel.getAttribute("data-tab") === name);
        });
      }

      buttons.forEach((btn) => {
        btn.addEventListener("click", () => {
          const target = btn.getAttribute("data-tab");
          if (!target) return;
          showTab(target);
        });
      });

      showTab("strategy");
    }

    function strategyLegLabel(leg) {
      const type = String(leg.option_type || "").toUpperCase();
      const delta = formatDeltaTarget(leg.target_delta);
      const dte = leg.target_dte == null ? "" : String(leg.target_dte);
      const entry = String(leg.entry_time || "");
      return `${type} Δ${delta} DTE ${dte} @ ${entry}`;
    }

    function populateStrategyOptionTypeOptions(optionTypes) {
      const selectEl = document.getElementById("strategyOptionType");
      if (!selectEl) return;
      const values = (Array.isArray(optionTypes) ? optionTypes : [])
        .map((value) => String(value || "").trim().toUpperCase())
        .filter((value, index, arr) => value && arr.indexOf(value) === index);
      const selected = String(selectEl.value || "").trim().toUpperCase();
      const normalized = values.length ? values : ["PUT"];
      selectEl.innerHTML = normalized
        .map((value) => `<option value="${escapeHtml(value)}">${escapeHtml(value)}</option>`)
        .join("");
      selectEl.value = normalized.includes(selected) ? selected : normalized[0];
      strategyState.optionTypes = normalized;
    }

    function hasMatchingStrategyLeg(candidate) {
      return strategyState.legs.some((leg) => (
        String(leg.side || "BUY") === String(candidate.side || "BUY")
        && String(leg.option_type || "PUT") === String(candidate.option_type || "PUT")
        && Number(leg.target_dte) === Number(candidate.target_dte)
        && Number(leg.target_delta) === Number(candidate.target_delta)
        && String(leg.entry_time || "") === String(candidate.entry_time || "")
        && String(leg.snapshot_from_date || "") === String(candidate.snapshot_from_date || "")
        && String(leg.snapshot_to_date || "") === String(candidate.snapshot_to_date || "")
      ));
    }

    function refreshStrategyExitCriteriaState() {
      const holdEl = document.getElementById("strategyHoldToExpiry");
      const exitDaysEl = document.getElementById("strategyExitDays");
      const exitTimeEl = document.getElementById("strategyExitTime");
      if (!holdEl || !exitDaysEl || !exitTimeEl) return;
      const disabled = Boolean(holdEl.checked);
      exitDaysEl.disabled = disabled;
      exitTimeEl.disabled = disabled;
      const wrappers = [exitDaysEl.parentElement, exitTimeEl.parentElement];
      wrappers.forEach((wrapper) => {
        if (!wrapper) return;
        wrapper.classList.toggle("field-disabled", disabled);
      });
    }

    function refreshStrategyRunButtonVisibility() {
      const runBtn = document.getElementById("strategyRunBtn");
      const analysisMeta = document.getElementById("strategyAnalysisMeta");
      if (!runBtn || !analysisMeta) return;
      const hasLegs = strategyState.legs.length > 0;
      runBtn.style.display = hasLegs ? "inline-block" : "none";
      if (!hasLegs) {
        analysisMeta.textContent = "Resolve at least one leg to analyze.";
      }
      updateStrategyShareControls();
    }

    function setStrategyShareFeedback(message, tone, shareUrl) {
      const feedback = document.getElementById("strategyShareFeedback");
      if (!feedback) return;
      feedback.innerHTML = "";
      feedback.className = `meta share-feedback${tone ? ` ${tone}` : ""}`;
      if (!message && !shareUrl) {
        feedback.classList.add("is-hidden");
        return;
      }
      if (message) {
        const text = document.createElement("span");
        text.textContent = message;
        feedback.appendChild(text);
      }
      if (shareUrl) {
        if (message) {
          feedback.appendChild(document.createTextNode(" "));
        }
        const link = document.createElement("a");
        link.href = shareUrl;
        link.target = "_blank";
        link.rel = "noopener noreferrer";
        link.textContent = shareUrl;
        feedback.appendChild(link);
      }
      feedback.classList.remove("is-hidden");
    }

    function updateStrategySharedStateNote() {
      const note = document.getElementById("strategySharedState");
      if (!note) return;
      note.className = "meta share-snapshot-note";
      if (strategyState.resultsOrigin === "shared" && strategyState.isDirtySinceShareLoad) {
        note.textContent = "You edited this shared strategy. The visible stats and chart still reflect the original shared snapshot until you rerun.";
        note.classList.remove("is-hidden");
        return;
      }
      if (strategyState.resultsOrigin === "shared") {
        note.textContent = "Loaded from a shared strategy link. You can edit the strategy, rerun it locally, and create a new share link from your updated results.";
        note.classList.remove("is-hidden");
        return;
      }
      note.textContent = "";
      note.classList.add("is-hidden");
    }

    function updateStrategyShareControls() {
      const btn = document.getElementById("strategyShareBtn");
      if (!btn) return;
      const show = Boolean(strategyState.hasCompletedResults);
      btn.classList.toggle("is-hidden", !show);
      if (!show) {
        btn.disabled = true;
        setStrategyShareFeedback("", "");
        return;
      }
      btn.disabled = Boolean(strategyState.resultsOrigin === "shared" && strategyState.isDirtySinceShareLoad);
      if (btn.disabled) {
        setStrategyShareFeedback("Rerun to share your updated strategy.", "");
      }
    }

    function markStrategyDirtyFromShare() {
      if (!strategyState.hasCompletedResults || strategyState.resultsOrigin !== "shared" || strategyState.isDirtySinceShareLoad) {
        return;
      }
      strategyState.isDirtySinceShareLoad = true;
      updateStrategySharedStateNote();
      updateStrategyShareControls();
    }

    function applyStrategyResults(rows, options) {
      const config = options && typeof options === "object" ? options : {};
      const normalizedRows = Array.isArray(rows) ? rows : [];
      strategyState.historyRows = normalizedRows;
      strategyState.hasCompletedResults = normalizedRows.length > 0;
      strategyState.resultsOrigin = normalizedRows.length ? String(config.origin || "live") : "";
      strategyState.loadedShareToken = strategyState.resultsOrigin === "shared" ? String(config.shareToken || "") : "";
      strategyState.loadedShareUrl = strategyState.resultsOrigin === "shared" ? String(config.shareUrl || "") : "";
      strategyState.loadedShareCreatedAt = strategyState.resultsOrigin === "shared" ? String(config.createdAt || "") : "";
      strategyState.isDirtySinceShareLoad = false;
      setStrategyResultsVisibility(strategyState.hasCompletedResults);
      renderStrategyStats(normalizedRows);
      renderStrategySeriesTable(normalizedRows);
      renderStrategyIndexChart(normalizedRows);
      if (config.clearFeedback !== false) {
        setStrategyShareFeedback("", "");
      }
      updateStrategySharedStateNote();
      updateStrategyShareControls();
    }

    function renderStrategyLegsTable() {
      const body = document.querySelector("#strategyLegsTable tbody");
      const wrap = document.getElementById("strategyLegsWrap");
      const note = document.getElementById("strategyLegsNote");
      body.innerHTML = "";
      if (wrap) {
        wrap.classList.toggle("is-hidden", !strategyState.legs.length);
      }
      if (note) {
        note.classList.toggle("is-hidden", !strategyState.legs.length);
      }
      strategyState.legs.forEach((leg) => {
        const buyActive = leg.side === "BUY" ? "active" : "";
        const sellActive = leg.side === "SELL" ? "active" : "";
        const tr = document.createElement("tr");
        tr.innerHTML = `
          <td class="strategy-leg-remove"><button type="button" class="remove-leg" data-leg-id="${String(leg.id)}">x</button></td>
          <td class="strategy-leg-name">${escapeHtml(strategyLegLabel(leg))}</td>
          <td class="strategy-leg-side">
            <div class="strategy-leg-mobile-label">Side</div>
            <div class="side-group">
              <button type="button" class="side-btn buy-btn ${buyActive}" data-side-leg-id="${String(leg.id)}" data-side="BUY">BUY</button>
              <button type="button" class="side-btn sell-btn ${sellActive}" data-side-leg-id="${String(leg.id)}" data-side="SELL">SELL</button>
            </div>
          </td>
          <td class="strategy-leg-qty">
            <div class="strategy-leg-mobile-label">Quantity</div>
            <input class="input qty-input" type="number" min="1" step="1" value="${Number(leg.quantity) || 1}" data-qty-leg-id="${String(leg.id)}" />
          </td>
        `;
        body.appendChild(tr);
      });

      body.querySelectorAll("[data-leg-id]").forEach((btn) => {
        btn.addEventListener("click", (event) => {
          const rawId = event.currentTarget.getAttribute("data-leg-id");
          const id = parseInt(rawId || "", 10);
          if (!Number.isFinite(id)) return;
          strategyState.legs = strategyState.legs.filter((leg) => leg.id !== id);
          markStrategyDirtyFromShare();
          renderStrategyLegsTable();
        });
      });

      body.querySelectorAll("[data-side-leg-id]").forEach((btn) => {
        btn.addEventListener("click", (event) => {
          const rawId = event.currentTarget.getAttribute("data-side-leg-id");
          const side = event.currentTarget.getAttribute("data-side");
          const id = parseInt(rawId || "", 10);
          if (!Number.isFinite(id) || (side !== "BUY" && side !== "SELL")) return;
          const leg = strategyState.legs.find((row) => row.id === id);
          if (!leg) return;
          leg.side = side;
          markStrategyDirtyFromShare();
          renderStrategyLegsTable();
        });
      });

      body.querySelectorAll("[data-qty-leg-id]").forEach((inputEl) => {
        inputEl.addEventListener("input", (event) => {
          const rawId = event.currentTarget.getAttribute("data-qty-leg-id");
          const id = parseInt(rawId || "", 10);
          if (!Number.isFinite(id)) return;
          const leg = strategyState.legs.find((row) => row.id === id);
          if (!leg) return;
          const parsed = parseInt(event.currentTarget.value || "1", 10);
          leg.quantity = Number.isFinite(parsed) && parsed > 0 ? parsed : 1;
          markStrategyDirtyFromShare();
        });
      });
      refreshStrategyRunButtonVisibility();
    }

    function trackStrategyLegResult(outcome, payload, extra) {
      const merged = Object.assign({}, payload || {}, extra || {});
      return trackEvent("strategy_leg_add_result", merged, outcome);
    }

    function trackStrategyRunResult(outcome, extra) {
      const merged = Object.assign({}, currentStrategyRunTrackingPayload(), extra || {});
      return trackEvent("strategy_run_result", merged, outcome);
    }

    async function resolveStrategyLeg() {
      const meta = document.getElementById("strategyBuilderMeta");
      const symbol = document.getElementById("strategySymbol").value || "SPX";
      const side = document.getElementById("strategySide").value || "BUY";
      const optionType = document.getElementById("strategyOptionType").value || "PUT";
      const dte = parseInt(document.getElementById("strategyDte").value || "0", 10);
      const targetDelta = parseFloat(document.getElementById("strategyDelta").value || "0");
      const entryTime = document.getElementById("strategyEntryTime").value || "";
      const snapshotFromDate = document.getElementById("strategySnapshotFromDate").value || "";
      const snapshotToDate = document.getElementById("strategySnapshotToDate").value || "";
      const snapshotDates = Array.isArray(strategyState.snapshotDates) ? strategyState.snapshotDates : [];
      const hasSnapshotDate = (value) => !value || snapshotDates.includes(value);
      const roundedTargetDelta = Number.isFinite(targetDelta) ? Math.round(targetDelta) : null;
      const trackingPayload = {
        symbol,
        side,
        option_type: optionType,
        target_delta: roundedTargetDelta,
        target_dte: Number.isFinite(dte) ? dte : null,
        entry_time: entryTime,
        snapshot_from_date: snapshotFromDate,
        snapshot_to_date: snapshotToDate,
      };
      trackEvent("strategy_leg_add_attempt", trackingPayload);

      function fail(message, reason, outcome = "validation_error", extra = {}) {
        meta.textContent = message;
        meta.className = "meta danger";
        trackStrategyLegResult(outcome, trackingPayload, Object.assign({ reason }, extra));
      }

      if (!entryTime) {
        fail("Entry Time is required.", "missing_entry_time");
        return;
      }
      if (!Number.isFinite(dte) || dte < 0) {
        fail("DTE must be a non-negative integer.", "invalid_dte");
        return;
      }
      if (!Number.isFinite(targetDelta) || targetDelta <= 0) {
        fail("Delta must be > 0.", "invalid_delta");
        return;
      }
      if (!snapshotDates.length) {
        fail("No snapshot dates available for this symbol.", "no_snapshot_dates");
        return;
      }
      if (!hasSnapshotDate(snapshotFromDate) || !hasSnapshotDate(snapshotToDate)) {
        fail(
          "Choose Snapshot From/To dates from the available snapshot dates.",
          "invalid_snapshot_dates"
        );
        return;
      }
      const effectiveFromDate = snapshotFromDate || snapshotDates[0];
      const effectiveToDate = snapshotToDate || snapshotDates[snapshotDates.length - 1];
      trackingPayload.snapshot_from_date = effectiveFromDate;
      trackingPayload.snapshot_to_date = effectiveToDate;
      if (effectiveFromDate > effectiveToDate) {
        fail("Snapshot From must not be after Snapshot To.", "invalid_snapshot_range");
        return;
      }
      if (hasMatchingStrategyLeg({
        side,
        option_type: optionType,
        target_dte: dte,
        target_delta: roundedTargetDelta,
        entry_time: entryTime,
        snapshot_from_date: effectiveFromDate,
        snapshot_to_date: effectiveToDate,
      })) {
        fail(
          "You already have a leg with matching criteria added. Feel free to adjust the quantity.",
          "duplicate_leg"
        );
        return;
      }

      const params = new URLSearchParams({
        symbol,
        option_type: optionType,
        dte: String(dte),
        target_delta: String(roundedTargetDelta),
        entry_time: entryTime,
        target_side: side,
        strict_dte: "1",
      });
      params.set("snapshot_from", `${effectiveFromDate}T00:00:00`);
      params.set("snapshot_to", `${effectiveToDate}T23:59:59`);
      params.set("best_only", "1");

      try {
        const res = await fetch(`/api/options/resolve-leg?${params.toString()}`);
        const payload = await res.json();
        const resolvedContracts = Array.isArray(payload.contracts) ? payload.contracts : (payload.streamer_symbol ? [payload] : []);
        if (!res.ok || !resolvedContracts.length) {
          fail(
            "Could not resolve leg: " + (payload.error || "no match found."),
            payload.error || "no_match_found",
            "no_match"
          );
          return;
        }

        const keptContracts = resolvedContracts.slice(0, MAX_STRATEGY_RESOLVED_CONTRACTS);
        if (!keptContracts.length) {
          trackStrategyLegResult("no_match", trackingPayload, { reason: "no_kept_contracts" });
          meta.textContent = "";
          meta.className = "meta";
          renderStrategyLegsTable();
          return;
        }
        strategyState.legs.push({
          id: strategyState.nextLegId++,
          side,
          quantity: 1,
          option_type: optionType,
          target_delta: roundedTargetDelta,
          target_dte: dte,
          entry_time: entryTime,
          snapshot_from_date: effectiveFromDate,
          snapshot_to_date: effectiveToDate,
          isResolved: true,
          matched_count: Number(payload.count) > 0 ? Number(payload.count) : keptContracts.length,
          entry_snapshot_ts: keptContracts[0] ? keptContracts[0].snapshot_ts : null,
          resolved_contracts: keptContracts,
        });
        markStrategyDirtyFromShare();
        meta.textContent = "";
        meta.className = "meta";
        trackStrategyLegResult("success", trackingPayload, {
          matched_count: Number(payload.count) > 0 ? Number(payload.count) : keptContracts.length,
          resolved_count: keptContracts.length,
        });
        renderStrategyLegsTable();
      } catch {
        fail("Could not resolve leg: request failed.", "request_failed", "error");
      }
    }

    function transformStrategySeriesRows(rows, spotSeries, tradePlans, exitCriteria) {
      const nearestSpot = buildSpotLookup(spotSeries || []);
      const nearestVix = buildVixLookup(spotSeries || []);
      const rowsByStreamer = new Map();
      rows.forEach((row) => {
        const streamer = row.streamer_symbol;
        if (!streamer) return;
        if (!rowsByStreamer.has(streamer)) rowsByStreamer.set(streamer, []);
        rowsByStreamer.get(streamer).push(row);
      });
      rowsByStreamer.forEach((list) => {
        list.sort((a, b) => parseTimestamp(a.snapshot_ts) - parseTimestamp(b.snapshot_ts));
      });

      const rowByStreamerTs = new Map();
      rowsByStreamer.forEach((list, streamer) => {
        const byTs = new Map();
        list.forEach((row) => {
          const ts = String(row.snapshot_ts || "");
          if (!ts || byTs.has(ts)) return;
          byTs.set(ts, row);
        });
        rowByStreamerTs.set(streamer, byTs);
      });

      const allTimestamps = Array.from(new Set(rows.map((row) => String(row.snapshot_ts || "")).filter(Boolean)))
        .sort((a, b) => parseTimestamp(a) - parseTimestamp(b));

      const enrichedTrades = tradePlans.map((trade) => {
        const legs = trade.legs.map((leg) => {
          const list = rowsByStreamer.get(leg.streamer_symbol) || [];
          const entryTs = parseTimestamp(leg.entry_snapshot_ts);
          let entryRow = null;
          if (entryTs) {
            entryRow = list.find((row) => {
              const rowTs = parseTimestamp(row.snapshot_ts);
              return rowTs && rowTs.getTime() === entryTs.getTime();
            }) || [...list].reverse().find((row) => {
              const rowTs = parseTimestamp(row.snapshot_ts);
              return rowTs && row.value != null && rowTs <= entryTs;
            });
          }
          if (!entryRow) {
            entryRow = list.find((row) => row.value != null && Number.isFinite(Number(row.value))) || null;
          }
          const entryValue = entryRow && entryRow.value != null && Number.isFinite(Number(entryRow.value)) ? Number(entryRow.value) : null;
          return {
            ...leg,
            entry_ts: entryTs,
            entry_value: entryValue,
          };
        });
        const entryTimes = legs
          .map((leg) => leg.entry_ts)
          .filter((ts) => ts && Number.isFinite(ts.getTime()))
          .map((ts) => ts.getTime());
        const tradeStartTs = entryTimes.length ? new Date(Math.max(...entryTimes)) : null;
        const expirations = legs
          .map((leg) => String(leg.contract && leg.contract.expiration_date ? leg.contract.expiration_date : ""))
          .filter((value) => /^\\d{4}-\\d{2}-\\d{2}$/.test(value))
          .sort();
        const strategyExpirationYmd = expirations.length ? expirations[0] : "";
        return { ...trade, legs, trade_start_ts: tradeStartTs, strategy_expiration_ymd: strategyExpirationYmd };
      });

      const completedTrades = enrichedTrades.filter((trade) => {
        if (!trade.trade_start_ts) return false;
        return allTimestamps.some((ts) => {
          const currentTs = parseTimestamp(ts);
          if (!currentTs || currentTs < trade.trade_start_ts) return false;
          if (exitCriteria.holdTillExpiry) {
            if (!isAtOrAfterExpiryDate(currentTs, trade.strategy_expiration_ymd)) return false;
          } else if (!isAtOrAfterStrategyExit(currentTs, trade.trade_start_ts, exitCriteria.exitDays, exitCriteria.exitTime)) {
            return false;
          }
          return trade.legs.every((leg) => {
            const streamerRows = rowByStreamerTs.get(leg.streamer_symbol);
            const row = streamerRows ? streamerRows.get(ts) : null;
            const value = row && row.value != null ? Number(row.value) : null;
            if (!row || value == null || !Number.isFinite(value) || leg.entry_value == null || !Number.isFinite(leg.entry_value)) {
              return false;
            }
            if (leg.entry_ts && currentTs < leg.entry_ts) return false;
            return true;
          });
        });
      });

      const out = [];
      allTimestamps.forEach((ts) => {
        const perTradeRows = [];
        completedTrades.forEach((trade) => {
          const currentTs = parseTimestamp(ts);
          if (trade.trade_start_ts && currentTs && currentTs < trade.trade_start_ts) return;
          if (exitCriteria.holdTillExpiry) {
            if (!isBeforeOrOnExpiryDate(currentTs, trade.strategy_expiration_ymd)) return;
          }
          let strategyValue = 0;
          let strategyCost = 0;
          let complete = true;
          const tradeRows = [];
          trade.legs.forEach((leg) => {
            const streamerRows = rowByStreamerTs.get(leg.streamer_symbol);
            const row = streamerRows ? streamerRows.get(ts) : null;
            const value = row && row.value != null ? Number(row.value) : null;
            if (!row || value == null || !Number.isFinite(value) || leg.entry_value == null || !Number.isFinite(leg.entry_value)) {
              complete = false;
              return;
            }
            if (leg.entry_ts && currentTs && currentTs < leg.entry_ts) {
              complete = false;
              return;
            }
            if (!exitCriteria.holdTillExpiry && !isWithinStrategyExitWindow(currentTs, trade.trade_start_ts, exitCriteria.exitDays, exitCriteria.exitTime)) {
              complete = false;
              return;
            }
            const contribution = leg.sign * leg.quantity * value;
            strategyValue += contribution;
            strategyCost += leg.sign * leg.quantity * leg.entry_value;
            const indexed = leg.entry_value !== 0 ? (value / leg.entry_value) * 100 : null;
            tradeRows.push({
              ...row,
              snapshot_ts: ts,
              trade_index: trade.trade_index,
              indexed,
              spot_price: nearestSpot(ts),
              vix_price: nearestVix(ts),
              leg_contribution: contribution,
              resolved_contract: contractLabel(leg.contract),
              leg_label: strategyLegLabel(leg.leg_def),
              leg_side: leg.leg_def.side,
              isStrategySummary: false,
            });
          });
          if (!complete || tradeRows.length !== trade.legs.length || strategyCost === 0) return;
          const strategyIdx = (strategyValue / strategyCost) * 100;
          const strategyPnl = strategyValue - strategyCost;
          tradeRows.forEach((row) => {
            perTradeRows.push({
              ...row,
              strategy_price: strategyValue,
              strategy_cost: strategyCost,
              strategy_pnl: strategyPnl,
              strategy_indexed: strategyIdx,
            });
          });
        });

        perTradeRows.sort((a, b) => {
          if (a.trade_index !== b.trade_index) return a.trade_index - b.trade_index;
          return contractLabel(a).localeCompare(contractLabel(b));
        });
        perTradeRows.forEach((row) => out.push(row));
      });

      return out;
    }

    function renderStrategySeriesTable(rows) {
      const body = document.querySelector("#strategySeriesTable tbody");
      const meta = document.getElementById("strategySeriesMeta");
      if (!body) return;
      body.innerHTML = "";
      const allRows = Array.isArray(rows) ? rows : [];
      const visibleRows = allRows.slice(0, MAX_TABLE_RENDER_ROWS);
      if (meta) {
        meta.textContent = allRows.length > visibleRows.length
          ? `Showing first ${visibleRows.length} of ${allRows.length} rows. Chart, stats, and sharing use the full result set.`
          : `${allRows.length} rows.`;
      }
      visibleRows.forEach((row) => {
        const spread = row.isStrategySummary
          ? null
          : (row.bid_price == null || row.ask_price == null ? null : Number(row.ask_price) - Number(row.bid_price));
        const strategy = row.strategy_price == null ? "" : Number(row.strategy_price).toFixed(4);
        const strategyCost = row.strategy_cost == null ? "" : Number(row.strategy_cost).toFixed(4);
        const strategyPnl = row.strategy_pnl == null ? "" : Number(row.strategy_pnl).toFixed(4);
        const strategyIndexed = row.strategy_indexed == null ? "" : Number(row.strategy_indexed).toFixed(4);
        const contribution = row.leg_contribution == null ? "" : Number(row.leg_contribution).toFixed(4);
        const legLabel = row.isStrategySummary ? "" : escapeHtml(row.leg_label || "");
        const legSide = row.isStrategySummary ? "" : escapeHtml(row.leg_side || "");
        const price = row.value;
        const delta = row.isStrategySummary ? null : row.delta;
        const gamma = row.isStrategySummary ? null : row.gamma;
        const theta = row.isStrategySummary ? null : row.theta;
        const vega = row.isStrategySummary ? null : row.vega;
        const vol = row.isStrategySummary ? null : row.volatility;
        const tr = document.createElement("tr");
        tr.innerHTML = `
          <td>${row.snapshot_ts ? formatLocalDateTime(row.snapshot_ts) : ""}</td>
          <td>${row.isStrategySummary ? "Avg" : escapeHtml(String(row.trade_index == null ? "" : row.trade_index))}</td>
          <td>${row.isStrategySummary ? "Strategy" : escapeHtml(row.isStrategySummary ? "Strategy" : row.resolved_contract || contractLabel(row))}</td>
          <td>${legLabel}</td>
          <td>${legSide}</td>
          <td>${row.spot_price == null ? "" : Number(row.spot_price).toFixed(4)}</td>
          <td>${price == null ? "" : Number(price).toFixed(4)}</td>
          <td>${row.indexed == null ? "" : Number(row.indexed).toFixed(4)}</td>
          <td>${delta == null ? "" : Number(delta).toFixed(4)}</td>
          <td>${gamma == null ? "" : Number(gamma).toFixed(4)}</td>
          <td>${theta == null ? "" : Number(theta).toFixed(4)}</td>
          <td>${vega == null ? "" : Number(vega).toFixed(4)}</td>
          <td>${vol == null ? "" : Number(vol).toFixed(4)}</td>
          <td>${spread == null ? "" : spread.toFixed(4)}</td>
          <td>${contribution}</td>
          <td>${strategy}</td>
          <td>${strategyCost}</td>
          <td>${strategyPnl}</td>
          <td>${strategyIndexed}</td>
        `;
        if (row.isStrategySummary) tr.className = "strategy-summary";
        body.appendChild(tr);
      });
    }

    function renderStrategyTradeMatrixTable(rows) {
      const table = document.getElementById("strategyTradeMatrixTable");
      const head = table ? table.querySelector("thead") : null;
      const body = table ? table.querySelector("tbody") : null;
      if (!head || !body) return;

      head.innerHTML = "";
      body.innerHTML = "";

      const detailRows = (rows || []).filter((row) => !row.isStrategySummary);
      if (!detailRows.length) return;

      const byTrade = new Map();
      detailRows.forEach((row) => {
        const key = String(row.trade_index == null ? "" : row.trade_index);
        if (!key) return;
        if (!byTrade.has(key)) {
          byTrade.set(key, new Map());
        }
        const price = row.strategy_price;
        if (price == null || !Number.isFinite(Number(price))) return;
        const tradeMap = byTrade.get(key);
        const ts = String(row.snapshot_ts || "");
        if (!ts || tradeMap.has(ts)) return;
        tradeMap.set(ts, Number(price));
      });

      const sortedTrades = Array.from(byTrade.entries()).sort((a, b) => Number(a[0]) - Number(b[0]));
      const normalizedRows = sortedTrades.map(([tradeKey, tradeMap]) => {
        const sortedTs = Array.from(tradeMap.keys()).sort((a, b) => parseTimestamp(a) - parseTimestamp(b));
        if (!sortedTs.length) return { tradeKey, entryTs: "", indexed: [] };
        const entryTs = sortedTs[0];
        const entryPrice = tradeMap.get(entryTs);
        if (entryPrice == null || !Number.isFinite(Number(entryPrice)) || Number(entryPrice) === 0) {
          return { tradeKey, entryTs, indexed: sortedTs.map(() => "") };
        }
        const indexed = sortedTs.map((ts) => {
          const px = tradeMap.get(ts);
          if (px == null || !Number.isFinite(Number(px))) return "";
          return ((Number(px) / Number(entryPrice)) * 100).toFixed(4);
        });
        return { tradeKey, entryTs, indexed };
      });

      const maxSteps = normalizedRows.reduce((m, row) => Math.max(m, row.indexed.length), 0);
      if (!maxSteps) return;
      const headerCells = ["Trade", "Entry Snapshot", ...Array.from({ length: maxSteps }, (_, idx) => `T+${idx}`)];
      const trHead = document.createElement("tr");
      trHead.innerHTML = headerCells.map((label) => `<th>${escapeHtml(label)}</th>`).join("");
      head.appendChild(trHead);

      normalizedRows.forEach((row) => {
        const tr = document.createElement("tr");
        const cells = [
          row.tradeKey,
          row.entryTs ? formatLocalDateTime(row.entryTs) : "",
          ...Array.from({ length: maxSteps }, (_, idx) => row.indexed[idx] || ""),
        ];
        tr.innerHTML = cells.map((value) => `<td>${escapeHtml(String(value))}</td>`).join("");
        body.appendChild(tr);
      });
    }

    function summarizeStrategyTrades(rows) {
      const detailRows = (rows || []).filter((row) => !row.isStrategySummary);
      const finalsByTrade = new Map();
      detailRows.forEach((row) => {
        const tradeKey = String(row.trade_index == null ? "" : row.trade_index);
        const ts = String(row.snapshot_ts || "");
        const tsDate = parseTimestamp(ts);
        if (!tradeKey || !tsDate) return;
        const prior = finalsByTrade.get(tradeKey);
        if (!prior || tsDate > prior.tsDate) {
          finalsByTrade.set(tradeKey, {
            tradeKey,
            ts,
            tsDate,
            strategy_pnl: row.strategy_pnl == null ? null : Number(row.strategy_pnl),
            strategy_indexed: row.strategy_indexed == null ? null : Number(row.strategy_indexed),
            strategy_cost: row.strategy_cost == null ? null : Number(row.strategy_cost),
            strategy_price: row.strategy_price == null ? null : Number(row.strategy_price),
          });
        }
      });

      function strategyReturnPercent(row) {
        const pnl = row && row.strategy_pnl != null ? Number(row.strategy_pnl) : NaN;
        const costAbs = row && row.strategy_cost != null ? Math.abs(Number(row.strategy_cost)) : NaN;
        if (!Number.isFinite(pnl) || !Number.isFinite(costAbs) || costAbs === 0) return null;
        return (pnl / costAbs) * 100;
      }

      const finals = Array.from(finalsByTrade.values()).filter((row) => row.strategy_pnl != null && Number.isFinite(row.strategy_pnl));
      const wins = finals.filter((row) => row.strategy_pnl > 0);
      const losses = finals.filter((row) => row.strategy_pnl < 0);
      const flats = finals.filter((row) => row.strategy_pnl === 0);
      const totalPnl = finals.reduce((sum, row) => sum + row.strategy_pnl, 0);
      const grossWin = wins.reduce((sum, row) => sum + row.strategy_pnl, 0);
      const grossLossAbs = Math.abs(losses.reduce((sum, row) => sum + row.strategy_pnl, 0));
      const avg = (items, selector) => items.length ? items.reduce((sum, item) => sum + selector(item), 0) / items.length : null;
      const bestTrade = finals.length ? finals.reduce((best, row) => (best == null || row.strategy_pnl > best.strategy_pnl ? row : best), null) : null;
      const worstTrade = finals.length ? finals.reduce((worst, row) => (worst == null || row.strategy_pnl < worst.strategy_pnl ? row : worst), null) : null;

      return {
        tradeCount: finals.length,
        winCount: wins.length,
        lossCount: losses.length,
        flatCount: flats.length,
        winRate: finals.length ? (wins.length / finals.length) * 100 : null,
        avgWin: avg(wins, (row) => row.strategy_pnl),
        avgLoss: avg(losses, (row) => row.strategy_pnl),
        overallPnl: finals.length ? totalPnl : null,
        avgTradePnl: finals.length ? totalPnl / finals.length : null,
        avgGainLossPct: avg(
          finals.filter((row) => strategyReturnPercent(row) != null),
          (row) => strategyReturnPercent(row)
        ),
        bestTradePnl: bestTrade ? bestTrade.strategy_pnl : null,
        worstTradePnl: worstTrade ? worstTrade.strategy_pnl : null,
        profitFactor: grossLossAbs > 0 ? grossWin / grossLossAbs : (wins.length ? null : null),
      };
    }

    function renderStrategyStats(rows) {
      const grid = document.getElementById("strategyStatsGrid");
      const meta = document.getElementById("strategyStatsMeta");
      if (!grid || !meta) return;

      grid.innerHTML = "";
      const stats = summarizeStrategyTrades(rows);
      if (!stats.tradeCount) {
        meta.textContent = "Run analysis to compute trade-level summary stats.";
        return;
      }

      const tradeLabel = stats.tradeCount === 1 ? "trade" : "trades";
      meta.innerHTML = `Final outcome across completed trades.<span class="meta-emphasis">${escapeHtml(String(stats.tradeCount))} ${escapeHtml(tradeLabel)}</span>`;
      const items = [
        ["Win Rate", formatStatPercent(stats.winRate)],
        ["Avg Win", formatStatAmount(stats.avgWin)],
        ["Avg Loss", formatStatAmount(stats.avgLoss)],
        ["Avg Trade", formatStatAmount(stats.avgTradePnl)],
        ["Avg Trade Return %", formatStatPercent(stats.avgGainLossPct)],
        ["Best Trade", formatStatAmount(stats.bestTradePnl)],
        ["Worst Trade", formatStatAmount(stats.worstTradePnl)],
        ["Profit Factor (Gross)", stats.profitFactor != null && Number.isFinite(stats.profitFactor) ? stats.profitFactor.toFixed(2) : "n/a"],
      ];

      items.forEach(([label, value]) => {
        const tile = document.createElement("div");
        tile.className = "stat-tile";
        tile.innerHTML = `
          <div class="stat-label">${escapeHtml(label)}</div>
          <div class="stat-value">${escapeHtml(value || "n/a")}</div>
        `;
        grid.appendChild(tile);
      });
    }

    function setStrategyResultsVisibility(showResults) {
      const statsCard = document.getElementById("strategyStatsCard");
      const chartCard = document.getElementById("strategyIndexChartCard");
      const seriesCard = document.getElementById("strategyTimeSeriesCard");
      if (!showResults) {
        strategyState.hasCompletedResults = false;
        strategyState.resultsOrigin = "";
        strategyState.loadedShareToken = "";
        strategyState.loadedShareUrl = "";
        strategyState.loadedShareCreatedAt = "";
        strategyState.isDirtySinceShareLoad = false;
      }
      [statsCard, chartCard, seriesCard].forEach((el) => {
        if (!el) return;
        el.classList.toggle("is-hidden", !showResults);
      });
      if (seriesCard) {
        seriesCard.classList.add("is-hidden");
      }
      updateStrategySharedStateNote();
      updateStrategyShareControls();
    }

    function isWithinStrategyExitWindow(tsDate, tradeStartTs, exitDays, exitTime) {
      if (!tsDate || !tradeStartTs) return false;
      const currentDayUtc = dateUtcFromYmd(formatEtDateKey(tsDate));
      const startDayUtc = dateUtcFromYmd(formatEtDateKey(tradeStartTs));
      const exitDaysNumeric = Number(exitDays);
      if (currentDayUtc == null || startDayUtc == null || !Number.isFinite(exitDaysNumeric)) return false;
      const dayOffset = Math.round((currentDayUtc - startDayUtc) / 86400000);
      if (dayOffset < exitDaysNumeric) return true;
      if (dayOffset > exitDaysNumeric) return false;
      const currentMinutes = parseHmToMinutes(formatEtHm(tsDate));
      const exitMinutes = parseHmToMinutes(exitTime);
      if (currentMinutes == null || exitMinutes == null) return false;
      return currentMinutes <= exitMinutes;
    }

    function isAtOrAfterStrategyExit(tsDate, tradeStartTs, exitDays, exitTime) {
      if (!tsDate || !tradeStartTs) return false;
      const currentDayUtc = dateUtcFromYmd(formatEtDateKey(tsDate));
      const startDayUtc = dateUtcFromYmd(formatEtDateKey(tradeStartTs));
      const exitDaysNumeric = Number(exitDays);
      if (currentDayUtc == null || startDayUtc == null || !Number.isFinite(exitDaysNumeric)) return false;
      const dayOffset = Math.round((currentDayUtc - startDayUtc) / 86400000);
      if (dayOffset > exitDaysNumeric) return true;
      if (dayOffset < exitDaysNumeric) return false;
      const currentMinutes = parseHmToMinutes(formatEtHm(tsDate));
      const exitMinutes = parseHmToMinutes(exitTime);
      if (currentMinutes == null || exitMinutes == null) return false;
      return currentMinutes >= exitMinutes;
    }

    function isBeforeOrOnExpiryDate(tsDate, expirationYmd) {
      if (!tsDate || !expirationYmd) return false;
      const currentDayUtc = dateUtcFromYmd(formatEtDateKey(tsDate));
      const expiryUtc = dateUtcFromYmd(expirationYmd);
      if (currentDayUtc == null || expiryUtc == null) return false;
      return currentDayUtc <= expiryUtc;
    }

    function isAtOrAfterExpiryDate(tsDate, expirationYmd) {
      if (!tsDate || !expirationYmd) return false;
      const currentDayUtc = dateUtcFromYmd(formatEtDateKey(tsDate));
      const expiryUtc = dateUtcFromYmd(expirationYmd);
      if (currentDayUtc == null || expiryUtc == null) return false;
      return currentDayUtc >= expiryUtc;
    }

    function formatEtHm(dateObj) {
      if (!dateObj) return "";
      return new Intl.DateTimeFormat("en-US", {
        timeZone: "America/New_York",
        hour: "2-digit",
        minute: "2-digit",
        hour12: false,
      }).format(dateObj);
    }

    function formatEtDateKey(dateObj) {
      if (!dateObj) return "";
      const parts = new Intl.DateTimeFormat("en-CA", {
        timeZone: "America/New_York",
        year: "numeric",
        month: "2-digit",
        day: "2-digit",
      }).formatToParts(dateObj);
      const y = parts.find((p) => p.type === "year")?.value || "";
      const m = parts.find((p) => p.type === "month")?.value || "";
      const d = parts.find((p) => p.type === "day")?.value || "";
      return y && m && d ? `${y}-${m}-${d}` : "";
    }

    function dateUtcFromYmd(ymd) {
      if (!ymd || !/^\\d{4}-\\d{2}-\\d{2}$/.test(ymd)) return null;
      const [yy, mm, dd] = ymd.split("-").map((v) => Number(v));
      if (!Number.isFinite(yy) || !Number.isFinite(mm) || !Number.isFinite(dd)) return null;
      return Date.UTC(yy, mm - 1, dd);
    }

    function dteForTs(expirationYmd, tsDate) {
      const expUtc = dateUtcFromYmd(expirationYmd);
      const tickKey = formatEtDateKey(tsDate);
      const tickUtc = dateUtcFromYmd(tickKey);
      if (expUtc == null || tickUtc == null) return "";
      const diff = Math.round((expUtc - tickUtc) / 86400000);
      return String(Math.max(0, diff));
    }

    function formatStrategyHoverDelta(value, profitBelow100) {
      if (value == null || !Number.isFinite(Number(value))) return "";
      const rawDelta = Number(value) - 100;
      const signedDelta = profitBelow100 ? -rawDelta : rawDelta;
      const sign = signedDelta > 0 ? "+" : "";
      return `${sign}${signedDelta.toFixed(1)}%`;
    }

    function formatStrategyTooltipTimestamp(value) {
      const ts = value instanceof Date ? value : parseTimestamp(value);
      if (!ts) return "";
      return new Intl.DateTimeFormat("en-US", {
        timeZone: "America/New_York",
        hour: "numeric",
        minute: "2-digit",
        hour12: true,
      }).format(ts).replace(" ", "");
    }

    function isVisibleStrategyChartTime(ts) {
      if (!ts) return false;
      const etMinutes = parseHmToMinutes(formatEtHm(ts));
      if (etMinutes == null) return false;
      const regularSessionStart = 9 * 60 + 30;
      const regularSessionEnd = 16 * 60;
      return etMinutes >= regularSessionStart && etMinutes <= regularSessionEnd;
    }

    function isMobileStrategyChartViewport() {
      return typeof window !== "undefined"
        && typeof window.matchMedia === "function"
        && window.matchMedia("(max-width: 720px)").matches;
    }

    function stepPath(points, xScale, yScale) {
      let d = "";
      points.forEach((p, idx) => {
        const x = xScale(p.x).toFixed(2);
        const y = yScale(p.y).toFixed(2);
        if (idx === 0) {
          d += `M${x},${y} `;
          return;
        }
        const prev = points[idx - 1];
        const prevY = yScale(prev.y).toFixed(2);
        d += `L${x},${prevY} L${x},${y} `;
      });
      return d.trim();
    }

    function renderStrategyIndexChart(rows) {
      strategyState.historyRows = Array.isArray(rows) ? rows : [];
      const svg = document.getElementById("strategyIndexChartSvg");
      const meta = document.getElementById("strategyIndexChartMeta");
      const tooltip = document.getElementById("strategyIndexChartTooltip");
      const wrap = svg ? svg.closest(".chart-wrap") : null;
      const mobileViewport = isMobileStrategyChartViewport();
      if (!svg || !meta || !tooltip || !wrap) return;

      svg.innerHTML = "";
      tooltip.classList.remove("visible");
      tooltip.innerHTML = "";
      const detailRows = (rows || []).filter((row) => !row.isStrategySummary);
      if (!detailRows.length) {
        meta.textContent = "No strategy data to chart.";
        return;
      }

      const byTrade = new Map();
      detailRows.forEach((row) => {
        const tradeKey = String(row.trade_index == null ? "" : row.trade_index);
        if (!tradeKey) return;
        const ts = String(row.snapshot_ts || "");
        const tsDate = parseTimestamp(ts);
        const strategyPrice = row.strategy_price;
        const strategyCost = row.strategy_cost;
        if (!tsDate || strategyPrice == null || !Number.isFinite(Number(strategyPrice))) return;
        if (!byTrade.has(tradeKey)) byTrade.set(tradeKey, []);
        byTrade.get(tradeKey).push({
          ts,
          tsDate,
          strategyPrice: Number(strategyPrice),
          strategyCost: strategyCost == null ? null : Number(strategyCost),
          expirationDate: String(row.expiration_date || ""),
        });
      });

      const stepMs = 15 * 60 * 1000;
      const tradeSeries = [];
      Array.from(byTrade.entries())
        .sort((a, b) => Number(a[0]) - Number(b[0]))
        .forEach(([tradeKey, points]) => {
          const uniq = new Map();
          points
            .sort((a, b) => a.tsDate - b.tsDate)
            .forEach((p) => {
              if (!uniq.has(p.ts)) uniq.set(p.ts, p);
            });
          const sorted = Array.from(uniq.values());
          if (!sorted.length) return;
          const entry = sorted[0];
          if (!Number.isFinite(entry.strategyPrice) || entry.strategyPrice === 0) return;

          const normalized = sorted.map((p) => ({
            elapsedMs: p.tsDate.getTime() - entry.tsDate.getTime(),
            tsDate: p.tsDate,
            indexed: (p.strategyPrice / entry.strategyPrice) * 100,
          }));
          const maxElapsed = normalized[normalized.length - 1].elapsedMs;
          if (!Number.isFinite(maxElapsed) || maxElapsed < 0) return;

          function carryForward(ms) {
            if (ms < normalized[0].elapsedMs) return null;
            if (ms >= normalized[normalized.length - 1].elapsedMs) return normalized[normalized.length - 1].indexed;
            let value = normalized[0].indexed;
            for (let i = 1; i < normalized.length; i += 1) {
              if (ms < normalized[i].elapsedMs) break;
              value = normalized[i].indexed;
            }
            return value;
          }

          tradeSeries.push({
            tradeKey,
            entryDate: entry.tsDate,
            entryCost: entry.strategyCost,
            expirationDate: entry.expirationDate,
            maxElapsed,
            carryForward,
          });
        });

      if (!tradeSeries.length) {
        meta.textContent = "No plottable trade series after alignment.";
        return;
      }

      const maxSteps = tradeSeries.reduce((m, t) => Math.max(m, Math.ceil(t.maxElapsed / stepMs) + 1), 0);
      if (!maxSteps) {
        meta.textContent = "No aligned samples available for chart.";
        return;
      }

      tradeSeries.forEach((trade) => {
        const steps = [];
        for (let s = 0; s < maxSteps; s += 1) {
          const ms = s * stepMs;
          const val = trade.carryForward(ms);
          steps.push(val == null ? null : Number(val));
        }
        trade.steps = steps;
      });

      const blended = [];
      for (let i = 0; i < maxSteps; i += 1) {
        const vals = tradeSeries.map((t) => t.steps[i]).filter((v) => v != null && Number.isFinite(v));
        blended.push(vals.length ? vals.reduce((a, b) => a + b, 0) / vals.length : null);
      }

      const firstTrade = tradeSeries[0];
      const sampleTimes = Array.from({ length: maxSteps }, (_, i) => new Date(firstTrade.entryDate.getTime() + i * stepMs));
      const visibleIndexSet = new Set(sampleTimes
        .map((ts, idx) => (isVisibleStrategyChartTime(ts) ? idx : -1))
        .filter((idx) => idx >= 0));
      const visibleIndices = Array.from(visibleIndexSet).sort((a, b) => a - b);
      if (!visibleIndices.length) {
        meta.textContent = "No regular market-hours ET samples available for chart.";
        return;
      }

      const compressedIndexByOriginal = new Map();
      visibleIndices.forEach((idx, compressedIdx) => {
        compressedIndexByOriginal.set(idx, compressedIdx);
      });
      const plottedY = visibleIndices
        .map((idx) => blended[idx])
        .filter((v) => v != null && Number.isFinite(v));
      if (!plottedY.length) {
        meta.textContent = "No numeric values available for chart.";
        return;
      }

      const observedMin = Math.min(100, ...plottedY);
      const observedMax = Math.max(100, ...plottedY);
      const yRange = Math.max(1, observedMax - observedMin);
      const yPad = Math.max(2, yRange * 0.12);
      const yMin = Math.max(0, observedMin - yPad);
      const yMax = observedMax + yPad;

      const width = 1200;
      const height = mobileViewport ? 260 : 320;
      const m = mobileViewport
        ? { top: 18, right: 22, bottom: 62, left: 46 }
        : { top: 18, right: 56, bottom: 78, left: 56 };
      svg.setAttribute("viewBox", `0 0 ${width} ${height}`);
      const innerW = width - m.left - m.right;
      const innerH = height - m.top - m.bottom;
      const compressedXMax = Math.max(1, visibleIndices.length - 1);
      const xScale = (originalIdx) => {
        const compressedIdx = compressedIndexByOriginal.get(originalIdx);
        if (compressedIdx == null) return null;
        return m.left + (compressedIdx / compressedXMax) * innerW;
      };
      const yScale = (y) => m.top + ((yMax - y) / (yMax - yMin)) * innerH;

      const tradeTypes = new Set(
        tradeSeries.map((t) => (t.entryCost == null ? "unknown" : (t.entryCost < 0 ? "credit" : "debit")))
      );
      const mixedTradeTypes = tradeTypes.has("credit") && tradeTypes.has("debit");
      const referenceType = tradeSeries.find((t) => t.entryCost != null)?.entryCost < 0 ? "credit" : "debit";
      const profitBelow100 = referenceType === "credit";

      const dteRefExpiration = firstTrade.expirationDate;
      const gridLines = 6;
      for (let g = 0; g <= gridLines; g += 1) {
        const yVal = yMin + (g / gridLines) * (yMax - yMin);
        const y = yScale(yVal);
        const line = document.createElementNS("http://www.w3.org/2000/svg", "line");
        line.setAttribute("x1", String(m.left));
        line.setAttribute("x2", String(m.left + innerW));
        line.setAttribute("y1", String(y));
        line.setAttribute("y2", String(y));
        line.setAttribute("stroke", "#e2e8f0");
        line.setAttribute("stroke-width", "1");
        line.setAttribute("stroke-dasharray", "0");
        svg.appendChild(line);

        if (g === 0 || g === gridLines) {
          if (mobileViewport) continue;
          const txt = document.createElementNS("http://www.w3.org/2000/svg", "text");
          txt.setAttribute("x", String(m.left - 8));
          txt.setAttribute("y", String(y + 4));
          txt.setAttribute("text-anchor", "end");
          txt.setAttribute("font-size", mobileViewport ? "11" : "11");
          txt.setAttribute("fill", "#64748b");
          txt.textContent = formatStrategyIndexAxisLabel(yVal);
          svg.appendChild(txt);
        }
      }

      if (yMin <= 100 && yMax >= 100) {
        const baselineY = yScale(100);
        const baseline = document.createElementNS("http://www.w3.org/2000/svg", "line");
        baseline.setAttribute("x1", String(m.left));
        baseline.setAttribute("x2", String(m.left + innerW));
        baseline.setAttribute("y1", String(baselineY));
        baseline.setAttribute("y2", String(baselineY));
        baseline.setAttribute("stroke", "#64748b");
        baseline.setAttribute("stroke-width", "1.5");
        baseline.setAttribute("stroke-dasharray", "4 3");
        svg.appendChild(baseline);

        if (!mobileViewport) {
          const baselineLabel = document.createElementNS("http://www.w3.org/2000/svg", "text");
          baselineLabel.setAttribute("x", String(m.left - 8));
          baselineLabel.setAttribute("y", String(baselineY + 4));
          baselineLabel.setAttribute("text-anchor", "end");
          baselineLabel.setAttribute("font-size", mobileViewport ? "11" : "11");
          baselineLabel.setAttribute("fill", "#64748b");
          baselineLabel.textContent = "100%";
          svg.appendChild(baselineLabel);
        }
      }

      const visibleDayBoundaries = [];
      for (let i = 1; i < visibleIndices.length; i += 1) {
        const prevIdx = visibleIndices[i - 1];
        const currIdx = visibleIndices[i];
        const prevKey = formatEtDateKey(sampleTimes[prevIdx]);
        const currKey = formatEtDateKey(sampleTimes[currIdx]);
        if (prevKey && currKey && prevKey !== currKey) {
          visibleDayBoundaries.push(currIdx);
        }
      }
      visibleDayBoundaries.forEach((idx) => {
        const x = xScale(idx);
        if (x == null) return;
        const boundary = document.createElementNS("http://www.w3.org/2000/svg", "line");
        boundary.setAttribute("x1", String(x));
        boundary.setAttribute("x2", String(x));
        boundary.setAttribute("y1", String(m.top));
        boundary.setAttribute("y2", String(m.top + innerH));
        boundary.setAttribute("stroke", "#94a3b8");
        boundary.setAttribute("stroke-width", "1");
        boundary.setAttribute("stroke-dasharray", "5 5");
        boundary.setAttribute("opacity", "0.8");
        svg.appendChild(boundary);
      });

      const visibleBlendPts = visibleIndices
        .map((i) => (blended[i] == null || !Number.isFinite(blended[i]) ? null : ({ x: i, y: blended[i] })))
        .filter(Boolean);

      for (let i = 1; i < visibleBlendPts.length; i += 1) {
        const prevPoint = visibleBlendPts[i - 1];
        const nextPoint = visibleBlendPts[i];
        const a = prevPoint.y;
        const b = nextPoint.y;
        if (a == null || b == null) continue;
        const isProfit = profitBelow100 ? a <= 100 : a >= 100;
        const path = document.createElementNS("http://www.w3.org/2000/svg", "path");
        const x1 = xScale(prevPoint.x);
        const x2 = xScale(nextPoint.x);
        if (x1 == null || x2 == null) continue;
        const y = yScale(a);
        const yBase = yScale(100);
        path.setAttribute("d", `M${x1},${yBase} L${x1},${y} L${x2},${y} L${x2},${yBase} Z`);
        path.setAttribute("fill", isProfit ? "rgba(22,163,74,0.24)" : "rgba(220,38,38,0.22)");
        svg.appendChild(path);
      }

      if (visibleBlendPts.length >= 2) {
        const blendPath = document.createElementNS("http://www.w3.org/2000/svg", "path");
        blendPath.setAttribute("d", stepPath(visibleBlendPts, xScale, yScale));
        blendPath.setAttribute("fill", "none");
        blendPath.setAttribute("stroke", "#0f172a");
        blendPath.setAttribute("stroke-width", "2.4");
        svg.appendChild(blendPath);
      }

      const hoverGuide = document.createElementNS("http://www.w3.org/2000/svg", "line");
      hoverGuide.setAttribute("y1", String(m.top));
      hoverGuide.setAttribute("y2", String(m.top + innerH));
      hoverGuide.setAttribute("stroke", "#0f172a");
      hoverGuide.setAttribute("stroke-width", "1");
      hoverGuide.setAttribute("stroke-dasharray", "4 4");
      hoverGuide.setAttribute("opacity", "0");
      hoverGuide.setAttribute("pointer-events", "none");
      svg.appendChild(hoverGuide);

      const hoverMarker = document.createElementNS("http://www.w3.org/2000/svg", "circle");
      hoverMarker.setAttribute("r", "4.5");
      hoverMarker.setAttribute("fill", "#ffffff");
      hoverMarker.setAttribute("stroke", "#0f172a");
      hoverMarker.setAttribute("stroke-width", "2");
      hoverMarker.setAttribute("opacity", "0");
      hoverMarker.setAttribute("pointer-events", "none");
      svg.appendChild(hoverMarker);

      function hideTooltip() {
        tooltip.classList.remove("visible");
        tooltip.innerHTML = "";
        tooltip.style.transform = "translate(12px, -12px)";
        hoverGuide.setAttribute("opacity", "0");
        hoverMarker.setAttribute("opacity", "0");
      }

      function findNearestBlendedIndex(target) {
        if (!visibleIndices.length) return -1;
        let bestIdx = -1;
        let bestDist = Infinity;
        for (const i of visibleIndices) {
          const value = blended[i];
          if (value == null || !Number.isFinite(value)) continue;
          const dist = Math.abs(i - target);
          if (dist < bestDist) {
            bestDist = dist;
            bestIdx = i;
          }
        }
        return bestIdx;
      }

      function updateTooltipFromPoint(clientX, clientY) {
        const rect = svg.getBoundingClientRect();
        const wrapRect = wrap.getBoundingClientRect();
        if (!rect.width || !rect.height) {
          hideTooltip();
          return false;
        }

        const relX = ((clientX - rect.left) / rect.width) * width;
        if (relX < m.left || relX > m.left + innerW) {
          hideTooltip();
          return false;
        }

        const targetCompressed = Math.round(((relX - m.left) / innerW) * compressedXMax);
        const clampedCompressed = Math.max(0, Math.min(compressedXMax, targetCompressed));
        const targetOriginal = visibleIndices[clampedCompressed];
        const idx = findNearestBlendedIndex(targetOriginal);
        if (idx < 0) {
          hideTooltip();
          return false;
        }

        const ts = sampleTimes[idx];
        const value = blended[idx];
        const chartX = xScale(idx);
        const chartY = yScale(value);
        hoverGuide.setAttribute("x1", String(chartX));
        hoverGuide.setAttribute("x2", String(chartX));
        hoverGuide.setAttribute("opacity", "0.7");
        hoverMarker.setAttribute("cx", String(chartX));
        hoverMarker.setAttribute("cy", String(chartY));
        hoverMarker.setAttribute("opacity", "1");

        const dte = dteForTs(dteRefExpiration, ts);
        tooltip.innerHTML = `
          <div class="chart-tooltip-debug">DTE = ${escapeHtml(dte)}</div>
          <div class="chart-tooltip-debug">${escapeHtml(formatStrategyTooltipTimestamp(ts))}</div>
          <div class="chart-tooltip-value">${escapeHtml(formatStrategyHoverDelta(value, profitBelow100))}</div>
        `;
        tooltip.classList.add("visible");

        const tooltipWidth = tooltip.offsetWidth || 0;
        const tooltipHeight = tooltip.offsetHeight || 0;
        const cursorLeft = clientX - wrapRect.left;
        const cursorTop = clientY - wrapRect.top;
        const spaceRight = wrapRect.width - cursorLeft;
        const placeLeft = spaceRight < tooltipWidth + 28;
        const left = Math.min(wrapRect.width - 12, Math.max(12, cursorLeft));
        const top = Math.min(wrapRect.height - 12, Math.max(tooltipHeight + 12, cursorTop));
        tooltip.style.left = `${left}px`;
        tooltip.style.top = `${top}px`;
        tooltip.style.transform = placeLeft
          ? "translate(calc(-100% - 12px), -12px)"
          : "translate(12px, -12px)";
        return true;
      }

      function eventClientPoint(event) {
        if (event && Number.isFinite(event.clientX) && Number.isFinite(event.clientY)) {
          return { x: event.clientX, y: event.clientY };
        }
        const touch = event?.touches?.[0] || event?.changedTouches?.[0];
        if (touch && Number.isFinite(touch.clientX) && Number.isFinite(touch.clientY)) {
          return { x: touch.clientX, y: touch.clientY };
        }
        return null;
      }

      function handlePointerMove(event) {
        const point = eventClientPoint(event);
        if (!point) {
          hideTooltip();
          return;
        }
        if (event.type.startsWith("touch")) event.preventDefault();
        updateTooltipFromPoint(point.x, point.y);
      }

      svg.addEventListener("mouseleave", hideTooltip);
      svg.addEventListener("mousemove", handlePointerMove);
      svg.addEventListener("touchstart", handlePointerMove, { passive: false });
      svg.addEventListener("touchmove", handlePointerMove, { passive: false });
      svg.addEventListener("touchend", hideTooltip);
      svg.addEventListener("touchcancel", hideTooltip);

      const tickTarget = mobileViewport ? 3 : 8;
      const tickEvery = Math.max(1, Math.ceil(visibleIndices.length / tickTarget));
      for (let visiblePos = 0; visiblePos < visibleIndices.length; visiblePos += tickEvery) {
        const i = visibleIndices[visiblePos];
        const x = xScale(i);
        if (x == null) continue;
        const tick = document.createElementNS("http://www.w3.org/2000/svg", "line");
        tick.setAttribute("x1", String(x));
        tick.setAttribute("x2", String(x));
        tick.setAttribute("y1", String(m.top + innerH));
        tick.setAttribute("y2", String(m.top + innerH + 6));
        tick.setAttribute("stroke", "#94a3b8");
        tick.setAttribute("stroke-width", "1");
        svg.appendChild(tick);

        if (mobileViewport) continue;
        const ts = sampleTimes[i];
        const hm = formatEtHm(ts);
        const dte = dteForTs(dteRefExpiration, ts);
        const label = document.createElementNS("http://www.w3.org/2000/svg", "text");
        label.setAttribute("x", String(x));
        label.setAttribute("y", String(m.top + innerH + (mobileViewport ? 20 : 22)));
        label.setAttribute("text-anchor", "middle");
        label.setAttribute("font-size", mobileViewport ? "11.5" : "10");
        label.setAttribute("fill", "#64748b");
        label.textContent = mobileViewport ? hm : `${hm} (DTE ${dte})`;
        if (!mobileViewport) {
          label.setAttribute("transform", `rotate(-28 ${x} ${m.top + innerH + 22})`);
        }
        svg.appendChild(label);
      }

      const xAxis = document.createElementNS("http://www.w3.org/2000/svg", "line");
      xAxis.setAttribute("x1", String(m.left));
      xAxis.setAttribute("x2", String(m.left + innerW));
      xAxis.setAttribute("y1", String(m.top + innerH));
      xAxis.setAttribute("y2", String(m.top + innerH));
      xAxis.setAttribute("stroke", "#cbd5e1");
      xAxis.setAttribute("stroke-width", "1");
      svg.appendChild(xAxis);

      let metaMsg = `Blended ${tradeSeries.length} aligned trade series on a 15-minute ET grid.`;
      metaMsg += " Only regular market hours from 9:30 AM to 4:00 PM ET are shown; dashed lines mark each new day.";
      if (mixedTradeTypes) {
        metaMsg += " Mixed debit/credit entries detected; shading uses reference trade semantics.";
      }
      meta.textContent = metaMsg;
    }

    async function runStrategyAnalysis() {
      const meta = document.getElementById("strategyAnalysisMeta");
      const resolvedLegs = strategyState.legs.filter((leg) => leg.isResolved && Array.isArray(leg.resolved_contracts) && leg.resolved_contracts.length > 0);
      trackEvent("strategy_run_attempt", currentStrategyRunTrackingPayload());
      if (!resolvedLegs.length) {
        setStrategyResultsVisibility(false);
        meta.textContent = "Please resolve at least one leg.";
        meta.className = "meta danger";
        renderStrategyStats([]);
        renderStrategySeriesTable([]);
        renderStrategyIndexChart([]);
        trackStrategyRunResult("validation_error", { reason: "no_resolved_legs" });
        return;
      }

      const symbol = document.getElementById("strategySymbol").value || "SPX";
      const snapshotFromDate = document.getElementById("strategySnapshotFromDate").value || "";
      const snapshotToDate = document.getElementById("strategySnapshotToDate").value || "";
      const holdTillExpiry = Boolean(document.getElementById("strategyHoldToExpiry")?.checked);
      const exitDays = parseInt(document.getElementById("strategyExitDays").value || "0", 10);
      const exitTime = document.getElementById("strategyExitTime").value || "";
      const allDates = Array.isArray(strategyState.snapshotDates) ? strategyState.snapshotDates : [];
      if (!allDates.length) {
        setStrategyResultsVisibility(false);
        meta.textContent = "No snapshot dates available for this symbol.";
        meta.className = "meta danger";
        renderStrategyStats([]);
        renderStrategySeriesTable([]);
        renderStrategyIndexChart([]);
        trackStrategyRunResult("validation_error", { reason: "no_snapshot_dates" });
        return;
      }
      const fromDate = snapshotFromDate || allDates[0];
      const toDate = snapshotToDate || allDates[allDates.length - 1];
      const entryTimes = resolvedLegs.map((leg) => parseHmToMinutes(leg.entry_time)).filter((value) => value != null);
      const latestEntryTime = entryTimes.length ? Math.max(...entryTimes) : null;
      if (fromDate > toDate) {
        setStrategyResultsVisibility(false);
        meta.textContent = "Snapshot From must not be after Snapshot To.";
        meta.className = "meta danger";
        renderStrategyStats([]);
        renderStrategySeriesTable([]);
        renderStrategyIndexChart([]);
        trackStrategyRunResult("validation_error", { reason: "invalid_snapshot_range" });
        return;
      }
      if (!holdTillExpiry && (!Number.isFinite(exitDays) || exitDays < 0)) {
        setStrategyResultsVisibility(false);
        meta.textContent = "Exit days must be a non-negative integer.";
        meta.className = "meta danger";
        renderStrategyStats([]);
        renderStrategySeriesTable([]);
        renderStrategyIndexChart([]);
        trackStrategyRunResult("validation_error", { reason: "invalid_exit_days" });
        return;
      }
      if (!holdTillExpiry && !exitTime) {
        setStrategyResultsVisibility(false);
        meta.textContent = "Exit Time is required.";
        meta.className = "meta danger";
        renderStrategyStats([]);
        renderStrategySeriesTable([]);
        renderStrategyIndexChart([]);
        trackStrategyRunResult("validation_error", { reason: "missing_exit_time" });
        return;
      }
      const exitMinutes = holdTillExpiry ? null : parseHmToMinutes(exitTime);
      if (!holdTillExpiry && exitMinutes == null) {
        setStrategyResultsVisibility(false);
        meta.textContent = "Exit time must be a valid ET time.";
        meta.className = "meta danger";
        renderStrategyStats([]);
        renderStrategySeriesTable([]);
        renderStrategyIndexChart([]);
        trackStrategyRunResult("validation_error", { reason: "invalid_exit_time" });
        return;
      }
      if (!holdTillExpiry && exitDays === 0 && latestEntryTime != null && exitMinutes <= latestEntryTime) {
        setStrategyResultsVisibility(false);
        meta.textContent = "When exit days is 0, exit time must be later than the strategy entry time.";
        meta.className = "meta danger";
        renderStrategyStats([]);
        renderStrategySeriesTable([]);
        renderStrategyIndexChart([]);
        trackStrategyRunResult("validation_error", { reason: "exit_not_after_entry" });
        return;
      }
      const tradeDates = allDates.filter((d) => d >= fromDate && d <= toDate);
      if (!tradeDates.length) {
        setStrategyResultsVisibility(false);
        meta.textContent = "No snapshot dates in the selected range.";
        meta.className = "meta danger";
        renderStrategyStats([]);
        renderStrategySeriesTable([]);
        renderStrategyIndexChart([]);
        trackStrategyRunResult("empty", { reason: "no_trade_dates" });
        return;
      }

      try {
        const planRes = await fetch("/api/options/strategy-plan", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            symbol,
            trade_dates: tradeDates,
            window_minutes: 5,
            legs: resolvedLegs.map((leg) => ({
              side: String(leg.side || "BUY"),
              quantity: Number(leg.quantity) > 0 ? Number(leg.quantity) : 1,
              option_type: String(leg.option_type || "PUT"),
              dte: Number(leg.target_dte),
              target_delta: Number(leg.target_delta),
              entry_time: String(leg.entry_time || ""),
            })),
          }),
        });
        const planPayload = await planRes.json();
        if (!planRes.ok) {
          setStrategyResultsVisibility(false);
          meta.textContent = "Error resolving daily trades: " + (planPayload.error || "unknown");
          meta.className = "meta danger";
          renderStrategyStats([]);
          renderStrategySeriesTable([]);
          renderStrategyIndexChart([]);
          trackStrategyRunResult("error", {
            reason: planPayload.error || "strategy_plan_error",
            trade_dates_count: tradeDates.length,
          });
          return;
        }
        const tradePlans = Array.isArray(planPayload.trade_plans) ? planPayload.trade_plans : [];
        const skippedDates = Number(planPayload.skipped_dates) || 0;
        if (!tradePlans.length) {
          setStrategyResultsVisibility(false);
          meta.textContent = "No daily trades could be opened at the requested entry time/delta in this range.";
          meta.className = "meta danger";
          renderStrategyStats([]);
          renderStrategySeriesTable([]);
          renderStrategyIndexChart([]);
          trackStrategyRunResult("empty", {
            reason: "no_trade_plans",
            trade_dates_count: tradeDates.length,
            skipped_dates: skippedDates,
          });
          return;
        }

        const streamers = Array.from(new Set(tradePlans.flatMap((trade) => trade.legs.map((leg) => leg.streamer_symbol))));
        if (!streamers.length) {
          setStrategyResultsVisibility(false);
          meta.textContent = "No contracts resolved for the current legs.";
          meta.className = "meta danger";
          renderStrategyStats([]);
          renderStrategySeriesTable([]);
          renderStrategyIndexChart([]);
          trackStrategyRunResult("empty", {
            reason: "no_streamers",
            trade_plan_count: tradePlans.length,
            skipped_dates: skippedDates,
          });
          return;
        }
        if (streamers.length > MAX_STRATEGY_ANALYSIS_STREAMERS) {
          setStrategyResultsVisibility(false);
          meta.textContent = `Resolved contracts exceed ${MAX_STRATEGY_ANALYSIS_STREAMERS} streamers for series analysis. Reduce selected range or legs.`;
          meta.className = "meta danger";
          renderStrategyStats([]);
          renderStrategySeriesTable([]);
          renderStrategyIndexChart([]);
          trackStrategyRunResult("validation_error", {
            reason: "too_many_streamers",
            trade_plan_count: tradePlans.length,
            completed_contract_count: streamers.length,
            skipped_dates: skippedDates,
          });
          return;
        }
        const seriesParams = new URLSearchParams({
          symbol,
          streamers: streamers.join(","),
          field: "mid_price",
          from: `${fromDate}T00:00:00`,
          to: `${toDate}T23:59:59`,
        });
        const summaryParams = new URLSearchParams({
          symbol,
          from: `${fromDate}T00:00:00`,
          to: `${toDate}T23:59:59`,
        });

        const [seriesRes, summaryRes] = await Promise.all([
          fetch(`/api/options/series?${seriesParams.toString()}`),
          fetch(`/api/options/summary?${summaryParams.toString()}`),
        ]);
        const seriesData = await seriesRes.json();
        const summaryData = await summaryRes.json();
        if (!seriesRes.ok) {
          setStrategyResultsVisibility(false);
          meta.textContent = "Error loading series: " + (seriesData.error || "unknown");
          meta.className = "meta danger";
          renderStrategyStats([]);
          renderStrategySeriesTable([]);
          renderStrategyIndexChart([]);
          trackStrategyRunResult("error", {
            reason: seriesData.error || "series_error",
            trade_plan_count: tradePlans.length,
            skipped_dates: skippedDates,
          });
          return;
        }
        if (!summaryRes.ok) {
          meta.textContent = "Warning: could not load spot/summary data.";
          meta.className = "meta danger";
        }

        const rows = Array.isArray(seriesData.rows) ? seriesData.rows : [];
        if (!rows.length) {
          setStrategyResultsVisibility(false);
          meta.textContent = "No data for the selected legs.";
          meta.className = "meta danger";
          renderStrategyStats([]);
          renderStrategySeriesTable([]);
          renderStrategyTradeMatrixTable([]);
          renderStrategyIndexChart([]);
          trackStrategyRunResult("empty", {
            reason: "no_series_rows",
            trade_plan_count: tradePlans.length,
            skipped_dates: skippedDates,
          });
          return;
        }
        const transformed = transformStrategySeriesRows(rows, summaryData.market_series || [], tradePlans, { holdTillExpiry, exitDays, exitTime });
        if (!transformed.length) {
          setStrategyResultsVisibility(false);
          meta.textContent = "No completed trades yet for the selected exit criteria.";
          meta.className = "meta danger";
          renderStrategyStats([]);
          renderStrategySeriesTable([]);
          renderStrategyTradeMatrixTable([]);
          renderStrategyIndexChart([]);
          trackStrategyRunResult("empty", {
            reason: "no_completed_trades",
            trade_plan_count: tradePlans.length,
            trade_dates_count: tradeDates.length,
            series_rows_count: rows.length,
            skipped_dates: skippedDates,
          });
          return;
        }
        const completedTradeCount = new Set(transformed.map((row) => row.trade_index).filter((value) => value != null)).size;
        const completedContracts = new Set(
          transformed
            .map((row) => row.streamer_symbol)
            .filter((value) => value != null && value !== "")
        ).size;
        applyStrategyResults(transformed, { origin: "live" });
        meta.textContent = "";
        meta.className = "meta";
        trackStrategyRunResult("success", {
          trade_dates_count: tradeDates.length,
          trade_plan_count: tradePlans.length,
          completed_trade_count: completedTradeCount,
          completed_contract_count: completedContracts,
          series_rows_count: rows.length,
          skipped_dates: skippedDates,
        });
      } catch {
        setStrategyResultsVisibility(false);
        meta.textContent = "Request failed while running strategy.";
        meta.className = "meta danger";
        renderStrategyStats([]);
        renderStrategySeriesTable([]);
        renderStrategyIndexChart([]);
        trackStrategyRunResult("error", { reason: "request_failed" });
      }
    }

    function initStrategyTab() {
      if (tabInitState.strategy) return;
      tabInitState.strategy = true;

      loadStrategySnapshotDateOptions();
      document
        .getElementById("strategySymbol")
        .addEventListener("change", async () => {
          markStrategyDirtyFromShare();
          await loadStrategySnapshotDateOptions();
        });
      ["strategyEntryTime", "strategySnapshotFromDate", "strategySnapshotToDate", "strategyExitTime"].forEach((id) => {
        const input = document.getElementById(id);
        if (!input) return;
        input.addEventListener("input", () => {
          syncStrategyMobileNativeDisplays();
          markStrategyDirtyFromShare();
        });
        input.addEventListener("change", () => {
          syncStrategyMobileNativeDisplays();
          markStrategyDirtyFromShare();
        });
      });
      const exitDaysEl = document.getElementById("strategyExitDays");
      if (exitDaysEl) {
        exitDaysEl.addEventListener("input", markStrategyDirtyFromShare);
        exitDaysEl.addEventListener("change", markStrategyDirtyFromShare);
      }
      document.getElementById("strategyResolveBtn").addEventListener("click", resolveStrategyLeg);
      document.getElementById("strategyRunBtn").addEventListener("click", runStrategyAnalysis);
      document.getElementById("strategyShareBtn").addEventListener("click", shareCurrentStrategy);
      document.getElementById("strategyHoldToExpiry").addEventListener("change", () => {
        refreshStrategyExitCriteriaState();
        markStrategyDirtyFromShare();
      });
      refreshStrategyExitCriteriaState();
      setStrategyResultsVisibility(false);
      renderStrategyLegsTable();
      syncStrategyMobileNativeDisplays();
    }

    async function loadStrategySnapshotDateOptions() {
      const symbol = document.getElementById("strategySymbol").value || "SPX";
      const fromEl = document.getElementById("strategySnapshotFromDate");
      const toEl = document.getElementById("strategySnapshotToDate");
      const fromList = document.getElementById("strategySnapshotFromDateList");
      const toList = document.getElementById("strategySnapshotToDateList");
      if (!fromEl || !toEl || !fromList || !toList) return;

      try {
        const [datesRes, typesRes] = await Promise.all([
          fetch(`/api/options/snapshot-dates?symbol=${encodeURIComponent(symbol)}`),
          fetch(`/api/options/option-types?symbol=${encodeURIComponent(symbol)}`),
        ]);
        const payload = await datesRes.json();
        const typesPayload = await typesRes.json();
        if (!datesRes.ok || !typesRes.ok) {
          strategyState.snapshotDates = [];
          strategyState.optionTypes = [];
          populateStrategyOptionTypeOptions([]);
          fromList.innerHTML = "";
          toList.innerHTML = "";
          fromEl.value = "";
          toEl.value = "";
          syncStrategyMobileNativeDisplays();
          return;
        }

        const dates = Array.isArray(payload.dates) ? payload.dates : [];
        const optionTypes = Array.isArray(typesPayload.option_types) ? typesPayload.option_types : [];
        strategyState.snapshotDates = dates;
        populateStrategyOptionTypeOptions(optionTypes);

        const html = dates.map((d) => `<option value="${escapeHtml(String(d))}"></option>`).join("");
        fromList.innerHTML = html;
        toList.innerHTML = html;

        if (dates.length) {
          fromEl.min = dates[0];
          fromEl.max = dates[dates.length - 1];
          toEl.min = dates[0];
          toEl.max = dates[dates.length - 1];
          if (!dates.includes(fromEl.value)) {
            fromEl.value = dates[0];
          }
          if (!dates.includes(toEl.value)) {
            toEl.value = dates[dates.length - 1];
          }
          if (fromEl.value > toEl.value) {
            toEl.value = fromEl.value;
          }
        } else {
          fromEl.min = "";
          fromEl.max = "";
          toEl.min = "";
          toEl.max = "";
          fromEl.value = "";
          toEl.value = "";
        }
        syncStrategyMobileNativeDisplays();
      } catch {
        strategyState.snapshotDates = [];
        strategyState.optionTypes = [];
        populateStrategyOptionTypeOptions([]);
        fromList.innerHTML = "";
        toList.innerHTML = "";
        fromEl.value = "";
        toEl.value = "";
        syncStrategyMobileNativeDisplays();
      }
    }

    async function shareCurrentStrategy() {
      const button = document.getElementById("strategyShareBtn");
      if (!button || !strategyState.hasCompletedResults || !Array.isArray(strategyState.historyRows) || !strategyState.historyRows.length) {
        return;
      }
      trackEvent("strategy_share_attempt", currentStrategyShareTrackingPayload());
      button.disabled = true;
      setStrategyShareFeedback("Creating share link...", "");
      try {
        const response = await fetch("/api/strategy-shares", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(buildStrategySharePayload()),
        });
        const payload = await response.json();
        if (!response.ok || !payload.share_url) {
          const reason = payload && payload.error ? String(payload.error) : "request_failed";
          setStrategyShareFeedback(`Could not create share link: ${reason}.`, "danger");
          trackStrategyShareResult("error", { reason });
          return;
        }
        let copied = false;
        try {
          if (navigator.clipboard && typeof navigator.clipboard.writeText === "function") {
            await navigator.clipboard.writeText(String(payload.share_url));
            copied = true;
          }
        } catch {}
        setStrategyShareFeedback(
          copied ? "Share link copied." : "Share link ready:",
          "success",
          String(payload.share_url)
        );
        trackStrategyShareResult("success", {
          share_token: String(payload.share_token || ""),
          copied_to_clipboard: copied,
        });
      } catch {
        setStrategyShareFeedback("Could not create share link: request failed.", "danger");
        trackStrategyShareResult("error", { reason: "request_failed" });
      } finally {
        updateStrategyShareControls();
      }
    }

    function applySharedStrategyDefinition(strategy) {
      const definition = strategy && typeof strategy === "object" ? strategy : {};
      const symbol = String(definition.symbol || "SPX");
      const symbolEl = document.getElementById("strategySymbol");
      const fromEl = document.getElementById("strategySnapshotFromDate");
      const toEl = document.getElementById("strategySnapshotToDate");
      const holdEl = document.getElementById("strategyHoldToExpiry");
      const exitDaysEl = document.getElementById("strategyExitDays");
      const exitTimeEl = document.getElementById("strategyExitTime");
      if (symbolEl) symbolEl.value = symbol;
      if (fromEl) fromEl.value = String(definition.snapshot_from_date || "");
      if (toEl) toEl.value = String(definition.snapshot_to_date || "");
      if (holdEl) holdEl.checked = Boolean(definition.hold_till_expiry);
      if (exitDaysEl) exitDaysEl.value = String(definition.exit_days == null ? 0 : definition.exit_days);
      if (exitTimeEl) exitTimeEl.value = String(definition.exit_time || "15:30");
      const legs = Array.isArray(definition.legs) ? definition.legs : [];
      strategyState.legs = legs.map((leg, index) => ({
        id: index + 1,
        side: String(leg && leg.side ? leg.side : "BUY").toUpperCase(),
        quantity: leg && leg.quantity != null ? Number(leg.quantity) || 1 : 1,
        option_type: String(leg && leg.option_type ? leg.option_type : "PUT").toUpperCase(),
        target_delta: leg && leg.target_delta != null ? Number(leg.target_delta) : null,
        target_dte: leg && leg.target_dte != null ? Number(leg.target_dte) : null,
        entry_time: String(leg && leg.entry_time ? leg.entry_time : ""),
        snapshot_from_date: String(leg && leg.snapshot_from_date ? leg.snapshot_from_date : definition.snapshot_from_date || ""),
        snapshot_to_date: String(leg && leg.snapshot_to_date ? leg.snapshot_to_date : definition.snapshot_to_date || ""),
        isResolved: Boolean(leg && leg.isResolved !== false),
        matched_count: leg && leg.matched_count != null ? Number(leg.matched_count) : null,
        entry_snapshot_ts: String(leg && leg.entry_snapshot_ts ? leg.entry_snapshot_ts : ""),
        resolved_contracts: Array.isArray(leg && leg.resolved_contracts) ? cloneJsonSafe(leg.resolved_contracts) : [],
      }));
      strategyState.nextLegId = strategyState.legs.length + 1;
      refreshStrategyExitCriteriaState();
      renderStrategyLegsTable();
      syncStrategyMobileNativeDisplays();
    }

    async function loadSharedStrategyFromUrl() {
      const params = new URLSearchParams(window.location.search || "");
      const shareToken = String(params.get("share") || "").trim();
      if (!shareToken) return;
      const meta = document.getElementById("strategyAnalysisMeta");
      if (meta) {
        meta.textContent = "Loading shared strategy...";
        meta.className = "meta";
      }
      try {
        const response = await fetch(`/api/strategy-shares/${encodeURIComponent(shareToken)}`);
        const payload = await response.json();
        if (!response.ok) {
          const reason = payload && payload.error ? String(payload.error) : "not_found";
          if (meta) {
            meta.textContent = `Could not load shared strategy: ${reason}.`;
            meta.className = "meta danger";
          }
          trackEvent("strategy_share_open", { share_token: shareToken, reason }, "error");
          return;
        }
        applySharedStrategyDefinition(payload.strategy || {});
        await loadStrategySnapshotDateOptions();
        applySharedStrategyDefinition(payload.strategy || {});
        applyStrategyResults(
          payload && payload.results && Array.isArray(payload.results.rows) ? payload.results.rows : [],
          {
            origin: "shared",
            shareToken: String(payload.share_token || shareToken),
            shareUrl: String(payload.share_url || ""),
            createdAt: String(payload.created_at_utc || ""),
          }
        );
        if (meta) {
          meta.textContent = "Shared strategy loaded. Edit the strategy or rerun it locally to refresh the results.";
          meta.className = "meta success";
        }
        trackEvent(
          "strategy_share_open",
          currentStrategyShareTrackingPayload({ share_token: String(payload.share_token || shareToken) }),
          "success"
        );
      } catch {
        if (meta) {
          meta.textContent = "Could not load shared strategy: request failed.";
          meta.className = "meta danger";
        }
        trackEvent("strategy_share_open", { share_token: shareToken, reason: "request_failed" }, "error");
      }
    }

    function appendAnalyzerOption(list, contract, selectedStreamers) {
      const option = document.createElement("option");
      option.value = contract.streamer_symbol;
      option.textContent = contractLabel(contract);
      option.selected = selectedStreamers.has(contract.streamer_symbol);
      list.appendChild(option);
    }

    function analyzerEnsureLeg(streamer, contract = null) {
      if (!analyzerState.legs.has(streamer)) {
        analyzerState.legs.set(streamer, {
          side: "BUY",
          quantity: 1,
          contract: null,
        });
      }
      const leg = analyzerState.legs.get(streamer);
      if (contract) leg.contract = contract;
      return leg;
    }

    function syncAnalyzerLegsWithSelection() {
      Array.from(analyzerState.legs.keys()).forEach((streamer) => {
        if (!analyzerState.selectedStreamers.has(streamer)) {
          analyzerState.legs.delete(streamer);
        }
      });
      analyzerState.selectedStreamers.forEach((streamer) => {
        const contract = analyzerState.contractByStreamer.get(streamer);
        if (contract) analyzerEnsureLeg(streamer, contract);
      });
    }

    function renderAnalyzerLegsTable() {
      const body = document.querySelector("#analyzerLegsTable tbody");
      body.innerHTML = "";
      Array.from(analyzerState.selectedStreamers).forEach((streamer) => {
        const contract = analyzerState.contractByStreamer.get(streamer);
        if (!contract) return;
        const leg = analyzerEnsureLeg(streamer, contract);
        const buyActive = leg.side === "BUY" ? "active" : "";
        const sellActive = leg.side === "SELL" ? "active" : "";
        const tr = document.createElement("tr");
        tr.innerHTML = `
          <td><button type="button" class="remove-leg" data-analyzer-streamer="${escapeHtml(streamer)}">x</button></td>
          <td>${escapeHtml(contractLabel(contract))}</td>
          <td>
            <div class="side-group">
              <button type="button" class="side-btn buy-btn ${buyActive}" data-analyzer-side-streamer="${escapeHtml(streamer)}" data-side="BUY">BUY</button>
              <button type="button" class="side-btn sell-btn ${sellActive}" data-analyzer-side-streamer="${escapeHtml(streamer)}" data-side="SELL">SELL</button>
            </div>
          </td>
          <td><input class="input qty-input" type="number" min="1" step="1" value="${Number(leg.quantity) || 1}" data-analyzer-qty-streamer="${escapeHtml(streamer)}" /></td>
        `;
        body.appendChild(tr);
      });

      body.querySelectorAll("[data-analyzer-streamer]").forEach((btn) => {
        btn.addEventListener("click", (event) => {
          const streamer = event.currentTarget.getAttribute("data-analyzer-streamer");
          if (!streamer) return;
          analyzerState.selectedStreamers.delete(streamer);
          analyzerState.legs.delete(streamer);
          renderAnalyzerContractSelector();
          renderAnalyzerLegsTable();
        });
      });

      body.querySelectorAll("[data-analyzer-side-streamer]").forEach((btn) => {
        btn.addEventListener("click", (event) => {
          const streamer = event.currentTarget.getAttribute("data-analyzer-side-streamer");
          const side = event.currentTarget.getAttribute("data-side");
          if (!streamer || (side !== "BUY" && side !== "SELL")) return;
          const leg = analyzerEnsureLeg(streamer, analyzerState.contractByStreamer.get(streamer) || null);
          leg.side = side;
          renderAnalyzerLegsTable();
        });
      });

      body.querySelectorAll("[data-analyzer-qty-streamer]").forEach((inputEl) => {
        inputEl.addEventListener("input", (event) => {
          const streamer = event.currentTarget.getAttribute("data-analyzer-qty-streamer");
          if (!streamer) return;
          const leg = analyzerEnsureLeg(streamer, analyzerState.contractByStreamer.get(streamer) || null);
          const parsed = parseInt(event.currentTarget.value || "1", 10);
          leg.quantity = Number.isFinite(parsed) && parsed > 0 ? parsed : 1;
        });
      });
    }

    function getAnalyzerFilterState() {
      return {
        symbol: document.getElementById("analyzerSymbol").value || "SPX",
        optionType: document.getElementById("analyzerOptionType").value || "",
        expiration: document.getElementById("analyzerExpiration").value || "",
      };
    }

    function filteredAnalyzerContracts(contracts) {
      const { optionType, expiration } = getAnalyzerFilterState();
      return contracts.filter((contract) => {
        if (optionType && contract.option_type !== optionType) return false;
        if (expiration && String(contract.expiration_date) !== String(expiration)) return false;
        return true;
      });
    }

    function populateAnalyzerExpirationFilter(contracts) {
      const { optionType } = getAnalyzerFilterState();
      const expirationFilter = document.getElementById("analyzerExpiration");
      const selected = expirationFilter.value;
      const values = [];
      const seen = new Set();
      contracts
        .filter((contract) => !optionType || contract.option_type === optionType)
        .forEach((contract) => {
          const value = String(contract.expiration_date || "").trim();
          if (!value || seen.has(value)) return;
          seen.add(value);
          values.push(value);
        });
      values.sort();

      expirationFilter.innerHTML = `<option value="">All Expirations</option>`;
      values.forEach((value) => {
        const option = document.createElement("option");
        option.value = value;
        option.textContent = value;
        expirationFilter.appendChild(option);
      });
      if (selected && values.includes(selected)) expirationFilter.value = selected;
    }

    function renderAnalyzerContractSelector() {
      const list = document.getElementById("analyzerContracts");
      const filtered = filteredAnalyzerContracts(analyzerState.loadedContracts);
      const visibleSelected = new Set(filtered.map((contract) => contract.streamer_symbol));
      list.innerHTML = "";
      filtered.forEach((contract) => appendAnalyzerOption(list, contract, analyzerState.selectedStreamers));
      analyzerState.selectedStreamers.forEach((streamer) => {
        if (visibleSelected.has(streamer)) return;
        const contract = analyzerState.contractByStreamer.get(streamer);
        if (!contract) return;
        appendAnalyzerOption(list, contract, analyzerState.selectedStreamers);
      });
      syncAnalyzerLegsWithSelection();
      renderAnalyzerLegsTable();
    }

    async function loadAnalyzerContracts() {
      const symbol = document.getElementById("analyzerSymbol").value || "SPX";
      const optionType = document.getElementById("analyzerOptionType").value || "";
      const preservedSelections = new Set(analyzerState.selectedStreamers);
      const params = new URLSearchParams({ symbol, limit: "800" });
      if (optionType) params.set("type", optionType);

      const meta = document.getElementById("analyzerMeta");
      meta.textContent = "Loading contracts...";
      meta.className = "meta";
      const res = await fetch(`/api/options/contracts?${params.toString()}`);
      const payload = await res.json();
      if (!res.ok) {
        meta.textContent = "Error loading contracts: " + (payload.error || "unknown");
        meta.className = "meta danger";
        return;
      }

      analyzerState.loadedContracts = Array.isArray(payload.contracts) ? payload.contracts : [];
      analyzerState.contractByStreamer.clear();
      analyzerState.loadedContracts.forEach((contract) => {
        if (contract && contract.streamer_symbol) {
          analyzerState.contractByStreamer.set(contract.streamer_symbol, contract);
        }
      });
      preservedSelections.forEach((streamer) => {
        if (!analyzerState.contractByStreamer.has(streamer)) {
          analyzerState.selectedStreamers.delete(streamer);
        }
      });
      analyzerState.loadedContracts.sort((a, b) => {
        const dt = String(a.expiration_date).localeCompare(String(b.expiration_date));
        if (dt !== 0) return dt;
        return Number(a.strike_price) - Number(b.strike_price);
      });
      populateAnalyzerExpirationFilter(analyzerState.loadedContracts);
      renderAnalyzerContractSelector();
      meta.textContent = `Loaded ${analyzerState.loadedContracts.length} contracts`;
      meta.className = "meta success";
    }

    function bindAnalyzerSelectionUX() {
      const list = document.getElementById("analyzerContracts");
      list.addEventListener("mousedown", (event) => {
        const selectEl = event.currentTarget;
        const target = event.target;
        if (!target || target.tagName !== "OPTION") return;
        const streamer = target.value;
        if (!streamer) return;
        const meta = document.getElementById("analyzerMeta");
        const currentlySelectedCount = analyzerState.selectedStreamers.size;
        const isSelected = analyzerState.selectedStreamers.has(streamer);
        const priorScrollTop = selectEl.scrollTop;

        if (!isSelected && currentlySelectedCount >= MAX_ANALYZER_SELECTED_CONTRACTS) {
          event.preventDefault();
          meta.textContent = `You can select up to ${MAX_ANALYZER_SELECTED_CONTRACTS} contracts.`;
          meta.className = "meta danger";
          return;
        }
        event.preventDefault();
        if (isSelected) {
          analyzerState.selectedStreamers.delete(streamer);
        } else {
          analyzerState.selectedStreamers.add(streamer);
        }
        syncAnalyzerLegsWithSelection();
        renderAnalyzerContractSelector();
        requestAnimationFrame(() => {
          selectEl.scrollTop = priorScrollTop;
          selectEl.focus({ preventScroll: true });
        });
      });

      list.addEventListener("click", (event) => {
        const target = event.target;
        if (!target || target.tagName !== "OPTION") return;
        event.preventDefault();
      });

      list.addEventListener("change", () => {
        const selectedVisible = new Set(Array.from(list.selectedOptions).map((optionEl) => optionEl.value));
        Array.from(list.options).forEach((optionEl) => {
          const streamer = optionEl.value;
          if (!streamer) return;
          if (selectedVisible.has(streamer)) {
            analyzerState.selectedStreamers.add(streamer);
          } else {
            analyzerState.selectedStreamers.delete(streamer);
          }
        });
        syncAnalyzerLegsWithSelection();
        renderAnalyzerLegsTable();
      });
    }

    function transformAnalyzerRows(rows, spotSeries) {
      const grouped = {};
      rows.forEach((row) => {
        const key = row.streamer_symbol;
        if (!grouped[key]) grouped[key] = [];
        grouped[key].push(row);
      });
      const nearestSpot = buildSpotLookup(spotSeries || []);
      const transformed = [];
      Object.entries(grouped).forEach(([, contractRows]) => {
        const sorted = [...contractRows].sort((a, b) => parseTimestamp(a.snapshot_ts) - parseTimestamp(b.snapshot_ts));
        const first = sorted.find((row) => row.value !== null && Number.isFinite(Number(row.value)));
        const firstValue = first && first.value != null && Number.isFinite(Number(first.value)) ? Number(first.value) : null;

        sorted.forEach((row) => {
          const value = row.value == null ? null : Number(row.value);
          const indexed = value != null && firstValue != null && firstValue !== 0 && Number.isFinite(value) && Number.isFinite(firstValue)
            ? (value / firstValue) * 100
            : null;
          const spread = row.bid_price == null || row.ask_price == null ? null : Number(row.ask_price) - Number(row.bid_price);
          transformed.push({
            ...row,
            indexed,
            spread,
            spot_price: nearestSpot(row.snapshot_ts),
          });
        });
      });
      return transformed.sort((a, b) => {
        const aTs = parseTimestamp(a.snapshot_ts);
        const bTs = parseTimestamp(b.snapshot_ts);
        if ((aTs - bTs) !== 0) return aTs - bTs;
        const aLabel = contractLabel(a);
        const bLabel = contractLabel(b);
        return aLabel.localeCompare(bLabel);
      });
    }

    function renderAnalyzerSeriesTable(rows) {
      const body = document.querySelector("#analyzerSeriesTable tbody");
      body.innerHTML = "";
      const visibleRows = (Array.isArray(rows) ? rows : []).slice(0, MAX_TABLE_RENDER_ROWS);
      visibleRows.forEach((row) => {
        const spread = row.spread == null ? "" : Number(row.spread).toFixed(4);
        const tr = document.createElement("tr");
        tr.innerHTML = `
          <td>${row.snapshot_ts ? formatLocalDateTime(row.snapshot_ts) : ""}</td>
          <td>${escapeHtml(contractLabel(row))}</td>
          <td>${row.spot_price == null ? "" : Number(row.spot_price).toFixed(4)}</td>
          <td>${row.value == null ? "" : Number(row.value).toFixed(4)}</td>
          <td>${row.indexed == null ? "" : Number(row.indexed).toFixed(4)}</td>
          <td>${row.delta == null ? "" : Number(row.delta).toFixed(4)}</td>
          <td>${row.gamma == null ? "" : Number(row.gamma).toFixed(4)}</td>
          <td>${row.theta == null ? "" : Number(row.theta).toFixed(4)}</td>
          <td>${row.vega == null ? "" : Number(row.vega).toFixed(4)}</td>
          <td>${row.volatility == null ? "" : Number(row.volatility).toFixed(4)}</td>
          <td>${spread}</td>
        `;
        body.appendChild(tr);
      });
    }

    function transformAnalyzerStrategyRows(rows, spotSeries) {
      const grouped = {};
      rows.forEach((row) => {
        const key = row.streamer_symbol;
        if (!grouped[key]) grouped[key] = [];
        grouped[key].push(row);
      });
      const nearestSpot = buildSpotLookup(spotSeries || []);
      const transformedRows = [];
      const legMetaByStreamer = {};

      Object.entries(grouped).forEach(([streamer, contractRows]) => {
        const leg = analyzerState.legs.get(streamer);
        const contract = analyzerState.contractByStreamer.get(streamer);
        if (!leg || !contract) return;
        const sorted = [...contractRows].sort((a, b) => parseTimestamp(a.snapshot_ts) - parseTimestamp(b.snapshot_ts));
        const sign = leg.side === "SELL" ? -1 : 1;
        const quantity = Number(leg.quantity) > 0 ? Number(leg.quantity) : 1;
        const first = sorted.find((row) => row.value !== null && Number.isFinite(Number(row.value)));
        const entryValue = first ? Number(first.value) : null;
        legMetaByStreamer[streamer] = {
          side: leg.side,
          quantity,
          contract,
          sign,
          entryValue,
        };

        sorted.forEach((row) => {
          const value = row.value == null ? null : Number(row.value);
          const contribution = value != null && Number.isFinite(value) ? sign * quantity * value : null;
          const indexed = value != null && entryValue != null && entryValue !== 0 && Number.isFinite(value) && Number.isFinite(entryValue)
            ? (value / entryValue) * 100
            : null;
          transformedRows.push({
            ...row,
            indexed,
            spot_price: nearestSpot(row.snapshot_ts),
            leg_contribution: contribution,
            isStrategySummary: false,
          });
        });
      });

      const bySnapshot = new Map();
      transformedRows.forEach((row) => {
        const key = row.snapshot_ts || "";
        if (!bySnapshot.has(key)) bySnapshot.set(key, []);
        bySnapshot.get(key).push(row);
      });

      const spreadRows = [];
      const sortedTs = Array.from(bySnapshot.keys()).sort((a, b) => parseTimestamp(a) - parseTimestamp(b));
      sortedTs.forEach((ts) => {
        const rowsForTs = bySnapshot.get(ts) || [];
        rowsForTs.sort((a, b) => contractLabel(a).localeCompare(contractLabel(b)));
        rowsForTs.forEach((row) => spreadRows.push(row));

        let hasAllContributions = true;
        let strategyValue = 0;
        let strategyCost = null;
        rowsForTs.forEach((row) => {
          const meta = legMetaByStreamer[row.streamer_symbol];
          if (!meta) return;
          if (row.leg_contribution == null || !Number.isFinite(row.leg_contribution)) {
            hasAllContributions = false;
            return;
          }
          strategyValue += row.leg_contribution;
          if (meta.entryValue != null && Number.isFinite(meta.entryValue)) {
            strategyCost = (strategyCost == null ? 0 : strategyCost) + meta.sign * meta.quantity * meta.entryValue;
          }
        });

        const strategyIdx = strategyCost && Number.isFinite(strategyCost) && strategyCost !== 0 ? (strategyValue / strategyCost) * 100 : null;
        spreadRows.push({
          snapshot_ts: ts,
          spot_price: rowsForTs.length ? rowsForTs[0].spot_price : null,
          value: hasAllContributions ? strategyValue : null,
          indexed: strategyIdx,
          strategy_price: hasAllContributions ? strategyValue : null,
          strategy_cost: strategyCost,
          strategy_pnl: strategyCost != null && hasAllContributions ? strategyValue - strategyCost : null,
          strategy_indexed: strategyIdx,
          isStrategySummary: true,
        });
      });

      return spreadRows;
    }

    function renderAnalyzerStrategySeriesTable(rows) {
      const body = document.querySelector("#analyzerStrategySeriesTable tbody");
      body.innerHTML = "";
      const visibleRows = (Array.isArray(rows) ? rows : []).slice(0, MAX_TABLE_RENDER_ROWS);
      visibleRows.forEach((row) => {
        const spread = row.bid_price == null || row.ask_price == null ? "" : (Number(row.ask_price) - Number(row.bid_price)).toFixed(4);
        const strategy = row.strategy_price == null ? "" : Number(row.strategy_price).toFixed(4);
        const strategyCost = row.strategy_cost == null ? "" : Number(row.strategy_cost).toFixed(4);
        const strategyPnl = row.strategy_pnl == null ? "" : Number(row.strategy_pnl).toFixed(4);
        const strategyIndexed = row.strategy_indexed == null ? "" : Number(row.strategy_indexed).toFixed(4);
        const contribution = row.leg_contribution == null ? "" : Number(row.leg_contribution).toFixed(4);
        const tr = document.createElement("tr");
        tr.innerHTML = `
          <td>${row.snapshot_ts ? formatLocalDateTime(row.snapshot_ts) : ""}</td>
          <td>${row.isStrategySummary ? "Strategy" : escapeHtml(contractLabel(row))}</td>
          <td>${row.spot_price == null ? "" : Number(row.spot_price).toFixed(4)}</td>
          <td>${row.value == null ? "" : Number(row.value).toFixed(4)}</td>
          <td>${row.indexed == null ? "" : Number(row.indexed).toFixed(4)}</td>
          <td>${contribution}</td>
          <td>${strategy}</td>
          <td>${strategyCost}</td>
          <td>${strategyPnl}</td>
          <td>${strategyIndexed}</td>
          <td>${spread}</td>
        `;
        if (row.isStrategySummary) tr.className = "strategy-summary";
        body.appendChild(tr);
      });
    }

    async function runAnalyzerAnalysis() {
      const meta = document.getElementById("analyzerSeriesMeta");
      const streamers = Array.from(analyzerState.selectedStreamers).filter((streamer) => analyzerState.contractByStreamer.has(streamer));
      if (!streamers.length) {
        meta.textContent = "Please select at least one contract.";
        meta.className = "meta danger";
        renderAnalyzerSeriesTable([]);
        return;
      }

      const symbol = document.getElementById("analyzerSymbol").value || "SPX";
      const seriesParams = new URLSearchParams({ symbol, streamers: streamers.join(","), field: "mid_price" });
      const [seriesRes, summaryRes] = await Promise.all([
        fetch(`/api/options/series?${seriesParams.toString()}`),
        fetch(`/api/options/summary?${new URLSearchParams({ symbol })}`),
      ]);
      const seriesData = await seriesRes.json();
      const summaryData = await summaryRes.json();
      if (!seriesRes.ok) {
        meta.textContent = "Error loading series: " + (seriesData.error || "unknown");
        meta.className = "meta danger";
        return;
      }
      if (!summaryRes.ok) {
        meta.textContent = "Warning: could not load spot/summary data.";
        meta.className = "meta danger";
      }

      const rows = Array.isArray(seriesData.rows) ? seriesData.rows : [];
      if (!rows.length) {
        meta.textContent = "No data for the selected contracts.";
        meta.className = "meta danger";
        renderAnalyzerSeriesTable([]);
        return;
      }

      const transformed = transformAnalyzerRows(rows, summaryData.market_series || []);
      renderAnalyzerSeriesTable(transformed);
      meta.textContent = rows.length > MAX_TABLE_RENDER_ROWS
        ? `Loaded ${streamers.length} contracts, ${rows.length} rows. Showing first ${MAX_TABLE_RENDER_ROWS} table rows.`
        : `Loaded ${streamers.length} contracts, ${rows.length} rows.`;
      meta.className = "meta success";
    }

    async function runAnalyzerStrategyAnalysis() {
      const meta = document.getElementById("analyzerStrategyMeta");
      const streamers = Array.from(analyzerState.selectedStreamers).filter((streamer) => analyzerState.contractByStreamer.has(streamer));
      if (!streamers.length) {
        meta.textContent = "Please select at least one contract.";
        meta.className = "meta danger";
        renderAnalyzerStrategySeriesTable([]);
        return;
      }

      const usableLegs = streamers.filter((streamer) => {
        const leg = analyzerState.legs.get(streamer);
        return leg && Number(leg.quantity) > 0 && (leg.side === "BUY" || leg.side === "SELL");
      });
      if (!usableLegs.length) {
        meta.textContent = "Please set at least one valid leg (BUY/SELL with qty > 0).";
        meta.className = "meta danger";
        renderAnalyzerStrategySeriesTable([]);
        return;
      }

      const symbol = document.getElementById("analyzerSymbol").value || "SPX";
      const seriesParams = new URLSearchParams({ symbol, streamers: usableLegs.join(","), field: "mid_price" });
      const [seriesRes, summaryRes] = await Promise.all([
        fetch(`/api/options/series?${seriesParams.toString()}`),
        fetch(`/api/options/summary?${new URLSearchParams({ symbol })}`),
      ]);
      const seriesData = await seriesRes.json();
      const summaryData = await summaryRes.json();
      if (!seriesRes.ok) {
        meta.textContent = "Error loading strategy series: " + (seriesData.error || "unknown");
        meta.className = "meta danger";
        renderAnalyzerStrategySeriesTable([]);
        return;
      }
      if (!summaryRes.ok) {
        meta.textContent = "Warning: could not load spot/summary data.";
        meta.className = "meta danger";
      }

      const rows = Array.isArray(seriesData.rows) ? seriesData.rows : [];
      if (!rows.length) {
        meta.textContent = "No data for selected strategy legs.";
        meta.className = "meta danger";
        renderAnalyzerStrategySeriesTable([]);
        return;
      }

      const transformed = transformAnalyzerStrategyRows(rows, summaryData.market_series || []);
      renderAnalyzerStrategySeriesTable(transformed);
      meta.textContent = rows.length > MAX_TABLE_RENDER_ROWS
        ? `Loaded ${usableLegs.length} strategy legs, ${rows.length} rows. Showing first ${MAX_TABLE_RENDER_ROWS} table rows.`
        : `Loaded ${usableLegs.length} strategy legs, ${rows.length} rows.`;
      meta.className = "meta success";
    }

    function initAnalyzerTab() {
      if (tabInitState.analyzer) return;
      if (!document.getElementById("analyzerSymbol")) return;
      tabInitState.analyzer = true;

      bindAnalyzerSelectionUX();
      document.getElementById("analyzerOptionType").addEventListener("change", loadAnalyzerContracts);
      document.getElementById("analyzerExpiration").addEventListener("change", () => {
        populateAnalyzerExpirationFilter(analyzerState.loadedContracts);
        renderAnalyzerContractSelector();
      });
      document.getElementById("analyzerSymbol").addEventListener("change", loadAnalyzerContracts);
      document.getElementById("analyzerRunStrategyBtn").addEventListener("click", runAnalyzerStrategyAnalysis);
      renderAnalyzerLegsTable();
      loadAnalyzerContracts();
    }

    async function initPage() {
      initTabs();
      initStrategyTab();
      initAnalyzerTab();
      await loadSharedStrategyFromUrl();
      trackPageView();
    }

    initPage();
  </script>
</body>
</html>
"""


if __name__ == "__main__":
    main()
