from __future__ import annotations

import argparse
import json
import time
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


UTC = timezone.utc


def utc_now() -> datetime:
    return datetime.now(UTC)


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(float(value))
    except Exception:
        return default


def read_json(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def iso_parse(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except Exception:
        return None


@dataclass
class MonitorState:
    last_event: str = ""
    last_symbol: str = ""
    last_bot_pid: int = 0
    last_open_position: bool = False
    last_cycle_index: int = -1
    samples: int = 0
    stale_hits: int = 0
    stopped_hits: int = 0
    api_error_hits: int = 0
    logic_error_hits: int = 0
    cycle_error_hits: int = 0
    open_transitions: int = 0
    close_transitions: int = 0
    pid_changes: int = 0
    issue_counter: Counter[str] = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        if self.issue_counter is None:
            self.issue_counter = Counter()


def build_sample(payload: dict[str, Any]) -> tuple[dict[str, Any], list[str]]:
    ts_utc = utc_now().isoformat()
    status = str(payload.get("status") or "unknown")
    bot_mode = str(payload.get("bot_mode") or payload.get("mode") or "-")
    event_name = str(payload.get("event") or "-")
    symbol = str(payload.get("symbol") or "-")
    cycle_index = safe_int(payload.get("cycle_index"), -1)
    price = safe_float(payload.get("price"), 0.0)
    dry_run = bool(payload.get("dry_run", False))
    bot_pid = safe_int(payload.get("bot_pid"), 0)
    has_open_position = bool(payload.get("has_open_position", False))
    market_regime = str(payload.get("market_regime") or payload.get("regime") or "-")
    human_reason = str(payload.get("human_reason") or "")
    risk_guard = payload.get("risk_guard") if isinstance(payload.get("risk_guard"), dict) else {}
    api_err = safe_int(risk_guard.get("consecutive_api_errors"), 0)
    logic_err = safe_int(risk_guard.get("consecutive_logic_errors"), 0)
    cycle_err = safe_int(risk_guard.get("consecutive_cycle_errors"), 0)
    status_dt = iso_parse(payload.get("status_write_ts_utc") or payload.get("ts_utc"))
    status_age_sec = max(0.0, (utc_now() - status_dt).total_seconds()) if status_dt else 999999.0

    issues: list[str] = []
    if status != "running":
        issues.append("bot_not_running")
    if status_age_sec >= 90.0:
        issues.append("status_stale")
    if api_err > 0:
        issues.append("api_errors")
    if logic_err > 0:
        issues.append("logic_errors")
    if cycle_err > 0:
        issues.append("cycle_errors")
    if event_name == "market_data_unavailable":
        issues.append("market_data_unavailable")
    if price <= 0 and event_name not in {"gui_starting", "gui_started", "gui_restart"}:
        issues.append("price_zero")
    if dry_run and bot_mode == "live":
        issues.append("dry_run_in_live")

    sample = {
        "ts_utc": ts_utc,
        "ok": len(issues) == 0,
        "issues": issues,
        "status": status,
        "bot_mode": bot_mode,
        "dry_run": dry_run,
        "symbol": symbol,
        "event_name": event_name,
        "cycle_index": cycle_index,
        "price": price,
        "bot_pid": bot_pid,
        "has_open_position": has_open_position,
        "market_regime": market_regime,
        "human_reason": human_reason,
        "api_err": api_err,
        "logic_err": logic_err,
        "cycle_err": cycle_err,
        "status_age_sec": round(status_age_sec, 3),
    }
    return sample, issues


def write_jsonl(path: Path, row: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(row, ensure_ascii=False) + "\n")


def run_monitor(base_dir: Path, hours: float, interval_sec: float) -> int:
    reports_dir = base_dir / "logs" / "reports"
    runtime_dir = base_dir / "logs" / "runtime"
    live_path = runtime_dir / "bot_live_status.json"
    stamp = utc_now().strftime("%Y%m%d_%H%M%S")
    jsonl_path = reports_dir / f"night_watch_{stamp}.jsonl"
    summary_path = reports_dir / f"night_watch_{stamp}_summary.json"

    write_jsonl(
        jsonl_path,
        {
            "ts_utc": utc_now().isoformat(),
            "event": "monitor_start",
            "duration_sec": int(hours * 3600),
            "interval_sec": interval_sec,
            "live_path": str(live_path),
        },
    )

    end_at = utc_now() + timedelta(hours=hours)
    state = MonitorState()

    while utc_now() < end_at:
        payload = read_json(live_path)
        if not payload:
            row = {
                "ts_utc": utc_now().isoformat(),
                "ok": False,
                "issues": ["live_status_unreadable"],
                "error": f"cannot_read:{live_path}",
            }
            write_jsonl(jsonl_path, row)
            state.issue_counter.update(row["issues"])
            state.samples += 1
            time.sleep(interval_sec)
            continue

        row, issues = build_sample(payload)
        write_jsonl(jsonl_path, row)
        state.samples += 1
        state.issue_counter.update(issues)

        if "status_stale" in issues:
            state.stale_hits += 1
        if "bot_not_running" in issues:
            state.stopped_hits += 1
        if "api_errors" in issues:
            state.api_error_hits += 1
        if "logic_errors" in issues:
            state.logic_error_hits += 1
        if "cycle_errors" in issues:
            state.cycle_error_hits += 1

        event_name = str(row["event_name"])
        symbol = str(row["symbol"])
        bot_pid = safe_int(row["bot_pid"], 0)
        has_open = bool(row["has_open_position"])
        cycle_index = safe_int(row["cycle_index"], -1)

        if state.last_event and event_name != state.last_event:
            write_jsonl(
                jsonl_path,
                {
                    "ts_utc": utc_now().isoformat(),
                    "event": "event_change",
                    "from": state.last_event,
                    "to": event_name,
                    "symbol": symbol,
                    "cycle_index": cycle_index,
                },
            )
        if state.last_symbol and symbol != state.last_symbol:
            write_jsonl(
                jsonl_path,
                {
                    "ts_utc": utc_now().isoformat(),
                    "event": "symbol_change",
                    "from": state.last_symbol,
                    "to": symbol,
                    "cycle_index": cycle_index,
                },
            )
        if state.last_bot_pid and bot_pid and bot_pid != state.last_bot_pid:
            state.pid_changes += 1
            write_jsonl(
                jsonl_path,
                {
                    "ts_utc": utc_now().isoformat(),
                    "event": "bot_pid_change",
                    "from": state.last_bot_pid,
                    "to": bot_pid,
                    "cycle_index": cycle_index,
                },
            )
        if has_open and not state.last_open_position:
            state.open_transitions += 1
            write_jsonl(
                jsonl_path,
                {
                    "ts_utc": utc_now().isoformat(),
                    "event": "position_opened",
                    "symbol": symbol,
                    "cycle_index": cycle_index,
                    "price": row["price"],
                },
            )
        if state.last_open_position and not has_open:
            state.close_transitions += 1
            write_jsonl(
                jsonl_path,
                {
                    "ts_utc": utc_now().isoformat(),
                    "event": "position_closed",
                    "symbol": symbol,
                    "cycle_index": cycle_index,
                    "price": row["price"],
                },
            )

        state.last_event = event_name
        state.last_symbol = symbol
        state.last_bot_pid = bot_pid or state.last_bot_pid
        state.last_open_position = has_open
        state.last_cycle_index = cycle_index
        time.sleep(interval_sec)

    summary = {
        "ts_utc": utc_now().isoformat(),
        "event": "monitor_end",
        "duration_sec": int(hours * 3600),
        "interval_sec": interval_sec,
        "samples": state.samples,
        "stale_hits": state.stale_hits,
        "stopped_hits": state.stopped_hits,
        "api_error_hits": state.api_error_hits,
        "logic_error_hits": state.logic_error_hits,
        "cycle_error_hits": state.cycle_error_hits,
        "position_opened_count": state.open_transitions,
        "position_closed_count": state.close_transitions,
        "pid_changes": state.pid_changes,
        "issue_counter": dict(state.issue_counter),
        "jsonl_path": str(jsonl_path),
    }
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    write_jsonl(jsonl_path, summary)
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-dir", default=".")
    parser.add_argument("--hours", type=float, default=8.0)
    parser.add_argument("--interval-sec", type=float, default=30.0)
    args = parser.parse_args()
    return run_monitor(Path(args.base_dir).resolve(), float(args.hours), float(args.interval_sec))


if __name__ == "__main__":
    raise SystemExit(main())
