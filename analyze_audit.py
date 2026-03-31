from __future__ import annotations

import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent
AUDIT_PATH = ROOT / "logs" / "runtime" / "trade_audit.jsonl"
REPORT_PATH = ROOT / "logs" / "reports" / "audit_summary.json"


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def load_rows(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not path.exists():
        return rows
    with path.open("r", encoding="utf-8") as fh:
        for raw in fh:
            line = raw.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except Exception:
                continue
            if isinstance(rec, dict):
                rows.append(rec)
    return rows


def build_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    by_event = Counter()
    by_symbol = Counter()
    top_blocks = Counter()
    signal_reasons = Counter()
    explainers = Counter()
    quality_by_event: dict[str, list[float]] = defaultdict(list)
    quality_by_symbol: dict[str, list[float]] = defaultdict(list)

    for rec in rows:
        event = str(rec.get("event", "") or "").strip() or "-"
        symbol = str(rec.get("symbol", "") or "").strip() or "-"
        by_event[event] += 1
        by_symbol[symbol] += 1

        if "data_quality_score" in rec:
            score = max(0.0, min(1.0, _safe_float(rec.get("data_quality_score"), 0.0)))
            quality_by_event[event].append(score)
            quality_by_symbol[symbol].append(score)

        for reason in rec.get("buy_block_reasons", []) if isinstance(rec.get("buy_block_reasons"), list) else []:
            reason_norm = str(reason or "").strip()
            if reason_norm:
                top_blocks[reason_norm] += 1

        signal_reason = str(rec.get("signal_reason", "") or "").strip()
        if signal_reason:
            signal_reasons[signal_reason] += 1

        for line in rec.get("signal_explainer", []) if isinstance(rec.get("signal_explainer"), list) else []:
            expl = str(line or "").strip()
            if expl:
                explainers[expl] += 1

    def _avg_map(src: dict[str, list[float]], limit: int) -> list[dict[str, Any]]:
        out = []
        ranked = sorted(src.items(), key=lambda item: len(item[1]), reverse=True)
        for key, values in ranked[:limit]:
            if not values:
                continue
            avg = sum(values) / max(1, len(values))
            out.append({"key": key, "count": len(values), "avg_data_quality_score": round(avg, 4)})
        return out

    return {
        "events_total": len(rows),
        "top_events": [{"event": k, "count": v} for k, v in by_event.most_common(12)],
        "top_symbols": [{"symbol": k, "count": v} for k, v in by_symbol.most_common(12)],
        "top_buy_block_reasons": [{"reason": k, "count": v} for k, v in top_blocks.most_common(12)],
        "top_signal_reasons": [{"reason": k, "count": v} for k, v in signal_reasons.most_common(12)],
        "top_explainers": [{"line": k, "count": v} for k, v in explainers.most_common(12)],
        "avg_data_quality_by_event": _avg_map(quality_by_event, 12),
        "avg_data_quality_by_symbol": _avg_map(quality_by_symbol, 12),
    }


def print_summary(summary: dict[str, Any]) -> None:
    print("=== AUDIT SUMMARY ===")
    print(f"Всего событий: {summary.get('events_total', 0)}")

    print("\nТоп событий:")
    for item in summary.get("top_events", []):
        print(f"  - {item['event']}: {item['count']}")

    print("\nТоп причин блокировки:")
    for item in summary.get("top_buy_block_reasons", []):
        print(f"  - {item['reason']}: {item['count']}")

    print("\nТоп explainers:")
    for item in summary.get("top_explainers", [])[:8]:
        print(f"  - {item['line']}: {item['count']}")

    print("\nСредний data_quality_score по событиям:")
    for item in summary.get("avg_data_quality_by_event", [])[:8]:
        print(f"  - {item['key']}: {item['avg_data_quality_score']} ({item['count']})")


def main() -> None:
    rows = load_rows(AUDIT_PATH)
    summary = build_summary(rows)
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print_summary(summary)
    print(f"\nОтчёт сохранён: {REPORT_PATH}")


if __name__ == "__main__":
    main()
