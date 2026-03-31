# WARNING: This is an educational bot and is NOT intended for real trading with real funds.

import json
import logging
import math
import os
import signal
import socket
import sys
import time
import threading
import asyncio
import numpy as np
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from collections import deque
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Tuple

from adaptive_agent import AdaptiveAgent
from ai_signal import AISignal, AISignalEngine
from bot_cycle_snapshot import CycleSnapshot
from bot_entry_policy import EntryPolicy
from bot_execution_engine import ExecutionEngine
from bot_position_state_machine import PositionStateMachine
from bot_risk_policy import RiskPolicy
from advisory_provider import AdvisoryProvider, AdvisorySignal
from engine_adapter import build_engine_adapter
from live_reason_mapper import build_human_reason
from market_context import MarketContextEngine
from model_evolver import ModelEvolver
from mexc import MexcSync
from position_overlays import PositionOverlayEngine
from env_utils import load_env_layers

try:
    import aiohttp  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    aiohttp = None

if getattr(sys, "frozen", False):
    APP_BASE_DIR = Path(sys.executable).resolve().parent
else:
    APP_BASE_DIR = Path(__file__).resolve().parent


def _env_bool_raw(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _safe_float(value: object, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def _lerp(lo: float, hi: float, t: float) -> float:
    return lo + (hi - lo) * _clamp(t, 0.0, 1.0)


def _pnl_pct(entry_price: float, price: float) -> float:
    if entry_price <= 0:
        return 0.0
    return (price / entry_price) - 1.0


def _iso_to_ts(value: object) -> float:
    raw = str(value or "").strip()
    if not raw:
        return 0.0
    try:
        norm = raw.replace("Z", "+00:00")
        return datetime.fromisoformat(norm).timestamp()
    except Exception:
        return 0.0


def _parse_runtime_log_ts(line: str) -> Optional[datetime]:
    raw = str(line or "")
    if len(raw) < 23:
        return None
    head = raw[:23]
    try:
        return datetime.strptime(head, "%Y-%m-%d %H:%M:%S,%f")
    except Exception:
        return None


def _prune_text_log_by_days(path: Path, retention_days: int) -> tuple[int, int]:
    if retention_days <= 0 or (not path.exists()) or (not path.is_file()):
        return (0, 0)
    try:
        original = path.read_text(encoding="utf-8-sig", errors="ignore")
    except Exception:
        return (0, 0)
    lines = original.splitlines(keepends=True)
    if not lines:
        return (0, 0)
    cutoff_dt = datetime.now() - timedelta(days=retention_days)
    kept: list[str] = []
    removed = 0
    keep_current_block = True
    for ln in lines:
        ts = _parse_runtime_log_ts(ln)
        if ts is not None:
            keep_current_block = ts >= cutoff_dt
        if keep_current_block:
            kept.append(ln)
        else:
            removed += 1
    if removed <= 0:
        return (0, len(lines))
    try:
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text("".join(kept), encoding="utf-8")
        tmp.replace(path)
        return (removed, len(lines))
    except Exception:
        return (0, len(lines))


def _prune_jsonl_by_days(path: Path, retention_days: int) -> tuple[int, int]:
    if retention_days <= 0 or (not path.exists()) or (not path.is_file()):
        return (0, 0)
    try:
        rows = path.read_text(encoding="utf-8-sig", errors="ignore").splitlines()
    except Exception:
        return (0, 0)
    if not rows:
        return (0, 0)
    cutoff_ts = time.time() - (retention_days * 86400.0)
    kept: list[str] = []
    removed = 0
    for row in rows:
        raw = row.strip()
        if not raw:
            continue
        keep = True
        try:
            payload = json.loads(raw)
            ts = _iso_to_ts(payload.get("ts_utc")) if isinstance(payload, dict) else 0.0
            if ts > 0 and ts < cutoff_ts:
                keep = False
        except Exception:
            # Keep malformed lines to avoid accidental data loss.
            keep = True
        if keep:
            kept.append(raw)
        else:
            removed += 1
    if removed <= 0:
        return (0, len(rows))
    try:
        tmp = path.with_suffix(path.suffix + ".tmp")
        body = ("\n".join(kept) + ("\n" if kept else ""))
        tmp.write_text(body, encoding="utf-8")
        tmp.replace(path)
        return (removed, len(rows))
    except Exception:
        return (0, len(rows))


def _auto_prune_runtime_logs(config: "Config") -> Dict[str, tuple[int, int]]:
    try:
        retention_days = int(float(os.getenv("RUNTIME_LOG_RETENTION_DAYS", "5")))
    except Exception:
        retention_days = 5
    retention_days = max(1, retention_days)

    bot_log_path = Path(config.BOT_LOG_PATH)
    if not bot_log_path.is_absolute():
        bot_log_path = APP_BASE_DIR / bot_log_path
    audit_log_path = Path(config.AUDIT_LOG_PATH)
    if not audit_log_path.is_absolute():
        audit_log_path = APP_BASE_DIR / audit_log_path

    return {
        "bot_log": _prune_text_log_by_days(bot_log_path, retention_days),
        "audit_log": _prune_jsonl_by_days(audit_log_path, retention_days),
    }


def _rotate_runtime_file(path: Path, *, max_bytes: int, keep_archives: int = 25) -> bool:
    if (not path.exists()) or (not path.is_file()):
        return False
    try:
        if path.stat().st_size <= max_bytes:
            return False
        archive_dir = path.parent / "archive"
        archive_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        archived = archive_dir / f"{path.stem}_{ts}{path.suffix}"
        path.replace(archived)
        archived_files = sorted(
            archive_dir.glob(f"{path.stem}_*{path.suffix}"),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        for old in archived_files[max(1, int(keep_archives)):]:
            try:
                old.unlink()
            except Exception:
                pass
        return True
    except Exception:
        return False


def _symbol_key(value: object) -> str:
    raw = str(value or "").strip().upper()
    if not raw:
        return ""
    if ":" in raw:
        raw = raw.split(":", 1)[0]
    return raw.replace("/", "")


def _force_utf8_stdio() -> None:
    """Force UTF-8 stdio on Windows to avoid Cyrillic mojibake in logs/UI pipes."""
    if os.name == "nt":
        try:
            import ctypes

            ctypes.windll.kernel32.SetConsoleOutputCP(65001)
            ctypes.windll.kernel32.SetConsoleCP(65001)
        except Exception:
            pass
    for stream_name in ("stdout", "stderr"):
        stream = getattr(sys, stream_name, None)
        if stream is None:
            continue
        try:
            # Python 3.7+: reconfigure TextIO wrapper in-place.
            stream.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]
        except Exception:
            pass
    os.environ["PYTHONIOENCODING"] = "utf-8"
    os.environ["PYTHONUTF8"] = "1"
    os.environ["PYTHONLEGACYWINDOWSSTDIO"] = "0"


class _WindowsExecutionKeeper:
    """
    Keep system awake while bot runs in background on Windows.
    This prevents silent cycle pauses when user switches to other apps.
    """

    ES_SYSTEM_REQUIRED = 0x00000001
    ES_CONTINUOUS = 0x80000000
    ES_AWAYMODE_REQUIRED = 0x00000040

    def __init__(self, enabled: bool, use_away_mode: bool = True) -> None:
        self.enabled = bool(enabled) and (os.name == "nt")
        self.use_away_mode = bool(use_away_mode)
        self._active = False

    def start(self) -> bool:
        if not self.enabled:
            return False
        try:
            import ctypes  # local import to avoid non-Windows issues

            flags = self.ES_CONTINUOUS | self.ES_SYSTEM_REQUIRED
            if self.use_away_mode:
                flags |= self.ES_AWAYMODE_REQUIRED
            prev = ctypes.windll.kernel32.SetThreadExecutionState(flags)
            self._active = bool(prev)
            return self._active
        except Exception:
            self._active = False
            return False

    def stop(self) -> None:
        if not self.enabled:
            return
        try:
            import ctypes

            ctypes.windll.kernel32.SetThreadExecutionState(self.ES_CONTINUOUS)
        except Exception:
            pass
        finally:
            self._active = False


def _repair_mojibake_ru(text: object) -> str:
    """
    Best-effort fix for common UTF-8/CP1251 mojibake like 'РџСЂРё...'.
    Keeps original text on any failure.
    """
    raw = str(text or "")
    if not raw:
        return raw
    # Repair only when text matches typical mojibake markers.
    mojibake_markers = (
        "Р°", "Рё", "Рѕ", "Рµ", "РЅ", "Рї", "Рґ", "Р»", "Рє",
        "С‚", "СЏ", "С…", "Сѓ", "С€", "СЂ", "СЃ", "СЌ",
        "Ð", "Ñ",
    )
    if not any(marker in raw for marker in mojibake_markers):
        return raw
    def _looks_broken(value: str) -> bool:
        return bool(value) and (("Р" in value and "С" in value) or ("Ð" in value and "Ñ" in value))
    try:
        repaired = raw.encode("cp1251", errors="strict").decode("utf-8", errors="strict")
        if repaired and not _looks_broken(repaired):
            return repaired
    except Exception:
        pass
    try:
        repaired2 = raw.encode("latin1", errors="strict").decode("utf-8", errors="strict")
        if repaired2 and not _looks_broken(repaired2):
            return repaired2
    except Exception:
        pass
    try:
        repaired3 = raw.encode("cp866", errors="strict").decode("utf-8", errors="strict")
        if repaired3 and not _looks_broken(repaired3):
            return repaired3
    except Exception:
        pass
    return raw


def _is_mojibake_ru(text: object) -> bool:
    raw = str(text or "")
    if not raw:
        return False
    return ("Р" in raw and "С" in raw) or ("Ð" in raw and "Ñ" in raw)


def _repair_payload_text_ru(value: object) -> object:
    if isinstance(value, str):
        return _repair_mojibake_ru(value)
    if isinstance(value, list):
        return [_repair_payload_text_ru(v) for v in value]
    if isinstance(value, tuple):
        return tuple(_repair_payload_text_ru(v) for v in value)
    if isinstance(value, dict):
        return {k: _repair_payload_text_ru(v) for k, v in value.items()}
    return value


class _MojibakeLogFilter(logging.Filter):
    """Best-effort runtime repair for garbled Cyrillic in log records."""

    def filter(self, record: logging.LogRecord) -> bool:
        try:
            if isinstance(record.msg, str):
                record.msg = _repair_mojibake_ru(record.msg)
            if getattr(record, "args", None):
                fixed_args = []
                for a in record.args:
                    fixed_args.append(_repair_mojibake_ru(a) if isinstance(a, str) else a)
                record.args = tuple(fixed_args)
        except Exception:
            pass
        return True


def _normalize_runtime_log_mojibake(path: Path, max_bytes: int = 8 * 1024 * 1024) -> tuple[int, int]:
    """Best-effort one-shot cleanup of already garbled lines in runtime text logs."""
    if (not path.exists()) or (not path.is_file()):
        return (0, 0)
    try:
        if path.stat().st_size > max_bytes:
            return (0, 0)
        original = path.read_text(encoding="utf-8-sig", errors="replace")
    except Exception:
        return (0, 0)
    lines = original.splitlines()
    if not lines:
        return (0, 0)
    changed = 0
    repaired_lines: list[str] = []
    for ln in lines:
        fixed = _repair_mojibake_ru(ln)
        if fixed != ln:
            changed += 1
        repaired_lines.append(fixed)
    if changed <= 0:
        return (0, len(lines))
    try:
        tmp = path.with_suffix(path.suffix + ".utf8fix.tmp")
        body = "\n".join(repaired_lines) + "\n"
        tmp.write_text(body, encoding="utf-8")
        tmp.replace(path)
        return (changed, len(lines))
    except Exception:
        return (0, len(lines))


def _pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    if pid == os.getpid():
        # Avoid self-signal edge cases on some Windows shells/runners.
        return True
    try:
        os.kill(pid, 0)
        return True
    except Exception:
        return False


class _JsonFileLock:
    """Best-effort cross-platform lock file for short JSON writes."""
    def __init__(self, lock_path: Path, timeout_sec: float = 1.5, stale_sec: float = 30.0):
        self.lock_path = lock_path
        self.timeout_sec = max(0.1, float(timeout_sec))
        self.stale_sec = max(5.0, float(stale_sec))
        self.owner_pid = os.getpid()
        self._acquired = False

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, exc_type, exc, tb):
        self.release()
        return False

    def acquire(self) -> bool:
        deadline = time.time() + self.timeout_sec
        payload = {
            "pid": self.owner_pid,
            "host": socket.gethostname(),
            "ts_utc": datetime.now(timezone.utc).isoformat(),
        }
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        while time.time() <= deadline:
            try:
                fd = os.open(str(self.lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                with os.fdopen(fd, "w", encoding="utf-8") as f:
                    f.write(json.dumps(payload, ensure_ascii=False))
                self._acquired = True
                return True
            except FileExistsError:
                if self._try_break_stale_lock():
                    continue
                time.sleep(0.05)
            except Exception:
                break
        return False

    def release(self) -> None:
        if not self._acquired:
            return
        try:
            self.lock_path.unlink(missing_ok=True)
        except Exception:
            pass
        self._acquired = False

    def _try_break_stale_lock(self) -> bool:
        try:
            raw = self.lock_path.read_text(encoding="utf-8-sig")
            data = json.loads(raw) if raw.strip() else {}
            pid = int(_safe_float(data.get("pid"), 0.0))
            ts = _iso_to_ts(data.get("ts_utc"))
        except Exception:
            pid = 0
            ts = 0.0
        age = max(0.0, time.time() - ts) if ts > 0 else self.stale_sec + 1.0
        if age > self.stale_sec or not _pid_alive(pid):
            try:
                self.lock_path.unlink(missing_ok=True)
                return True
            except Exception:
                return False
        return False


class _ProcessPidLock:
    """Single-instance process lock backed by a pid file."""

    def __init__(self, lock_path: Path):
        self.lock_path = lock_path
        self._acquired = False
        self._owner_pid = os.getpid()

    def acquire(self) -> bool:
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "pid": self._owner_pid,
            "host": socket.gethostname(),
            "started_utc": datetime.now(timezone.utc).isoformat(),
        }
        try:
            fd = os.open(str(self.lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                f.write(json.dumps(payload, ensure_ascii=False))
            self._acquired = True
            return True
        except FileExistsError:
            try:
                raw = self.lock_path.read_text(encoding="utf-8-sig")
                data = json.loads(raw) if raw.strip() else {}
                pid = int(_safe_float(data.get("pid"), 0.0))
                if pid > 0 and _pid_alive(pid):
                    return False
                # stale pid file
                self.lock_path.unlink(missing_ok=True)
            except Exception:
                return False
            # One retry after stale cleanup
            try:
                fd = os.open(str(self.lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                with os.fdopen(fd, "w", encoding="utf-8") as f:
                    f.write(json.dumps(payload, ensure_ascii=False))
                self._acquired = True
                return True
            except Exception:
                return False
        except Exception:
            return False

    def release(self) -> None:
        if not self._acquired:
            return
        try:
            raw = self.lock_path.read_text(encoding="utf-8-sig")
            data = json.loads(raw) if raw.strip() else {}
            pid = int(_safe_float(data.get("pid"), 0.0))
            if pid and pid != self._owner_pid:
                return
        except BaseException:
            pass
        try:
            self.lock_path.unlink(missing_ok=True)
        except BaseException:
            pass
        self._acquired = False


def _write_json_atomic(path: Path, payload: dict[str, object], backup_path: Optional[Path] = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(payload, ensure_ascii=False, indent=2)
    replaced = False
    last_exc: Exception | None = None
    # Use unique temp names to avoid collisions/locks on fixed *.tmp in tight loops.
    # Windows can temporarily deny write/replace when AV/indexer touches files.
    tmp_created: Path | None = None
    for idx, delay in enumerate((0.0, 0.03, 0.08, 0.15, 0.25)):
        if delay > 0:
            time.sleep(delay)
        tmp = path.with_suffix(path.suffix + f".tmp.{os.getpid()}.{threading.get_ident()}.{idx}")
        try:
            tmp.write_text(text, encoding="utf-8")
            tmp_created = tmp
            tmp.replace(path)
            replaced = True
            break
        except Exception as exc:
            last_exc = exc
            try:
                tmp.unlink(missing_ok=True)
            except Exception:
                pass
    if not replaced:
        # Fallback: direct write keeps status fresh even if atomic swap failed.
        for delay in (0.0, 0.05, 0.12):
            if delay > 0:
                time.sleep(delay)
            try:
                path.write_text(text, encoding="utf-8")
                replaced = True
                break
            except Exception as exc:
                last_exc = exc
        if tmp_created is not None:
            try:
                tmp_created.unlink(missing_ok=True)
            except Exception:
                pass
        if last_exc is not None and not replaced:
            raise last_exc
        if last_exc is not None:
            logging.debug("Atomic replace fallback used for %s: %s", path, last_exc)
    if backup_path is not None:
        backup_path.parent.mkdir(parents=True, exist_ok=True)
        bak_tmp = backup_path.with_suffix(backup_path.suffix + f".tmp.{os.getpid()}.{threading.get_ident()}")
        try:
            bak_tmp.write_text(text, encoding="utf-8")
            bak_tmp.replace(backup_path)
        except Exception:
            backup_path.write_text(text, encoding="utf-8")
            try:
                bak_tmp.unlink(missing_ok=True)
            except Exception:
                pass


class _MexcPublicWsFeed:
    """
    Best-effort public websocket feed for low-latency prices.
    Falls back silently when endpoint/network is unavailable.
    """

    def __init__(self, enabled: bool, ws_url: str, max_age_sec: float = 1.5) -> None:
        self.enabled = bool(enabled) and aiohttp is not None
        self.ws_url = str(ws_url or "wss://wbs.mexc.com/ws").strip()
        self.max_age_sec = max(0.2, float(max_age_sec))
        self._symbols: set[str] = set()
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._prices: Dict[str, Tuple[float, float]] = {}

    @staticmethod
    def _norm_symbol(symbol: str) -> str:
        raw = str(symbol or "").strip().upper()
        if ":" in raw:
            raw = raw.split(":", 1)[0]
        return raw.replace("/", "")

    def ensure_symbol(self, symbol: str) -> None:
        if not self.enabled:
            return
        sym = self._norm_symbol(symbol)
        if not sym:
            return
        with self._lock:
            self._symbols.add(sym)
        self._start_if_needed()

    def last_price(self, symbol: str, max_age_sec: Optional[float] = None) -> float:
        sym = self._norm_symbol(symbol)
        if not sym:
            return 0.0
        age_limit = self.max_age_sec if max_age_sec is None else max(0.05, float(max_age_sec))
        now = time.time()
        with self._lock:
            item = self._prices.get(sym)
        if not item:
            return 0.0
        px, ts = item
        if px <= 0:
            return 0.0
        if (now - ts) > age_limit:
            return 0.0
        return float(px)

    def stop(self) -> None:
        self._stop.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=1.2)

    def _start_if_needed(self) -> None:
        if not self.enabled:
            return
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._thread_main, daemon=True, name="mexc-public-ws")
        self._thread.start()

    def _thread_main(self) -> None:
        try:
            asyncio.run(self._run_loop())
        except Exception as exc:
            logging.debug("WS loop terminated: %s", exc)

    async def _run_loop(self) -> None:
        if aiohttp is None:
            return
        backoff = 1.0
        while not self._stop.is_set():
            try:
                await self._run_once()
                backoff = 1.0
            except Exception as exc:
                logging.debug("WS reconnect: %s", exc)
                await asyncio.sleep(min(8.0, backoff))
                backoff = min(8.0, backoff * 1.6)

    async def _run_once(self) -> None:
        if aiohttp is None:
            return
        timeout = aiohttp.ClientTimeout(total=None, sock_connect=6.0, sock_read=20.0)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.ws_connect(self.ws_url, heartbeat=15.0, autoping=True) as ws:
                subs_sent: set[str] = set()
                while not self._stop.is_set():
                    with self._lock:
                        syms = set(self._symbols)
                    new_syms = [s for s in syms if s not in subs_sent]
                    if new_syms:
                        await self._send_sub(ws, new_syms)
                        subs_sent.update(new_syms)
                    try:
                        msg = await ws.receive(timeout=4.0)
                    except asyncio.TimeoutError:
                        try:
                            await ws.ping()
                        except Exception:
                            break
                        continue
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        self._parse_text(msg.data)
                    elif msg.type in {aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.CLOSING, aiohttp.WSMsgType.ERROR}:
                        break

    async def _send_sub(self, ws: "aiohttp.ClientWebSocketResponse", symbols: list[str]) -> None:
        # MEXC public WS (spot v3) channels. We subscribe to multiple lightweight channels.
        params: list[str] = []
        for sym in symbols:
            params.extend(
                [
                    f"spot@public.bookTicker.v3.api@{sym}",
                    f"spot@public.deals.v3.api@{sym}",
                    f"spot@public.ticker.v3.api@{sym}",
                ]
            )
        payload = {"method": "SUBSCRIPTION", "params": params, "id": int(time.time() * 1000) % 1_000_000}
        await ws.send_json(payload)

    def _parse_text(self, raw: str) -> None:
        try:
            data = json.loads(raw)
        except Exception:
            return

        def _update(sym_raw: object, price_raw: object) -> None:
            sym = self._norm_symbol(str(sym_raw or ""))
            px = _safe_float(price_raw, 0.0)
            if not sym or px <= 0:
                return
            with self._lock:
                self._prices[sym] = (px, time.time())

        if isinstance(data, dict):
            d = data.get("d")
            payloads = []
            if isinstance(d, dict):
                payloads.append(d)
            if isinstance(d, list):
                payloads.extend([x for x in d if isinstance(x, dict)])
            if isinstance(data.get("data"), dict):
                payloads.append(data.get("data"))
            if not payloads:
                payloads.append(data)
            for item in payloads:
                if not isinstance(item, dict):
                    continue
                sym = item.get("s") or item.get("symbol") or item.get("symbolName")
                price = (
                    item.get("c")
                    or item.get("p")
                    or item.get("price")
                    or item.get("lastPrice")
                    or item.get("last")
                    or item.get("a")
                    or item.get("b")
                )
                _update(sym, price)


class Config:
    """Encapsulates all bot configuration."""

    def __init__(self) -> None:
        self.BOT_MODE = os.getenv("BOT_MODE", "").strip().lower()
        self.TRADE_MARKET = str(os.getenv("TRADE_MARKET", "spot") or "spot").strip().lower()
        if self.TRADE_MARKET not in {"spot", "futures"}:
            self.TRADE_MARKET = "spot"
        self.FUTURES_LEVERAGE = int(_safe_float(os.getenv("FUTURES_LEVERAGE", "2"), 2.0))
        self.FUTURES_LEVERAGE = max(1, min(20, self.FUTURES_LEVERAGE))
        self.FUTURES_MARGIN_MODE = str(os.getenv("FUTURES_MARGIN_MODE", "cross") or "cross").strip().lower()
        if self.FUTURES_MARGIN_MODE not in {"cross", "isolated"}:
            self.FUTURES_MARGIN_MODE = "cross"
        # Spot-only config
        self.AUTO_PILOT_MODE = _env_bool_raw("AUTO_PILOT_MODE", default=True)
        self.SYMBOL = os.getenv("SPOT_SYMBOL", "SOL/USDT")
        if self.TRADE_MARKET == "futures":
            self.SYMBOL = os.getenv("FUTURES_SYMBOL", self.SYMBOL)
        self.SYMBOL = str(self.SYMBOL or "SOL/USDT").strip().upper()
        if self.SYMBOL == "BTC/USDT":
            self.SYMBOL = "SOL/USDT"
        self.AUTO_SYMBOL_SELECTION = _env_bool_raw("AUTO_SYMBOL_SELECTION", default=False)
        self.SYMBOL_SCAN_LIMIT = int(os.getenv("SYMBOL_SCAN_LIMIT", "20"))
        self.MIN_QUOTE_VOLUME_USDT = float(os.getenv("MIN_QUOTE_VOLUME_USDT", "3000000"))
        stable_symbols_raw = os.getenv(
            "PREFERRED_SPOT_SYMBOLS",
            "ETH/USDT,BNB/USDT,SOL/USDT,XRP/USDT,ADA/USDT,LTC/USDT,TRX/USDT,LINK/USDT,DOT/USDT",
        )
        raw_preferred = [s.strip().upper() for s in stable_symbols_raw.split(",") if s.strip()]
        self.PREFERRED_SPOT_SYMBOLS = []
        for sym in raw_preferred:
            if sym == "BTC/USDT":
                continue
            if sym not in self.PREFERRED_SPOT_SYMBOLS:
                self.PREFERRED_SPOT_SYMBOLS.append(sym)
        if not self.PREFERRED_SPOT_SYMBOLS:
            self.PREFERRED_SPOT_SYMBOLS = [
                "ETH/USDT",
                "BNB/USDT",
                "SOL/USDT",
                "XRP/USDT",
                "ADA/USDT",
                "LTC/USDT",
                "TRX/USDT",
                "LINK/USDT",
                "DOT/USDT",
            ]

        # AI config
        self.USE_AI_SIGNAL = _env_bool_raw("USE_AI_SIGNAL", default=True)
        self.AI_TRAINING_MODE = _env_bool_raw("AI_TRAINING_MODE", default=False)
        self.AI_USE_INTERNET_DATA = _env_bool_raw("AI_USE_INTERNET_DATA", default=True)
        self.AI_USE_OKX_MARKET_DATA = _env_bool_raw("AI_USE_OKX_MARKET_DATA", default=True)
        self.AI_USE_GLOBAL_MARKET_DATA = _env_bool_raw("AI_USE_GLOBAL_MARKET_DATA", default=True)
        self.AI_LOOKBACK = int(os.getenv("AI_LOOKBACK", "80"))
        self.AI_MIN_CONFIDENCE = float(os.getenv("AI_MIN_CONFIDENCE", "0.60"))
        self.AI_MODEL_PATH = os.getenv("AI_MODEL_PATH", "logs/training/ai_model.json")
        self.AI_TRAINING_LOG_PATH = os.getenv("AI_TRAINING_LOG_PATH", "logs/training/ai_training_log.csv")
        self.AI_STATUS_PATH = os.getenv("AI_STATUS_PATH", "logs/training/ai_status.json")
        self.AI_ADAPTIVE_STATE_PATH = os.getenv("AI_ADAPTIVE_STATE_PATH", "logs/training/adaptive_state.json")
        self.AI_ADAPTIVE_TRADES_PATH = os.getenv("AI_ADAPTIVE_TRADES_PATH", "logs/training/adaptive_trades.jsonl")
        self.AI_MODEL_REGISTRY_PATH = os.getenv("AI_MODEL_REGISTRY_PATH", "logs/training/model_registry.json")
        self.BOT_TELEMETRY_PATH = os.getenv("BOT_TELEMETRY_PATH", "logs/runtime/bot_telemetry.json")
        self.BOT_LIVE_STATUS_PATH = os.getenv("BOT_LIVE_STATUS_PATH", "logs/runtime/bot_live_status.json")
        self.BOT_LOG_PATH = os.getenv("BOT_LOG_PATH", "logs/runtime/mexc_bot.log")
        self.AUDIT_LOG_PATH = os.getenv("AUDIT_LOG_PATH", "logs/runtime/trade_audit.jsonl")
        self.POSITION_STATE_PATH = os.getenv("POSITION_STATE_PATH", "logs/runtime/position_state.json")
        self.BOT_PROCESS_LOCK_PATH = os.getenv("BOT_PROCESS_LOCK_PATH", "logs/runtime/bot_process.lock")
        self.ENGINE_BACKEND = str(os.getenv("ENGINE_BACKEND", "python") or "python").strip().lower()
        self.RUST_ENGINE_URL = str(os.getenv("RUST_ENGINE_URL", "http://127.0.0.1:17890") or "").strip()
        self.ENGINE_STRICT = _env_bool_raw("ENGINE_STRICT", default=False)
        self.AUTO_STRICT_FALLBACK_IN_LIVE = _env_bool_raw("AUTO_STRICT_FALLBACK_IN_LIVE", default=True)
        self.LIVE_STRICT_REARM_SEC = float(os.getenv("LIVE_STRICT_REARM_SEC", "90.0"))
        self.ENGINE_HTTP_TIMEOUT_SEC = float(os.getenv("ENGINE_HTTP_TIMEOUT_SEC", "0.45"))
        self.ENGINE_EXECUTION_GATEWAY = _env_bool_raw("ENGINE_EXECUTION_GATEWAY", default=False)
        self.ENGINE_ORDER_TIMEOUT_SEC = float(os.getenv("ENGINE_ORDER_TIMEOUT_SEC", "0.70"))
        self.ENGINE_SNAPSHOT_MAX_AGE_SEC = float(os.getenv("ENGINE_SNAPSHOT_MAX_AGE_SEC", "20.0"))
        self.ADVISORY_PROVIDER_ENABLED = _env_bool_raw("ADVISORY_PROVIDER_ENABLED", default=False)
        self.ADVISORY_PROVIDER_NAME = str(os.getenv("ADVISORY_PROVIDER_NAME", "re7labs") or "re7labs").strip()
        self.ADVISORY_PROVIDER_URL = str(os.getenv("ADVISORY_PROVIDER_URL", "") or "").strip()
        self.ADVISORY_PROVIDER_TIMEOUT_SEC = float(os.getenv("ADVISORY_PROVIDER_TIMEOUT_SEC", "0.35"))
        self.ADVISORY_PROVIDER_TTL_SEC = float(os.getenv("ADVISORY_PROVIDER_TTL_SEC", "10.0"))
        self.ADVISORY_PROVIDER_WEIGHT = float(os.getenv("ADVISORY_PROVIDER_WEIGHT", "0.20"))
        self.ADVISORY_PROVIDER_WEIGHT_TRAINING = float(
            os.getenv("ADVISORY_PROVIDER_WEIGHT_TRAINING", str(self.ADVISORY_PROVIDER_WEIGHT))
        )
        self.ADVISORY_PROVIDER_WEIGHT_LIVE = float(
            os.getenv("ADVISORY_PROVIDER_WEIGHT_LIVE", str(self.ADVISORY_PROVIDER_WEIGHT))
        )
        self.EXCHANGE_HTTP_TIMEOUT_MS = int(os.getenv("EXCHANGE_HTTP_TIMEOUT_MS", "2500"))
        self.AI_LEARNING_RATE = float(os.getenv("AI_LEARNING_RATE", "0.08"))
        self.AI_HORIZON_CANDLES = int(os.getenv("AI_HORIZON_CANDLES", "3"))
        self.AI_LABEL_THRESHOLD = float(os.getenv("AI_LABEL_THRESHOLD", "0.0015"))
        self.AI_HEALTH_MIN_WF_HIT_RATE = float(os.getenv("AI_HEALTH_MIN_WF_HIT_RATE", "0.53"))
        self.AI_UNCERTAINTY_HIGH = float(os.getenv("AI_UNCERTAINTY_HIGH", "0.65"))
        self.AI_RISK_DOWNSHIFT_FACTOR = float(os.getenv("AI_RISK_DOWNSHIFT_FACTOR", "0.55"))
        self.AI_ENTRY_MIN_QUALITY = float(os.getenv("AI_ENTRY_MIN_QUALITY", "0.55"))
        self.AI_ENTRY_SOFT_QUALITY = float(os.getenv("AI_ENTRY_SOFT_QUALITY", "0.70"))
        self.AI_LOW_QUALITY_RISK_FACTOR = float(os.getenv("AI_LOW_QUALITY_RISK_FACTOR", "0.65"))
        self.AI_FEE_BPS = float(os.getenv("AI_FEE_BPS", "10.0"))
        self.AI_SLIPPAGE_BPS = float(os.getenv("AI_SLIPPAGE_BPS", "6.0"))
        self.MAX_ENTRY_SLIPPAGE_BPS = float(os.getenv("MAX_ENTRY_SLIPPAGE_BPS", "22.0"))
        self.MAX_DATA_STALE_SEC = int(os.getenv("MAX_DATA_STALE_SEC", "180"))
        self.PAIR_QUARANTINE_LOSS_STREAK = int(os.getenv("PAIR_QUARANTINE_LOSS_STREAK", "3"))
        self.PAIR_QUARANTINE_MINUTES = int(os.getenv("PAIR_QUARANTINE_MINUTES", "120"))
        self.SIDE_STOPLOSS_STREAK_GUARD = int(os.getenv("SIDE_STOPLOSS_STREAK_GUARD", "4"))

        # Risk / execution config
        self.DYNAMIC_POSITION_SIZE = _env_bool_raw("DYNAMIC_POSITION_SIZE", default=True)
        self.SPOT_BALANCE_RISK_FRACTION = float(os.getenv("SPOT_BALANCE_RISK_FRACTION", "0.5"))
        self.SPOT_MIN_BUY_USDT = float(os.getenv("SPOT_MIN_BUY_USDT", "1.0"))
        self.SPOT_MAX_BUY_USDT = float(os.getenv("SPOT_MAX_BUY_USDT", "50.0"))
        self.SPOT_USE_FULL_BALANCE = _env_bool_raw("SPOT_USE_FULL_BALANCE", default=True)
        self.SPOT_DUST_RECOVERY_BUY = _env_bool_raw("SPOT_DUST_RECOVERY_BUY", default=True)
        self.SPOT_STOP_LOSS_PCT = float(os.getenv("SPOT_STOP_LOSS_PCT", "0.012"))
        self.SPOT_TAKE_PROFIT_PCT = float(os.getenv("SPOT_TAKE_PROFIT_PCT", "0.018"))
        self.SPOT_TRAILING_STOP_PCT = float(os.getenv("SPOT_TRAILING_STOP_PCT", "0.009"))
        self.POSITION_TIME_STOP_CANDLES = int(os.getenv("POSITION_TIME_STOP_CANDLES", "24"))
        self.POSITION_STALL_CANDLES = int(os.getenv("POSITION_STALL_CANDLES", "10"))
        self.POSITION_STALL_PULLBACK_PCT = float(os.getenv("POSITION_STALL_PULLBACK_PCT", "0.003"))
        self.POSITION_STALL_MIN_PNL_PCT = float(os.getenv("POSITION_STALL_MIN_PNL_PCT", "0.003"))
        self.TIME_STOP_MIN_CANDLES = int(os.getenv("TIME_STOP_MIN_CANDLES", "15"))
        self.TIME_STOP_MAX_CANDLES = int(os.getenv("TIME_STOP_MAX_CANDLES", "180"))
        self.TIME_STOP_VOL_REF = float(os.getenv("TIME_STOP_VOL_REF", "0.007"))
        self.TIME_STOP_AFTER_EXIT_BLOCK_CANDLES = int(os.getenv("TIME_STOP_AFTER_EXIT_BLOCK_CANDLES", "30"))
        self.AI_EXIT_QUALITY_DROP = float(os.getenv("AI_EXIT_QUALITY_DROP", "0.16"))
        self.MIN_EXPECTED_EDGE_PCT = float(os.getenv("MIN_EXPECTED_EDGE_PCT", "0.003"))
        self.TRAILING_ACTIVATION_PCT = float(os.getenv("TRAILING_ACTIVATION_PCT", "0.003"))
        self.AI_AUTO_RISK = _env_bool_raw("AI_AUTO_RISK", default=True)
        self.AI_RISK_FRACTION_MIN = float(os.getenv("AI_RISK_FRACTION_MIN", "0.12"))
        self.AI_RISK_FRACTION_MAX = float(os.getenv("AI_RISK_FRACTION_MAX", "0.55"))
        self.AI_STOP_LOSS_MIN = float(os.getenv("AI_STOP_LOSS_MIN", "0.006"))
        self.AI_STOP_LOSS_MAX = float(os.getenv("AI_STOP_LOSS_MAX", "0.020"))
        self.AI_TAKE_PROFIT_MIN = float(os.getenv("AI_TAKE_PROFIT_MIN", "0.010"))
        self.AI_TAKE_PROFIT_MAX = float(os.getenv("AI_TAKE_PROFIT_MAX", "0.040"))
        self.AI_TRAILING_STOP_MIN = float(os.getenv("AI_TRAILING_STOP_MIN", "0.004"))
        self.AI_TRAILING_STOP_MAX = float(os.getenv("AI_TRAILING_STOP_MAX", "0.020"))
        self.AI_ATR_STOP_MULT = float(os.getenv("AI_ATR_STOP_MULT", "1.0"))
        self.AI_ATR_TAKE_PROFIT_MULT = float(os.getenv("AI_ATR_TAKE_PROFIT_MULT", "1.8"))
        self.AI_ATR_TRAILING_MULT = float(os.getenv("AI_ATR_TRAILING_MULT", "1.1"))
        self.AI_MIN_RR = float(os.getenv("AI_MIN_RR", "1.5"))
        self.RECV_WINDOW_MS = int(os.getenv("RECV_WINDOW_MS", "60000"))
        self.ORDER_CONFIRM_TIMEOUT_SEC = float(os.getenv("ORDER_CONFIRM_TIMEOUT_SEC", "3"))
        self.ORDER_CONFIRM_RETRIES = int(os.getenv("ORDER_CONFIRM_RETRIES", "3"))
        self.ORDER_CONFIRM_POLL_SEC = float(os.getenv("ORDER_CONFIRM_POLL_SEC", "0.5"))
        self.ORDER_CONFIRM_TOTAL_TIMEOUT_SEC = float(os.getenv("ORDER_CONFIRM_TOTAL_TIMEOUT_SEC", "6.0"))
        self.POSITION_RECONCILE_ON_START = _env_bool_raw("POSITION_RECONCILE_ON_START", default=True)
        self.RISK_GUARD_ENABLED = _env_bool_raw("RISK_GUARD_ENABLED", default=True)
        self.DAILY_MAX_DRAWDOWN_PCT = float(os.getenv("DAILY_MAX_DRAWDOWN_PCT", "0.08"))
        self.DAILY_MAX_DRAWDOWN_USDT = float(os.getenv("DAILY_MAX_DRAWDOWN_USDT", "25.0"))
        self.MAX_TRADES_PER_DAY = int(os.getenv("MAX_TRADES_PER_DAY", "60"))
        self.MAX_MARKET_VOLATILITY = float(os.getenv("MAX_MARKET_VOLATILITY", "0.03"))
        self.MAX_PRICE_JUMP_PCT = float(os.getenv("MAX_PRICE_JUMP_PCT", "0.10"))
        self.ANOMALY_PAUSE_MINUTES = int(os.getenv("ANOMALY_PAUSE_MINUTES", "20"))
        self.MAX_CYCLE_ERRORS = int(os.getenv("MAX_CYCLE_ERRORS", "4"))
        self.API_SOFT_GUARD_WINDOW_SEC = int(os.getenv("API_SOFT_GUARD_WINDOW_SEC", "180"))
        self.API_SOFT_GUARD_MAX_ERRORS = int(os.getenv("API_SOFT_GUARD_MAX_ERRORS", "5"))
        self.API_SOFT_GUARD_PAUSE_SEC = int(os.getenv("API_SOFT_GUARD_PAUSE_SEC", "300"))
        self.RISK_GUARD_STATE_PATH = os.getenv("RISK_GUARD_STATE_PATH", "logs/runtime/risk_guard_state.json")

        # Strategy config
        self.TIMEFRAME = "1m"
        self.SMA_PERIOD = 20
        self.THRESHOLD = float(os.getenv("THRESHOLD", "0.002"))
        self.SLEEP_SECONDS = int(os.getenv("SLEEP_SECONDS", "30"))
        self.TRADE_COOLDOWN_SECONDS = int(os.getenv("TRADE_COOLDOWN_SECONDS", "120"))
        self.LIVE_FAST_LOOP = _env_bool_raw("LIVE_FAST_LOOP", default=True)
        self.LIVE_ACTIVE_SLEEP_SEC = float(os.getenv("LIVE_ACTIVE_SLEEP_SEC", "1.2"))
        self.LIVE_IDLE_SLEEP_SEC = float(os.getenv("LIVE_IDLE_SLEEP_SEC", "3.0"))
        self.LIVE_HEARTBEAT_SEC = float(os.getenv("LIVE_HEARTBEAT_SEC", "1.0"))
        self.LOW_LATENCY_MODE = _env_bool_raw("LOW_LATENCY_MODE", default=True)
        self.USE_WS_MARKET_DATA = _env_bool_raw("USE_WS_MARKET_DATA", default=True)
        self.WS_MAX_AGE_SEC = float(os.getenv("WS_MAX_AGE_SEC", "1.5"))
        self.MEXC_WS_PUBLIC_URL = str(os.getenv("MEXC_WS_PUBLIC_URL", "wss://wbs.mexc.com/ws") or "wss://wbs.mexc.com/ws").strip()
        self.CYCLE_IO_TIMEOUT_SEC = float(os.getenv("CYCLE_IO_TIMEOUT_SEC", "2.8"))
        self.OHLCV_REFRESH_SEC = float(os.getenv("OHLCV_REFRESH_SEC", "2.2"))
        self.PROFILE_CYCLE_STAGES = _env_bool_raw("PROFILE_CYCLE_STAGES", default=True)
        self.SYMBOL_RESCAN_COOLDOWN_SEC = float(os.getenv("SYMBOL_RESCAN_COOLDOWN_SEC", "9.0"))
        self.BACKGROUND_KEEP_AWAKE = _env_bool_raw("BACKGROUND_KEEP_AWAKE", default=True)
        self.BACKGROUND_KEEP_AWAKE_AWAYMODE = _env_bool_raw("BACKGROUND_KEEP_AWAKE_AWAYMODE", default=True)
        self.DRY_RUN = _env_bool_raw("DRY_RUN", default=True)
        self.PAPER_START_USDT = float(os.getenv("PAPER_START_USDT", "0.0"))
        self.LIVE_ACTIVE_SLEEP_SEC = _clamp(self.LIVE_ACTIVE_SLEEP_SEC, 0.8, 30.0)
        self.LIVE_IDLE_SLEEP_SEC = _clamp(self.LIVE_IDLE_SLEEP_SEC, 1.0, 60.0)
        self.LIVE_HEARTBEAT_SEC = _clamp(self.LIVE_HEARTBEAT_SEC, 0.8, 15.0)
        self._apply_mode_overrides()
        if self.AUTO_PILOT_MODE:
            self._apply_autopilot_defaults()

    def _apply_mode_overrides(self) -> None:
        if self.BOT_MODE == "training":
            self.DRY_RUN = True
            self.AI_TRAINING_MODE = True
            self.AUTO_PILOT_MODE = True
        elif self.BOT_MODE == "live":
            self.DRY_RUN = False
            self.AI_TRAINING_MODE = False
            self.AUTO_PILOT_MODE = True

    def _apply_autopilot_defaults(self) -> None:
        # Keep bot self-managed for users who do not tune advanced parameters.
        self.AUTO_SYMBOL_SELECTION = True
        self.DYNAMIC_POSITION_SIZE = True
        self.USE_AI_SIGNAL = True
        self.AI_AUTO_RISK = True
        self.SPOT_USE_FULL_BALANCE = True
        self.SPOT_DUST_RECOVERY_BUY = True
        self.AI_MIN_CONFIDENCE = _clamp(self.AI_MIN_CONFIDENCE, 0.55, 0.80)
        self.TRADE_COOLDOWN_SECONDS = max(60, self.TRADE_COOLDOWN_SECONDS)


class Trader:
    """Handles all communication with the exchange."""
    def __init__(self, config: Config):
        self.config = config
        self._has_api_credentials = bool(os.getenv("MEXC_API_KEY") and os.getenv("MEXC_API_SECRET"))
        self._last_balance_cache: Dict[str, object] = {}
        self._last_balance_cache_source: str = "unknown"
        self._last_balance_source: str = "unknown"
        self._ticker_cache: Dict[str, Tuple[float, float]] = {}
        self._candidate_symbols_cache: list[str] = []
        self._candidate_symbols_cache_ts: float = 0.0
        self._candidate_universe_meta: Dict[str, object] = {}
        self._markets_cache: Dict[str, object] = {}
        self._markets_cache_ts: float = 0.0
        self._markets_ttl_sec: float = 300.0
        self._tickers24_cache: Dict[str, object] = {}
        self._tickers24_cache_ts: float = 0.0
        self._tickers24_ttl_sec: float = 18.0 if bool(getattr(self.config, "LIVE_FAST_LOOP", False)) else 45.0
        self._single_ticker_rest_backoff_until_ts: Dict[str, float] = {}
        self._single_ticker_rest_fail_count: Dict[str, int] = {}
        self._tickers24_rest_backoff_until_ts: float = 0.0
        self._tickers24_rest_fail_count: int = 0
        # Keep ticker cache short so live mode tracks market moves with low lag.
        if (not self.config.DRY_RUN) and bool(getattr(self.config, "LIVE_FAST_LOOP", False)):
            active_sleep = max(0.8, float(getattr(self.config, "LIVE_ACTIVE_SLEEP_SEC", 1.2)))
            self._ticker_ttl_sec = max(0.6, min(2.0, active_sleep * 0.9))
        else:
            self._ticker_ttl_sec = max(1.0, min(10.0, float(self.config.SLEEP_SECONDS) * 0.35))
        self._ohlcv_ttl_sec = max(0.6, float(getattr(self.config, "OHLCV_REFRESH_SEC", 2.2)))
        self._api_error_events: deque[float] = deque(maxlen=512)
        self._last_ohlcv_cache: Dict[str, Tuple[float, list[list[float]]]] = {}
        self._last_position_cache: Dict[str, Dict[str, object]] = {}
        self._io_timeout_warn_ts: Dict[str, float] = {}
        self._hot_state: Dict[str, object] = {}
        self._hot_state_lock = threading.Lock()
        self.exchange = self._create_exchange()
        self.ws_feed = _MexcPublicWsFeed(
            enabled=bool(self.config.LOW_LATENCY_MODE and self.config.USE_WS_MARKET_DATA),
            ws_url=self.config.MEXC_WS_PUBLIC_URL,
            max_age_sec=self.config.WS_MAX_AGE_SEC,
        )

    def _create_exchange(self) -> MexcSync:
        api_key = os.getenv("MEXC_API_KEY")
        api_secret = os.getenv("MEXC_API_SECRET")
        default_type = "swap" if self.config.TRADE_MARKET == "futures" else "spot"
        exchange_config = {
            "enableRateLimit": True,
            "timeout": max(800, int(getattr(self.config, "EXCHANGE_HTTP_TIMEOUT_MS", 2500))),
            "options": {
                "adjustForTimeDifference": True,
                "recvWindow": self.config.RECV_WINDOW_MS,
                "defaultType": default_type,
                "defaultSubType": "linear",
            },
        }

        if api_key and api_secret:
            exchange_config["apiKey"] = api_key
            exchange_config["secret"] = api_secret
        elif not self.config.DRY_RUN:
            raise RuntimeError("Set MEXC_API_KEY and MEXC_API_SECRET.")

        exchange = MexcSync(exchange_config)
        if self.config.TRADE_MARKET == "spot":
            # The bundled MEXC adapter merges spot + swap markets in fetch_markets(),
            # which can hit contract-detail timeouts even for spot-only runs.
            exchange.fetch_markets = lambda params={}: exchange.fetch_spot_markets(params)
        if hasattr(exchange, "load_time_difference"):
            try:
                exchange.load_time_difference()
            except Exception as e:
                logging.warning("Не удалось загрузить разницу времени: %s", e)
        try:
            self._call_with_retries(
                "load_markets_init_spot" if self.config.TRADE_MARKET == "spot" else "load_markets_init",
                lambda: exchange.load_markets(),
                retries=1,
                base_sleep_sec=0.20,
                retry_timeout_sec=max(1.2, float(self.config.CYCLE_IO_TIMEOUT_SEC) * 1.35),
            )
        except Exception as exc:
            logging.warning("Стартовая загрузка рынков не удалась, продолжим с ленивой инициализацией. Ошибка: %s", exc)
        return exchange

    def _ensure_exchange_markets_loaded(self, context: str = "load_markets_lazy") -> Dict[str, object]:
        markets = getattr(self.exchange, "markets", None)
        if isinstance(markets, dict) and markets:
            return markets
        loaded = self._call_with_retries(
            context,
            lambda: self.exchange.load_markets(),
            retries=1,
            base_sleep_sec=0.20,
            retry_timeout_sec=max(1.0, float(self.config.CYCLE_IO_TIMEOUT_SEC) * 1.2),
        )
        return loaded if isinstance(loaded, dict) else {}

    def _symbol_for_market(self, symbol: str) -> str:
        sym = str(symbol or "").strip().upper()
        if self.config.TRADE_MARKET != "futures":
            return sym
        if ":" in sym:
            return sym
        if sym.endswith("/USDT"):
            return f"{sym}:USDT"
        return sym

    def _record_api_error(self, context: str, exc: Exception) -> None:
        self._api_error_events.append(time.time())
        logging.warning("API call failed (%s): %s", context, exc)

    def consume_api_error_events(self) -> list[float]:
        events = list(self._api_error_events)
        self._api_error_events.clear()
        return events

    @staticmethod
    def _is_retryable_api_error(exc: Exception) -> bool:
        text = str(exc).lower()
        retry_hints = (
            "timeout",
            "timed out",
            "network",
            "temporarily",
            "rate limit",
            "too many requests",
            "429",
            "503",
            "502",
            "connection",
        )
        return any(hint in text for hint in retry_hints)

    def _call_with_retries(
        self,
        context: str,
        fn: Callable[[], Any],
        *,
        retries: int = 2,
        base_sleep_sec: float = 0.25,
        retry_timeout_sec: float | None = None,
    ) -> Any:
        attempts = max(1, int(retries) + 1)
        last_exc: Optional[Exception] = None
        t0 = time.time()
        for attempt in range(attempts):
            try:
                return fn()
            except Exception as exc:
                self._record_api_error(context, exc)
                last_exc = exc
                if attempt >= (attempts - 1) or not self._is_retryable_api_error(exc):
                    break
                if retry_timeout_sec is not None and (time.time() - t0) >= max(0.1, float(retry_timeout_sec)):
                    break
                # small jitter prevents synchronized retry bursts.
                sleep_sec = (base_sleep_sec * (2 ** attempt)) + (0.03 * (attempt + 1))
                time.sleep(sleep_sec)
        if last_exc is not None:
            raise last_exc
        raise RuntimeError(f"API call failed without exception: {context}")

    def get_market(self, symbol: str) -> Dict[str, object]:
        trade_symbol = self._symbol_for_market(symbol)
        try:
            self._ensure_exchange_markets_loaded(f"load_markets_for_market:{trade_symbol}")
            return self.exchange.market(trade_symbol)
        except Exception as exc:
            logging.warning("Не удалось получить market для %s: %s", trade_symbol, exc)
            return {}

    def fetch_ohlcv(self, symbol: str, timeframe: str, limit: int, force_refresh: bool = False) -> list[list[float]]:
        trade_symbol = self._symbol_for_market(symbol)
        now = time.time()
        if not force_refresh:
            cached = self._last_ohlcv_cache.get(trade_symbol)
            if isinstance(cached, tuple):
                c_ts, c_data = cached
                if (now - c_ts) <= self._ohlcv_ttl_sec and isinstance(c_data, list) and c_data:
                    return c_data
        try:
            data = self._call_with_retries(
                f"fetch_ohlcv:{trade_symbol}:{timeframe}",
                lambda: self.exchange.fetch_ohlcv(trade_symbol, timeframe, limit=limit),
                retries=2,
                base_sleep_sec=0.20,
                retry_timeout_sec=max(0.7, float(self.config.CYCLE_IO_TIMEOUT_SEC) * 0.9),
            )
        except Exception as exc:
            logging.warning("fetch_ohlcv failed for %s (%s), using cached snapshot.", trade_symbol, exc)
            cached = self._last_ohlcv_cache.get(trade_symbol)
            if isinstance(cached, tuple) and isinstance(cached[1], list) and cached[1]:
                return cached[1]
            with self._hot_state_lock:
                hs = dict(self._hot_state) if isinstance(self._hot_state, dict) else {}
            hs_ohlcv = hs.get("ohlcv")
            hs_symbol = str(hs.get("symbol", "") or "")
            if isinstance(hs_ohlcv, list) and hs_ohlcv and hs_symbol == trade_symbol:
                return hs_ohlcv
            return []
        if isinstance(data, list) and data:
            self._last_ohlcv_cache[trade_symbol] = (now, data)
        return data

    def last_price(self, symbol: str, ttl_sec: Optional[float] = None, force_refresh: bool = False) -> float:
        trade_symbol = self._symbol_for_market(symbol)
        self.ws_feed.ensure_symbol(trade_symbol)
        ws_price = self.ws_feed.last_price(trade_symbol, max_age_sec=self.config.WS_MAX_AGE_SEC)
        if ws_price > 0 and not force_refresh:
            self._ticker_cache[trade_symbol] = (time.time(), ws_price)
            return float(ws_price)
        ttl = self._ticker_ttl_sec if ttl_sec is None else max(0.0, float(ttl_sec))
        now = time.time()
        if not force_refresh:
            cached = self._ticker_cache.get(trade_symbol)
            if isinstance(cached, tuple):
                ts, px = cached
                if px > 0 and (now - ts) <= ttl:
                    return float(px)
        rest_backoff_until = _safe_float(self._single_ticker_rest_backoff_until_ts.get(trade_symbol), 0.0)
        if rest_backoff_until > now:
            if ws_price > 0:
                self._ticker_cache[trade_symbol] = (time.time(), ws_price)
                return float(ws_price)
            cached = self._ticker_cache.get(trade_symbol)
            if isinstance(cached, tuple):
                _, px = cached
                if px > 0:
                    return float(px)
        try:
            ticker = self._call_with_retries(
                f"fetch_ticker:{trade_symbol}",
                lambda: self.exchange.fetch_ticker(trade_symbol),
                retries=2,
                base_sleep_sec=0.20,
                retry_timeout_sec=max(0.7, float(self.config.CYCLE_IO_TIMEOUT_SEC) * 0.8),
            )
            price = _safe_float(ticker.get("last"), 0.0)
            self._single_ticker_rest_fail_count[trade_symbol] = 0
            self._single_ticker_rest_backoff_until_ts.pop(trade_symbol, None)
        except Exception as exc:
            logging.warning("fetch_ticker failed for %s (%s), using fallback price.", trade_symbol, exc)
            price = 0.0
            fail_count = int(self._single_ticker_rest_fail_count.get(trade_symbol, 0)) + 1
            self._single_ticker_rest_fail_count[trade_symbol] = fail_count
            self._single_ticker_rest_backoff_until_ts[trade_symbol] = now + min(25.0, 3.0 + (fail_count * 3.0))
            ws_fb = self.ws_feed.last_price(trade_symbol, max_age_sec=max(0.2, self.config.WS_MAX_AGE_SEC * 2.5))
            if ws_fb > 0:
                price = float(ws_fb)
            if price <= 0:
                cached = self._ticker_cache.get(trade_symbol)
                if isinstance(cached, tuple):
                    _, px = cached
                    price = _safe_float(px, 0.0)
            if price <= 0:
                with self._hot_state_lock:
                    hs = dict(self._hot_state) if isinstance(self._hot_state, dict) else {}
                if str(hs.get("symbol", "")) == trade_symbol:
                    price = _safe_float(hs.get("price"), 0.0)
        if price > 0:
            self._ticker_cache[trade_symbol] = (now, price)
        return price

    def _fallback_balance(self, symbol: str) -> Dict[str, object]:
        base_currency = symbol.split("/")[0]
        usdt_default = max(0.0, self.config.PAPER_START_USDT if self.config.DRY_RUN else 0.0)
        return {
            "free": {
                "USDT": usdt_default,
                base_currency: 0.0,
            }
        }

    def fetch_balance_safe(self, symbol: str) -> Dict[str, object]:
        if not self._has_api_credentials:
            fallback = self._fallback_balance(symbol)
            self._last_balance_cache = fallback
            self._last_balance_cache_source = "fallback_no_credentials"
            self._last_balance_source = "fallback_no_credentials"
            return fallback
        try:
            balance = self._call_with_retries(
                "fetch_balance",
                lambda: self.exchange.fetch_balance(),
                retries=2,
                base_sleep_sec=0.25,
            )
            if isinstance(balance, dict):
                self._last_balance_cache = balance
                self._last_balance_cache_source = "exchange"
                self._last_balance_source = "exchange"
            return balance
        except Exception as exc:
            logging.warning("fetch_balance failed, using cached/fallback balance: %s", exc)
            if isinstance(self._last_balance_cache, dict) and self._last_balance_cache:
                src = str(self._last_balance_cache_source or "unknown").strip().lower()
                self._last_balance_source = f"cache_{src}" if src else "cache_unknown"
                return self._last_balance_cache
            fallback = self._fallback_balance(symbol)
            self._last_balance_cache = fallback
            self._last_balance_cache_source = "fallback_error"
            self._last_balance_source = "fallback_error"
            return fallback

    def get_position(self, symbol: str) -> Dict[str, object]:
        if self.config.TRADE_MARKET == "futures":
            pos = self._get_futures_position(symbol)
            self._last_position_cache[self._symbol_for_market(symbol)] = dict(pos)
            return pos
        balance = self.fetch_balance_safe(symbol)
        base_currency, _ = symbol.split("/")
        usdt_free = self._get_free_balance(balance, "USDT")
        base_free = self._get_free_balance(balance, base_currency)
        pos = {
            "usdt_free": usdt_free,
            "base_free": base_free,
            "base_currency": base_currency,
            "position_side": "LONG" if base_free > 0 else "",
            "position_qty": base_free,
            "balance_source": str(self._last_balance_source or "unknown"),
            "raw": balance,
        }
        self._last_position_cache[self._symbol_for_market(symbol)] = dict(pos)
        return pos

    def fetch_cycle_bundle(self, symbol: str, timeframe: str, limit: int) -> Dict[str, object]:
        """
        Low-latency parallel IO bundle:
        - OHLCV
        - Position/balances
        - Last price
        with strict timeout and fallback to hot cache.
        """
        trade_symbol = self._symbol_for_market(symbol)
        timeout_sec = max(0.8, float(self.config.CYCLE_IO_TIMEOUT_SEC))
        profile: Dict[str, float] = {
            "market_fetch_ms": 0.0,
            "position_ms": 0.0,
            "price_ms": 0.0,
        }
        t_fetch0 = time.perf_counter()

        def _safe_future_result(fut, timeout: float, name: str, fallback: object) -> object:
            t0 = time.perf_counter()
            try:
                val = fut.result(timeout=timeout)
                return val
            except FuturesTimeoutError:
                warn_key = f"{name}:{trade_symbol}"
                now_ts = time.time()
                last_warn_ts = _safe_float(self._io_timeout_warn_ts.get(warn_key), 0.0)
                if (now_ts - last_warn_ts) >= 90.0:
                    logging.warning("I/O timeout in %s for %s (%.2fs). Using fallback.", name, trade_symbol, timeout)
                    self._io_timeout_warn_ts[warn_key] = now_ts
                try:
                    fut.cancel()
                except Exception:
                    pass
                return fallback
            except Exception as exc:
                self._record_api_error(name, exc if isinstance(exc, Exception) else Exception(str(exc)))
                return fallback
            finally:
                profile_key = "market_fetch_ms" if name == "fetch_ohlcv" else ("position_ms" if name == "get_position" else "price_ms")
                profile[profile_key] = round((time.perf_counter() - t0) * 1000.0, 1)

        ohlcv_fb = []
        ohlcv_cached = self._last_ohlcv_cache.get(trade_symbol)
        if isinstance(ohlcv_cached, tuple) and isinstance(ohlcv_cached[1], list):
            ohlcv_fb = ohlcv_cached[1]
        pos_fb = self._last_position_cache.get(
            trade_symbol,
            {
                "usdt_free": _safe_float(self._fallback_balance(symbol).get("free", {}).get("USDT"), 0.0),
                "base_free": 0.0,
                "base_currency": symbol.split("/")[0],
                "position_side": "",
                "position_qty": 0.0,
                "balance_source": "fallback_bundle",
                "raw": self._fallback_balance(symbol),
            },
        )
        price_fb = self.ws_feed.last_price(trade_symbol, max_age_sec=max(0.2, self.config.WS_MAX_AGE_SEC * 2.0))

        pool = ThreadPoolExecutor(max_workers=3)
        try:
            f_ohlcv = pool.submit(self.fetch_ohlcv, symbol, timeframe, limit)
            f_pos = pool.submit(self.get_position, symbol)
            f_px = pool.submit(self.last_price, symbol, 0.0, False)

            ohlcv = _safe_future_result(f_ohlcv, timeout_sec, "fetch_ohlcv", ohlcv_fb)
            pos = _safe_future_result(f_pos, timeout_sec, "get_position", pos_fb)
            price = _safe_future_result(f_px, timeout_sec, "last_price", price_fb)
        finally:
            # Do not block cycle on lingering network calls.
            pool.shutdown(wait=False, cancel_futures=True)

        if (not price or _safe_float(price, 0.0) <= 0.0) and isinstance(ohlcv, list) and ohlcv:
            try:
                price = _safe_float(ohlcv[-1][4], 0.0)
            except Exception:
                pass

        with self._hot_state_lock:
            self._hot_state = {
                "symbol": trade_symbol,
                "ts_utc": datetime.now(timezone.utc).isoformat(),
                "price": _safe_float(price, 0.0),
                "ohlcv": ohlcv if isinstance(ohlcv, list) else [],
                "position": dict(pos) if isinstance(pos, dict) else {},
                "market_fetch_ms": round((time.perf_counter() - t_fetch0) * 1000.0, 1),
            }

        return {
            "ohlcv": ohlcv if isinstance(ohlcv, list) else [],
            "position": pos if isinstance(pos, dict) else {},
            "price": _safe_float(price, 0.0),
            "profile": profile,
        }

    def shutdown(self) -> None:
        try:
            self.ws_feed.stop()
        except Exception:
            pass

    def _get_futures_position(self, symbol: str) -> Dict[str, object]:
        trade_symbol = self._symbol_for_market(symbol)
        balance = self.fetch_balance_safe(symbol)
        usdt_free = self._get_free_balance(balance, "USDT")
        base_currency = trade_symbol.split("/")[0]
        side = ""
        qty = 0.0
        entry_price = 0.0
        unrealized_pnl = 0.0
        try:
            positions = self._call_with_retries(
                f"fetch_positions:{trade_symbol}",
                lambda: self.exchange.fetch_positions([trade_symbol]),
                retries=1,
                base_sleep_sec=0.20,
            )
            if isinstance(positions, list):
                for item in positions:
                    if not isinstance(item, dict):
                        continue
                    contracts = _safe_float(item.get("contracts"), 0.0)
                    side_raw = str(item.get("side", "") or "").strip().lower()
                    if contracts <= 0 and side_raw not in {"long", "short"}:
                        continue
                    if contracts <= 0:
                        continue
                    qty = abs(contracts)
                    side = "LONG" if side_raw == "long" else ("SHORT" if side_raw == "short" else "")
                    entry_price = _safe_float(item.get("entryPrice"), 0.0)
                    unrealized_pnl = _safe_float(item.get("unrealizedPnl"), 0.0)
                    break
        except Exception as exc:
            self._record_api_error(f"fetch_positions:{trade_symbol}", exc)
        return {
            "usdt_free": usdt_free,
            "base_free": qty,
            "base_currency": base_currency,
            "position_side": side,
            "position_qty": qty,
            "entry_price": entry_price,
            "unrealized_pnl": unrealized_pnl,
            "raw": balance,
        }

    def buy_market(self, symbol: str, usdt_cost: float):
        trade_symbol = self._symbol_for_market(symbol)
        if usdt_cost <= 0:
            raise ValueError("Buy notional must be > 0.")
        if self.config.TRADE_MARKET == "futures":
            return self.futures_open_market(symbol, "LONG", usdt_cost)
        if not self._can_buy_by_notional(trade_symbol, usdt_cost):
            min_cost = self._market_min_cost(trade_symbol)
            raise ValueError(f"Buy notional {usdt_cost} below min cost {min_cost}.")
        if hasattr(self.exchange, "create_market_buy_order_with_cost"):
            try:
                order = self._call_with_retries(
                    f"buy_with_cost:{trade_symbol}",
                    lambda: self.exchange.create_market_buy_order_with_cost(trade_symbol, usdt_cost),
                    retries=2,
                    base_sleep_sec=0.30,
                )
                return self._enrich_order_fill(trade_symbol, order)
            except Exception as exc:
                logging.warning(f"create_market_buy_order_with_cost failed ({exc}). Fallback to amount buy.")

        price = self.last_price(trade_symbol)
        if price <= 0:
            raise ValueError(f"Не удалось получить корректную цену рынка для {trade_symbol}.")
        amount = float(self._normalize_amount(trade_symbol, usdt_cost / price))
        if amount <= 0:
            raise ValueError("Calculated buy amount <= 0.")
        order = self._call_with_retries(
            f"create_order_buy:{trade_symbol}",
            lambda: self.exchange.create_order(trade_symbol, "market", "buy", amount),
            retries=2,
            base_sleep_sec=0.30,
        )
        return self._enrich_order_fill(trade_symbol, order)

    def sell_market(self, symbol: str, amount_base: float):
        trade_symbol = self._symbol_for_market(symbol)
        if self.config.TRADE_MARKET == "futures":
            return self.futures_close_market(symbol, "LONG", amount_base)
        normalized_amount = float(self._normalize_amount(trade_symbol, amount_base))
        if normalized_amount <= 0:
            raise ValueError("Sell amount below market minimum or zero.")
        if not self._can_sell_by_notional(trade_symbol, normalized_amount):
            min_cost = self._market_min_cost(trade_symbol)
            raise ValueError(f"Sell notional below min cost {min_cost}.")
        order = self._call_with_retries(
            f"create_order_sell:{trade_symbol}",
            lambda: self.exchange.create_order(trade_symbol, "market", "sell", normalized_amount),
            retries=2,
            base_sleep_sec=0.30,
        )
        return self._enrich_order_fill(trade_symbol, order)

    def futures_open_market(self, symbol: str, side: str, usdt_margin: float):
        trade_symbol = self._symbol_for_market(symbol)
        if self.config.TRADE_MARKET != "futures":
            raise ValueError("futures_open_market called in non-futures mode")
        if usdt_margin <= 0:
            raise ValueError("Futures margin must be > 0.")
        px = self.last_price(trade_symbol, force_refresh=True)
        if px <= 0:
            raise ValueError(f"Invalid market price for {trade_symbol}.")
        notional = usdt_margin * float(self.config.FUTURES_LEVERAGE)
        contracts = float(self._normalize_amount(trade_symbol, notional / px))
        if contracts <= 0:
            raise ValueError("Calculated futures amount <= 0.")
        order_side = "buy" if str(side).upper() == "LONG" else "sell"
        params = {"leverage": int(self.config.FUTURES_LEVERAGE), "marginMode": self.config.FUTURES_MARGIN_MODE}
        order = self._call_with_retries(
            f"create_futures_open:{trade_symbol}:{order_side}",
            lambda: self.exchange.create_order(trade_symbol, "market", order_side, contracts, None, params),
            retries=2,
            base_sleep_sec=0.30,
        )
        return self._enrich_order_fill(trade_symbol, order)

    def futures_close_market(self, symbol: str, side: str, contracts: float):
        trade_symbol = self._symbol_for_market(symbol)
        if self.config.TRADE_MARKET != "futures":
            raise ValueError("futures_close_market called in non-futures mode")
        amount = float(self._normalize_amount(trade_symbol, contracts))
        if amount <= 0:
            raise ValueError("Futures close amount <= 0.")
        close_side = "sell" if str(side).upper() == "LONG" else "buy"
        params = {"reduceOnly": True, "marginMode": self.config.FUTURES_MARGIN_MODE}
        order = self._call_with_retries(
            f"create_futures_close:{trade_symbol}:{close_side}",
            lambda: self.exchange.create_order(trade_symbol, "market", close_side, amount, None, params),
            retries=2,
            base_sleep_sec=0.30,
        )
        return self._enrich_order_fill(trade_symbol, order)

    def can_sell_position(self, symbol: str, base_amount: float, price_hint: float = 0.0) -> bool:
        if self.config.TRADE_MARKET == "futures":
            return base_amount > 0
        if base_amount <= 0:
            return False
        min_cost = self._market_min_cost(symbol)
        if min_cost <= 0:
            return True
        price = price_hint if price_hint > 0 else self.last_price(symbol)
        if price <= 0:
            return False
        return (base_amount * price) >= min_cost

    def candidate_spot_symbols(self) -> list[str]:
        now = time.time()
        fast_cache_ttl = 12.0 if bool(getattr(self.config, "LIVE_FAST_LOOP", False)) else 25.0
        if self._candidate_symbols_cache and (now - self._candidate_symbols_cache_ts) <= fast_cache_ttl:
            return list(self._candidate_symbols_cache)
        limit_cfg = int(_safe_float(getattr(self.config, "SYMBOL_SCAN_LIMIT", 20), 20.0))
        effective_limit = max(4, min(30, limit_cfg if limit_cfg > 0 else 12))
        try:
            if self._markets_cache and (now - self._markets_cache_ts) <= self._markets_ttl_sec:
                markets = self._markets_cache
            else:
                # MEXC spot reload can hit noisy contract-detail paths; plain load is more stable here.
                if self.config.TRADE_MARKET == "spot":
                    markets = self._call_with_retries(
                        "load_markets_spot",
                        lambda: self.exchange.load_markets(),
                        retries=1,
                        base_sleep_sec=0.20,
                        retry_timeout_sec=max(1.0, float(self.config.CYCLE_IO_TIMEOUT_SEC) * 1.2),
                    )
                else:
                    markets = self._call_with_retries(
                        "load_markets_reload",
                        lambda: self.exchange.load_markets(True),
                        retries=1,
                        base_sleep_sec=0.20,
                        retry_timeout_sec=max(1.0, float(self.config.CYCLE_IO_TIMEOUT_SEC) * 1.2),
                    )
                if isinstance(markets, dict):
                    self._markets_cache = markets
                    self._markets_cache_ts = now
        except (TypeError, Exception) as e:
            logging.warning("Не удалось загрузить рынки, используем fallback. Ошибка: %s", e)
            markets = self._call_with_retries(
                "load_markets_fallback",
                lambda: self.exchange.load_markets(),
                retries=1,
                base_sleep_sec=0.20,
                retry_timeout_sec=max(1.0, float(self.config.CYCLE_IO_TIMEOUT_SEC) * 1.2),
            )
            if isinstance(markets, dict):
                self._markets_cache = markets
                self._markets_cache_ts = now
        tickers: Dict[str, object] = {}
        tickers_failed = False
        try:
            if self._tickers24_cache and (now - self._tickers24_cache_ts) <= self._tickers24_ttl_sec:
                tickers = self._tickers24_cache
            elif self._tickers24_rest_backoff_until_ts > now:
                tickers_failed = True
                if self._tickers24_cache and (now - self._tickers24_cache_ts) <= max(180.0, self._tickers24_ttl_sec * 6.0):
                    tickers = self._tickers24_cache
                    tickers_failed = False
            else:
                tickers = self._call_with_retries(
                    "fetch_tickers",
                    lambda: self.exchange.fetch_tickers(),
                    retries=2,
                    base_sleep_sec=0.25,
                    retry_timeout_sec=max(1.2, float(self.config.CYCLE_IO_TIMEOUT_SEC) * 1.25),
                )
                if isinstance(tickers, dict) and tickers:
                    self._tickers24_cache = tickers
                    self._tickers24_cache_ts = now
                    self._tickers24_rest_fail_count = 0
                    self._tickers24_rest_backoff_until_ts = 0.0
        except Exception as exc:
            tickers_failed = True
            logging.warning("fetch_tickers timeout/failure, using fallback candidates: %s", exc)
            self._tickers24_rest_fail_count = int(self._tickers24_rest_fail_count) + 1
            self._tickers24_rest_backoff_until_ts = now + min(45.0, 6.0 + (self._tickers24_rest_fail_count * 4.0))
            if self._tickers24_cache and (now - self._tickers24_cache_ts) <= max(120.0, self._tickers24_ttl_sec * 4.0):
                tickers = self._tickers24_cache
                tickers_failed = False

        preferred: list[str] = []
        if self.config.PREFERRED_SPOT_SYMBOLS:
            for sym in self.config.PREFERRED_SPOT_SYMBOLS[:effective_limit]:
                market = markets.get(sym)
                if not self._is_spot_usdt_market(market):
                    continue
                preferred.append(sym)

        if preferred and tickers_failed:
            candidate_limit = min(effective_limit, max(4, len(preferred)))
            selected = list(preferred[:candidate_limit])
            self._candidate_universe_meta = {
                "source": "preferred_fallback",
                "base_limit": effective_limit,
                "effective_limit": candidate_limit,
                "preferred_count": len(preferred),
                "market_count": len(markets) if isinstance(markets, dict) else 0,
                "selected_count": len(selected),
                "tickers_failed": True,
            }
            self._candidate_symbols_cache = list(selected)
            self._candidate_symbols_cache_ts = time.time()
            return list(selected)

        preferred_set = {str(sym or "").upper().strip() for sym in preferred}
        preferred_volume_floor = max(250000.0, float(self.config.MIN_QUOTE_VOLUME_USDT) * 0.35)
        candidates: list[Tuple[str, int, float, float]] = []
        for sym, market in markets.items():
            if not self._is_spot_usdt_market(market):
                continue
            ticker = tickers.get(sym, {})
            quote_volume = _safe_float(ticker.get("quoteVolume"), 0.0)
            is_preferred = sym.upper() in preferred_set
            min_quote_volume = preferred_volume_floor if is_preferred else float(self.config.MIN_QUOTE_VOLUME_USDT)
            if quote_volume < min_quote_volume:
                continue
            priority = 1 if is_preferred else 0
            pct_move = abs(_safe_float(ticker.get("percentage"), 0.0))
            activity_score = min(30.0, pct_move) * 0.45 + math.log10(max(1.0, quote_volume))
            candidates.append((sym, priority, activity_score, quote_volume))

        candidates.sort(key=lambda x: (x[1], x[2], x[3]), reverse=True)
        candidate_limit = effective_limit
        if tickers_failed:
            candidate_limit = min(candidate_limit, max(4, len(preferred) if preferred else 4))
        elif self._tickers24_rest_backoff_until_ts > now:
            candidate_limit = min(candidate_limit, max(4, effective_limit - 1))
        elif bool(getattr(self.config, "LIVE_FAST_LOOP", False)):
            candidate_limit = min(candidate_limit, max(4, effective_limit - 1))

        out = [sym for sym, _, _, _ in candidates[:candidate_limit]]
        if out:
            self._candidate_symbols_cache = list(out)
            self._candidate_symbols_cache_ts = time.time()
            self._candidate_universe_meta = {
                "source": "tickers24",
                "base_limit": effective_limit,
                "effective_limit": candidate_limit,
                "preferred_count": len(preferred),
                "market_count": len(markets) if isinstance(markets, dict) else 0,
                "selected_count": len(out),
                "tickers_failed": bool(tickers_failed),
            }
            return out
        # Last-resort fallback: use recent cache for a few minutes.
        if self._candidate_symbols_cache and (time.time() - self._candidate_symbols_cache_ts) <= 600:
            return list(self._candidate_symbols_cache)
        return []
    
    def _get_free_balance(self, balance: Dict[str, object], currency: str) -> float:
        direct_free = _safe_float(balance.get(currency, {}).get("free", 0.0))
        mapped_free = _safe_float(balance.get("free", {}).get(currency, 0.0))
        return max(direct_free, mapped_free)

    def _normalize_amount(self, symbol: str, amount: float) -> str:
        if amount <= 0:
            return "0"
        market = self.get_market(symbol)
        min_amount = _safe_float(market.get("limits", {}).get("amount", {}).get("min", 0.0), 0.0)
        normalized = _safe_float(self.exchange.amount_to_precision(symbol, amount), 0.0)
        if min_amount > 0 and normalized < min_amount:
            return "0"
        return self.exchange.amount_to_precision(symbol, normalized)

    def _enrich_order_fill(self, symbol: str, order: object) -> object:
        if not isinstance(order, dict):
            return order
        order_id = str(order.get("id") or order.get("orderId") or "").strip()
        if not order_id:
            return order
        filled = _safe_float(order.get("filled"), 0.0)
        cost = _safe_float(order.get("cost"), 0.0)
        price = _safe_float(order.get("average"), 0.0) or _safe_float(order.get("price"), 0.0)
        # If the order already has execution metrics, keep it only when sane.
        if filled > 0 and (cost > 0 or price > 0):
            used_px = price if price > 0 else (cost / filled if (cost > 0 and filled > 0) else 0.0)
            ref_px = 0.0
            try:
                ref_px = _safe_float(self.last_price(symbol, ttl_sec=0.2, force_refresh=True), 0.0)
            except Exception:
                ref_px = 0.0
            if used_px > 0 and ref_px > 0:
                drift = abs((used_px / ref_px) - 1.0)
                if drift <= 0.60:
                    return order
            elif used_px > 0:
                # No market reference -> keep legacy behavior.
                return order
            # Suspicious execution payload (e.g. avg=1 for BNB/ETH): try to refetch full order.
            logging.warning(
                "Order fill sanity check failed for %s (id=%s): used_px=%.6f, ref_px=%.6f. Refetching.",
                symbol,
                order_id,
                used_px,
                ref_px,
            )
        try:
            fetched = self._call_with_retries(
                f"fetch_order:{symbol}:{order_id}",
                lambda: self.exchange.fetch_order(order_id, symbol),
                retries=1,
                base_sleep_sec=0.25,
            )
            if isinstance(fetched, dict):
                merged = dict(order)
                for key in ("filled", "cost", "average", "price", "status", "fee", "fees", "trades"):
                    if fetched.get(key) is not None:
                        merged[key] = fetched.get(key)
                return merged
        except Exception:
            # Best-effort enrichment only.
            return order
        return order
    
    def _can_buy_by_notional(self, symbol: str, usdt_amount: float) -> bool:
        min_cost = self._market_min_cost(symbol)
        return usdt_amount >= min_cost if min_cost > 0 else True
        
    def _can_sell_by_notional(self, symbol: str, base_amount: float) -> bool:
        min_cost = self._market_min_cost(symbol)
        if min_cost <= 0:
            return True
        price = self.last_price(symbol)
        if price <= 0:
            return False
        return (base_amount * price) >= min_cost

    def _market_min_cost(self, symbol: str) -> float:
        market = self.get_market(symbol)
        return _safe_float(market.get("limits", {}).get("cost", {}).get("min", 0.0), 0.0)

    def market_min_cost(self, symbol: str) -> float:
        return self._market_min_cost(symbol)

    @staticmethod
    def _is_spot_usdt_market(market: object) -> bool:
        if not isinstance(market, dict):
            return False
        if market.get("spot") is False:
            return False
        if market.get("quote") != "USDT":
            return False
        # Some exchange adapters may not provide a strict bool; only explicit False should block.
        if market.get("active") is False:
            return False
        return True


class Bot:
    """The main trading bot logic."""
    def __init__(
        self,
        config: Config,
        trader: Any,
        ai_engine: Optional[AISignalEngine],
        adaptive_agent: AdaptiveAgent,
        market_ctx_engine: MarketContextEngine,
        overlay_engine: PositionOverlayEngine,
        model_evolver: ModelEvolver,
        advisory_provider: Optional[AdvisoryProvider] = None,
    ):
        self.config = config
        self.trader = trader
        self.engine_backend = str(getattr(trader, "backend_name", "python") or "python")
        self.ai_engine = ai_engine
        self.adaptive_agent = adaptive_agent
        self.market_ctx_engine = market_ctx_engine
        self.overlay_engine = overlay_engine
        self.model_evolver = model_evolver
        self.advisory_provider = advisory_provider
        self.spot_state: Dict[str, Dict[str, float]] = {}
        self.position_memory: Dict[str, Dict[str, float | str]] = {}
        self.paper_usdt_free: Optional[float] = None
        self.paper_positions: Dict[str, float] = {}
        self.runtime_params: Dict[str, float] = self.adaptive_agent.current_base_params()
        self.runtime_market_ctx: Dict[str, object] = {}
        self.runtime_adaptation: Dict[str, object] = dict(self.adaptive_agent.last_adaptation)
        self.runtime_smart_exit: Dict[str, object] = {}
        self.runtime_edge_floor: Dict[str, object] = {}
        self.regime_edge_autotune: Dict[str, Dict[str, float]] = {}
        self._last_regime_autotune_save_ts: float = 0.0
        self._cycle_compute_cache: Dict[str, Dict[str, object]] = {}
        self.entry_policy = EntryPolicy()
        self.execution_engine = ExecutionEngine()
        self.position_sm = PositionStateMachine()
        self.risk_policy = RiskPolicy()
        self.current_cycle_trade_meta: Dict[str, object] = {}
        self.real_trade_entry_meta: Dict[str, Dict[str, object]] = {}
        self.loss_streak_live: int = 0
        self.exec_slippage_ema_bps: float = 0.0
        self.symbol_quality_ema: Dict[str, float] = {}
        self.symbol_trade_count: Dict[str, int] = {}
        self.symbol_exec_slippage_ema_bps: Dict[str, float] = {}
        self.symbol_exec_quality_ema: Dict[str, float] = {}
        self.regime_trade_feedback: Dict[str, Dict[str, float]] = {}
        self.symbol_regime_trade_feedback: Dict[str, Dict[str, float]] = {}
        self.shared_learning_feedback: Dict[str, float] = {
            "ema_pass": 0.25,
            "ema_quality": 0.50,
            "ema_conf": 0.50,
            "ema_edge_margin": 0.0,
            "ema_trade_score": 0.50,
            "ema_cycle_score": 0.50,
            "training_weight": 0.0,
            "live_weight": 0.0,
            "updated_ts": 0.0,
        }
        self.confidence_calibration_state: Dict[str, object] = {
            "overall_ema_score": 0.50,
            "overall_ema_margin": 0.0,
            "bucket_stats": {},
            "updated_ts": 0.0,
        }
        self.missed_opportunity_feedback: Dict[str, Dict[str, float]] = {}
        self.pending_opportunity_setups: deque[Dict[str, object]] = deque(maxlen=160)
        self.recent_trade_scores: deque[float] = deque(maxlen=120)
        self.recent_cycle_feedback: deque[Dict[str, object]] = deque(maxlen=220)
        self.recent_decision_scores: deque[float] = deque(maxlen=220)
        self.symbol_loss_streak: Dict[str, int] = {}
        self.symbol_quarantine_until_ts: Dict[str, float] = {}
        self._symbol_quarantine_log_ts: Dict[str, float] = {}
        self.side_stoploss_streak: Dict[str, int] = {"LONG": 0, "SHORT": 0}
        self.force_exit_reasons: Dict[str, str] = {}
        self._last_audit_signature: str = ""
        self._last_entry_block_log_sig: str = ""
        self._last_entry_block_log_ts: float = 0.0
        self._no_trade_streak_cycles: int = 0
        self._last_trade_event_ts: float = 0.0
        self._run_session_id: str = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
        self._partial_sell_skip_until_ts: Dict[str, float] = {}
        self._partial_sell_warn_until_ts: Dict[str, float] = {}
        self._last_symbol_scan_ts: float = 0.0
        self._last_symbol_scan_result: Tuple[str, int, int] = (self.config.SYMBOL, 1, 1)
        self._last_cycle_started_ts: float = 0.0
        self._last_cycle_completed_ts: float = 0.0
        self._slow_cycle_streak: int = 0
        self._last_cycle_watchdog_event: str = ""
        self._last_cycle_watchdog_log_ts: float = 0.0
        self.runtime_advisory: Dict[str, object] = {}
        self._mtf_cache: Dict[str, Tuple[float, bool, Dict[str, float]]] = {}
        self._symbol_rr_index: int = 0
        self._buy_settle_until_ts: Dict[str, float] = {}
        self._symbol_last_price: Dict[str, float] = {}
        self._last_confirmed_exchange_usdt_free: float = 0.0
        self._last_confirmed_exchange_equity_usdt: float = 0.0
        self._last_confirmed_exchange_balance_source: str = "unknown"
        self.active_symbol: Optional[str] = None
        self.last_trade_ts: Optional[float] = None
        self.api_error_ts: deque[float] = deque(maxlen=256)
        self.time_stop_buy_block_until_candle_ts: Dict[str, int] = {}
        self.signal_debounce_counter: Dict[str, int] = {}
        self.symbol_internal_cooldown_until_ts: Dict[str, float] = {}
        self.recent_orders: deque[Dict[str, object]] = deque(maxlen=20)
        self.stop_requested = False
        self._last_live_status_payload: Dict[str, object] = {}
        self._last_has_open_position: bool = False
        self._last_cycle_event: str = "booting"
        self._guard_state_file_lock = threading.Lock()
        self.guard_state: Dict[str, object] = {
            "day_utc": "",
            "start_equity_usdt": 0.0,
            "max_equity_usdt": 0.0,
            "trade_count": 0,
            "lock_until_ts": 0.0,
            "lock_reason": "",
            "consecutive_cycle_errors": 0,
            "consecutive_api_errors": 0,
            "consecutive_logic_errors": 0,
            "last_error_type": "",
            "last_error": "",
        }
        self._load_position_memory()
        self._load_paper_state()
        self._load_guard_state()
        self._startup_reconcile_positions()

    def _regime_target_entry_pass(self, regime: str, flags: set[str]) -> float:
        r = str(regime or "flat").strip().lower()
        if r in {"trend"}:
            base = 0.42
        elif r in {"impulse"}:
            base = 0.38
        elif r in {"high_vol"}:
            base = 0.30
        elif r in {"dangerous"}:
            base = 0.16
        else:
            base = 0.24
        if "low_vol" in flags or "low_volatility" in flags:
            base -= 0.04
        return _clamp(base, 0.12, 0.50)

    def _regime_edge_multiplier(self, regime: str, flags: set[str], dangerous: bool) -> float:
        r = str(regime or "flat").strip().lower()
        # Base regime prior (conservative): trend a bit looser, flat/noisy a bit stricter.
        prior = 1.0
        if r == "trend":
            prior = 0.96
        elif r == "impulse":
            prior = 0.94
        elif r in {"flat", "range"}:
            prior = 1.06
        elif r == "high_vol":
            prior = 1.03
        elif r == "dangerous":
            prior = 1.12
        if "low_vol" in flags or "low_volatility" in flags:
            prior += 0.04
        if dangerous:
            prior += 0.05

        st = self.regime_edge_autotune.get(r)
        learned = _safe_float((st or {}).get("mult"), prior)
        bias = self._regime_trade_bias(r)
        learned *= _safe_float(bias.get("edge_mult"), 1.0)
        # Keep live stricter bounds than training.
        lo = 0.78 if self.config.DRY_RUN else 0.84
        hi = 1.36 if self.config.DRY_RUN else 1.30
        return _clamp(learned, lo, hi)

    def _update_regime_edge_autotune(
        self,
        *,
        regime: str,
        flags: set[str],
        dangerous: bool,
        expected_edge_pct: float,
        min_expected_edge_pct: float,
        ai_quality: float,
        ai_conf: float,
        expected_edge_ok: bool,
        ai_quality_ok: bool,
        has_open_position: bool,
    ) -> None:
        # Learn only from entry context (no open position).
        if has_open_position:
            return
        r = str(regime or "flat").strip().lower()
        st = self.regime_edge_autotune.get(r, {})
        mult = _safe_float(st.get("mult"), self._regime_edge_multiplier(r, flags, dangerous))
        ema_pass = _safe_float(st.get("ema_pass"), 0.25)
        ema_margin = _safe_float(st.get("ema_margin"), 0.0)
        ema_quality = _safe_float(st.get("ema_quality"), 0.5)
        ema_conf = _safe_float(st.get("ema_conf"), 0.5)

        alpha = 0.06
        pass_now = 1.0 if (expected_edge_ok and ai_quality_ok) else 0.0
        margin_now = float(expected_edge_pct - min_expected_edge_pct)
        ema_pass = ((1.0 - alpha) * ema_pass) + (alpha * pass_now)
        ema_margin = ((1.0 - alpha) * ema_margin) + (alpha * margin_now)
        ema_quality = ((1.0 - alpha) * ema_quality) + (alpha * _clamp(ai_quality, 0.0, 1.0))
        ema_conf = ((1.0 - alpha) * ema_conf) + (alpha * _clamp(ai_conf, 0.0, 1.0))

        target_pass = self._regime_target_entry_pass(r, flags)
        pass_gap = target_pass - ema_pass
        # Small autonomous correction step per cycle.
        step = _clamp(pass_gap * 0.08, -0.014, 0.014)
        # If quality/conf are weak, avoid over-loosening edge floor.
        if ema_quality < 0.45 or ema_conf < 0.52:
            step = max(step, -0.004)
        # If market looks dangerous, bias to stricter edge.
        if dangerous:
            step = max(step, 0.0035)
        # If margin is consistently very positive with high pass rate, tighten a bit.
        if ema_pass > (target_pass + 0.12) and ema_margin > 0.0012:
            step = max(step, 0.004)
        # If margin is consistently negative and pass too low, loosen slightly faster.
        if ema_pass < (target_pass - 0.10) and ema_margin < -0.0009:
            step = min(step, -0.008)

        mult = _clamp(mult + step, 0.78 if self.config.DRY_RUN else 0.84, 1.36 if self.config.DRY_RUN else 1.30)
        self.regime_edge_autotune[r] = {
            "mult": float(mult),
            "ema_pass": float(ema_pass),
            "ema_margin": float(ema_margin),
            "ema_quality": float(ema_quality),
            "ema_conf": float(ema_conf),
            "target_pass": float(target_pass),
            "updated_ts": float(time.time()),
        }
        now_ts = time.time()
        if (now_ts - self._last_regime_autotune_save_ts) >= 35.0:
            self._save_guard_state()
            self._last_regime_autotune_save_ts = now_ts

    def _manual_command_path(self) -> Path:
        path = APP_BASE_DIR / "logs" / "runtime" / "manual_command.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        return path

    def _consume_manual_sell_signal(self, symbol: str) -> bool:
        path = self._manual_command_path()
        if not path.exists():
            return False
        try:
            payload = json.loads(path.read_text(encoding="utf-8-sig"))
            if not isinstance(payload, dict):
                return False
            action = str(payload.get("action", "")).strip().lower()
            target_symbol = str(payload.get("symbol", "")).strip().upper()
            if action not in {"sell_now", "force_sell", "close_position"}:
                return False
            if target_symbol and target_symbol not in {"*", symbol.upper()}:
                return False
            return True
        except Exception:
            return False
        finally:
            try:
                path.unlink(missing_ok=True)
            except Exception:
                pass

    def _position_state_path(self) -> Path:
        path = Path(self.config.POSITION_STATE_PATH)
        if not path.is_absolute():
            path = APP_BASE_DIR / path
        return path

    def _audit_log_path(self) -> Path:
        path = Path(self.config.AUDIT_LOG_PATH)
        if not path.is_absolute():
            path = APP_BASE_DIR / path
        return path

    def _position_state_backup_path(self) -> Path:
        path = self._position_state_path()
        return path.with_suffix(path.suffix + ".bak")

    def _paper_state_path(self) -> Path:
        base = self._position_state_path()
        return base.with_name("paper_state.json")

    def _default_paper_usdt(self) -> float:
        cfg_start = _safe_float(getattr(self.config, "PAPER_START_USDT", 0.0), 0.0)
        if cfg_start > 0:
            return cfg_start
        min_seed = max(_safe_float(getattr(self.config, "SPOT_MIN_BUY_USDT", 5.0), 5.0) * 5.0, 5.0)
        return min_seed

    def _load_paper_state(self) -> None:
        path = self._paper_state_path()
        default_usdt = self._default_paper_usdt()
        self.paper_usdt_free = default_usdt
        self.paper_positions = {}
        if not path.exists():
            return
        try:
            payload = json.loads(path.read_text(encoding="utf-8-sig"))
            if not isinstance(payload, dict):
                return
            self.paper_usdt_free = max(0.0, _safe_float(payload.get("paper_usdt_free"), default_usdt))
            raw_positions = payload.get("paper_positions", {})
            if isinstance(raw_positions, dict):
                clean: Dict[str, float] = {}
                for sym, qty_raw in raw_positions.items():
                    qty = _safe_float(qty_raw, 0.0)
                    if qty > 0:
                        clean[str(sym).upper()] = qty
                self.paper_positions = clean
            # Guard against stale tiny paper_state from previous sessions.
            # In training mode, if there is no open paper position and stored balance is below configured start,
            # restore configured paper start amount automatically.
            if self.config.AI_TRAINING_MODE and self.config.DRY_RUN:
                cur_usdt = max(0.0, _safe_float(self.paper_usdt_free, 0.0))
                if (not self.paper_positions) and cur_usdt < default_usdt:
                    self.paper_usdt_free = default_usdt
                    self._save_paper_state()
                # Corruption guard: prevent runaway paper balance from transient price desync bugs.
                max_reasonable = max(default_usdt * 1000.0, default_usdt + 10_000.0)
                if (not self.paper_positions) and cur_usdt > max_reasonable:
                    logging.warning(
                        "Paper balance anomaly detected (%.4f > %.4f). Reset to configured start %.4f.",
                        cur_usdt,
                        max_reasonable,
                        default_usdt,
                    )
                    self.paper_usdt_free = default_usdt
                    self._save_paper_state()
        except Exception as exc:
            logging.info("Файл paper_state не читается (%s): %s. Используем дефолт.", path.name, exc)

    def _save_paper_state(self) -> None:
        path = self._paper_state_path()
        try:
            payload = {
                "ts_utc": datetime.now(timezone.utc).isoformat(),
                "paper_usdt_free": max(0.0, _safe_float(self.paper_usdt_free, 0.0)),
                "paper_positions": {k: float(v) for k, v in self.paper_positions.items() if _safe_float(v, 0.0) > 0},
            }
            _write_json_atomic(path, payload)
        except Exception as exc:
            logging.warning("Не удалось сохранить paper_state: %s", exc)

    def _cleanup_stale_runtime_artifacts(self) -> None:
        targets: list[Path] = []
        for raw in (
            self.config.BOT_LIVE_STATUS_PATH,
            self.config.BOT_TELEMETRY_PATH,
            self.config.RISK_GUARD_STATE_PATH,
            self.config.POSITION_STATE_PATH,
            self.config.BOT_PROCESS_LOCK_PATH,
        ):
            p = Path(raw)
            if not p.is_absolute():
                p = APP_BASE_DIR / p
            targets.append(p.with_suffix((p.suffix or "") + ".tmp"))
            targets.append(p.with_suffix((p.suffix or "") + ".lock"))

        removed = 0
        for p in targets:
            try:
                if not p.exists() or not p.is_file():
                    continue
                if p.suffix.lower().endswith(".tmp"):
                    p.unlink(missing_ok=True)
                    removed += 1
                    continue
                # lock files: keep only if owner pid is alive.
                try:
                    payload = json.loads(p.read_text(encoding="utf-8-sig"))
                except Exception:
                    payload = {}
                pid = int(_safe_float(payload.get("pid"), 0.0)) if isinstance(payload, dict) else 0
                if pid <= 0 or (not _pid_alive(pid)):
                    p.unlink(missing_ok=True)
                    removed += 1
            except Exception:
                continue
        if removed > 0:
            logging.info("Startup cleanup: removed stale runtime artifacts: %d", removed)

    def _append_audit_event(
        self,
        *,
        event: str,
        symbol: str,
        reason: str,
        guard_reason: str,
        buy_block_reasons: list[str],
        sell_reason: str,
        ai_action: str,
        ai_quality: float,
        expected_edge_pct: float,
        min_expected_edge_pct: float,
        market_regime: str,
        has_open_position: bool,
        pnl_pct: float,
        price: float,
        usdt_free: float,
        base_free: float,
        signal_reason: str = "",
        signal_explainer: list[str] | None = None,
        no_entry_reasons: list[str] | None = None,
        data_quality_score: float = 0.0,
    ) -> None:
        evt = str(event or "").strip()
        if not evt:
            return
        blocks = [str(x).strip() for x in buy_block_reasons if str(x).strip()]
        key = "|".join(
            [
                evt,
                str(symbol or ""),
                str(reason or ""),
                str(guard_reason or ""),
                str(sell_reason or ""),
                ",".join(blocks[:3]),
                "1" if has_open_position else "0",
            ]
        )
        if evt in {"no_trade_signal", "training_no_signal", "cycle_busy"} and key == self._last_audit_signature:
            return
        self._last_audit_signature = key
        try:
            payload = {
                "ts_utc": datetime.now(timezone.utc).isoformat(),
                "event": evt,
                "symbol": str(symbol or ""),
                "reason": str(reason or ""),
                "guard_reason": str(guard_reason or ""),
                "buy_block_reasons": blocks[:5],
                "sell_reason": str(sell_reason or ""),
                "ai_action": str(ai_action or ""),
                "ai_quality": float(ai_quality),
                "expected_edge_pct": float(expected_edge_pct),
                "min_expected_edge_pct": float(min_expected_edge_pct),
                "market_regime": str(market_regime or "flat"),
                "has_open_position": bool(has_open_position),
                "pnl_pct": float(pnl_pct),
                "price": float(price),
                "usdt_free": float(usdt_free),
                "base_free": float(base_free),
                "signal_reason": str(signal_reason or ""),
                "signal_explainer": [str(x).strip() for x in (signal_explainer or []) if str(x).strip()][:8],
                "no_entry_reasons": [str(x).strip() for x in (no_entry_reasons or []) if str(x).strip()][:8],
                "data_quality_score": float(_clamp(data_quality_score, 0.0, 1.0)),
                "dry_run": bool(self.config.DRY_RUN),
                "mode": (
                    "training"
                    if (self.config.DRY_RUN and self.config.AI_TRAINING_MODE)
                    else ("live" if ((not self.config.DRY_RUN) and (not self.config.AI_TRAINING_MODE)) else "custom")
                ),
                "bot_pid": os.getpid(),
                "session_id": self._run_session_id,
            }
            payload = _repair_payload_text_ru(payload)
            ap = self._audit_log_path()
            ap.parent.mkdir(parents=True, exist_ok=True)
            with ap.open("a", encoding="utf-8") as f:
                f.write(json.dumps(payload, ensure_ascii=False) + "\n")
        except Exception as exc:
            now_ts = time.time()
            last_warn_ts = _safe_float(getattr(self, "_last_audit_write_error_ts", 0.0), 0.0)
            if now_ts - last_warn_ts >= 60.0:
                logging.warning("Audit write failed: %s", exc)
                self._last_audit_write_error_ts = now_ts

    def _compute_data_quality_score(
        self,
        *,
        market_data_stale: bool,
        market_data_stale_sec: float,
        ticker_rest_backoff_sec: float,
        tickers24_rest_backoff_sec: float,
        ticker_rest_fail_count: float,
        tickers24_rest_fail_count: float,
        market_volatility: float,
        regime_flags: list[str],
        universe_tickers_failed: bool,
    ) -> float:
        score = 1.0
        flags = {str(x).strip().lower() for x in (regime_flags or []) if str(x).strip()}
        stale_sec = max(0.0, _safe_float(market_data_stale_sec, 0.0))
        if market_data_stale:
            score -= 0.42
        score -= min(0.24, stale_sec / 90.0)
        score -= min(0.18, max(0.0, _safe_float(ticker_rest_backoff_sec, 0.0)) / 60.0)
        score -= min(0.16, max(0.0, _safe_float(tickers24_rest_backoff_sec, 0.0)) / 60.0)
        score -= min(0.10, max(0.0, _safe_float(ticker_rest_fail_count, 0.0)) * 0.015)
        score -= min(0.10, max(0.0, _safe_float(tickers24_rest_fail_count, 0.0)) * 0.015)
        if universe_tickers_failed:
            score -= 0.12
        vol = max(0.0, _safe_float(market_volatility, 0.0))
        if vol >= 0.035:
            score -= 0.12
        elif vol >= 0.020:
            score -= 0.06
        if "anomaly" in flags:
            score -= 0.08
        if "low_liquidity" in flags:
            score -= 0.08
        return float(_clamp(score, 0.0, 1.0))

    def _build_signal_explainer(
        self,
        *,
        event: str,
        ai_action: str,
        ai_conf: float,
        ai_conf_required: float,
        ai_quality: float,
        ai_quality_required: float,
        expected_edge_pct: float,
        min_expected_edge_pct: float,
        buy_block_reasons: list[str],
        market_data_stale: bool,
        data_quality_score: float,
        market_regime: str,
        market_flags: list[str],
        has_open_position: bool,
    ) -> list[str]:
        explainers: list[str] = []
        flags = {str(x).strip().lower() for x in (market_flags or []) if str(x).strip()}
        blocks = [str(x or "").strip().lower() for x in (buy_block_reasons or []) if str(x or "").strip()]
        evt = str(event or "").strip().lower()
        action = str(ai_action or "").strip().upper()

        if has_open_position:
            explainers.append("Позиция уже открыта: бот сопровождает сделку.")
        elif market_data_stale:
            explainers.append("Рыночные данные устарели: вход временно пропускается.")
        elif data_quality_score < 0.55:
            explainers.append(f"Качество данных низкое ({data_quality_score:.2f}): сигнал считается ненадёжным.")

        if action and action not in {"LONG", "SHORT"}:
            explainers.append(f"AI не дал рабочего направления входа ({action}).")

        edge_margin = float(expected_edge_pct - min_expected_edge_pct)
        if any("expected_edge" in r or "edge" in r for r in blocks):
            explainers.append(
                f"Ожидаемое преимущество слабое: {expected_edge_pct * 100.0:.2f}% при минимуме {min_expected_edge_pct * 100.0:.2f}%."
            )
        elif edge_margin > 0:
            explainers.append(
                f"Ожидаемое преимущество проходит порог с запасом {edge_margin * 100.0:.2f}%."
            )

        if any("quality" in r for r in blocks):
            explainers.append(
                f"Качество сигнала ниже порога: {ai_quality:.2f} при минимуме {ai_quality_required:.2f}."
            )
        if any("confidence" in r or "conf" in r for r in blocks):
            explainers.append(
                f"Уверенность AI ниже порога: {ai_conf:.2f} при минимуме {ai_conf_required:.2f}."
            )
        if any("mtf" in r for r in blocks):
            explainers.append("Старший таймфрейм не подтвердил вход.")
        if any("debounce" in r for r in blocks):
            explainers.append("Сигнал слишком нестабилен: дебаунс ещё не пройден.")
        if any("guard" in r or "pretrade:" in r for r in blocks):
            explainers.append("Защитные фильтры риска заблокировали вход.")

        if evt in {"buy", "training_buy_signal"} and not explainers:
            explainers.append("Все основные фильтры прошли: бот открыл позицию.")
        if evt in {"training_no_signal", "no_trade_signal", "entry_blocked"} and not explainers:
            explainers.append("Подходящего сетапа для входа пока нет.")

        regime_name = str(market_regime or "flat")
        if ("low_vol" in flags or "low_volatility" in flags) and len(explainers) < 6:
            explainers.append(f"Рынок сейчас вялый ({regime_name}): сильные входы встречаются реже.")
        elif regime_name in {"range", "flat"} and len(explainers) < 6:
            explainers.append(f"Режим рынка {regime_name}: бот ждёт более чистый импульс.")

        unique: list[str] = []
        seen: set[str] = set()
        for item in explainers:
            key = str(item).strip()
            if key and key not in seen:
                seen.add(key)
                unique.append(key)
        return unique[:6]

    def _load_position_memory(self) -> None:
        if self.config.AI_TRAINING_MODE and self.config.DRY_RUN:
            return
        paths = [self._position_state_path(), self._position_state_backup_path()]
        invalid_paths: list[Path] = []
        parsed_ok = False
        for idx, path in enumerate(paths):
            if not path.exists():
                continue
            try:
                payload = json.loads(path.read_text(encoding="utf-8-sig"))
                raw_positions = payload.get("positions", {}) if isinstance(payload, dict) else {}
                if not isinstance(raw_positions, dict):
                    raise ValueError("positions must be an object")
                parsed_ok = True
                for symbol, raw in raw_positions.items():
                    if not isinstance(raw, dict):
                        continue
                    entry = _safe_float(raw.get("entry_price"), 0.0)
                    peak = _safe_float(raw.get("peak_price"), 0.0)
                    stop_loss = _safe_float(raw.get("stop_loss"), 0.0)
                    take_profit = _safe_float(raw.get("take_profit"), 0.0)
                    trailing_stop = _safe_float(raw.get("trailing_stop"), 0.0)
                    fraction = _safe_float(raw.get("fraction"), self.config.SPOT_BALANCE_RISK_FRACTION)
                    entry_candle_ts = int(_safe_float(raw.get("entry_candle_ts"), 0.0))
                    peak_candle_ts = int(_safe_float(raw.get("peak_candle_ts"), 0.0))
                    position_side = str(raw.get("position_side", "LONG") or "LONG").upper()
                    if position_side not in {"LONG", "SHORT"}:
                        position_side = "LONG"
                    if entry <= 0 or peak <= 0 or stop_loss <= 0 or take_profit <= 0 or trailing_stop <= 0:
                        continue
                    sym = str(symbol).upper()
                    self.position_memory[sym] = {
                        "entry_price": entry,
                        "peak_price": peak,
                        "stop_loss": stop_loss,
                        "take_profit": take_profit,
                        "trailing_stop": trailing_stop,
                        "fraction": fraction,
                        "entry_candle_ts": entry_candle_ts,
                        "peak_candle_ts": peak_candle_ts if peak_candle_ts > 0 else entry_candle_ts,
                        "position_side": position_side,
                        "source": str(raw.get("source", "restored")),
                    }
                    self.spot_state[sym] = {
                        "entry_price": entry,
                        "peak_price": peak,
                        "entry_candle_ts": float(entry_candle_ts if entry_candle_ts > 0 else 0),
                        "peak_candle_ts": float(peak_candle_ts if peak_candle_ts > 0 else entry_candle_ts),
                        "position_side": position_side,
                    }
                source = "backup" if idx == 1 else "main"
                if self.position_memory:
                    logging.info("Загружено сохраненное состояние позиции (%s) для %d символ(ов).", source, len(self.position_memory))
                # First valid file is authoritative. Do not fall through to backup.
                break
            except Exception as exc:
                invalid_paths.append(path)
                logging.info("Файл состояния позиции не читается (%s): %s", path.name, exc)
        if not parsed_ok and any(p.exists() for p in paths):
            logging.info("Состояние позиции недоступно/повреждено. Продолжаем без восстановления позиции.")
            existing = [p for p in paths if p.exists()]
            if existing and len(invalid_paths) >= len(existing):
                try:
                    payload = {
                        "ts_utc": datetime.now(timezone.utc).isoformat(),
                        "positions": {},
                    }
                    _write_json_atomic(self._position_state_path(), payload, backup_path=self._position_state_backup_path())
                    logging.info("Файлы состояния позиции автоматически пересозданы.")
                except Exception as exc:
                    logging.warning("Не удалось переинициализировать поврежденные файлы состояния позиции: %s", exc)

    def _save_position_memory(self) -> None:
        if self.config.AI_TRAINING_MODE and self.config.DRY_RUN:
            return
        path = self._position_state_path()
        backup = self._position_state_backup_path()
        try:
            payload = {
                "ts_utc": datetime.now(timezone.utc).isoformat(),
                "positions": self.position_memory,
            }
            _write_json_atomic(path, payload, backup_path=backup)
        except Exception as exc:
            logging.warning("Не удалось сохранить состояние позиции: %s", exc)

    def _risk_from_memory(self, symbol: str) -> Optional[dict]:
        item = self.position_memory.get(symbol.upper())
        if not isinstance(item, dict):
            return None
        stop_loss = _safe_float(item.get("stop_loss"), 0.0)
        take_profit = _safe_float(item.get("take_profit"), 0.0)
        trailing_stop = _safe_float(item.get("trailing_stop"), 0.0)
        fraction = _safe_float(item.get("fraction"), self.config.SPOT_BALANCE_RISK_FRACTION)
        if stop_loss <= 0 or take_profit <= 0 or trailing_stop <= 0:
            return None
        return {
            "fraction": fraction,
            "stop_loss": stop_loss,
            "take_profit": take_profit,
            "trailing_stop": trailing_stop,
        }

    def _ensure_position_memory(self, symbol: str, price: float, candle_ts: int, active_risk: dict) -> None:
        sym = symbol.upper()
        mem = self.position_memory.get(sym)
        if isinstance(mem, dict):
            entry = _safe_float(mem.get("entry_price"), 0.0)
            peak = _safe_float(mem.get("peak_price"), 0.0)
            entry_ts = int(_safe_float(mem.get("entry_candle_ts"), 0.0))
            peak_ts = int(_safe_float(mem.get("peak_candle_ts"), 0.0))
            if entry > 0 and peak > 0:
                # Keep persisted risk in sync with runtime-adapted risk for open positions.
                changed = False
                for key, fallback in (
                    ("stop_loss", self.config.SPOT_STOP_LOSS_PCT),
                    ("take_profit", self.config.SPOT_TAKE_PROFIT_PCT),
                    ("trailing_stop", self.config.SPOT_TRAILING_STOP_PCT),
                    ("fraction", self.config.SPOT_BALANCE_RISK_FRACTION),
                ):
                    new_val = _safe_float(active_risk.get(key), fallback)
                    old_val = _safe_float(mem.get(key), fallback)
                    if abs(new_val - old_val) > 1e-12:
                        mem[key] = new_val
                        changed = True
                if changed:
                    self.position_memory[sym] = mem
                    self._save_position_memory()
                self.spot_state[sym] = {
                    "entry_price": entry,
                    "peak_price": peak,
                    "entry_candle_ts": float(entry_ts if entry_ts > 0 else candle_ts),
                    "peak_candle_ts": float(peak_ts if peak_ts > 0 else (entry_ts if entry_ts > 0 else candle_ts)),
                    "position_side": str(mem.get("position_side", "LONG") or "LONG").upper(),
                }
                return
        if price <= 0:
            return
        self.position_memory[sym] = {
            "entry_price": price,
            "peak_price": price,
            "stop_loss": _safe_float(active_risk.get("stop_loss"), self.config.SPOT_STOP_LOSS_PCT),
            "take_profit": _safe_float(active_risk.get("take_profit"), self.config.SPOT_TAKE_PROFIT_PCT),
            "trailing_stop": _safe_float(active_risk.get("trailing_stop"), self.config.SPOT_TRAILING_STOP_PCT),
            "fraction": _safe_float(active_risk.get("fraction"), self.config.SPOT_BALANCE_RISK_FRACTION),
            "entry_candle_ts": candle_ts,
            "peak_candle_ts": candle_ts,
            "position_side": "LONG",
            "source": "reconstructed",
        }
        self.spot_state[sym] = {
            "entry_price": price,
            "peak_price": price,
            "entry_candle_ts": float(candle_ts),
            "peak_candle_ts": float(candle_ts),
            "position_side": "LONG",
        }
        self._save_position_memory()
        logging.info("Восстановлено состояние позиции для %s с ценой входа %.6f после перезапуска.", sym, price)

    def _sync_position_peak(self, symbol: str, peak_price: float, peak_candle_ts: int) -> None:
        if peak_price <= 0:
            return
        sym = symbol.upper()
        item = self.position_memory.get(sym)
        if not isinstance(item, dict):
            return
        prev_peak = _safe_float(item.get("peak_price"), 0.0)
        if peak_price <= prev_peak:
            return
        item["peak_price"] = peak_price
        if peak_candle_ts > 0:
            item["peak_candle_ts"] = peak_candle_ts
        self._save_position_memory()

    def _lock_position_memory(self, symbol: str, entry_price: float, peak_price: float, candle_ts: int, active_risk: dict, source: str, position_side: str = "LONG") -> None:
        if entry_price <= 0:
            return
        sym = symbol.upper()
        side = "SHORT" if str(position_side).upper() == "SHORT" else "LONG"
        self.position_memory[sym] = {
            "entry_price": entry_price,
            "peak_price": max(peak_price, entry_price),
            "stop_loss": _safe_float(active_risk.get("stop_loss"), self.config.SPOT_STOP_LOSS_PCT),
            "take_profit": _safe_float(active_risk.get("take_profit"), self.config.SPOT_TAKE_PROFIT_PCT),
            "trailing_stop": _safe_float(active_risk.get("trailing_stop"), self.config.SPOT_TRAILING_STOP_PCT),
            "fraction": _safe_float(active_risk.get("fraction"), self.config.SPOT_BALANCE_RISK_FRACTION),
            "entry_candle_ts": candle_ts,
            "peak_candle_ts": candle_ts,
            "position_side": side,
            "source": source,
        }
        self.spot_state[sym] = {
            "entry_price": entry_price,
            "peak_price": max(peak_price, entry_price),
            "entry_candle_ts": float(candle_ts),
            "peak_candle_ts": float(candle_ts),
            "position_side": side,
        }
        self._save_position_memory()

    def _clear_position_memory(self, symbol: str) -> None:
        sym = symbol.upper()
        self.position_memory.pop(sym, None)
        self.spot_state.pop(sym, None)
        self._buy_settle_until_ts.pop(_symbol_key(sym), None)
        self._save_position_memory()

    def _risk_guard_state_path(self) -> Path:
        path = Path(self.config.RISK_GUARD_STATE_PATH)
        if not path.is_absolute():
            path = APP_BASE_DIR / path
        return path

    @staticmethod
    def _utc_day_key(ts: Optional[float] = None) -> str:
        now = datetime.fromtimestamp(ts if ts is not None else time.time(), tz=timezone.utc)
        return now.strftime("%Y-%m-%d")

    @staticmethod
    def _seconds_until_next_utc_day() -> int:
        now = datetime.now(timezone.utc)
        tomorrow = datetime(now.year, now.month, now.day, tzinfo=timezone.utc).timestamp() + 86_400
        return max(60, int(tomorrow - now.timestamp()))

    def _load_guard_state(self) -> None:
        path = self._risk_guard_state_path()
        if not path.exists():
            return
        try:
            payload = json.loads(path.read_text(encoding="utf-8-sig"))
            if not isinstance(payload, dict):
                return
            current_scope = (
                "training"
                if (self.config.AI_TRAINING_MODE and self.config.DRY_RUN)
                else ("live" if ((not self.config.DRY_RUN) and (not self.config.AI_TRAINING_MODE)) else "custom")
            )
            saved_scope = str(payload.get("state_scope", "") or "").strip().lower()
            if (not saved_scope) or saved_scope != current_scope:
                # Never mix guard counters/equity between training and live profiles.
                # Older files without scope are also ignored for safety.
                return
            self.guard_state["day_utc"] = str(payload.get("day_utc", "") or "")
            self.guard_state["start_equity_usdt"] = _safe_float(payload.get("start_equity_usdt"), 0.0)
            self.guard_state["max_equity_usdt"] = _safe_float(payload.get("max_equity_usdt"), 0.0)
            self.guard_state["trade_count"] = int(_safe_float(payload.get("trade_count"), 0.0))
            self.guard_state["lock_until_ts"] = _safe_float(payload.get("lock_until_ts"), 0.0)
            self.guard_state["lock_reason"] = str(payload.get("lock_reason", "") or "")
            self.guard_state["consecutive_cycle_errors"] = int(_safe_float(payload.get("consecutive_cycle_errors"), 0.0))
            self.guard_state["consecutive_api_errors"] = int(_safe_float(payload.get("consecutive_api_errors"), 0.0))
            self.guard_state["consecutive_logic_errors"] = int(_safe_float(payload.get("consecutive_logic_errors"), 0.0))
            self.guard_state["last_error_type"] = str(payload.get("last_error_type", "") or "")
            self.guard_state["last_error"] = str(payload.get("last_error", "") or "")
            raw_regime = payload.get("regime_edge_autotune", {})
            if isinstance(raw_regime, dict):
                restored: Dict[str, Dict[str, float]] = {}
                for k, v in raw_regime.items():
                    rk = str(k or "").strip().lower()
                    if not rk or not isinstance(v, dict):
                        continue
                    restored[rk] = {
                        "mult": _safe_float(v.get("mult"), 1.0),
                        "ema_pass": _safe_float(v.get("ema_pass"), 0.25),
                        "ema_margin": _safe_float(v.get("ema_margin"), 0.0),
                        "ema_quality": _safe_float(v.get("ema_quality"), 0.5),
                        "ema_conf": _safe_float(v.get("ema_conf"), 0.5),
                        "target_pass": _safe_float(v.get("target_pass"), 0.25),
                        "updated_ts": _safe_float(v.get("updated_ts"), 0.0),
                    }
                self.regime_edge_autotune = restored
            raw_feedback = payload.get("regime_trade_feedback", {})
            if isinstance(raw_feedback, dict):
                restored_feedback: Dict[str, Dict[str, float]] = {}
                for k, v in raw_feedback.items():
                    rk = str(k or "").strip().lower()
                    if not rk or not isinstance(v, dict):
                        continue
                    restored_feedback[rk] = {
                        "ema_pnl": _safe_float(v.get("ema_pnl"), 0.0),
                        "ema_win": _safe_float(v.get("ema_win"), 0.5),
                        "ema_quality": _safe_float(v.get("ema_quality"), 0.5),
                        "ema_edge": _safe_float(v.get("ema_edge"), 0.0),
                        "ema_exit_efficiency": _safe_float(v.get("ema_exit_efficiency"), 0.5),
                        "count": _safe_float(v.get("count"), 0.0),
                        "updated_ts": _safe_float(v.get("updated_ts"), 0.0),
                    }
                self.regime_trade_feedback = restored_feedback
            raw_symbol_feedback = payload.get("symbol_regime_trade_feedback", {})
            if isinstance(raw_symbol_feedback, dict):
                restored_symbol_feedback: Dict[str, Dict[str, float]] = {}
                for k, v in raw_symbol_feedback.items():
                    sk = str(k or "").strip().upper()
                    if not sk or not isinstance(v, dict):
                        continue
                    restored_symbol_feedback[sk] = {
                        "ema_pnl": _safe_float(v.get("ema_pnl"), 0.0),
                        "ema_win": _safe_float(v.get("ema_win"), 0.5),
                        "ema_decision": _safe_float(v.get("ema_decision"), 0.5),
                        "count": _safe_float(v.get("count"), 0.0),
                        "updated_ts": _safe_float(v.get("updated_ts"), 0.0),
                    }
                self.symbol_regime_trade_feedback = restored_symbol_feedback
            raw_symbol_exec_slippage = payload.get("symbol_exec_slippage_ema_bps", {})
            if isinstance(raw_symbol_exec_slippage, dict):
                self.symbol_exec_slippage_ema_bps = {
                    str(k or "").strip().upper(): _safe_float(v, 0.0)
                    for k, v in raw_symbol_exec_slippage.items()
                    if str(k or "").strip()
                }
            raw_symbol_exec_quality = payload.get("symbol_exec_quality_ema", {})
            if isinstance(raw_symbol_exec_quality, dict):
                self.symbol_exec_quality_ema = {
                    str(k or "").strip().upper(): _safe_float(v, 0.0)
                    for k, v in raw_symbol_exec_quality.items()
                    if str(k or "").strip()
                }
            raw_shared_learning = payload.get("shared_learning_feedback", {})
            if isinstance(raw_shared_learning, dict):
                self.shared_learning_feedback = {
                    "ema_pass": _safe_float(raw_shared_learning.get("ema_pass"), 0.25),
                    "ema_quality": _safe_float(raw_shared_learning.get("ema_quality"), 0.50),
                    "ema_conf": _safe_float(raw_shared_learning.get("ema_conf"), 0.50),
                    "ema_edge_margin": _safe_float(raw_shared_learning.get("ema_edge_margin"), 0.0),
                    "ema_trade_score": _safe_float(raw_shared_learning.get("ema_trade_score"), 0.50),
                    "ema_cycle_score": _safe_float(raw_shared_learning.get("ema_cycle_score"), 0.50),
                    "training_weight": _safe_float(raw_shared_learning.get("training_weight"), 0.0),
                    "live_weight": _safe_float(raw_shared_learning.get("live_weight"), 0.0),
                    "updated_ts": _safe_float(raw_shared_learning.get("updated_ts"), 0.0),
                }
            raw_conf_cal = payload.get("confidence_calibration_state", {})
            if isinstance(raw_conf_cal, dict):
                bucket_stats_raw = raw_conf_cal.get("bucket_stats", {})
                bucket_stats: Dict[str, Dict[str, float]] = {}
                if isinstance(bucket_stats_raw, dict):
                    for k, v in bucket_stats_raw.items():
                        bk = str(k or "").strip().lower()
                        if not bk or not isinstance(v, dict):
                            continue
                        bucket_stats[bk] = {
                            "ema_score": _safe_float(v.get("ema_score"), 0.50),
                            "ema_margin": _safe_float(v.get("ema_margin"), 0.0),
                            "count": _safe_float(v.get("count"), 0.0),
                            "updated_ts": _safe_float(v.get("updated_ts"), 0.0),
                        }
                self.confidence_calibration_state = {
                    "overall_ema_score": _safe_float(raw_conf_cal.get("overall_ema_score"), 0.50),
                    "overall_ema_margin": _safe_float(raw_conf_cal.get("overall_ema_margin"), 0.0),
                    "bucket_stats": bucket_stats,
                    "updated_ts": _safe_float(raw_conf_cal.get("updated_ts"), 0.0),
                }
            raw_missed = payload.get("missed_opportunity_feedback", {})
            if isinstance(raw_missed, dict):
                restored_missed: Dict[str, Dict[str, float]] = {}
                for k, v in raw_missed.items():
                    mk = str(k or "").strip().upper()
                    if not mk or not isinstance(v, dict):
                        continue
                    restored_missed[mk] = {
                        "ema_good": _safe_float(v.get("ema_good"), 0.5),
                        "ema_bad": _safe_float(v.get("ema_bad"), 0.5),
                        "ema_move": _safe_float(v.get("ema_move"), 0.0),
                        "count": _safe_float(v.get("count"), 0.0),
                        "updated_ts": _safe_float(v.get("updated_ts"), 0.0),
                    }
                self.missed_opportunity_feedback = restored_missed
        except Exception as exc:
            logging.warning("Не удалось загрузить состояние risk guard: %s", exc)

    def _save_guard_state(self) -> None:
        try:
            with self._guard_state_file_lock:
                path = self._risk_guard_state_path()
                path.parent.mkdir(parents=True, exist_ok=True)
                payload = {
                    "ts_utc": datetime.now(timezone.utc).isoformat(),
                    "state_scope": (
                        "training"
                        if (self.config.AI_TRAINING_MODE and self.config.DRY_RUN)
                        else ("live" if ((not self.config.DRY_RUN) and (not self.config.AI_TRAINING_MODE)) else "custom")
                    ),
                    "day_utc": str(self.guard_state.get("day_utc", "") or ""),
                    "start_equity_usdt": _safe_float(self.guard_state.get("start_equity_usdt"), 0.0),
                    "max_equity_usdt": _safe_float(self.guard_state.get("max_equity_usdt"), 0.0),
                    "trade_count": int(_safe_float(self.guard_state.get("trade_count"), 0.0)),
                    "lock_until_ts": _safe_float(self.guard_state.get("lock_until_ts"), 0.0),
                    "lock_reason": str(self.guard_state.get("lock_reason", "") or ""),
                    "consecutive_cycle_errors": int(_safe_float(self.guard_state.get("consecutive_cycle_errors"), 0.0)),
                    "consecutive_api_errors": int(_safe_float(self.guard_state.get("consecutive_api_errors"), 0.0)),
                    "consecutive_logic_errors": int(_safe_float(self.guard_state.get("consecutive_logic_errors"), 0.0)),
                    "last_error_type": str(self.guard_state.get("last_error_type", "") or ""),
                    "last_error": str(self.guard_state.get("last_error", "") or ""),
                    "regime_edge_autotune": dict(self.regime_edge_autotune),
                    "regime_trade_feedback": dict(self.regime_trade_feedback),
                    "symbol_regime_trade_feedback": dict(self.symbol_regime_trade_feedback),
                    "symbol_exec_slippage_ema_bps": dict(self.symbol_exec_slippage_ema_bps),
                    "symbol_exec_quality_ema": dict(self.symbol_exec_quality_ema),
                    "shared_learning_feedback": dict(self.shared_learning_feedback),
                    "confidence_calibration_state": dict(self.confidence_calibration_state),
                    "missed_opportunity_feedback": dict(self.missed_opportunity_feedback),
                }
                _write_json_atomic(path, payload)
        except Exception as exc:
            # On Windows file can be briefly locked by AV/indexer; keep bot running without noisy WARN flood.
            if isinstance(exc, PermissionError) or getattr(exc, "winerror", 0) == 5:
                logging.info("Состояние risk guard временно занято ОС, пропускаем этот цикл сохранения.")
            else:
                logging.warning("Не удалось сохранить состояние risk guard: %s", exc)

    def _rollover_guard_day(self, equity_usdt: float = 0.0) -> None:
        day = self._utc_day_key()
        current_day = str(self.guard_state.get("day_utc", "") or "")
        if day != current_day:
            self.guard_state["day_utc"] = day
            self.guard_state["start_equity_usdt"] = max(0.0, equity_usdt)
            self.guard_state["max_equity_usdt"] = max(0.0, equity_usdt)
            self.guard_state["trade_count"] = 0
            self.guard_state["lock_until_ts"] = 0.0
            self.guard_state["lock_reason"] = ""
            self.guard_state["consecutive_cycle_errors"] = 0
            self.guard_state["consecutive_api_errors"] = 0
            self.guard_state["consecutive_logic_errors"] = 0
            self.guard_state["last_error_type"] = ""
            self.guard_state["last_error"] = ""
            self._save_guard_state()
            return
        if equity_usdt > 0 and _safe_float(self.guard_state.get("start_equity_usdt"), 0.0) <= 0:
            self.guard_state["start_equity_usdt"] = equity_usdt
        if equity_usdt > _safe_float(self.guard_state.get("max_equity_usdt"), 0.0):
            self.guard_state["max_equity_usdt"] = equity_usdt

    def _unlock_guard_if_expired(self) -> None:
        lock_until = _safe_float(self.guard_state.get("lock_until_ts"), 0.0)
        if lock_until > 0 and time.time() >= lock_until:
            self.guard_state["lock_until_ts"] = 0.0
            self.guard_state["lock_reason"] = ""
            self._save_guard_state()

    def _engage_guard_lock(self, reason: str, duration_sec: int) -> None:
        if not self.config.RISK_GUARD_ENABLED:
            return
        now_ts = time.time()
        lock_until = max(_safe_float(self.guard_state.get("lock_until_ts"), 0.0), now_ts + max(60, int(duration_sec)))
        previous_reason = str(self.guard_state.get("lock_reason", "") or "")
        self.guard_state["lock_until_ts"] = lock_until
        self.guard_state["lock_reason"] = reason
        if previous_reason != reason:
            logging.warning(
                "Risk guard активирован: %s (до %s UTC)",
                reason,
                datetime.fromtimestamp(lock_until, tz=timezone.utc).isoformat(),
            )
        self._save_guard_state()

    def _guard_drawdown(self, equity_usdt: float) -> Tuple[float, float]:
        start_equity = _safe_float(self.guard_state.get("start_equity_usdt"), 0.0)
        if start_equity <= 0 or equity_usdt <= 0:
            return 0.0, 0.0
        dd_usdt = max(0.0, start_equity - equity_usdt)
        dd_pct = dd_usdt / start_equity if start_equity > 0 else 0.0
        return dd_usdt, dd_pct

    def _guard_payload(self, equity_usdt: float = 0.0) -> Dict[str, object]:
        self._unlock_guard_if_expired()
        now_ts = time.time()
        window_sec = max(30, int(self.config.API_SOFT_GUARD_WINDOW_SEC))
        while self.api_error_ts and (now_ts - self.api_error_ts[0]) > window_sec:
            self.api_error_ts.popleft()
        start = _safe_float(self.guard_state.get("start_equity_usdt"), 0.0)
        dd_usdt, dd_pct = self._guard_drawdown(equity_usdt if equity_usdt > 0 else start)
        lock_until = _safe_float(self.guard_state.get("lock_until_ts"), 0.0)
        current_equity = equity_usdt if equity_usdt > 0 else start
        return {
            "enabled": bool(self.config.RISK_GUARD_ENABLED),
            "day_utc": str(self.guard_state.get("day_utc", "") or ""),
            "start_equity_usdt": start,
            "max_equity_usdt": _safe_float(self.guard_state.get("max_equity_usdt"), 0.0),
            "current_equity_usdt": current_equity,
            "drawdown_usdt": dd_usdt,
            "drawdown_pct": dd_pct,
            "trade_count": int(_safe_float(self.guard_state.get("trade_count"), 0.0)),
            "max_trades_per_day": max(0, self.config.MAX_TRADES_PER_DAY),
            "locked": lock_until > time.time(),
            "lock_reason": str(self.guard_state.get("lock_reason", "") or ""),
            "lock_until_ts": lock_until,
            "consecutive_cycle_errors": int(_safe_float(self.guard_state.get("consecutive_cycle_errors"), 0.0)),
            "consecutive_api_errors": int(_safe_float(self.guard_state.get("consecutive_api_errors"), 0.0)),
            "consecutive_logic_errors": int(_safe_float(self.guard_state.get("consecutive_logic_errors"), 0.0)),
            "last_error_type": str(self.guard_state.get("last_error_type", "") or ""),
            "last_error": str(self.guard_state.get("last_error", "") or ""),
            "api_soft_guard_window_sec": window_sec,
            "api_soft_guard_max_errors": int(self.config.API_SOFT_GUARD_MAX_ERRORS),
            "api_error_count_window": len(self.api_error_ts),
        }

    def _register_trade_execution(self) -> None:
        if self.config.AI_TRAINING_MODE and self.config.DRY_RUN:
            return
        self._rollover_guard_day()
        self.guard_state["trade_count"] = int(_safe_float(self.guard_state.get("trade_count"), 0.0)) + 1
        max_trades = max(0, self.config.MAX_TRADES_PER_DAY)
        if self.config.RISK_GUARD_ENABLED and max_trades > 0 and int(self.guard_state["trade_count"]) >= max_trades:
            self._engage_guard_lock("max_trades_per_day", self._seconds_until_next_utc_day())
        else:
            self._save_guard_state()

    def _register_cycle_success(self) -> None:
        if int(_safe_float(self.guard_state.get("consecutive_cycle_errors"), 0.0)) == 0:
            return
        self.guard_state["consecutive_cycle_errors"] = 0
        self.guard_state["consecutive_api_errors"] = 0
        self.guard_state["consecutive_logic_errors"] = 0
        self.guard_state["last_error_type"] = ""
        self.guard_state["last_error"] = ""
        self._save_guard_state()

    def _register_cycle_error(self, error_message: str) -> None:
        is_api = self._is_api_related_error(error_message)
        if is_api:
            self.guard_state["consecutive_api_errors"] = int(_safe_float(self.guard_state.get("consecutive_api_errors"), 0.0)) + 1
            self.guard_state["consecutive_logic_errors"] = 0
            self.guard_state["last_error_type"] = "api_error"
        else:
            self.guard_state["consecutive_logic_errors"] = int(_safe_float(self.guard_state.get("consecutive_logic_errors"), 0.0)) + 1
            self.guard_state["consecutive_api_errors"] = 0
            self.guard_state["last_error_type"] = "logic_error"
        self.guard_state["consecutive_cycle_errors"] = int(_safe_float(self.guard_state.get("consecutive_cycle_errors"), 0.0)) + 1
        self.guard_state["last_error"] = str(error_message or "")
        self._register_api_soft_guard_if_needed(error_message)
        consecutive_logic_errors = int(_safe_float(self.guard_state.get("consecutive_logic_errors"), 0.0))
        if self.config.RISK_GUARD_ENABLED and consecutive_logic_errors >= max(1, self.config.MAX_CYCLE_ERRORS):
            self._engage_guard_lock("too_many_cycle_errors", max(60, self.config.ANOMALY_PAUSE_MINUTES * 60))
        else:
            self._save_guard_state()

    @staticmethod
    def _is_api_related_error(error_message: str) -> bool:
        text = str(error_message or "").lower()
        api_hints = (
            "authenticationerror",
            "networkerror",
            "requesttimeout",
            "ratelimit",
            "rate limit",
            "recvwindow",
            "timestamp",
            "exchange not available",
            "service unavailable",
            "ddosprotection",
            "connection reset",
            "temporarily unavailable",
            "invalid api-key",
            "requires \"apikey\" credential",
        )
        return any(h in text for h in api_hints)

    def _register_api_soft_guard_if_needed(self, error_message: str) -> None:
        if not self.config.RISK_GUARD_ENABLED:
            return
        if not self._is_api_related_error(error_message):
            return
        now_ts = time.time()
        window_sec = max(30, int(self.config.API_SOFT_GUARD_WINDOW_SEC))
        self.api_error_ts.append(now_ts)
        while self.api_error_ts and (now_ts - self.api_error_ts[0]) > window_sec:
            self.api_error_ts.popleft()
        max_errors = max(2, int(self.config.API_SOFT_GUARD_MAX_ERRORS))
        if len(self.api_error_ts) >= max_errors:
            pause_sec = max(60, int(self.config.API_SOFT_GUARD_PAUSE_SEC))
            self._engage_guard_lock("api_unstable_soft_guard", pause_sec)

    def _register_trader_api_errors(self) -> None:
        if not self.config.RISK_GUARD_ENABLED:
            return
        events = self.trader.consume_api_error_events()
        if not events:
            return
        now_ts = time.time()
        window_sec = max(30, int(self.config.API_SOFT_GUARD_WINDOW_SEC))
        for ts in events:
            self.api_error_ts.append(ts if ts > 0 else now_ts)
        while self.api_error_ts and (now_ts - self.api_error_ts[0]) > window_sec:
            self.api_error_ts.popleft()
        max_errors = max(2, int(self.config.API_SOFT_GUARD_MAX_ERRORS))
        if len(self.api_error_ts) >= max_errors:
            pause_sec = max(60, int(self.config.API_SOFT_GUARD_PAUSE_SEC))
            self._engage_guard_lock("api_unstable_soft_guard", pause_sec)

    @staticmethod
    def _price_jump_pct(ohlcv: list[list[float]]) -> float:
        if len(ohlcv) < 2:
            return 0.0
        current = _safe_float(ohlcv[-1][4], 0.0)
        previous = _safe_float(ohlcv[-2][4], 0.0)
        if current <= 0 or previous <= 0:
            return 0.0
        return (current / previous) - 1.0

    def _evaluate_guard_on_cycle(self, equity_usdt: float, market_volatility: float, price_jump_pct: float) -> str:
        if self.config.AI_TRAINING_MODE and self.config.DRY_RUN:
            return ""
        if not self.config.RISK_GUARD_ENABLED:
            return ""
        self._unlock_guard_if_expired()
        self._rollover_guard_day(equity_usdt)

        lock_until = _safe_float(self.guard_state.get("lock_until_ts"), 0.0)
        if lock_until > time.time():
            return str(self.guard_state.get("lock_reason", "") or "risk_guard_lock")

        dd_usdt, dd_pct = self._guard_drawdown(equity_usdt)
        if self.config.DAILY_MAX_DRAWDOWN_USDT > 0 and dd_usdt >= self.config.DAILY_MAX_DRAWDOWN_USDT:
            self._engage_guard_lock("daily_drawdown_usdt_limit", self._seconds_until_next_utc_day())
            return "daily_drawdown_usdt_limit"
        if self.config.DAILY_MAX_DRAWDOWN_PCT > 0 and dd_pct >= self.config.DAILY_MAX_DRAWDOWN_PCT:
            self._engage_guard_lock("daily_drawdown_pct_limit", self._seconds_until_next_utc_day())
            return "daily_drawdown_pct_limit"

        trades_today = int(_safe_float(self.guard_state.get("trade_count"), 0.0))
        max_trades = max(0, self.config.MAX_TRADES_PER_DAY)
        if max_trades > 0 and trades_today >= max_trades:
            self._engage_guard_lock("max_trades_per_day", self._seconds_until_next_utc_day())
            return "max_trades_per_day"

        if self.config.MAX_MARKET_VOLATILITY > 0 and market_volatility >= self.config.MAX_MARKET_VOLATILITY:
            self._engage_guard_lock("market_volatility_anomaly", max(60, self.config.ANOMALY_PAUSE_MINUTES * 60))
            return "market_volatility_anomaly"
        if self.config.MAX_PRICE_JUMP_PCT > 0 and abs(price_jump_pct) >= self.config.MAX_PRICE_JUMP_PCT:
            self._engage_guard_lock("price_jump_anomaly", max(60, self.config.ANOMALY_PAUSE_MINUTES * 60))
            return "price_jump_anomaly"

        side_streak_limit = max(2, int(self.config.SIDE_STOPLOSS_STREAK_GUARD))
        long_streak = int(_safe_float(self.side_stoploss_streak.get("LONG"), 0.0))
        short_streak = int(_safe_float(self.side_stoploss_streak.get("SHORT"), 0.0))
        if long_streak >= side_streak_limit:
            self.side_stoploss_streak["LONG"] = max(0, side_streak_limit - 1)
            self._engage_guard_lock("side_stoploss_streak_guard_long", max(180, self.config.ANOMALY_PAUSE_MINUTES * 60))
            return "side_stoploss_streak_guard_long"
        if short_streak >= side_streak_limit:
            self.side_stoploss_streak["SHORT"] = max(0, side_streak_limit - 1)
            self._engage_guard_lock("side_stoploss_streak_guard_short", max(180, self.config.ANOMALY_PAUSE_MINUTES * 60))
            return "side_stoploss_streak_guard_short"

        self._save_guard_state()
        return ""

    def _discover_open_positions_from_balance(self, balance_raw: object) -> Dict[str, float]:
        if not isinstance(balance_raw, dict):
            return {}
        markets = getattr(self.trader.exchange, "markets", {}) or {}
        result: Dict[str, float] = {}

        free_map = balance_raw.get("free", {}) if isinstance(balance_raw.get("free"), dict) else {}
        for currency, free_raw in free_map.items():
            ccy = str(currency or "").upper().strip()
            if not ccy or ccy == "USDT":
                continue
            free_amt = _safe_float(free_raw, 0.0)
            if free_amt <= 1e-12:
                continue
            symbol = f"{ccy}/USDT"
            market = markets.get(symbol)
            if self.trader._is_spot_usdt_market(market):
                result[symbol] = max(result.get(symbol, 0.0), free_amt)

        for key, raw in balance_raw.items():
            if not isinstance(raw, dict):
                continue
            ccy = str(key or "").upper().strip()
            if not ccy or ccy in {"FREE", "USED", "TOTAL", "INFO", "USDT"}:
                continue
            free_amt = _safe_float(raw.get("free"), 0.0)
            if free_amt <= 1e-12:
                continue
            symbol = f"{ccy}/USDT"
            market = markets.get(symbol)
            if self.trader._is_spot_usdt_market(market):
                result[symbol] = max(result.get(symbol, 0.0), free_amt)
        return result

    def _startup_reconcile_positions(self) -> None:
        if self.config.AI_TRAINING_MODE and self.config.DRY_RUN:
            return
        if not self.config.POSITION_RECONCILE_ON_START:
            return
        try:
            balance = self.trader.fetch_balance_safe(self.config.SYMBOL)
            open_positions = self._discover_open_positions_from_balance(balance)
            now_candle_ts = int(time.time() * 1000)

            restored = 0
            for symbol in sorted(open_positions):
                price = self.trader.last_price(symbol, ttl_sec=2.0, force_refresh=True)
                if price <= 0:
                    continue
                risk = self._risk_from_memory(symbol) or {
                    "fraction": self.config.SPOT_BALANCE_RISK_FRACTION,
                    "stop_loss": self.config.SPOT_STOP_LOSS_PCT,
                    "take_profit": self.config.SPOT_TAKE_PROFIT_PCT,
                    "trailing_stop": self.config.SPOT_TRAILING_STOP_PCT,
                }
                self._ensure_position_memory(symbol, price, now_candle_ts, risk)
                restored += 1
                if not self.active_symbol:
                    self.active_symbol = symbol

            stale_symbols = [sym for sym in list(self.position_memory.keys()) if sym not in open_positions]
            for sym in stale_symbols:
                self._clear_position_memory(sym)

            if restored > 0 or stale_symbols:
                logging.info(
                    "Синхронизация позиций при старте: восстановлено=%d очищено_устаревших=%d active_symbol=%s",
                    restored,
                    len(stale_symbols),
                    self.active_symbol or "-",
                )
        except Exception as exc:
            logging.warning("Сбой синхронизации позиций при старте: %s", exc)

    def _request_stop(self, _signum: int, _frame: object) -> None:
        logging.info("Получен запрос на остановку.")
        self.stop_requested = True

    def run(self):
        self._cleanup_stale_runtime_artifacts()
        self._setup_signal_handlers()
        exec_keeper = _WindowsExecutionKeeper(
            enabled=bool(getattr(self.config, "BACKGROUND_KEEP_AWAKE", True)),
            use_away_mode=bool(getattr(self.config, "BACKGROUND_KEEP_AWAKE_AWAYMODE", True)),
        )
        if exec_keeper.start():
            logging.info("Background keep-awake enabled (Windows).")
        logging.info(f"Бот запущен | DRY_RUN={self.config.DRY_RUN} | AI_TRAINING_MODE={self.config.AI_TRAINING_MODE}")
        logging.info(
            "[TRADE] Режим=%s | Автопилот=%s | AI-сигнал=%s | Пара=%s",
            self.config.BOT_MODE or ("training" if self.config.AI_TRAINING_MODE else ("live" if not self.config.DRY_RUN else "custom")),
            self.config.AUTO_PILOT_MODE,
            self.config.USE_AI_SIGNAL,
            self.config.SYMBOL,
        )
        mode_label = "futures" if self.config.TRADE_MARKET == "futures" else "spot"
        self._write_live_status({
            "ts_utc": datetime.now(timezone.utc).isoformat(),
            "mode": mode_label,
            "cycle_index": 0,
            "event": "booting",
            "symbol": self.config.SYMBOL,
            "auto_symbol_selection": self.config.AUTO_SYMBOL_SELECTION,
            "ai_auto_risk": self.config.AI_AUTO_RISK,
            "dry_run": self.config.DRY_RUN,
            "recent_orders": list(self.recent_orders),
            "risk_guard": self._guard_payload(),
        })
        
        try:
            cycle_index = 0
            while not self.stop_requested:
                cycle_index += 1
                cycle_started_ts = time.time()
                self._last_cycle_started_ts = cycle_started_ts
                try:
                    self._run_cycle(cycle_index)
                    self._register_cycle_success()
                except Exception as exc:
                    logging.exception("Необработанная ошибка в цикле %s", cycle_index)
                    self._register_cycle_error(str(exc))
                    self._write_live_status({
                        "ts_utc": datetime.now(timezone.utc).isoformat(),
                        "mode": mode_label,
                        "cycle_index": cycle_index,
                        "event": "error",
                        "symbol": self.config.SYMBOL,
                        "auto_symbol_selection": self.config.AUTO_SYMBOL_SELECTION,
                        "ai_auto_risk": self.config.AI_AUTO_RISK,
                        "dry_run": self.config.DRY_RUN,
                        "error": str(exc),
                        "recent_orders": list(self.recent_orders),
                        "risk_guard": self._guard_payload(),
                    })
                finally:
                    cycle_elapsed_sec = max(0.0, time.time() - cycle_started_ts)
                    self._last_cycle_completed_ts = time.time()
                    watchdog_res = self._cycle_runtime_watchdog(
                        cycle_index=cycle_index,
                        cycle_elapsed_sec=cycle_elapsed_sec,
                        last_event=str(getattr(self, "_last_cycle_event", "") or ""),
                    )
                    if bool(watchdog_res.get("triggered", False)):
                        logging.warning(
                            "Cycle watchdog | action=%s | cycle=%s | elapsed=%.2fs | event=%s | reason=%s",
                            str(watchdog_res.get("action", "") or "-"),
                            cycle_index,
                            cycle_elapsed_sec,
                            str(watchdog_res.get("event", "") or "-"),
                            str(watchdog_res.get("reason", "") or "-"),
                        )

                self._sleep_with_countdown(self._next_cycle_sleep_seconds())
        finally:
            if self.ai_engine:
                self.ai_engine.flush()
            exec_keeper.stop()
            logging.info("Бот остановлен.")

    def _run_cycle(self, cycle_index: int):
        cycle_t0 = time.perf_counter()
        stage_profile: Dict[str, float] = {
            "market_fetch_ms": 0.0,
            "features_ms": 0.0,
            "signal_ms": 0.0,
            "order_ms": 0.0,
            "cycle_ms": 0.0,
        }
        mode_label = "futures" if self.config.TRADE_MARKET == "futures" else "spot"
        self._register_trader_api_errors()
        # Early heartbeat so GUI/live monitor stays fresh even during long API calls.
        busy_symbol = self.active_symbol or self.config.SYMBOL
        prev_live = dict(self._last_live_status_payload) if isinstance(self._last_live_status_payload, dict) else {}
        prev_busy_symbol = str(prev_live.get("symbol", "") or "").strip()
        keep_prev_busy_quote = bool(prev_busy_symbol and (_symbol_key(prev_busy_symbol) == _symbol_key(busy_symbol)))
        self._write_live_status({
            "ts_utc": datetime.now(timezone.utc).isoformat(),
            "mode": mode_label,
            "cycle_index": cycle_index,
            "event": "cycle_busy",
            "symbol": busy_symbol,
            # Busy heartbeat keeps last known quote for the same symbol,
            # but stays neutral on symbol switch to avoid cross-pair confusion.
            "price": _safe_float(prev_live.get("price"), 0.0) if keep_prev_busy_quote else 0.0,
            "sma": _safe_float(prev_live.get("sma"), 0.0) if keep_prev_busy_quote else 0.0,
            "deviation": _safe_float(prev_live.get("deviation"), 0.0) if keep_prev_busy_quote else 0.0,
            "auto_symbol_selection": self.config.AUTO_SYMBOL_SELECTION,
            "ai_auto_risk": self.config.AI_AUTO_RISK,
            "dry_run": self.config.DRY_RUN,
            "cycle_elapsed_ms": 0.0,
            "recent_orders": list(self.recent_orders),
            "risk_guard": self._guard_payload(),
        })
        # Keep AI status fresh for GUI tabs even in cycles without new labels.
        if self.ai_engine:
            try:
                self.ai_engine.heartbeat_status(min_interval_sec=2.0)
            except Exception:
                pass
        # Pull recent API failures before symbol selection to reduce overload loops.
        self._register_trader_api_errors()
        symbol, universe_total, universe_scanned = self._select_symbol()
        market = self.trader.get_market(symbol)
        if not market or market.get("active") is False:
            if self.active_symbol == symbol:
                self.active_symbol = None
            logging.warning("Пара %s неактивна, цикл пропущен.", symbol)
            self._write_live_status({
                "ts_utc": datetime.now(timezone.utc).isoformat(),
                "mode": mode_label,
                "cycle_index": cycle_index,
                "event": "no_trade_signal",
                "symbol": symbol,
                "auto_symbol_selection": self.config.AUTO_SYMBOL_SELECTION,
                "ai_auto_risk": self.config.AI_AUTO_RISK,
                "universe_total": universe_total,
                "universe_scanned": universe_scanned,
                "dry_run": self.config.DRY_RUN,
                "cycle_elapsed_ms": round((time.perf_counter() - cycle_t0) * 1000.0, 1),
                "error": f"symbol_inactive:{symbol}",
                "recent_orders": list(self.recent_orders),
                "risk_guard": self._guard_payload(),
            })
            return

        t_fetch = time.perf_counter()
        cycle_bundle = self.trader.fetch_cycle_bundle(symbol, self.config.TIMEFRAME, self.config.AI_LOOKBACK)
        stage_profile.update(cycle_bundle.get("profile", {}) if isinstance(cycle_bundle.get("profile"), dict) else {})
        ohlcv = cycle_bundle.get("ohlcv", []) if isinstance(cycle_bundle.get("ohlcv"), list) else []
        hot_price = _safe_float(cycle_bundle.get("price"), 0.0)
        price, sma = self._price_sma_from_ohlcv(ohlcv)
        if price <= 0 and hot_price > 0:
            price = hot_price
        # Per-symbol sanity guard against cross-symbol price contamination.
        # Example this prevents: SOL suddenly receiving ETH-like price in one cycle.
        sym_guard_key = symbol.upper()
        prev_sym_price = _safe_float(self._symbol_last_price.get(sym_guard_key), 0.0)
        if price > 0 and prev_sym_price > 0:
            jump = abs((price / prev_sym_price) - 1.0)
            if jump > 0.35:
                logging.warning(
                    "Market price sanity guard: symbol=%s prev=%.6f now=%.6f jump=%.2f%%. Cycle skipped.",
                    symbol,
                    prev_sym_price,
                    price,
                    jump * 100.0,
                )
                self._register_api_soft_guard_if_needed("market_data_unavailable:price_sanity_guard")
                self._write_live_status({
                    "ts_utc": datetime.now(timezone.utc).isoformat(),
                    "mode": mode_label,
                    "cycle_index": cycle_index,
                    "event": "market_data_unavailable",
                    "symbol": symbol,
                    "auto_symbol_selection": self.config.AUTO_SYMBOL_SELECTION,
                    "ai_auto_risk": self.config.AI_AUTO_RISK,
                    "universe_total": universe_total,
                    "universe_scanned": universe_scanned,
                    "price": prev_sym_price,
                    "sma": 0.0,
                    "deviation": 0.0,
                    "dry_run": self.config.DRY_RUN,
                    "cycle_elapsed_ms": round((time.perf_counter() - cycle_t0) * 1000.0, 1),
                    "error": "price_sanity_guard",
                    "recent_orders": list(self.recent_orders),
                    "risk_guard": self._guard_payload(),
                })
                return
        if price > 0:
            self._symbol_last_price[sym_guard_key] = float(price)
        stage_profile["market_fetch_ms"] = round(max(stage_profile.get("market_fetch_ms", 0.0), (time.perf_counter() - t_fetch) * 1000.0), 1)
        latest_candle_ts = self._latest_candle_ts(ohlcv)
        now_ms = int(time.time() * 1000)
        max_stale_ms = max(1, int(self.config.MAX_DATA_STALE_SEC)) * 1000
        market_data_stale = (latest_candle_ts <= 0) or ((now_ms - latest_candle_ts) > max_stale_ms)
        market_data_stale_sec = (max(0, now_ms - latest_candle_ts) / 1000.0) if latest_candle_ts > 0 else float(max_stale_ms) / 1000.0
        market_volatility = self._market_volatility(ohlcv)
        price_jump_pct = self._price_jump_pct(ohlcv)
        if price <= 0:
            self._register_cycle_error("market_data_unavailable:empty_price")
            self._write_live_status({
                "ts_utc": datetime.now(timezone.utc).isoformat(),
                "mode": mode_label,
                "cycle_index": cycle_index,
                "event": "market_data_unavailable",
                "symbol": symbol,
                "auto_symbol_selection": self.config.AUTO_SYMBOL_SELECTION,
                "ai_auto_risk": self.config.AI_AUTO_RISK,
                "universe_total": universe_total,
                "universe_scanned": universe_scanned,
                "price": 0.0,
                "sma": 0.0,
                "dry_run": self.config.DRY_RUN,
                "cycle_elapsed_ms": round((time.perf_counter() - cycle_t0) * 1000.0, 1),
                "error": "empty_or_invalid_price",
                "recent_orders": list(self.recent_orders),
                "risk_guard": self._guard_payload(),
            })
            return

        pos = cycle_bundle.get("position", {}) if isinstance(cycle_bundle.get("position"), dict) else {}
        if not pos:
            pos = self.trader.get_position(symbol)
        exchange_usdt_free = _safe_float(pos.get("usdt_free"), 0.0)
        exchange_base_free = _safe_float(pos.get("base_free"), 0.0)
        exchange_balance_source = str(pos.get("balance_source", "unknown") or "unknown").strip().lower()
        usdt_free, base_free, base_currency = self._effective_balances(symbol, pos)
        position_side = str(pos.get("position_side", "") or "").upper().strip()
        position_qty = _safe_float(pos.get("position_qty"), base_free)
        if position_qty > 0:
            base_free = position_qty
        if position_side not in {"LONG", "SHORT"}:
            position_side = "LONG" if base_free > 0 else ""
        equity_usdt = self._estimate_cycle_equity_usdt(symbol, usdt_free, base_free, price)
        exchange_equity_usdt = max(0.0, exchange_usdt_free) + (max(0.0, exchange_base_free) * max(0.0, price))
        trusted_exchange_balance = exchange_balance_source in {"exchange", "cache_exchange"}
        if trusted_exchange_balance:
            self._last_confirmed_exchange_usdt_free = max(0.0, exchange_usdt_free)
            self._last_confirmed_exchange_equity_usdt = max(0.0, exchange_equity_usdt)
            self._last_confirmed_exchange_balance_source = exchange_balance_source
        exchange_usdt_report = (
            max(0.0, exchange_usdt_free)
            if trusted_exchange_balance
            else max(0.0, self._last_confirmed_exchange_usdt_free)
        )
        exchange_equity_report = (
            max(0.0, exchange_equity_usdt)
            if trusted_exchange_balance
            else max(0.0, self._last_confirmed_exchange_equity_usdt)
        )
        if self.paper_usdt_free is None:
            self.paper_usdt_free = self._default_paper_usdt()
        paper_usdt_free = max(0.0, _safe_float(self.paper_usdt_free, 0.0))
        paper_base_free = max(0.0, _safe_float(self.paper_positions.get(symbol, 0.0), 0.0))
        paper_equity_usdt = self._estimate_cycle_equity_usdt(symbol, paper_usdt_free, paper_base_free, price)
        self._register_trader_api_errors()
        risk_eval = self.risk_policy.evaluate(
            lambda: self._evaluate_guard_on_cycle(equity_usdt, market_volatility, price_jump_pct)
        )
        guard_reason = str(risk_eval.reason or "")
        buy_blocked_by_guard = bool(risk_eval.blocked)
        if base_free > 0:
            self.active_symbol = symbol
        elif self.config.TRADE_MARKET != "futures" and not (self.config.AI_TRAINING_MODE and self.config.DRY_RUN):
            held_symbol = self._detect_open_symbol_from_balance(pos.get("raw"), current_symbol=symbol)
            if held_symbol and held_symbol != symbol:
                self.active_symbol = held_symbol
                logging.info("Detected open balance on %s, prioritizing it next cycle.", held_symbol)
                self._write_live_status({
                    "ts_utc": datetime.now(timezone.utc).isoformat(),
                    "mode": mode_label,
                    "cycle_index": cycle_index,
                    "event": "no_trade_signal",
                    "symbol": symbol,
                    "auto_symbol_selection": self.config.AUTO_SYMBOL_SELECTION,
                    "ai_auto_risk": self.config.AI_AUTO_RISK,
                    "universe_total": universe_total,
                    "universe_scanned": universe_scanned,
                    "price": price,
                    "sma": sma,
                    "deviation": 0.0,
                    "usdt_free": usdt_free,
                    "base_currency": base_currency,
                    "base_free": base_free,
                    "spot_buy_usdt": 0.0,
                    "market_volatility": market_volatility,
                    "price_jump_pct": price_jump_pct,
                    "active_risk_fraction": 0.0,
                    "active_stop_loss_pct": 0.0,
                    "active_take_profit_pct": 0.0,
                    "active_trailing_stop_pct": 0.0,
                    "entry_price": 0.0,
                    "peak_price": 0.0,
                    "pnl_pct": 0.0,
                    "sell_reason": "",
                    "dry_run": self.config.DRY_RUN,
                    "cycle_elapsed_ms": round((time.perf_counter() - cycle_t0) * 1000.0, 1),
                    "ai": self._ai_payload(None),
                    "recent_orders": list(self.recent_orders),
                    "error": f"open_position_on_other_symbol:{held_symbol}",
                    "guard_block_reason": guard_reason,
                    "risk_guard": self._guard_payload(equity_usdt),
                })
                return

        sma_buy_trigger, sma_sell_trigger, deviation = self._get_sma_signal(price, sma)
        buy_trigger, sell_trigger = sma_buy_trigger, sma_sell_trigger

        t_features = time.perf_counter()
        cache_key = symbol.upper()
        cache_hit = False
        cached = self._cycle_compute_cache.get(cache_key, {})
        if isinstance(cached, dict):
            cached_ts = int(_safe_float(cached.get("latest_candle_ts"), 0.0))
            if latest_candle_ts > 0 and cached_ts == latest_candle_ts:
                cache_hit = True
        if cache_hit:
            market_ctx = dict(cached.get("market_ctx", {})) if isinstance(cached.get("market_ctx"), dict) else {}
            self.runtime_market_ctx = dict(market_ctx)
            self.runtime_params = dict(cached.get("runtime_params", {})) if isinstance(cached.get("runtime_params"), dict) else self.adaptive_agent.current_base_params()
            ai_context_pack = dict(cached.get("ai_context_pack", {})) if isinstance(cached.get("ai_context_pack"), dict) else {}
            extra_features = dict(cached.get("extra_features", {})) if isinstance(cached.get("extra_features"), dict) else {}
            anomalies = dict(cached.get("anomalies", {})) if isinstance(cached.get("anomalies"), dict) else {}
            ai_signal = cached.get("ai_signal") if isinstance(cached.get("ai_signal"), AISignal) else None
            ai_action = str(cached.get("ai_action", "")).upper().strip()
            ai_conf = _safe_float(cached.get("ai_conf"), 0.0)
            ai_entry_quality = _safe_float(cached.get("ai_entry_quality"), 0.0)
            ai_filter_ok = bool(cached.get("ai_filter_ok", False))
            drift_score_runtime = _safe_float(cached.get("drift_score_runtime"), 0.0)
            drift_active_runtime = bool(cached.get("drift_active_runtime", False))
            side_perf = dict(cached.get("side_perf", {})) if isinstance(cached.get("side_perf"), dict) else {}
            short_model_weak = bool(cached.get("short_model_weak", False))
            short_deficit = _safe_float(cached.get("short_deficit"), 0.0)
            stage_profile["features_ms"] = 0.2
            stage_profile["signal_ms"] = 0.2
        else:
            market_ctx = self.adaptive_agent.detect_market_regime(ohlcv)
            self.runtime_market_ctx = market_ctx
            self.runtime_params = self.adaptive_agent.build_runtime_overrides(market_ctx)
            ai_context_pack = self.market_ctx_engine.build(
                trader=self.trader,
                symbol=symbol,
                ohlcv_1m=ohlcv,
                runtime_params=self.runtime_params,
                ai_quality=0.0,
                expected_edge_pct=0.0,
                regime_label=str(market_ctx.get("regime", "flat")),
            )
            extra_features = ai_context_pack.get("extra_features", {}) if isinstance(ai_context_pack, dict) else {}
            anomalies = ai_context_pack.get("anomalies", {}) if isinstance(ai_context_pack, dict) else {}
            stage_profile["features_ms"] = round((time.perf_counter() - t_features) * 1000.0, 1)
            if self.ai_engine:
                self.ai_engine.set_runtime_mode(self.config.DRY_RUN)
            t_signal = time.perf_counter()
            ai_signal = self.ai_engine.predict(ohlcv, symbol, extra_features=extra_features) if self.ai_engine else None
            stage_profile["signal_ms"] = round((time.perf_counter() - t_signal) * 1000.0, 1)
            ai_action = str(ai_signal.action).upper().strip() if ai_signal else ""
            ai_conf = ai_signal.calibrated_confidence if ai_signal and ai_signal.calibrated_confidence > 0 else (ai_signal.confidence if ai_signal else 0.0)
            ai_entry_quality = self._quality_for_entry(ai_signal)
            ai_filter_ok = bool(ai_signal.trade_filter_pass) if ai_signal else False
            drift_score_runtime = 0.0
            drift_active_runtime = False
            if self.ai_engine and hasattr(self.ai_engine, "diagnostics"):
                try:
                    diag = self.ai_engine.diagnostics()
                    if isinstance(diag, dict):
                        drift_score_runtime = _safe_float(diag.get("drift_score"), 0.0)
                        drift_active_runtime = bool(diag.get("drift_active", False))
                except Exception:
                    drift_score_runtime = 0.0
                    drift_active_runtime = False
            side_perf = {}
            short_model_weak = False
            short_deficit = 0.0
            if self.ai_engine and hasattr(self.ai_engine, "side_performance_snapshot"):
                try:
                    side_perf = self.ai_engine.side_performance_snapshot()
                    short_deficit = _safe_float(side_perf.get("short_deficit"), 0.0)
                    short_labels = int(_safe_float(side_perf.get("short_labels"), 0.0))
                    short_model_weak = short_labels >= 60 and short_deficit >= 0.12
                except Exception:
                    side_perf = {}
                    short_model_weak = False
                    short_deficit = 0.0
            self._cycle_compute_cache[cache_key] = {
                "latest_candle_ts": int(latest_candle_ts),
                "market_ctx": dict(market_ctx) if isinstance(market_ctx, dict) else {},
                "runtime_params": dict(self.runtime_params),
                "ai_context_pack": dict(ai_context_pack) if isinstance(ai_context_pack, dict) else {},
                "extra_features": dict(extra_features) if isinstance(extra_features, dict) else {},
                "anomalies": dict(anomalies) if isinstance(anomalies, dict) else {},
                "ai_signal": ai_signal,
                "ai_action": ai_action,
                "ai_conf": float(ai_conf),
                "ai_entry_quality": float(ai_entry_quality),
                "ai_filter_ok": bool(ai_filter_ok),
                "drift_score_runtime": float(drift_score_runtime),
                "drift_active_runtime": bool(drift_active_runtime),
                "side_perf": dict(side_perf) if isinstance(side_perf, dict) else {},
                "short_model_weak": bool(short_model_weak),
                "short_deficit": float(short_deficit),
            }
        advisory: Optional[AdvisorySignal] = None
        advisory_edge_bias_pct = 0.0
        advisory_meta: Dict[str, object] = {
            "enabled": bool(self.advisory_provider and self.advisory_provider.enabled),
            "source": "",
            "ok": False,
            "action": "HOLD",
            "confidence": 0.0,
            "quality": 0.0,
            "edge_bias_pct": 0.0,
            "applied": False,
            "reason": "",
            "weight": 0.0,
            "mode": "",
        }
        mode_for_advisory = str(self.config.BOT_MODE or ("training" if self.config.AI_TRAINING_MODE else "live")).strip().lower()
        if mode_for_advisory == "training":
            advisory_weight_raw = _safe_float(self.config.ADVISORY_PROVIDER_WEIGHT_TRAINING, self.config.ADVISORY_PROVIDER_WEIGHT)
        else:
            advisory_weight_raw = _safe_float(self.config.ADVISORY_PROVIDER_WEIGHT_LIVE, self.config.ADVISORY_PROVIDER_WEIGHT)
        advisory_weight = _clamp(advisory_weight_raw, 0.0, 0.35)
        advisory_meta["weight"] = float(advisory_weight)
        advisory_meta["mode"] = mode_for_advisory
        if self.advisory_provider is not None and self.advisory_provider.enabled:
            t_adv = time.perf_counter()
            advisory = self.advisory_provider.fetch(
                symbol=symbol,
                timeframe=self.config.TIMEFRAME,
                mode=mode_for_advisory,
                regime=str(market_ctx.get("regime", "flat")),
            )
            stage_profile["signal_ms"] = round(stage_profile.get("signal_ms", 0.0) + ((time.perf_counter() - t_adv) * 1000.0), 1)
            if advisory is not None:
                advisory_meta.update(
                    {
                        "source": str(advisory.source),
                        "ok": bool(advisory.ok),
                        "action": str(advisory.action),
                        "confidence": float(advisory.confidence),
                        "quality": float(advisory.quality),
                        "edge_bias_pct": float(advisory.edge_bias_pct),
                        "reason": str(advisory.reason or ""),
                    }
                )
                if advisory.ok and ai_signal is not None:
                    if advisory.action in {"LONG", "SHORT"} and advisory.action == ai_action:
                        ai_conf = min(1.0, ai_conf + (advisory_weight * advisory.confidence * 0.25))
                        ai_entry_quality = min(1.0, ai_entry_quality + (advisory_weight * advisory.quality * 0.20))
                        advisory_edge_bias_pct = advisory.edge_bias_pct * advisory_weight
                        advisory_meta["applied"] = True
                    elif advisory.action in {"LONG", "SHORT"} and advisory.action != ai_action:
                        penalty = advisory_weight * max(0.0, advisory.confidence - 0.45) * 0.35
                        ai_conf = max(0.0, ai_conf - penalty)
                        ai_entry_quality = max(0.0, ai_entry_quality - (penalty * 0.75))
                        advisory_edge_bias_pct = -abs(advisory.edge_bias_pct) * advisory_weight
                        advisory_meta["applied"] = True
        self.runtime_advisory = advisory_meta
        ai_entry_min_quality = self._param_f("AI_ENTRY_MIN_QUALITY", self.config.AI_ENTRY_MIN_QUALITY)
        ai_min_conf_required = self.config.AI_MIN_CONFIDENCE
        training_runtime = bool(self.config.DRY_RUN and self.config.AI_TRAINING_MODE)
        flat_market_entry_filter = False
        micro_noise = self._micro_noise_score(ohlcv)
        regime_name = str(market_ctx.get("regime", "flat") or "flat").lower().strip()
        regime_flags_raw = market_ctx.get("flags", [])
        regime_flags = {str(x).lower().strip() for x in regime_flags_raw} if isinstance(regime_flags_raw, list) else set()
        regime_bucket = self._market_regime_bucket(
            regime_name,
            regime_flags,
            market_volatility=market_volatility,
            anomaly_score=_safe_float(anomalies.get("anomaly_score"), 0.0) if isinstance(anomalies, dict) else 0.0,
            momentum_speed=_safe_float(extra_features.get("momentum_speed"), 0.0) if isinstance(extra_features, dict) else 0.0,
        )
        if isinstance(self.runtime_market_ctx, dict):
            self.runtime_market_ctx["bucket"] = regime_bucket
        symbol_regime_bias = self._symbol_regime_trade_bias(symbol, regime_name)
        symbol_live_bias = self._symbol_regime_live_bias(symbol, regime_name)
        cycle_bias = self._cycle_adaptive_bias(symbol, regime_name)
        execution_profile = self._execution_profile_bias()
        shared_learning_bias = self._shared_learning_bias()
        execution_stress = self._execution_stress_mode()
        confidence_bias = self._confidence_calibration_bias(ai_conf)
        missed_opportunity_bias = self._missed_opportunity_bias(symbol, regime_bucket)
        anti_fragile_mode = self._anti_fragile_mode(symbol, regime_bucket)
        no_trade_streak_cycles = int(max(0, self._no_trade_streak_cycles))
        tiny_balance_weak_market = (
            equity_usdt > 0
            and equity_usdt <= 5.0
            and (regime_name in {"flat", "range"} or ("low_vol" in regime_flags) or ("low_volatility" in regime_flags))
        )
        low_vol_flat_market = regime_name in {"flat", "range"} and (
            "low_vol" in regime_flags or "low_volatility" in regime_flags
        )
        if low_vol_flat_market:
            flat_market_entry_filter = True
            # In live flat/low-vol markets require stronger quality/confidence.
            # In paper-training we still tighten, but softer to avoid full dead-zones.
            flat_quality_tighten = 0.020 if training_runtime else 0.035
            flat_conf_tighten = 0.016 if training_runtime else 0.028
            ai_entry_min_quality = min(0.88, ai_entry_min_quality + flat_quality_tighten)
            ai_min_conf_required = min(0.91, ai_min_conf_required + flat_conf_tighten)
        if micro_noise >= 0.62:
            noise_quality_tighten = 0.028 if training_runtime else 0.05
            noise_conf_tighten = 0.022 if training_runtime else 0.04
            ai_entry_min_quality = min(0.92, ai_entry_min_quality + noise_quality_tighten)
            ai_min_conf_required = min(0.94, ai_min_conf_required + noise_conf_tighten)
        if short_model_weak and self.config.TRADE_MARKET == "futures":
            # When downside detector lags behind long quality, tighten entries.
            # For LONG signals we apply a softer penalty, because spot entries are long-only.
            tighten = min(0.08, short_deficit * 0.20)
            if ai_action == "LONG":
                tighten *= 0.30
            ai_entry_min_quality = min(0.95, ai_entry_min_quality + tighten)
            ai_min_conf_required = min(0.97, ai_min_conf_required + (tighten * 0.80))
        if drift_score_runtime >= 0.78:
            drift_tighten = min(0.06, max(0.0, (drift_score_runtime - 0.78) * 0.10))
            ai_entry_min_quality = min(0.95, ai_entry_min_quality + drift_tighten)
            ai_min_conf_required = min(0.95, ai_min_conf_required + (drift_tighten * 0.80))
        if tiny_balance_weak_market:
            # Keep strict filters, but avoid full freeze for micro-balance in flat sessions.
            ai_entry_min_quality = max(self.config.AI_ENTRY_MIN_QUALITY + 0.02, ai_entry_min_quality - 0.035)
            ai_min_conf_required = max(self.config.AI_MIN_CONFIDENCE + 0.01, ai_min_conf_required - 0.025)
        if (
            low_vol_flat_market
            and not bool(market_ctx.get("dangerous", False))
            and ai_action == "LONG"
        ):
            # Adaptive soft-relax in weak market to reduce "dead" cycles.
            relax_quality = 0.015
            relax_conf = 0.012
            if tiny_balance_weak_market:
                relax_quality += 0.010
                relax_conf += 0.008
            if drift_active_runtime:
                relax_quality *= 0.55
                relax_conf *= 0.55
            ai_entry_min_quality = max(self.config.AI_ENTRY_MIN_QUALITY + 0.005, ai_entry_min_quality - relax_quality)
            ai_min_conf_required = max(self.config.AI_MIN_CONFIDENCE + 0.005, ai_min_conf_required - relax_conf)
        if regime_bucket == "flat_low_vol":
            protective_pressure = 0.0
            if self.loss_streak_live >= 2:
                protective_pressure += min(0.040, 0.012 * float(self.loss_streak_live))
            if _safe_float(symbol_live_bias.get("risk_mult"), 1.0) < 0.98:
                protective_pressure += min(0.020, (1.0 - _safe_float(symbol_live_bias.get("risk_mult"), 1.0)) * 0.12)
            if _safe_float(symbol_regime_bias.get("edge_delta"), 0.0) > 0:
                protective_pressure += min(0.015, _safe_float(symbol_regime_bias.get("edge_delta"), 0.0) * 8.0)
            if protective_pressure > 0:
                ai_entry_min_quality = min(0.95, ai_entry_min_quality + protective_pressure)
                ai_min_conf_required = min(0.95, ai_min_conf_required + min(0.030, protective_pressure * 0.75))
        if training_runtime:
            # Training mode should collect more market episodes; keep live strictness untouched.
            ai_entry_min_quality = max(
                self.config.AI_ENTRY_MIN_QUALITY - 0.10,
                ai_entry_min_quality - 0.10,
            )
            ai_min_conf_required = max(
                self.config.AI_MIN_CONFIDENCE - 0.08,
                ai_min_conf_required - 0.08,
            )
        elif (not self.config.DRY_RUN) and (not self.config.AI_TRAINING_MODE):
            # Live mode: softer gate in weak market to reduce overblocking.
            ai_entry_min_quality = max(
                self.config.AI_ENTRY_MIN_QUALITY - 0.02,
                ai_entry_min_quality - 0.03,
            )
            ai_min_conf_required = max(
                self.config.AI_MIN_CONFIDENCE - 0.02,
                ai_min_conf_required - 0.02,
            )
        ai_entry_min_quality = _clamp(
            ai_entry_min_quality
            + _safe_float(symbol_regime_bias.get("quality_delta"), 0.0)
            + _safe_float(symbol_live_bias.get("quality_delta"), 0.0)
            + _safe_float(cycle_bias.get("quality_delta"), 0.0)
            + _safe_float(execution_profile.get("quality_delta"), 0.0),
            max(0.18, self.config.AI_ENTRY_MIN_QUALITY - 0.10),
            0.96,
        )
        ai_entry_min_quality = _clamp(
            ai_entry_min_quality
            + _safe_float(shared_learning_bias.get("quality_delta"), 0.0)
            + _safe_float(confidence_bias.get("quality_delta"), 0.0)
            + _safe_float(missed_opportunity_bias.get("quality_delta"), 0.0)
            + _safe_float(anti_fragile_mode.get("quality_delta"), 0.0)
            + _safe_float(execution_stress.get("quality_delta"), 0.0),
            max(0.18, self.config.AI_ENTRY_MIN_QUALITY - 0.10),
            0.96,
        )
        ai_min_conf_required = _clamp(
            ai_min_conf_required
            + _safe_float(cycle_bias.get("conf_delta"), 0.0)
            + _safe_float(execution_profile.get("conf_delta"), 0.0)
            + _safe_float(shared_learning_bias.get("conf_delta"), 0.0)
            + _safe_float(confidence_bias.get("conf_delta"), 0.0)
            + _safe_float(anti_fragile_mode.get("conf_delta"), 0.0)
            + _safe_float(execution_stress.get("conf_delta"), 0.0),
            max(0.18, self.config.AI_MIN_CONFIDENCE - 0.10),
            0.98,
        )
        if training_runtime and not bool(market_ctx.get("dangerous", False)):
            # Paper-training should see more near-miss episodes so the model learns from them.
            ai_entry_min_quality = min(ai_entry_min_quality, max(0.34, self.config.AI_ENTRY_MIN_QUALITY - 0.11))
            ai_min_conf_required = min(ai_min_conf_required, max(0.46, self.config.AI_MIN_CONFIDENCE - 0.10))
        deadlock_soft_relax = 0.0
        deadlock_conf_relax = 0.0
        deadlock_debounce_relax = 0
        if (
            no_trade_streak_cycles >= (14 if training_runtime else 42)
            and not bool(market_ctx.get("dangerous", False))
            and regime_bucket in {"flat_low_vol", "trend", "range", "mixed", "flat"}
        ):
            extra_steps = max(0, (no_trade_streak_cycles - (14 if training_runtime else 42)) // (10 if training_runtime else 18))
            deadlock_soft_relax = min(0.08 if training_runtime else 0.045, 0.012 + (extra_steps * (0.010 if training_runtime else 0.006)))
            deadlock_conf_relax = min(0.07 if training_runtime else 0.04, 0.010 + (extra_steps * (0.009 if training_runtime else 0.005)))
            if regime_bucket == "flat_low_vol":
                deadlock_soft_relax *= 0.75
                deadlock_conf_relax *= 0.75
            if self.loss_streak_live >= 2:
                deadlock_soft_relax *= 0.55
                deadlock_conf_relax *= 0.55
            ai_entry_min_quality = max(
                0.30 if training_runtime else max(0.36, self.config.AI_ENTRY_MIN_QUALITY - 0.08),
                ai_entry_min_quality - deadlock_soft_relax,
            )
            ai_min_conf_required = max(
                0.42 if training_runtime else max(0.50, self.config.AI_MIN_CONFIDENCE - 0.07),
                ai_min_conf_required - deadlock_conf_relax,
            )
            deadlock_debounce_relax = 1 + min(1, extra_steps // 2)
        ai_quality_ok = ai_entry_quality >= ai_entry_min_quality
        ai_gate_ok = bool(ai_signal) and ai_filter_ok and ai_conf >= ai_min_conf_required and ai_quality_ok
        mtf_pass, mtf_meta = self._mtf_confirmation(symbol)
        mtf_soft_bypass = False
        # Used in mtf soft-bypass checks below; recalculated later with active_risk.
        expected_edge_pct = 0.0
        if ai_gate_ok and not mtf_pass:
            if (
                self.config.DRY_RUN
                and self.config.AI_TRAINING_MODE
                and ai_action == "LONG"
                and micro_noise < 0.66
                and not bool(market_ctx.get("dangerous", False))
                and ai_entry_quality >= (ai_entry_min_quality * 0.90)
                and ai_conf >= (ai_min_conf_required * 0.90)
            ):
                mtf_soft_bypass = True
            elif (
                self.config.DRY_RUN
                and self.config.AI_TRAINING_MODE
                and no_trade_streak_cycles >= 12
                and ai_action == "LONG"
                and regime_bucket in {"flat_low_vol", "range", "flat", "mixed", "trend"}
                and micro_noise < 0.72
                and not bool(market_ctx.get("dangerous", False))
                and ai_entry_quality >= max(0.34, ai_entry_min_quality * 0.84)
                and ai_conf >= max(0.48, ai_min_conf_required * 0.86)
                and _safe_float(mtf_meta.get("m5_trend"), 0.0) >= -0.0018
                and _safe_float(mtf_meta.get("m15_trend"), 0.0) >= -0.0026
            ):
                mtf_soft_bypass = True
            elif (
                (not self.config.DRY_RUN)
                and (not self.config.AI_TRAINING_MODE)
                and ai_action == "LONG"
                and micro_noise < 0.58
                and not bool(market_ctx.get("dangerous", False))
                and ai_entry_quality >= (ai_entry_min_quality * 0.96)
                and ai_conf >= (ai_min_conf_required * 0.96)
                and (abs(_safe_float(ai_signal.score if ai_signal else 0.0, 0.0)) >= 0.22)
            ):
                mtf_soft_bypass = True
            else:
                ai_gate_ok = False
        if ai_gate_ok and micro_noise >= 0.78 and base_free <= 0:
            ai_gate_ok = False
        ai_enabled = bool(self.ai_engine) and self.config.USE_AI_SIGNAL
        short_entry_trigger = False
        if ai_enabled:
            if self.config.TRADE_MARKET == "futures":
                buy_trigger = bool(ai_gate_ok and ai_signal and ai_signal.action == "LONG")
                short_entry_trigger = bool(ai_gate_ok and ai_signal and ai_signal.action == "SHORT")
                sell_trigger = False
            else:
                # Strict AI mode: never open a new position unless the AI gate explicitly approves LONG.
                buy_trigger = bool(ai_gate_ok and ai_signal and ai_signal.action == "LONG")
                # Keep AI-driven exits, while risk exits are still handled below via sell_reason.
                sell_trigger = bool(ai_gate_ok and ai_signal and ai_signal.action == "SHORT")
        signal_debounce_required = 2
        signal_debounce_pass = True
        signal_debounce_count = 0
        signal_debounce_mode = "adaptive"

        has_open_position = base_free > 0
        if has_open_position and not (self.config.AI_TRAINING_MODE and self.config.DRY_RUN):
            if self.config.TRADE_MARKET == "futures":
                has_open_position = base_free > 0
            else:
                has_open_position = self.trader.can_sell_position(symbol, base_free, price)
            if not has_open_position:
                logging.info("Ignoring dust position for trade logic: %s %.10f (below min notional).", symbol, base_free)
        if has_open_position and not position_side:
            position_side = str(self.spot_state.get(symbol, {}).get("position_side", "") or "").upper().strip()
        if has_open_position and position_side not in {"LONG", "SHORT"}:
            position_side = "LONG"
        symbol_key = _symbol_key(symbol)
        if has_open_position:
            self._buy_settle_until_ts.pop(symbol_key, None)
        if base_free <= 0 and symbol.upper() in self.position_memory:
            now_ts = time.time()
            settle_until = _safe_float(self._buy_settle_until_ts.get(symbol_key), 0.0)
            if settle_until > now_ts:
                logging.info(
                    "Позиция по %s еще синхронизируется после BUY (wait %.1fs), память не очищаем.",
                    symbol,
                    max(0.0, settle_until - now_ts),
                )
            else:
                logging.info("Открытая позиция по %s не обнаружена, очищаем сохраненное состояние позиции.", symbol)
                self._clear_position_memory(symbol)

        locked_risk = self._risk_from_memory(symbol) if has_open_position else None
        adaptive_symbol_cooldown = self._adaptive_symbol_market_cooldown(symbol, regime_name)
        active_risk = locked_risk if locked_risk else self._get_active_risk_profile(symbol, ai_signal, ohlcv)
        atr_pct = self._atr_pct(ohlcv)
        dynamic_time_stop_candles = self._dynamic_time_stop_candles(active_risk, ai_signal, atr_pct)
        expected_edge_pct = self._expected_edge_pct(active_risk, ai_entry_quality)
        if advisory_edge_bias_pct != 0.0:
            expected_edge_pct = _clamp(expected_edge_pct + advisory_edge_bias_pct, -0.05, 0.20)
        momentum_speed = _safe_float(extra_features.get("momentum_speed"), 0.0) if isinstance(extra_features, dict) else 0.0
        anomaly_score = _safe_float(anomalies.get("anomaly_score"), 0.0) if isinstance(anomalies, dict) else 0.0
        if anomaly_score >= 0.70:
            active_risk["fraction"] = max(self.config.AI_RISK_FRACTION_MIN, active_risk["fraction"] * 0.60)
        if (not self.config.DRY_RUN) and drift_score_runtime >= 0.78:
            drift_scale = max(0.72, 0.90 - min(0.18, (drift_score_runtime - 0.78) * 0.10))
            active_risk["fraction"] = max(self.config.AI_RISK_FRACTION_MIN, active_risk["fraction"] * drift_scale)
        if (not self.config.DRY_RUN) and short_model_weak and self.config.TRADE_MARKET == "futures":
            risk_penalty = min(0.20, short_deficit * 0.70)
            if ai_action == "LONG":
                risk_penalty *= 0.50
            active_risk["fraction"] = max(
                self.config.AI_RISK_FRACTION_MIN,
                active_risk["fraction"] * (1.0 - risk_penalty),
            )
        if not self.config.DRY_RUN:
            active_risk["fraction"] = max(
                self.config.AI_RISK_FRACTION_MIN,
                active_risk["fraction"] * self._loss_streak_risk_scale(),
            )
        active_risk["fraction"] = max(
            self.config.AI_RISK_FRACTION_MIN,
            active_risk["fraction"]
            * _safe_float(symbol_live_bias.get("risk_mult"), 1.0)
            * _safe_float(execution_profile.get("risk_mult"), 1.0),
        )
        active_risk["fraction"] = max(
            self.config.AI_RISK_FRACTION_MIN,
            active_risk["fraction"]
            * _safe_float(execution_stress.get("risk_mult"), 1.0)
            * _safe_float(missed_opportunity_bias.get("risk_mult"), 1.0)
            * _safe_float(anti_fragile_mode.get("risk_mult"), 1.0),
        )
        active_risk["take_profit"] = max(
            self.config.AI_TAKE_PROFIT_MIN,
            min(self.config.AI_TAKE_PROFIT_MAX, active_risk["take_profit"] * _safe_float(anti_fragile_mode.get("tp_mult"), 1.0)),
        )
        active_risk["trailing_stop"] = max(
            self.config.AI_TRAILING_STOP_MIN,
            min(self.config.AI_TRAILING_STOP_MAX, active_risk["trailing_stop"] * _safe_float(anti_fragile_mode.get("ts_mult"), 1.0)),
        )
        active_risk["stop_loss"] = max(
            self.config.AI_STOP_LOSS_MIN,
            min(self.config.AI_STOP_LOSS_MAX, active_risk["stop_loss"] * _safe_float(anti_fragile_mode.get("sl_mult"), 1.0)),
        )
        active_risk, entry_tp_cap = self._apply_entry_tp_reachability_cap(
            symbol=symbol,
            has_open_position=has_open_position,
            active_risk=active_risk,
            market_ctx=market_ctx,
            ai_signal=ai_signal,
            atr_pct=atr_pct,
            cycle_equity_usdt=equity_usdt,
        )
        active_risk, tp_adaptation = self._apply_open_position_tp_adaptation(
            symbol=symbol,
            has_open_position=has_open_position,
            current_price=price,
            cycle_equity_usdt=equity_usdt,
            active_risk=active_risk,
            market_ctx=market_ctx,
            ai_signal=ai_signal,
            momentum_speed=momentum_speed,
            anomaly_score=anomaly_score,
        )
        adaptive_cooldown_score = _safe_float(adaptive_symbol_cooldown.get("score"), 0.0)
        if adaptive_cooldown_score >= 0.24:
            active_risk["fraction"] = max(
                self.config.AI_RISK_FRACTION_MIN,
                active_risk["fraction"] * _clamp(0.96 - (adaptive_cooldown_score * 0.18), 0.80, 0.96),
            )
        # Entry TP sanity cap for tiny balances in weak spot market.
        if (
            not has_open_position
            and self.config.TRADE_MARKET != "futures"
            and equity_usdt > 0
            and equity_usdt <= 5.0
        ):
            regime_name_local = str(market_ctx.get("regime", "flat") or "flat").lower().strip()
            flags_local_raw = market_ctx.get("flags", [])
            flags_local = {str(x).lower().strip() for x in flags_local_raw} if isinstance(flags_local_raw, list) else set()
            weak_market = regime_name_local in {"flat", "range"} or ("low_vol" in flags_local) or ("low_volatility" in flags_local)
            if weak_market:
                old_tp = _safe_float(active_risk.get("take_profit"), 0.0)
                old_ts = _safe_float(active_risk.get("trailing_stop"), 0.0)
                tp_cap = 0.0125
                ts_cap = 0.0090
                active_risk["take_profit"] = min(max(self.config.AI_TAKE_PROFIT_MIN, old_tp), tp_cap)
                active_risk["trailing_stop"] = min(max(self.config.AI_TRAILING_STOP_MIN, old_ts), ts_cap)
                if old_tp > active_risk["take_profit"] + 1e-12:
                    tp_adaptation = dict(tp_adaptation)
                    tp_adaptation["entry_cap_applied"] = True
                    tp_adaptation["entry_cap_reason"] = "tiny_balance_weak_market"
                    tp_adaptation["entry_tp_from"] = float(old_tp)
                    tp_adaptation["entry_tp_to"] = float(active_risk["take_profit"])
                    tp_adaptation["entry_ts_from"] = float(old_ts)
                    tp_adaptation["entry_ts_to"] = float(active_risk["trailing_stop"])
        if bool(entry_tp_cap.get("applied", False)):
            tp_adaptation = dict(tp_adaptation)
            tp_adaptation["entry_reachability_cap"] = dict(entry_tp_cap)
        post_time_stop_block = False
        post_time_stop_wait_candles = 0
        block_until_candle_ts = int(self.time_stop_buy_block_until_candle_ts.get(symbol.upper(), 0))
        if latest_candle_ts > 0 and block_until_candle_ts > latest_candle_ts:
            post_time_stop_block = True
            post_time_stop_wait_candles = self._candles_since(block_until_candle_ts, latest_candle_ts, self.config.TIMEFRAME)
        elif block_until_candle_ts > 0:
            self.time_stop_buy_block_until_candle_ts.pop(symbol.upper(), None)

        expected_edge_pct = self._expected_edge_pct(active_risk, ai_entry_quality)
        if advisory_edge_bias_pct != 0.0:
            expected_edge_pct = _clamp(expected_edge_pct + advisory_edge_bias_pct, -0.05, 0.20)
        min_expected_edge_pct = self._dynamic_min_expected_edge_pct(
            symbol=symbol,
            active_risk=active_risk,
            market_volatility=market_volatility,
            micro_noise=micro_noise,
            equity_usdt=equity_usdt,
        )
        if flat_market_entry_filter:
            min_expected_edge_pct += 0.0003
        if (not self.config.DRY_RUN) and self.exec_slippage_ema_bps >= 9.0:
            min_expected_edge_pct += 0.0012
        if short_model_weak and self.config.TRADE_MARKET == "futures":
            short_edge_penalty = min(0.0015, short_deficit * 0.004)
            if ai_action == "LONG":
                short_edge_penalty *= 0.40
            min_expected_edge_pct += short_edge_penalty
        if tiny_balance_weak_market:
            # For tiny balance in weak markets allow slightly lower edge to avoid deadlock.
            min_expected_edge_pct = max(0.0026, min_expected_edge_pct - 0.0024)
        if (
            low_vol_flat_market
            and ai_action == "LONG"
            and not bool(market_ctx.get("dangerous", False))
            and ai_entry_quality >= (ai_entry_min_quality * 0.92)
            and ai_conf >= (ai_min_conf_required * 0.94)
        ):
            # Keep edge filter, but avoid overblocking when quality/confidence are close to passing.
            min_expected_edge_pct = max(0.0019, min_expected_edge_pct - 0.0008)
        if (
            regime_name in {"flat", "range"}
            and ai_action == "LONG"
            and not bool(market_ctx.get("dangerous", False))
            and ai_filter_ok
            and ai_conf >= max(0.50, ai_min_conf_required * 0.92)
            and ai_entry_quality >= max(0.40, ai_entry_min_quality * 0.90)
        ):
            # Additional adaptive softening in range markets to avoid long dead-zones.
            quality_headroom = max(0.0, ai_entry_quality - ai_entry_min_quality)
            relax_extra = min(0.0012, 0.00045 + (quality_headroom * 0.45))
            if self.config.AI_TRAINING_MODE and self.config.DRY_RUN:
                relax_extra *= 1.10
            min_expected_edge_pct = max(0.0014, min_expected_edge_pct - relax_extra)
        if (
            tiny_balance_weak_market
            and ai_action == "LONG"
            and ai_filter_ok
            and ai_conf >= max(0.58, ai_min_conf_required * 0.90)
            and ai_entry_quality >= max(0.48, ai_entry_min_quality * 0.88)
        ):
            # Small balance mode: avoid excessive dead-zones while keeping positive edge filter.
            min_expected_edge_pct = max(0.0012, min_expected_edge_pct - 0.00035)
        if training_runtime:
            # Looser edge floor in paper-training to avoid zero-trade regime.
            min_expected_edge_pct = max(0.0007, min_expected_edge_pct - 0.0021)
        elif (not self.config.DRY_RUN) and (not self.config.AI_TRAINING_MODE):
            # Live mode: keep edge guard, but avoid dead periods in flat low-vol.
            min_expected_edge_pct = max(0.00145, min_expected_edge_pct - 0.00085)
        min_expected_edge_pct = max(
            0.0010,
            min_expected_edge_pct
            + _safe_float(symbol_regime_bias.get("edge_delta"), 0.0)
            + _safe_float(symbol_live_bias.get("edge_delta"), 0.0)
            + _safe_float(cycle_bias.get("edge_delta"), 0.0)
            + _safe_float(shared_learning_bias.get("edge_delta"), 0.0),
        )
        min_expected_edge_pct = max(0.0010, min_expected_edge_pct + _safe_float(confidence_bias.get("edge_delta"), 0.0))
        min_expected_edge_pct = max(0.0010, min_expected_edge_pct + _safe_float(missed_opportunity_bias.get("edge_delta"), 0.0))
        min_expected_edge_pct = max(0.0010, min_expected_edge_pct + _safe_float(anti_fragile_mode.get("edge_delta"), 0.0))
        min_expected_edge_pct = max(0.0010, min_expected_edge_pct + _safe_float(execution_profile.get("edge_delta"), 0.0))
        min_expected_edge_pct = max(0.0010, min_expected_edge_pct + _safe_float(execution_stress.get("edge_delta"), 0.0))
        if adaptive_cooldown_score >= 0.18:
            min_expected_edge_pct = max(0.0010, min_expected_edge_pct + min(0.0011, adaptive_cooldown_score * 0.0014))
            signal_debounce_required = max(signal_debounce_required, 3 if adaptive_cooldown_score >= 0.42 else 2)

        # Autonomous regime-aware edge adaptation (internal, no manual knobs).
        regime_mult = self._regime_edge_multiplier(
            regime=regime_name,
            flags=regime_flags,
            dangerous=bool(market_ctx.get("dangerous", False)),
        )
        min_expected_edge_pct = max(0.0010, min_expected_edge_pct * regime_mult)
        # Anti-deadlock: if bot sits too long without trades in calm market,
        # gradually relax edge floor inside strict safety bounds.
        if (
            (not has_open_position)
            and (not bool(market_ctx.get("dangerous", False)))
            and (self._no_trade_streak_cycles >= (18 if training_runtime else 40))
            and (regime_name in {"flat", "range", "trend", "impulse"})
            and (ai_conf >= max(0.50, ai_min_conf_required * 0.88))
            and (ai_entry_quality >= max(0.40, ai_entry_min_quality * 0.84))
        ):
            extra_steps = max(0, (self._no_trade_streak_cycles - (18 if training_runtime else 40)) // (12 if training_runtime else 20))
            deadlock_relax = min(0.00135 if training_runtime else 0.0010, 0.00018 + (extra_steps * (0.00014 if training_runtime else 0.00009)))
            floor_limit = 0.0012 if ((not self.config.DRY_RUN) and (not self.config.AI_TRAINING_MODE)) else 0.0008
            min_expected_edge_pct = max(floor_limit, min_expected_edge_pct - deadlock_relax)
        if (
            training_runtime
            and (not has_open_position)
            and (not bool(market_ctx.get("dangerous", False)))
            and ai_action == "LONG"
            and ai_filter_ok
            and ai_conf >= max(0.48, ai_min_conf_required * 0.84)
            and ai_entry_quality >= max(0.34, ai_entry_min_quality * 0.82)
            and regime_bucket in {"flat_low_vol", "range", "flat", "mixed", "trend"}
        ):
            training_edge_relax = 0.00022
            if no_trade_streak_cycles >= 14:
                training_edge_relax += min(0.00055, (no_trade_streak_cycles - 14) * 0.00003)
            if regime_bucket == "flat_low_vol":
                training_edge_relax *= 0.85
            min_expected_edge_pct = max(0.0007, min_expected_edge_pct - training_edge_relax)
        expected_edge_ok = expected_edge_pct >= min_expected_edge_pct
        self._update_regime_edge_autotune(
            regime=regime_name,
            flags=regime_flags,
            dangerous=bool(market_ctx.get("dangerous", False)),
            expected_edge_pct=expected_edge_pct,
            min_expected_edge_pct=min_expected_edge_pct,
            ai_quality=ai_entry_quality,
            ai_conf=ai_conf,
            expected_edge_ok=expected_edge_ok,
            ai_quality_ok=ai_quality_ok,
            has_open_position=has_open_position,
        )
        cycle_snapshot = CycleSnapshot(
            symbol=symbol,
            price=float(price),
            market_volatility=float(market_volatility),
            price_jump_pct=float(price_jump_pct),
            equity_usdt=float(equity_usdt),
            usdt_free=float(usdt_free),
            base_free=float(base_free),
            has_open_position=bool(has_open_position),
            regime=str(regime_name),
            dangerous=bool(market_ctx.get("dangerous", False)),
            micro_noise=float(micro_noise),
            anomaly_score=float(anomaly_score),
            ai_action=str(ai_action),
            ai_conf=float(ai_conf),
            ai_quality=float(ai_entry_quality),
            expected_edge_pct=float(expected_edge_pct),
            min_expected_edge_pct=float(min_expected_edge_pct),
        )
        debounce_required = 2
        if regime_name in {"flat", "range"} or ("low_vol" in regime_flags) or ("low_volatility" in regime_flags):
            debounce_required += 1
        if micro_noise >= 0.74:
            debounce_required += 1
        if anomaly_score >= 0.80:
            debounce_required += 1
        strong_edge = expected_edge_pct >= (min_expected_edge_pct + 0.0030)
        very_strong_signal = (
            ai_conf >= (ai_min_conf_required + 0.10)
            and ai_entry_quality >= (ai_entry_min_quality + 0.08)
            and strong_edge
        )
        if very_strong_signal:
            debounce_required -= 1
        if regime_name == "trend" and micro_noise < 0.58 and anomaly_score < 0.65:
            debounce_required -= 1
        debounce_required += int(_safe_float(cycle_bias.get("debounce_delta"), 0.0))
        debounce_required += int(_safe_float(symbol_live_bias.get("debounce_delta"), 0.0))
        debounce_required += int(_safe_float(execution_profile.get("debounce_delta"), 0.0))
        signal_debounce_required = max(1, min(4, debounce_required))
        if training_runtime:
            signal_debounce_required = max(1, signal_debounce_required - 1)
            if (
                ai_action == "LONG"
                and not bool(market_ctx.get("dangerous", False))
                and ai_conf >= max(0.48, ai_min_conf_required * 0.90)
                and ai_entry_quality >= max(0.36, ai_entry_min_quality * 0.88)
            ):
                signal_debounce_required = max(1, signal_debounce_required - 1)
        elif (not self.config.DRY_RUN) and (not self.config.AI_TRAINING_MODE):
            signal_debounce_required = max(1, signal_debounce_required - 1)
        if deadlock_debounce_relax > 0:
            signal_debounce_required = max(1, signal_debounce_required - deadlock_debounce_relax)
        debounce_action = ""
        if ai_action == "LONG":
            debounce_action = "LONG"
        elif self.config.TRADE_MARKET == "futures" and ai_action == "SHORT":
            debounce_action = "SHORT"
        if ai_enabled and debounce_action and not has_open_position:
            sym_key = f"{symbol.upper()}|{debounce_action}"
            if (debounce_action == "LONG" and buy_trigger) or (debounce_action == "SHORT" and short_entry_trigger):
                self.signal_debounce_counter[sym_key] = int(self.signal_debounce_counter.get(sym_key, 0)) + 1
            else:
                self.signal_debounce_counter[sym_key] = 0
            opposite = "SHORT" if debounce_action == "LONG" else "LONG"
            self.signal_debounce_counter[f"{symbol.upper()}|{opposite}"] = 0
            signal_debounce_count = int(self.signal_debounce_counter.get(sym_key, 0))
            signal_debounce_pass = signal_debounce_count >= signal_debounce_required
            if not signal_debounce_pass:
                if debounce_action == "LONG":
                    buy_trigger = False
                else:
                    short_entry_trigger = False
        else:
            self.signal_debounce_counter[f"{symbol.upper()}|LONG"] = 0
            self.signal_debounce_counter[f"{symbol.upper()}|SHORT"] = 0
        edge_override_buy = False
        if (
            ai_enabled
            and ai_signal is not None
            and ai_action == "LONG"
            and ai_filter_ok
            and not has_open_position
            and mtf_pass
            and anomaly_score < 0.70
            and not post_time_stop_block
        ):
            strong_edge = expected_edge_pct >= max(min_expected_edge_pct + 0.0024, 0.0082)
            near_quality = ai_entry_quality >= (ai_entry_min_quality * 0.90)
            near_conf = ai_conf >= (ai_min_conf_required * 0.93)
            if strong_edge and near_quality and near_conf:
                edge_override_buy = True
                buy_trigger = True
        if has_open_position:
            self._ensure_position_memory(symbol, price, latest_candle_ts, active_risk)

        effective_base_for_logic = base_free if has_open_position else 0.0
        entry_price, peak_price, pnl, sell_reason, candles_in_pos, candles_since_peak = self._manage_position_state(
            symbol,
            price,
            effective_base_for_logic,
            active_risk,
            dynamic_time_stop_candles,
            latest_candle_ts,
            ai_signal,
            position_side=position_side or "LONG",
        )
        overlay_decision = self.overlay_engine.apply(
            active_risk=active_risk,
            ai_quality=ai_entry_quality,
            momentum_speed=momentum_speed,
            anomaly_score=anomaly_score,
            pnl_pct=pnl,
            has_open_position=has_open_position,
        )
        active_risk = dict(overlay_decision.risk)
        if has_open_position:
            self._sync_position_peak(symbol, peak_price, latest_candle_ts)
        stop_loss_price, take_profit_price, trailing_stop_price = self._risk_levels(entry_price, peak_price, active_risk, position_side=position_side or "LONG")
        if sell_reason:
            sell_trigger = True
        if self.config.TRADE_MARKET == "futures" and has_open_position:
            if position_side == "LONG" and ai_action == "SHORT" and ai_gate_ok:
                sell_trigger = True
                if not sell_reason:
                    sell_reason = "signal_flip"
            elif position_side == "SHORT" and ai_action == "LONG" and ai_gate_ok:
                sell_trigger = True
                if not sell_reason:
                    sell_reason = "signal_flip"
        manual_sell = bool(has_open_position and self._consume_manual_sell_signal(symbol))
        if manual_sell:
            sell_trigger = True
            sell_reason = "manual_force_sell"
        forced_sell_reason = self._consume_forced_exit(symbol) if has_open_position else ""
        if forced_sell_reason:
            sell_trigger = True
            sell_reason = forced_sell_reason

        spot_buy_usdt = self._compute_spot_buy_usdt(usdt_free, active_risk["fraction"])
        has_dust_position = (
            self.config.TRADE_MARKET != "futures"
            and base_free > 0
            and not has_open_position
            and not (self.config.AI_TRAINING_MODE and self.config.DRY_RUN)
        )
        topup_usdt = 0.0
        if has_dust_position:
            topup_usdt = self._required_topup_usdt(symbol, base_free, price)
            if topup_usdt > 0:
                spendable = max(0.0, usdt_free * 0.98)
                spot_buy_usdt = min(spendable, max(spot_buy_usdt, topup_usdt))

        dust_recovery_buy = bool(
            self.config.SPOT_DUST_RECOVERY_BUY
            and has_dust_position
            and topup_usdt > 0
        )
        static_symbol_cooldown_sec = self._symbol_internal_cooldown_remaining_sec(symbol)
        adaptive_symbol_cooldown = self._adaptive_symbol_market_cooldown(symbol, regime_name)
        adaptive_symbol_cooldown_sec = int(max(0.0, _safe_float(adaptive_symbol_cooldown.get("seconds"), 0.0)))
        symbol_internal_cooldown_sec = max(static_symbol_cooldown_sec, adaptive_symbol_cooldown_sec)
        symbol_internal_cooldown_block = (
            symbol_internal_cooldown_sec > 0
            and not has_open_position
            and not dust_recovery_buy
        )

        self.current_cycle_trade_meta = {
            "symbol": symbol,
            "entry_price_est": price,
            "entry_candle_ts": latest_candle_ts,
            "entry_quality": ai_entry_quality,
            "ai_confidence": ai_conf,
            "expected_edge_pct": expected_edge_pct,
            "dynamic_time_stop_candles": dynamic_time_stop_candles,
            "active_risk": dict(active_risk),
            "market_ctx": dict(market_ctx),
            "runtime_params": dict(self.runtime_params),
            "atr_pct": atr_pct,
            "tp_adaptation": dict(tp_adaptation),
        }
        universe_meta = dict(getattr(self.trader, "_candidate_universe_meta", {}) or {})
        ticker_rest_backoff_sec = max(
            0.0,
            _safe_float(
                self.trader._single_ticker_rest_backoff_until_ts.get(self.trader._symbol_for_market(symbol), 0.0),
                0.0,
            ) - time.time(),
        ) if hasattr(self.trader, "_single_ticker_rest_backoff_until_ts") else 0.0
        ticker_rest_fail_count = _safe_float(
            getattr(self.trader, "_single_ticker_rest_fail_count", {}).get(self.trader._symbol_for_market(symbol), 0),
            0.0,
        ) if hasattr(self.trader, "_single_ticker_rest_fail_count") else 0.0
        tickers24_rest_backoff_sec = max(
            0.0,
            _safe_float(getattr(self.trader, "_tickers24_rest_backoff_until_ts", 0.0), 0.0) - time.time(),
        )
        tickers24_rest_fail_count = _safe_float(getattr(self.trader, "_tickers24_rest_fail_count", 0), 0.0)
        data_quality_score = self._compute_data_quality_score(
            market_data_stale=bool(market_data_stale),
            market_data_stale_sec=_safe_float(market_data_stale_sec, 0.0),
            ticker_rest_backoff_sec=ticker_rest_backoff_sec,
            tickers24_rest_backoff_sec=tickers24_rest_backoff_sec,
            ticker_rest_fail_count=ticker_rest_fail_count,
            tickers24_rest_fail_count=tickers24_rest_fail_count,
            market_volatility=_safe_float(market_volatility, 0.0),
            regime_flags=regime_flags,
            universe_tickers_failed=bool(universe_meta.get("tickers_failed", False)),
        )
        data_quality_floor = 0.46 if training_runtime else 0.62
        if bool(market_ctx.get("dangerous", False)):
            data_quality_floor = max(data_quality_floor, 0.68 if training_runtime else 0.76)
        data_quality_low = bool(data_quality_score < data_quality_floor)

        buy_intent = bool(
            (buy_trigger or dust_recovery_buy)
            or (self.config.TRADE_MARKET == "futures" and short_entry_trigger and not has_open_position)
        )
        entry_policy = self.entry_policy.evaluate(
            ai_enabled=ai_enabled,
            ai_quality_ok=ai_quality_ok,
            edge_override_buy=edge_override_buy,
            ai_conf_ok=(ai_conf >= ai_min_conf_required),
            mtf_ok=mtf_pass,
            mtf_soft_bypass=mtf_soft_bypass,
            is_short_spot_block=(self.config.TRADE_MARKET != "futures" and ai_enabled and ai_action == "SHORT" and not has_open_position),
            market_noise_high=(micro_noise >= 0.70),
            market_data_stale=market_data_stale,
            data_quality_low=data_quality_low,
            expected_edge_ok=expected_edge_ok,
            short_model_weak_block=(
                self.config.TRADE_MARKET == "futures"
                and short_model_weak
                and (not edge_override_buy)
                and ai_action != "LONG"
            ),
            anomaly_risk_high=(anomaly_score >= 0.85),
            post_time_stop_block=post_time_stop_block,
            signal_debounce_pass=signal_debounce_pass,
            symbol_internal_cooldown_block=symbol_internal_cooldown_block,
            buy_blocked_by_guard=buy_blocked_by_guard,
            has_open_position=has_open_position,
            buy_intent=buy_intent,
        )
        buy_block_reasons = list(entry_policy.reasons)

        event = "no_trade_signal"
        t_order = time.perf_counter()
        order_attempted = False
        if self._is_cooldown() and not has_open_position:
            event = "cooldown"
        elif market_data_stale and not has_open_position:
            event = "market_data_unavailable"
        elif (
            entry_policy.can_enter
            and self._can_afford_buy(usdt_free, spot_buy_usdt)
        ):
            guard_ok, guard_reason_pretrade = self._pretrade_execution_guard(
                symbol=symbol,
                side="buy",
                notional_usdt=spot_buy_usdt,
                price_hint=price,
                ai_quality=ai_entry_quality,
                expected_edge_pct=expected_edge_pct,
            )
            if not guard_ok:
                buy_block_reasons.append(f"pretrade:{guard_reason_pretrade}")
                event = "entry_blocked"
            else:
                order_attempted = True
                buy_reason = "dust_recovery" if dust_recovery_buy and not buy_trigger else "signal"
                if self.config.TRADE_MARKET == "futures":
                    entry_side = "SHORT" if (short_entry_trigger and not buy_trigger) else "LONG"
                    entry_exec = self.execution_engine.run(
                        lambda: self._execute_futures_entry(
                            symbol, spot_buy_usdt, price, latest_candle_ts, active_risk, entry_side=entry_side, reason=buy_reason
                        ),
                        ok_event="buy",
                        fail_event="training_no_signal" if self.config.AI_TRAINING_MODE else "entry_blocked",
                    )
                    did_buy = bool(entry_exec.ok)
                else:
                    entry_exec = self.execution_engine.run(
                        lambda: self._execute_buy(symbol, spot_buy_usdt, price, latest_candle_ts, active_risk, reason=buy_reason),
                        ok_event="buy",
                        fail_event="training_no_signal" if self.config.AI_TRAINING_MODE else "entry_blocked",
                    )
                    did_buy = bool(entry_exec.ok)
                if did_buy:
                    event = "training_buy_signal" if self.config.AI_TRAINING_MODE else "buy"
                elif self.config.AI_TRAINING_MODE:
                    event = "training_no_signal"
        elif buy_intent and not has_open_position and (
            buy_blocked_by_guard or (not entry_policy.can_enter)
        ):
            event = "risk_guard_blocked" if buy_blocked_by_guard else "entry_blocked"
        elif (
            self.config.TRADE_MARKET != "futures"
            and
            has_open_position
            and overlay_decision.partial_exit_fraction > 0
            and not sell_trigger
            and not self.config.DRY_RUN
        ):
            order_attempted = True
            partial_base = max(0.0, base_free * overlay_decision.partial_exit_fraction)
            part_exec = self.execution_engine.run(
                lambda: self._execute_partial_sell(symbol, partial_base, overlay_decision.partial_reason, price),
                ok_event="partial_sell",
                fail_event="no_trade_signal",
            )
            did_partial = bool(part_exec.ok)
            if did_partial:
                event = "partial_sell"
        elif sell_trigger and has_open_position:
            order_attempted = True
            reason = sell_reason if sell_reason else "signal"
            if self.config.TRADE_MARKET == "futures":
                sell_exec = self.execution_engine.run(
                    lambda: self._execute_futures_close(symbol, base_free, position_side or "LONG", reason, price),
                    ok_event="sell",
                    fail_event="training_no_signal" if self.config.AI_TRAINING_MODE else "no_trade_signal",
                )
                did_sell = bool(sell_exec.ok)
            else:
                sell_exec = self.execution_engine.run(
                    lambda: self._execute_sell(symbol, base_free, reason, price),
                    ok_event="sell",
                    fail_event="training_no_signal" if self.config.AI_TRAINING_MODE else "no_trade_signal",
                )
                did_sell = bool(sell_exec.ok)
            if did_sell:
                weak_exit_reasons = {"time_stop", "smart_stagnation_exit", "stall_exit", "ai_quality_fade"}
                if reason in weak_exit_reasons:
                    adaptive_symbol_pause = 70
                    if regime_name in {"flat", "range"}:
                        adaptive_symbol_pause += 35
                    if micro_noise >= 0.70:
                        adaptive_symbol_pause += 20
                    if anomaly_score >= 0.70:
                        adaptive_symbol_pause += 25
                    self._set_symbol_internal_cooldown(symbol, adaptive_symbol_pause)
                else:
                    self._set_symbol_internal_cooldown(symbol, 0.0)
                self.signal_debounce_counter[symbol.upper()] = 0
                if reason == "time_stop":
                    block_candles = max(0, self.config.TIME_STOP_AFTER_EXIT_BLOCK_CANDLES)
                    if latest_candle_ts > 0 and block_candles > 0:
                        step_ms = 60_000
                        tf = (self.config.TIMEFRAME or "1m").lower().strip()
                        if tf.endswith("m"):
                            step_ms = max(60_000, int(_safe_float(tf[:-1], 1.0) * 60_000))
                        elif tf.endswith("h"):
                            step_ms = max(60_000, int(_safe_float(tf[:-1], 1.0) * 3_600_000))
                        elif tf.endswith("d"):
                            step_ms = max(60_000, int(_safe_float(tf[:-1], 1.0) * 86_400_000))
                        self.time_stop_buy_block_until_candle_ts[symbol.upper()] = latest_candle_ts + (block_candles * step_ms)
                event = "training_sell_signal" if self.config.AI_TRAINING_MODE else "sell"
            elif self.config.AI_TRAINING_MODE:
                event = "training_no_signal"
        elif self.config.AI_TRAINING_MODE:
            event = "training_no_signal"
        if order_attempted:
            stage_profile["order_ms"] = round((time.perf_counter() - t_order) * 1000.0, 1)

        trade_events = {"buy", "sell", "training_buy_signal", "training_sell_signal", "partial_sell"}
        no_trade_events = {"no_trade_signal", "training_no_signal", "entry_blocked", "risk_guard_blocked", "cooldown", "market_data_unavailable"}
        if event in trade_events:
            self._no_trade_streak_cycles = 0
            self._last_trade_event_ts = time.time()
        elif event in no_trade_events:
            self._no_trade_streak_cycles = min(5000, int(self._no_trade_streak_cycles) + 1)
        self._refresh_missed_opportunities(symbol, price, latest_candle_ts)
        self._register_missed_opportunity_candidate(
            symbol=symbol,
            regime_bucket=regime_bucket,
            event=event,
            ai_action=ai_action,
            ai_conf=ai_conf,
            ai_quality=ai_entry_quality,
            expected_edge_pct=expected_edge_pct,
            min_expected_edge_pct=min_expected_edge_pct,
            price=price,
            latest_candle_ts=latest_candle_ts,
            buy_block_reasons=buy_block_reasons,
        )
        self._update_cycle_feedback(
            symbol=symbol,
            regime=regime_name,
            regime_bucket=regime_bucket,
            event=event,
            buy_block_reasons=buy_block_reasons,
            ai_quality=ai_entry_quality,
            ai_conf=ai_conf,
            expected_edge_pct=expected_edge_pct,
            min_expected_edge_pct=min_expected_edge_pct,
            market_data_stale=bool(market_data_stale),
        )
        runtime_recover = self._runtime_degradation_recover(
            event=event,
            symbol=symbol,
            market_data_stale=bool(market_data_stale),
        )
        if bool(runtime_recover.get("triggered", False)):
            logging.info(
                "Runtime auto-recover | action=%s | symbol=%s | reason=%s",
                str(runtime_recover.get("action", "") or "-"),
                symbol,
                str(runtime_recover.get("reason", "") or "-"),
            )

        normalized_buy_block_reasons = sorted({str(r or "").strip() for r in buy_block_reasons if str(r or "").strip()})
        buy_block_summary = ", ".join(normalized_buy_block_reasons[:3]) if normalized_buy_block_reasons else ""
        signal_explainer = self._build_signal_explainer(
            event=event,
            ai_action=ai_action,
            ai_conf=ai_conf,
            ai_conf_required=ai_min_conf_required,
            ai_quality=ai_entry_quality,
            ai_quality_required=ai_entry_min_quality,
            expected_edge_pct=expected_edge_pct,
            min_expected_edge_pct=min_expected_edge_pct,
            buy_block_reasons=normalized_buy_block_reasons,
            market_data_stale=bool(market_data_stale),
            data_quality_score=data_quality_score,
            market_regime=str(market_ctx.get("regime", "flat")),
            market_flags=regime_flags,
            has_open_position=bool(base_free > 0),
        )

        if event in {"entry_blocked", "risk_guard_blocked"}:
            if guard_reason:
                msg = f"guard:{guard_reason}"
                text = f"Покупка заблокирована risk guard: {guard_reason}"
            else:
                primary_reason = normalized_buy_block_reasons[0] if normalized_buy_block_reasons else "unknown"
                reasons_text = ",".join(normalized_buy_block_reasons) if normalized_buy_block_reasons else "unknown"
                # Use stable signature for throttling to avoid log spam when reasons order/value jitter changes.
                msg = f"entry:{primary_reason}"
                text = f"Покупка заблокирована фильтрами входа: {reasons_text}"
            now_ts = time.time()
            if msg != self._last_entry_block_log_sig or (now_ts - self._last_entry_block_log_ts) >= 15.0:
                logging.info(text)
                self._last_entry_block_log_sig = msg
                self._last_entry_block_log_ts = now_ts

        # Reflect paper-trade balance changes immediately in training live view.
        if self.config.AI_TRAINING_MODE and self.config.DRY_RUN:
            usdt_free, base_free, base_currency = self._effective_balances(symbol, pos)
        elif event in {"buy", "sell"} and not self.config.DRY_RUN:
            try:
                pos = self.trader.get_position(symbol)
                usdt_free, base_free, base_currency = self._effective_balances(symbol, pos)
                equity_usdt = self._estimate_cycle_equity_usdt(symbol, usdt_free, base_free, price)
            except Exception:
                logging.warning("Не удалось обновить баланс после события %s для %s.", event, symbol)

        self._register_trader_api_errors()
        # Recompute position snapshot after potential trade execution, so live status
        # always reflects the current cycle result (not pre-trade values).
        has_open_position_live = base_free > 0
        if has_open_position_live and not (self.config.AI_TRAINING_MODE and self.config.DRY_RUN):
            has_open_position_live = self.trader.can_sell_position(symbol, base_free, price)
        if has_open_position_live:
            self._ensure_position_memory(symbol, price, latest_candle_ts, active_risk)
        locked_risk_live = self._risk_from_memory(symbol) if has_open_position_live else None
        status_risk = dict(locked_risk_live if locked_risk_live else active_risk)
        effective_base_for_status = base_free if has_open_position_live else 0.0
        status_entry_price, status_peak_price, status_pnl, status_sell_reason, status_candles_in_pos, status_candles_since_peak = self._manage_position_state(
            symbol,
            price,
            effective_base_for_status,
            status_risk,
            dynamic_time_stop_candles,
            latest_candle_ts,
            ai_signal,
            position_side=position_side if has_open_position_live else "LONG",
        )
        if has_open_position_live:
            self._sync_position_peak(symbol, status_peak_price, latest_candle_ts)
        status_stop_loss_price, status_take_profit_price, status_trailing_stop_price = self._risk_levels(
            status_entry_price,
            status_peak_price,
            status_risk,
            position_side=position_side if has_open_position_live else "LONG",
        )

        telemetry = self.market_ctx_engine.record_telemetry(
            ai_quality=ai_entry_quality,
            expected_edge_pct=expected_edge_pct,
            regime_label=str(market_ctx.get("regime", "flat")),
            runtime_params=self.runtime_params,
        )
        pos_state = self.position_sm.transition(has_open_position=has_open_position_live, event=event)

        audit_reason = ""
        if event in {"entry_blocked", "risk_guard_blocked"}:
            audit_reason = guard_reason if event == "risk_guard_blocked" else str(entry_policy.primary_reason or "entry_filters")
        elif event in {"buy", "training_buy_signal"}:
            audit_reason = "entry_opened"
        elif event in {"sell", "training_sell_signal", "partial_sell"}:
            audit_reason = str(sell_reason or "exit")
        elif event == "market_data_unavailable":
            audit_reason = "market_data_unavailable"
        else:
            audit_reason = "wait_signal"
        self._append_audit_event(
            event=event,
            symbol=symbol,
            reason=audit_reason,
            guard_reason=guard_reason,
            buy_block_reasons=buy_block_reasons,
            sell_reason=str(status_sell_reason or sell_reason or ""),
            ai_action=ai_action,
            ai_quality=ai_entry_quality,
            expected_edge_pct=expected_edge_pct,
            min_expected_edge_pct=min_expected_edge_pct,
            market_regime=str(market_ctx.get("regime", "flat")),
            has_open_position=has_open_position_live,
            pnl_pct=status_pnl,
            price=price,
            usdt_free=usdt_free,
            base_free=base_free,
            signal_reason=(signal_explainer[0] if signal_explainer else audit_reason),
            signal_explainer=signal_explainer,
            no_entry_reasons=normalized_buy_block_reasons,
            data_quality_score=data_quality_score,
        )
        recent_decision_quality = 0.0
        trade_quality = 0.0
        cycle_quality = 0.0
        if self.recent_trade_scores:
            trade_quality = float(sum(self.recent_trade_scores) / max(1, len(self.recent_trade_scores)))
        if self.recent_decision_scores:
            cycle_quality = float(sum(self.recent_decision_scores) / max(1, len(self.recent_decision_scores)))
        if self.recent_trade_scores and self.recent_decision_scores:
            recent_decision_quality = float(_clamp((trade_quality * 0.42) + (cycle_quality * 0.58), 0.0, 1.0))
        elif self.recent_decision_scores:
            recent_decision_quality = cycle_quality
        else:
            recent_decision_quality = trade_quality
        regime_bias_runtime = self._regime_trade_bias(str(market_ctx.get("regime", "flat")))
        stage_profile["cycle_ms"] = round((time.perf_counter() - cycle_t0) * 1000.0, 1)
        universe_meta = dict(getattr(self.trader, "_candidate_universe_meta", {}) or {})
        self._write_live_status({
            "ts_utc": datetime.now(timezone.utc).isoformat(),
            "mode": mode_label,
            "cycle_index": cycle_index,
            "event": event,
            "cooldown_remaining_sec": self._cooldown_remaining_sec(),
            "symbol": symbol,
            "auto_symbol_selection": self.config.AUTO_SYMBOL_SELECTION,
            "ai_auto_risk": self.config.AI_AUTO_RISK,
            "universe_total": universe_total,
            "universe_scanned": universe_scanned,
            "universe_source": str(universe_meta.get("source", "") or ""),
            "universe_effective_limit": int(_safe_float(universe_meta.get("effective_limit"), 0.0)),
            "universe_preferred_count": int(_safe_float(universe_meta.get("preferred_count"), 0.0)),
            "universe_selected_count": int(_safe_float(universe_meta.get("selected_count"), 0.0)),
            "universe_tickers_failed": bool(universe_meta.get("tickers_failed", False)),
            "price": price,
            "sma": sma,
            "deviation": deviation,
            "usdt_free": usdt_free,
            "base_currency": base_currency,
            "base_free": base_free,
            "equity_usdt": equity_usdt,
            "exchange_usdt_free": exchange_usdt_report,
            "exchange_equity_usdt": exchange_equity_report,
            "exchange_balance_source": exchange_balance_source,
            "exchange_balance_trusted": bool(trusted_exchange_balance),
            "paper_usdt_free": paper_usdt_free,
            "paper_equity_usdt": paper_equity_usdt,
            "spot_buy_usdt": spot_buy_usdt,
            "market_volatility": market_volatility,
            "price_jump_pct": price_jump_pct,
            "market_data_stale": market_data_stale,
            "market_data_stale_sec": market_data_stale_sec,
            "active_risk_fraction": active_risk["fraction"],
            "active_stop_loss_pct": active_risk["stop_loss"],
            "active_take_profit_pct": active_risk["take_profit"],
            "active_trailing_stop_pct": active_risk["trailing_stop"],
            "active_dynamic_time_stop_candles": dynamic_time_stop_candles,
            "expected_edge_pct": expected_edge_pct,
            "min_expected_edge_pct": min_expected_edge_pct,
            "edge_floor_components": dict(self.runtime_edge_floor),
            "expected_edge_ok": expected_edge_ok,
            "entry_policy_primary_reason": entry_policy.primary_reason,
            "anomaly_score": anomaly_score,
            "anomalies": anomalies if isinstance(anomalies, dict) else {},
            "momentum_speed": momentum_speed,
            "ai_entry_quality": ai_entry_quality,
            "ai_entry_min_quality": ai_entry_min_quality,
            "ai_min_conf_required": ai_min_conf_required,
            "ai_quality_ok": ai_quality_ok,
            "edge_override_buy": edge_override_buy,
            "ai_side_performance": side_perf,
            "short_model_weak": short_model_weak,
            "micro_noise_score": micro_noise,
            "mtf_confirmation": mtf_meta,
            "loss_streak_live": int(self.loss_streak_live),
            "exec_slippage_ema_bps": float(self.exec_slippage_ema_bps),
            "symbol_exec_slippage_ema_bps": float(_safe_float(self.symbol_exec_slippage_ema_bps.get(symbol.upper(), 0.0), 0.0)),
            "symbol_exec_quality_ema": float(_safe_float(self.symbol_exec_quality_ema.get(symbol.upper(), 0.50), 0.50)),
            "post_time_stop_block": post_time_stop_block,
            "post_time_stop_wait_candles": post_time_stop_wait_candles,
            "signal_debounce_count": signal_debounce_count,
            "signal_debounce_required": signal_debounce_required,
            "signal_debounce_pass": signal_debounce_pass,
            "signal_debounce_mode": signal_debounce_mode,
            "no_trade_streak_cycles": int(self._no_trade_streak_cycles),
            "deadlock_soft_relax": float(deadlock_soft_relax),
            "deadlock_conf_relax": float(deadlock_conf_relax),
            "deadlock_debounce_relax": int(deadlock_debounce_relax),
            "last_trade_event_age_sec": (0.0 if self._last_trade_event_ts <= 0 else max(0.0, time.time() - self._last_trade_event_ts)),
            "symbol_internal_cooldown_remaining_sec": symbol_internal_cooldown_sec,
            "adaptive_symbol_cooldown": dict(adaptive_symbol_cooldown),
            "buy_block_reasons": buy_block_reasons,
            "buy_block_reason_count": len(normalized_buy_block_reasons),
            "buy_block_summary": buy_block_summary,
            "ticker_rest_backoff_sec": ticker_rest_backoff_sec,
            "ticker_rest_fail_count": ticker_rest_fail_count,
            "tickers24_rest_backoff_sec": tickers24_rest_backoff_sec,
            "tickers24_rest_fail_count": tickers24_rest_fail_count,
            "market_regime": str(market_ctx.get("regime", "flat")),
            "market_regime_bucket": str(regime_bucket),
            "market_flags": market_ctx.get("flags", []),
            "market_dangerous": bool(market_ctx.get("dangerous", False)),
            "data_quality_score": float(data_quality_score),
            "signal_explainer": signal_explainer,
            "signal_reason": (signal_explainer[0] if signal_explainer else audit_reason),
            "overlay_partial_exit_fraction": overlay_decision.partial_exit_fraction,
            "overlay_partial_reason": overlay_decision.partial_reason,
            "overlay_breakeven_armed": overlay_decision.breakeven_armed,
            "tp_adaptation": tp_adaptation,
            "smart_exit": dict(self.runtime_smart_exit),
            "drift_score": drift_score_runtime,
            "drift_active": drift_active_runtime,
            "adaptive_runtime_params": dict(self.runtime_params),
            "adaptive_last_event": dict(self.runtime_adaptation),
            "runtime_recover": dict(runtime_recover),
            "shared_learning_bias": dict(shared_learning_bias),
            "confidence_bias": dict(confidence_bias),
            "missed_opportunity_bias": dict(missed_opportunity_bias),
            "anti_fragile_mode": dict(anti_fragile_mode),
            "execution_stress": dict(execution_stress),
            "regime_trade_feedback": dict(self.regime_trade_feedback.get(str(market_ctx.get("regime", "flat")).strip().lower(), {})),
            "regime_trade_bias": dict(regime_bias_runtime),
            "symbol_regime_bias": dict(symbol_regime_bias),
            "symbol_live_bias": dict(symbol_live_bias),
            "cycle_adaptive_bias": dict(cycle_bias),
            "execution_profile": dict(execution_profile),
            "recent_decision_quality": recent_decision_quality,
            "cycle_snapshot": dict(cycle_snapshot.__dict__),
            "position_state_machine": {
                "state": pos_state.state,
                "last_event": pos_state.last_event,
            },
            "telemetry": telemetry,
            "chart": self._chart_payload_from_ohlcv(ohlcv, limit=80),
            "position_risk_source": "persisted" if locked_risk_live else ("reconstructed" if has_open_position_live else "dynamic"),
            "has_open_position": has_open_position_live,
            "position_side": position_side if has_open_position_live else "",
            "entry_price": status_entry_price,
            "peak_price": status_peak_price,
            "stop_loss_price": status_stop_loss_price,
            "take_profit_price": status_take_profit_price,
            "trailing_stop_price": status_trailing_stop_price,
            "candles_in_position": status_candles_in_pos,
            "candles_since_peak": status_candles_since_peak,
            "pnl_pct": status_pnl,
            "sell_reason": status_sell_reason,
            "dry_run": self.config.DRY_RUN,
            "cycle_elapsed_ms": stage_profile["cycle_ms"],
            "cycle_profile_ms": dict(stage_profile),
            "market_fetch_ms": stage_profile.get("market_fetch_ms", 0.0),
            "features_ms": stage_profile.get("features_ms", 0.0),
            "signal_ms": stage_profile.get("signal_ms", 0.0),
            "order_ms": stage_profile.get("order_ms", 0.0),
            "guard_block_reason": guard_reason,
            "ai": self._ai_payload(ai_signal),
            "recent_orders": list(self.recent_orders),
            "risk_guard": self._guard_payload(equity_usdt),
        })
        self._last_has_open_position = bool(has_open_position_live)
        self._last_cycle_event = str(event)

    def _effective_balances(self, symbol: str, pos: Dict[str, object]) -> Tuple[float, float, str]:
        base_currency = str(pos.get("base_currency", symbol.split("/")[0]))
        if not (self.config.AI_TRAINING_MODE and self.config.DRY_RUN):
            usdt_free = _safe_float(pos.get("usdt_free"), 0.0)
            base_free = _safe_float(pos.get("base_free"), 0.0)
            return usdt_free, base_free, base_currency

        if self.paper_usdt_free is None:
            self.paper_usdt_free = self._default_paper_usdt()

        base_free = _safe_float(self.paper_positions.get(symbol, 0.0), 0.0)
        return _safe_float(self.paper_usdt_free, 0.0), base_free, base_currency

    def _estimate_cycle_equity_usdt(self, symbol: str, usdt_free: float, base_free: float, price: float) -> float:
        equity = max(0.0, usdt_free)
        if base_free > 0 and price > 0:
            equity += base_free * price
        if self.config.AI_TRAINING_MODE and self.config.DRY_RUN:
            for sym, qty_raw in self.paper_positions.items():
                qty = _safe_float(qty_raw, 0.0)
                if qty <= 0:
                    continue
                if sym == symbol and price > 0:
                    continue
                try:
                    px = self.trader.last_price(sym)
                except Exception:
                    px = 0.0
                if px > 0:
                    equity += qty * px
        return equity

    def _paper_has_open_position(self) -> bool:
        best_symbol = None
        best_qty = 0.0
        for sym, qty in self.paper_positions.items():
            q = _safe_float(qty, 0.0)
            if q > best_qty and q > 1e-12:
                best_qty = q
                best_symbol = sym
        if best_symbol:
            self.active_symbol = best_symbol
            return True
        self.active_symbol = None
        return False

    def _detect_open_symbol_from_balance(self, balance_raw: object, current_symbol: str) -> Optional[str]:
        open_positions = self._discover_open_positions_from_balance(balance_raw)
        if not open_positions:
            return None
        priority = [current_symbol, self.config.SYMBOL, *self.config.PREFERRED_SPOT_SYMBOLS[:10]]
        for sym in priority:
            key = str(sym or "").upper()
            if key in open_positions:
                return key
        return max(open_positions.items(), key=lambda item: item[1])[0]

    @staticmethod
    def _balance_free_from_raw(balance_raw: Dict[str, object], currency: str) -> float:
        direct = balance_raw.get(currency, {}) if isinstance(balance_raw, dict) else {}
        direct_free = _safe_float(direct.get("free", 0.0) if isinstance(direct, dict) else 0.0, 0.0)
        mapped = balance_raw.get("free", {}) if isinstance(balance_raw, dict) else {}
        mapped_free = _safe_float(mapped.get(currency, 0.0) if isinstance(mapped, dict) else 0.0, 0.0)
        return max(direct_free, mapped_free)

    def _select_symbol(self) -> Tuple[str, int, int]:
        if self.config.TRADE_MARKET == "futures":
            return self.config.SYMBOL, 1, 1
        if self.active_symbol:
            dyn_cd = self._adaptive_symbol_market_cooldown(self.active_symbol)
            blocked, _ = self._is_symbol_quarantined(self.active_symbol)
            if (not blocked) and _safe_float(dyn_cd.get("seconds"), 0.0) <= 0:
                return self.active_symbol, 1, 1
        if self.config.AI_TRAINING_MODE and self.config.DRY_RUN and self._paper_has_open_position():
            return self.active_symbol or self.config.SYMBOL, 1, 1
        if not self.config.AUTO_SYMBOL_SELECTION:
            return self.config.SYMBOL, 1, 1
        now_ts = time.time()
        # Load-shed symbol scanning during API instability: keep current/fallback symbol
        # and avoid heavy fetch_tickers/fetch_ohlcv fanout.
        lock_reason = str(self.guard_state.get("lock_reason", "") or "")
        lock_until = _safe_float(self.guard_state.get("lock_until_ts"), 0.0)
        recent_api_errors = len(self.api_error_ts)
        api_soft_threshold = max(2, int(self.config.API_SOFT_GUARD_MAX_ERRORS) - 1)
        if (
            (lock_reason == "api_unstable_soft_guard" and lock_until > now_ts)
            or (recent_api_errors >= api_soft_threshold)
        ):
            fallback = self.active_symbol or self._last_symbol_scan_result[0] or self.config.SYMBOL
            self._last_symbol_scan_ts = now_ts
            self._last_symbol_scan_result = (fallback, 1, 1)
            return fallback, 1, 1
        cooldown_sec = max(1.0, _safe_float(getattr(self.config, "SYMBOL_RESCAN_COOLDOWN_SEC", 9.0), 9.0))
        if (now_ts - self._last_symbol_scan_ts) < cooldown_sec:
            cached_symbol, cached_total, cached_scanned = self._last_symbol_scan_result
            if cached_symbol:
                return cached_symbol, max(1, cached_total), max(0, cached_scanned)
        
        try:
            symbols = self.trader.candidate_spot_symbols()
            if not symbols:
                logging.warning("No candidate symbols found for auto-selection.")
                return self.config.SYMBOL, 0, 0
            
            # Initialize with the first available candidate, not the static default.
            non_quarantined = []
            for sym in symbols:
                blocked, remaining = self._is_symbol_quarantined(sym)
                if blocked:
                    key = str(sym or "").upper().strip()
                    now_q = time.time()
                    last_q_log = _safe_float(self._symbol_quarantine_log_ts.get(key), 0.0)
                    # Throttle repetitive quarantine logs to reduce GUI/log pressure.
                    # Keep near-expiry updates more frequent so user still sees unban soon.
                    if (now_q - last_q_log) >= 120.0 or remaining <= 30:
                        logging.info("Пара в карантине, пропуск: %s (еще ~%d сек)", sym, remaining)
                        self._symbol_quarantine_log_ts[key] = now_q
                    continue
                non_quarantined.append(sym)
            if not non_quarantined:
                logging.warning("Все кандидаты сейчас в карантине, используется fallback.")
                fallback = self.active_symbol or self.config.SYMBOL
                self._last_symbol_scan_ts = now_ts
                self._last_symbol_scan_result = (fallback, len(symbols), 0)
                return fallback, len(symbols), 0

            effective_limit = max(
                4,
                min(
                    30,
                    min(
                        len(symbols),
                        int(_safe_float(getattr(self.config, "SYMBOL_SCAN_LIMIT", max(len(symbols), 4)), float(max(len(symbols), 4)))),
                    ),
                ),
            )
            preferred_rank = {
                str(sym or "").upper().strip(): idx
                for idx, sym in enumerate(self.config.PREFERRED_SPOT_SYMBOLS[:effective_limit])
            }
            best_symbol, best_score, scanned = non_quarantined[0], -1.0, 0
            has_valid_scored_candidate = False
            for sym in non_quarantined:
                scanned += 1
                try:
                    dyn_cd_meta = self._adaptive_symbol_market_cooldown(sym)
                    dyn_cd_sec = _safe_float(dyn_cd_meta.get("seconds"), 0.0)
                    if dyn_cd_sec > 0:
                        continue
                    ohlcv = self.trader.fetch_ohlcv(sym, self.config.TIMEFRAME, self.config.AI_LOOKBACK)
                    
                    score = -1.0
                    sym_key = str(sym or "").upper().strip()
                    pref_idx = preferred_rank.get(sym_key, effective_limit + 2)
                    pref_bonus = max(0.0, (effective_limit - pref_idx)) * 0.045
                    exec_quality_ema = _clamp(_safe_float(self.symbol_exec_quality_ema.get(sym_key, 0.50), 0.50), 0.0, 1.0)
                    quality_ema = _safe_float(self.symbol_quality_ema.get(sym_key, 0.0), 0.0)
                    loss_streak = int(_safe_float(self.symbol_loss_streak.get(sym_key, 0.0), 0.0))
                    if self.ai_engine:
                        sig = self.ai_engine.predict(ohlcv, sym)
                        sig_conf = sig.calibrated_confidence if sig.calibrated_confidence > 0 else sig.confidence
                        sig_quality = self._quality_for_entry(sig)
                        if (
                            sig.trade_filter_pass
                            and sig.action == "LONG"
                            and sig_conf >= self.config.AI_MIN_CONFIDENCE
                            and sig_quality >= self._param_f("AI_ENTRY_MIN_QUALITY", self.config.AI_ENTRY_MIN_QUALITY)
                        ):
                            score = sig.score
                            # Internal pair quality memory: prefer symbols with better recent realized quality.
                            score += quality_ema * 14.0
                            score += max(0.0, exec_quality_ema - 0.50) * 1.35
                            score += pref_bonus
                            score -= min(1.6, loss_streak * 0.30)
                            regime_local = "flat"
                            try:
                                market_ctx_local = self.adaptive_agent.detect_market_regime(ohlcv)
                                regime_local = str(market_ctx_local.get("regime", "flat") or "flat").lower().strip()
                            except Exception:
                                regime_local = "flat"
                                market_ctx_local = {}
                            regime_bucket_local = self._market_regime_bucket(
                                regime_local,
                                market_ctx_local.get("flags", []) if isinstance(market_ctx_local, dict) else [],
                            )
                            regime_bias = self._regime_trade_bias(regime_local)
                            symbol_regime_bias = self._symbol_regime_trade_bias(sym, regime_local)
                            symbol_live_bias = self._symbol_regime_live_bias(sym, regime_local)
                            symbol_exec_bias = self._symbol_execution_bias(sym)
                            missed_bias = self._missed_opportunity_bias(sym, regime_bucket_local)
                            anti_fragile = self._anti_fragile_mode(sym, regime_bucket_local)
                            score -= max(0.0, _safe_float(regime_bias.get("exit_bias"), 0.0)) * 0.90
                            score += max(0.0, 1.0 - _safe_float(regime_bias.get("edge_mult"), 1.0)) * 0.35
                            score -= max(0.0, _safe_float(symbol_regime_bias.get("edge_delta"), 0.0)) * 1200.0
                            score -= max(0.0, _safe_float(symbol_live_bias.get("edge_delta"), 0.0)) * 1000.0
                            score -= max(0.0, _safe_float(symbol_live_bias.get("quality_delta"), 0.0)) * 18.0
                            score -= max(0.0, _safe_float(missed_bias.get("edge_delta"), 0.0)) * 1050.0
                            score += max(0.0, -_safe_float(missed_bias.get("edge_delta"), 0.0)) * 560.0
                            score += max(0.0, _safe_float(symbol_live_bias.get("risk_mult"), 1.0) - 1.0) * 1.8
                            score -= max(0, int(_safe_float(symbol_live_bias.get("debounce_delta"), 0.0))) * 0.18
                            score -= max(0.0, _safe_float(symbol_exec_bias.get("edge_delta"), 0.0)) * 1300.0
                            score += max(0.0, _safe_float(symbol_exec_bias.get("risk_mult"), 1.0) - 1.0) * 1.6
                            score += max(0.0, _safe_float(symbol_exec_bias.get("exec_quality"), 0.5) - 0.5) * 1.9
                            score -= max(0.0, _safe_float(symbol_exec_bias.get("slippage_bps"), 0.0) - 2.0) * 0.015
                            score -= max(0.0, _safe_float(anti_fragile.get("edge_delta"), 0.0)) * 820.0
                            score += max(0.0, _safe_float(anti_fragile.get("risk_mult"), 1.0) - 1.0) * 1.4
                    else:
                        price, sma = self._price_sma_from_ohlcv(ohlcv)
                        buy, _, dev = self._get_sma_signal(price, sma)
                        if buy:
                            score = dev
                            score += pref_bonus
                            score += max(0.0, exec_quality_ema - 0.50) * 0.55
                            score += quality_ema * 6.0
                            score -= min(1.1, loss_streak * 0.22)
                    
                    if score > best_score:
                        best_score, best_symbol = score, sym
                    if score > -0.95:
                        has_valid_scored_candidate = True
                except Exception as exc:
                    logging.warning("Не удалось проанализировать пару %s: %s", sym, exc)
                    continue

            if not has_valid_scored_candidate and non_quarantined:
                # When all signals are weak, prefer the healthiest/liquid preferred symbol instead of blind round-robin.
                fallback_ranked = sorted(
                    non_quarantined,
                    key=lambda sym: (
                        preferred_rank.get(str(sym or "").upper().strip(), effective_limit + 2),
                        -_clamp(_safe_float(self.symbol_exec_quality_ema.get(str(sym or "").upper().strip(), 0.50), 0.50), 0.0, 1.0),
                        -_safe_float(self.symbol_quality_ema.get(str(sym or "").upper().strip(), 0.0), 0.0),
                        int(_safe_float(self.symbol_loss_streak.get(str(sym or "").upper().strip(), 0.0), 0.0)),
                        str(sym or ""),
                    ),
                )
                best_symbol = fallback_ranked[0]
            else:
                self._symbol_rr_index = (self._symbol_rr_index + 1) % max(1, len(non_quarantined))
            self._last_symbol_scan_ts = now_ts
            self._last_symbol_scan_result = (best_symbol, len(non_quarantined), scanned)
            return best_symbol, len(non_quarantined), scanned
        except Exception:
            logging.exception("Symbol selection failed")
            fallback = self.active_symbol or self.config.SYMBOL
            if self.config.PREFERRED_SPOT_SYMBOLS:
                fallback = self.config.PREFERRED_SPOT_SYMBOLS[0]
            self._last_symbol_scan_ts = now_ts
            self._last_symbol_scan_result = (fallback, 0, 0)
            return fallback, 0, 0

    def _price_sma_from_ohlcv(self, ohlcv: list[list[float]]) -> Tuple[float, float]:
        closes: list[float] = []
        for row in (ohlcv or []):
            if not isinstance(row, list) or len(row) < 5:
                continue
            c = _safe_float(row[4], 0.0)
            if c > 0:
                closes.append(c)
        if not closes:
            return 0.0, 0.0
        price = closes[-1]
        period = max(2, int(self.config.SMA_PERIOD))
        win = closes[-period:] if len(closes) >= period else closes
        if not win:
            return float(price), float(price)
        return float(price), float(sum(win) / float(len(win)))

    @staticmethod
    def _chart_payload_from_ohlcv(ohlcv: list[list[float]], limit: int = 80) -> dict[str, object]:
        items = ohlcv[-max(10, int(limit)):] if isinstance(ohlcv, list) else []
        candles: list[dict[str, float]] = []
        for row in items:
            if not isinstance(row, list) or len(row) < 6:
                continue
            ts = _safe_float(row[0], 0.0)
            o = _safe_float(row[1], 0.0)
            h = _safe_float(row[2], 0.0)
            l = _safe_float(row[3], 0.0)
            c = _safe_float(row[4], 0.0)
            v = _safe_float(row[5], 0.0)
            if c <= 0 or h <= 0 or l <= 0:
                continue
            candles.append({"ts": ts, "o": o, "h": h, "l": l, "c": c, "v": max(0.0, v)})
        return {"candles": candles}

    def _get_sma_signal(self, price: float, sma: float) -> Tuple[bool, bool, float]:
        deviation = (price / sma) - 1.0 if sma > 0 else 0.0
        return deviation > self.config.THRESHOLD, deviation < -self.config.THRESHOLD, deviation

    @staticmethod
    def _micro_noise_score(ohlcv: list[list[float]]) -> float:
        if len(ohlcv) < 12:
            return 0.0
        closes = np.array([_safe_float(c[4], 0.0) for c in ohlcv[-20:]], dtype=float)
        opens = np.array([_safe_float(c[1], 0.0) for c in ohlcv[-20:]], dtype=float)
        highs = np.array([_safe_float(c[2], 0.0) for c in ohlcv[-20:]], dtype=float)
        lows = np.array([_safe_float(c[3], 0.0) for c in ohlcv[-20:]], dtype=float)
        valid = (closes > 0) & (opens > 0) & (highs > 0) & (lows > 0)
        closes, opens, highs, lows = closes[valid], opens[valid], highs[valid], lows[valid]
        if len(closes) < 10:
            return 0.0
        body = np.abs(closes - opens)
        span = np.maximum(highs - lows, 1e-10)
        body_ratio = float(np.mean(body / span))
        sign = np.sign(np.diff(closes))
        flip_rate = float(np.mean(sign[1:] * sign[:-1] < 0)) if len(sign) > 2 else 0.0
        noise = (1.0 - _clamp(body_ratio, 0.0, 1.0)) * 0.6 + _clamp(flip_rate, 0.0, 1.0) * 0.4
        return _clamp(noise, 0.0, 1.0)

    def _mtf_confirmation(self, symbol: str) -> tuple[bool, dict[str, float]]:
        meta = {"m5_trend": 0.0, "m15_trend": 0.0, "pass": 0.0}
        key = str(symbol or "").upper()
        now_ts = time.time()
        cached = self._mtf_cache.get(key)
        ttl_sec = max(12.0, min(45.0, float(self.config.SLEEP_SECONDS) * 2.5))
        if isinstance(cached, tuple) and len(cached) == 3:
            cached_ts, cached_pass, cached_meta = cached
            if (now_ts - _safe_float(cached_ts, 0.0)) <= ttl_sec and isinstance(cached_meta, dict):
                return bool(cached_pass), dict(cached_meta)
        # During API instability, avoid extra 5m/15m calls to reduce timeout cascades.
        lock_reason = str(self.guard_state.get("lock_reason", "") or "")
        lock_until = _safe_float(self.guard_state.get("lock_until_ts"), 0.0)
        recent_api_errors = len(self.api_error_ts)
        api_soft_threshold = max(2, int(self.config.API_SOFT_GUARD_MAX_ERRORS) - 1)
        if (
            (lock_reason == "api_unstable_soft_guard" and lock_until > now_ts)
            or (recent_api_errors >= api_soft_threshold)
        ):
            if isinstance(cached, tuple) and len(cached) == 3:
                _, cached_pass, cached_meta = cached
                if isinstance(cached_meta, dict):
                    return bool(cached_pass), dict(cached_meta)
            self._mtf_cache[key] = (now_ts, True, dict(meta))
            return True, meta
        try:
            o5 = self.trader.fetch_ohlcv(symbol, "5m", 40)
            o15 = self.trader.fetch_ohlcv(symbol, "15m", 40)
            c5 = np.array([_safe_float(c[4], 0.0) for c in o5 if len(c) > 4], dtype=float)
            c15 = np.array([_safe_float(c[4], 0.0) for c in o15 if len(c) > 4], dtype=float)
            if len(c5) < 25 or len(c15) < 25:
                self._mtf_cache[key] = (now_ts, True, dict(meta))
                return True, meta
            m5_fast = float(np.mean(c5[-6:]))
            m5_slow = float(np.mean(c5[-20:]))
            m15_fast = float(np.mean(c15[-6:]))
            m15_slow = float(np.mean(c15[-20:]))
            t5 = (m5_fast / max(1e-10, m5_slow)) - 1.0
            t15 = (m15_fast / max(1e-10, m15_slow)) - 1.0
            meta["m5_trend"] = t5
            meta["m15_trend"] = t15
            passed = t5 >= -0.0008 and t15 >= -0.0012
            meta["pass"] = 1.0 if passed else 0.0
            self._mtf_cache[key] = (now_ts, passed, dict(meta))
            return passed, meta
        except Exception:
            if isinstance(cached, tuple) and len(cached) == 3:
                _, cached_pass, cached_meta = cached
                if isinstance(cached_meta, dict):
                    return bool(cached_pass), dict(cached_meta)
            return True, meta

    def _update_internal_learning(
        self,
        symbol: str,
        pnl_net_pct: float,
        entry_slippage_bps: float = 0.0,
        touch_outcome: bool = True,
    ) -> None:
        sym = str(symbol or "").upper()
        alpha = 0.18
        prev_q = _safe_float(self.symbol_quality_ema.get(sym, 0.0), 0.0)
        self.symbol_quality_ema[sym] = (1.0 - alpha) * prev_q + alpha * pnl_net_pct
        self.symbol_trade_count[sym] = int(self.symbol_trade_count.get(sym, 0)) + 1
        if touch_outcome:
            if pnl_net_pct < 0:
                self.loss_streak_live = min(8, self.loss_streak_live + 1)
            else:
                self.loss_streak_live = max(0, self.loss_streak_live - 1)
        if entry_slippage_bps > 0:
            beta = 0.20
            self.exec_slippage_ema_bps = (1.0 - beta) * self.exec_slippage_ema_bps + beta * entry_slippage_bps
            prev_symbol_slip = _safe_float(self.symbol_exec_slippage_ema_bps.get(sym, 0.0), 0.0)
            self.symbol_exec_slippage_ema_bps[sym] = (1.0 - beta) * prev_symbol_slip + beta * entry_slippage_bps
        exec_quality_score = 0.50
        slip_penalty = _clamp(max(0.0, entry_slippage_bps) / 14.0, 0.0, 1.0)
        if touch_outcome:
            pnl_component = _clamp(0.50 + (pnl_net_pct / 0.015), 0.0, 1.0)
            exec_quality_score = _clamp((0.58 * pnl_component) + (0.42 * (1.0 - slip_penalty)), 0.0, 1.0)
        elif entry_slippage_bps > 0:
            exec_quality_score = _clamp(0.56 * (1.0 - slip_penalty), 0.0, 1.0)
        gamma = 0.18
        prev_exec_q = _safe_float(self.symbol_exec_quality_ema.get(sym, 0.50), 0.50)
        self.symbol_exec_quality_ema[sym] = (1.0 - gamma) * prev_exec_q + gamma * exec_quality_score

    @staticmethod
    def _trade_learning_outcome(
        *,
        pnl_pct_net: float,
        entry_quality: float,
        entry_edge: float,
        exit_reason: str,
        duration_sec: float,
    ) -> dict[str, float | str]:
        exit_reason_norm = str(exit_reason or "").strip().lower()
        pnl_clamped = max(-0.03, min(0.03, float(pnl_pct_net)))
        win_now = 1.0 if pnl_pct_net > 0 else 0.0
        quality = _clamp(entry_quality, 0.0, 1.0)
        edge = max(-0.01, min(0.03, float(entry_edge)))
        duration_sec = max(0.0, float(duration_sec))

        exit_eff_now = 0.50
        bad_entry = 0.0
        tp_unreachable = 0.0
        if exit_reason_norm in {"take_profit", "trailing_stop"}:
            exit_eff_now = 1.0
        elif exit_reason_norm in {"smart_stagnation_exit", "stall_exit"}:
            exit_eff_now = 0.65 if pnl_pct_net >= 0 else 0.26
            if pnl_pct_net < 0:
                tp_unreachable = 1.0
                bad_entry = 0.75
        elif exit_reason_norm in {"stop_loss"}:
            exit_eff_now = 0.12 if pnl_pct_net < 0 else 0.42
            if pnl_pct_net < 0:
                bad_entry = 1.0
                if edge >= 0.0014 and quality >= 0.52:
                    tp_unreachable = 0.65
        elif exit_reason_norm in {"time_stop", "ai_quality_fade"}:
            exit_eff_now = 0.18 if pnl_pct_net < 0 else 0.44
            if pnl_pct_net < 0:
                bad_entry = 0.62
                if edge >= 0.0012:
                    tp_unreachable = 0.55

        if pnl_pct_net < 0 and quality < 0.48:
            bad_entry = max(bad_entry, 0.72)
        if pnl_pct_net < 0 and edge < 0.0010:
            bad_entry = max(bad_entry, 0.84)
        if pnl_pct_net > 0 and exit_reason_norm in {"take_profit", "trailing_stop"}:
            good_entry = 1.0
        else:
            good_entry = _clamp((win_now * 0.72) + (max(0.0, pnl_clamped) * 7.0) + (exit_eff_now * 0.14), 0.0, 1.0)

        if duration_sec <= 1200 and pnl_pct_net < 0 and exit_reason_norm in {"smart_stagnation_exit", "stall_exit"}:
            tp_unreachable = max(tp_unreachable, 0.78)

        score_now = _clamp(
            (good_entry * 0.52)
            + (exit_eff_now * 0.16)
            + (max(-0.03, min(0.03, pnl_pct_net)) * 6.5)
            - (bad_entry * 0.34)
            - (tp_unreachable * 0.22),
            0.0,
            1.0,
        )
        return {
            "score": float(score_now),
            "win": float(win_now),
            "exit_efficiency": float(_clamp(exit_eff_now, 0.0, 1.0)),
            "good_entry": float(_clamp(good_entry, 0.0, 1.0)),
            "bad_entry": float(_clamp(bad_entry, 0.0, 1.0)),
            "tp_unreachable": float(_clamp(tp_unreachable, 0.0, 1.0)),
            "label": (
                "good_entry"
                if good_entry >= 0.72 and bad_entry < 0.35
                else "tp_unreachable"
                if tp_unreachable >= 0.60
                else "bad_entry"
                if bad_entry >= 0.60
                else "neutral"
            ),
        }

    @staticmethod
    def _recency_alpha(duration_sec: float, dry_run: bool) -> float:
        dur = max(0.0, _safe_float(duration_sec, 0.0))
        base = 0.14 if dry_run else 0.11
        if dur <= 0:
            return base
        if dur <= 900:
            return min(0.24, base + 0.06)
        if dur <= 3600:
            return min(0.20, base + 0.03)
        return max(0.07, base - 0.02)

    def _update_regime_trade_feedback(
        self,
        *,
        symbol: str,
        regime: str,
        pnl_pct_net: float,
        entry_quality: float,
        entry_edge: float,
        exit_reason: str,
        duration_sec: float,
    ) -> None:
        rk = str(regime or "flat").strip().lower()
        if not rk:
            rk = "flat"
        st = self.regime_trade_feedback.get(rk, {})
        alpha = self._recency_alpha(duration_sec, bool(self.config.DRY_RUN))
        outcome = self._trade_learning_outcome(
            pnl_pct_net=pnl_pct_net,
            entry_quality=entry_quality,
            entry_edge=entry_edge,
            exit_reason=exit_reason,
            duration_sec=duration_sec,
        )
        win_now = _safe_float(outcome.get("win"), 0.0)
        exit_eff_now = _safe_float(outcome.get("exit_efficiency"), 0.5)
        good_entry_now = _safe_float(outcome.get("good_entry"), 0.5)
        bad_entry_now = _safe_float(outcome.get("bad_entry"), 0.0)
        tp_unreachable_now = _safe_float(outcome.get("tp_unreachable"), 0.0)
        ema_pnl = ((1.0 - alpha) * _safe_float(st.get("ema_pnl"), 0.0)) + (alpha * pnl_pct_net)
        ema_win = ((1.0 - alpha) * _safe_float(st.get("ema_win"), 0.5)) + (alpha * win_now)
        ema_quality = ((1.0 - alpha) * _safe_float(st.get("ema_quality"), 0.5)) + (alpha * _clamp(entry_quality, 0.0, 1.0))
        ema_edge = ((1.0 - alpha) * _safe_float(st.get("ema_edge"), 0.0)) + (alpha * entry_edge)
        ema_exit_eff = ((1.0 - alpha) * _safe_float(st.get("ema_exit_efficiency"), 0.5)) + (alpha * exit_eff_now)
        ema_good_entry = ((1.0 - alpha) * _safe_float(st.get("ema_good_entry"), 0.5)) + (alpha * good_entry_now)
        ema_bad_entry = ((1.0 - alpha) * _safe_float(st.get("ema_bad_entry"), 0.0)) + (alpha * bad_entry_now)
        ema_tp_unreachable = ((1.0 - alpha) * _safe_float(st.get("ema_tp_unreachable"), 0.0)) + (alpha * tp_unreachable_now)
        self.regime_trade_feedback[rk] = {
            "ema_pnl": float(ema_pnl),
            "ema_win": float(ema_win),
            "ema_quality": float(ema_quality),
            "ema_edge": float(ema_edge),
            "ema_exit_efficiency": float(ema_exit_eff),
            "ema_good_entry": float(ema_good_entry),
            "ema_bad_entry": float(ema_bad_entry),
            "ema_tp_unreachable": float(ema_tp_unreachable),
            "count": float(_safe_float(st.get("count"), 0.0) + 1.0),
            "updated_ts": float(time.time()),
        }
        score_now = _safe_float(outcome.get("score"), 0.5)
        self.recent_trade_scores.append(float(score_now))
        self._update_shared_learning_feedback(
            ai_quality=entry_quality,
            ai_conf=_clamp(entry_quality + 0.06, 0.0, 1.0),
            expected_edge_pct=entry_edge,
            min_expected_edge_pct=max(0.0, entry_edge * 0.88),
            decision_score=score_now,
            trade_score=score_now,
        )
        self._update_confidence_calibration(
            ai_conf=_clamp(entry_quality + 0.06, 0.0, 1.0),
            realized_score=score_now,
        )
        sym_key = f"{str(symbol or '').upper().strip()}|{rk}"
        if sym_key and "|" in sym_key:
            sym_state = self.symbol_regime_trade_feedback.get(sym_key, {})
            self.symbol_regime_trade_feedback[sym_key] = {
                "ema_pnl": float(((1.0 - alpha) * _safe_float(sym_state.get("ema_pnl"), 0.0)) + (alpha * pnl_pct_net)),
                "ema_win": float(((1.0 - alpha) * _safe_float(sym_state.get("ema_win"), 0.5)) + (alpha * win_now)),
                "ema_good_entry": float(((1.0 - alpha) * _safe_float(sym_state.get("ema_good_entry"), 0.5)) + (alpha * good_entry_now)),
                "ema_bad_entry": float(((1.0 - alpha) * _safe_float(sym_state.get("ema_bad_entry"), 0.0)) + (alpha * bad_entry_now)),
                "ema_tp_unreachable": float(((1.0 - alpha) * _safe_float(sym_state.get("ema_tp_unreachable"), 0.0)) + (alpha * tp_unreachable_now)),
                "count": float(_safe_float(sym_state.get("count"), 0.0) + 1.0),
                "updated_ts": float(time.time()),
            }

    def _regime_trade_bias(self, regime: str) -> Dict[str, float]:
        st = self.regime_trade_feedback.get(str(regime or "flat").strip().lower(), {})
        count = _safe_float(st.get("count"), 0.0)
        if count <= 0:
            return {"edge_mult": 1.0, "time_mult": 1.0, "exit_bias": 0.0}
        ema_pnl = _safe_float(st.get("ema_pnl"), 0.0)
        ema_win = _safe_float(st.get("ema_win"), 0.5)
        ema_exit_eff = _safe_float(st.get("ema_exit_efficiency"), 0.5)
        ema_good_entry = _safe_float(st.get("ema_good_entry"), 0.5)
        ema_bad_entry = _safe_float(st.get("ema_bad_entry"), 0.0)
        ema_tp_unreachable = _safe_float(st.get("ema_tp_unreachable"), 0.0)
        confidence = _clamp(count / (18.0 if self.config.DRY_RUN else 12.0), 0.0, 1.0)
        edge_mult = 1.0
        time_mult = 1.0
        exit_bias = 0.0
        if ema_pnl < -0.0020 or ema_win < 0.42:
            edge_mult += (0.07 * confidence)
            time_mult -= (0.10 * confidence)
            exit_bias += (0.10 * confidence)
        elif ema_pnl > 0.0014 and ema_win > 0.56 and ema_exit_eff > 0.52:
            edge_mult -= (0.05 * confidence)
            time_mult += (0.08 * confidence)
            exit_bias -= (0.05 * confidence)
        if ema_bad_entry >= 0.56:
            edge_mult += (0.06 * confidence)
            exit_bias += (0.08 * confidence)
        if ema_tp_unreachable >= 0.42:
            edge_mult += (0.05 * confidence)
            time_mult -= (0.08 * confidence)
            exit_bias += (0.06 * confidence)
        if ema_good_entry >= 0.62 and ema_exit_eff >= 0.56:
            edge_mult -= (0.03 * confidence)
            exit_bias -= (0.03 * confidence)
        return {
            "edge_mult": float(_clamp(edge_mult, 0.88, 1.18)),
            "time_mult": float(_clamp(time_mult, 0.82, 1.20)),
            "exit_bias": float(_clamp(exit_bias, -0.08, 0.16)),
        }

    def _symbol_regime_trade_bias(self, symbol: str, regime: str) -> Dict[str, float]:
        key = f"{str(symbol or '').upper().strip()}|{str(regime or 'flat').strip().lower()}"
        st = self.symbol_regime_trade_feedback.get(key, {})
        count = _safe_float(st.get("count"), 0.0)
        recent_items = [
            x for x in list(self.recent_cycle_feedback)[-24:]
            if isinstance(x, dict)
            and str(x.get("symbol", "")).strip().upper() == str(symbol or "").upper().strip()
            and str(x.get("regime", "")).strip().lower() == str(regime or "flat").strip().lower()
        ]
        if count <= 0 and not recent_items:
            return {"edge_delta": 0.0, "quality_delta": 0.0}
        ema_pnl = _safe_float(st.get("ema_pnl"), 0.0)
        ema_win = _safe_float(st.get("ema_win"), 0.5)
        ema_decision = _safe_float(st.get("ema_decision"), 0.5)
        ema_good_entry = _safe_float(st.get("ema_good_entry"), 0.5)
        ema_bad_entry = _safe_float(st.get("ema_bad_entry"), 0.0)
        ema_tp_unreachable = _safe_float(st.get("ema_tp_unreachable"), 0.0)
        confidence = _clamp(count / (10.0 if self.config.DRY_RUN else 7.0), 0.0, 1.0)
        edge_delta = 0.0
        quality_delta = 0.0
        if ema_pnl < -0.0018 or ema_win < 0.40:
            edge_delta += 0.00055 * confidence
            quality_delta += 0.018 * confidence
        elif ema_pnl > 0.0012 and ema_win > 0.58:
            edge_delta -= 0.00040 * confidence
            quality_delta -= 0.012 * confidence
        if ema_decision < 0.44:
            edge_delta += 0.00025 * max(0.35, confidence)
            quality_delta += 0.010 * max(0.35, confidence)
        elif ema_decision > 0.64:
            edge_delta -= 0.00018 * max(0.35, confidence)
            quality_delta -= 0.007 * max(0.35, confidence)
        if ema_bad_entry >= 0.58:
            edge_delta += 0.00028 * max(0.35, confidence)
            quality_delta += 0.009 * max(0.35, confidence)
        if ema_tp_unreachable >= 0.44:
            edge_delta += 0.00022 * max(0.35, confidence)
            quality_delta += 0.006 * max(0.35, confidence)
        if ema_good_entry >= 0.62 and ema_win >= 0.55:
            edge_delta -= 0.00016 * max(0.35, confidence)
            quality_delta -= 0.005 * max(0.35, confidence)
        if recent_items:
            recent_scores = [float(_safe_float(x.get("decision_score"), 0.5)) for x in recent_items]
            recent_avg = float(sum(recent_scores) / max(1, len(recent_scores)))
            if recent_avg < 0.40:
                edge_delta += 0.00022
                quality_delta += 0.008
            elif recent_avg > 0.66:
                edge_delta -= 0.00016
                quality_delta -= 0.006
        return {
            "edge_delta": float(_clamp(edge_delta, -0.0010, 0.0012)),
            "quality_delta": float(_clamp(quality_delta, -0.03, 0.04)),
        }

    def _symbol_regime_live_bias(self, symbol: str, regime: str) -> Dict[str, float]:
        sym = str(symbol or "").upper().strip()
        regime_key = str(regime or "flat").strip().lower()
        quality_ema = _safe_float(self.symbol_quality_ema.get(sym, 0.0), 0.0)
        streak = int(_safe_float(self.symbol_loss_streak.get(sym, 0.0), 0.0))
        recent_items = [
            x for x in list(self.recent_cycle_feedback)[-28:]
            if isinstance(x, dict)
            and str(x.get("symbol", "")).strip().upper() == sym
            and str(x.get("regime", "")).strip().lower() == regime_key
        ]
        edge_delta = 0.0
        quality_delta = 0.0
        risk_mult = 1.0
        debounce_delta = 0
        if quality_ema <= -0.0018:
            edge_delta += 0.00035
            quality_delta += 0.010
            risk_mult *= 0.90
        elif quality_ema >= 0.0012:
            edge_delta -= 0.00018
            quality_delta -= 0.006
            risk_mult *= 1.03
        if streak >= 2:
            edge_delta += min(0.0011, 0.00022 * streak)
            quality_delta += min(0.035, 0.008 * streak)
            risk_mult *= max(0.72, 1.0 - (0.08 * min(streak, 4)))
            debounce_delta += 1
        if recent_items:
            recent_scores = [float(_safe_float(x.get("decision_score"), 0.5)) for x in recent_items]
            recent_avg = float(sum(recent_scores) / max(1, len(recent_scores)))
            if recent_avg < 0.38:
                edge_delta += 0.00026
                quality_delta += 0.010
                risk_mult *= 0.92
            elif recent_avg > 0.68 and streak <= 0:
                edge_delta -= 0.00016
                quality_delta -= 0.005
                risk_mult *= 1.02
        return {
            "edge_delta": float(_clamp(edge_delta, -0.0008, 0.0014)),
            "quality_delta": float(_clamp(quality_delta, -0.025, 0.045)),
            "risk_mult": float(_clamp(risk_mult, 0.68, 1.06)),
            "debounce_delta": int(max(0, min(2, debounce_delta))),
        }

    def _execution_profile_bias(self) -> Dict[str, float]:
        api_errors = int(_safe_float(self.guard_state.get("consecutive_api_errors"), 0.0))
        logic_errors = int(_safe_float(self.guard_state.get("consecutive_logic_errors"), 0.0))
        slow_cycles = int(max(0, self._slow_cycle_streak))
        slip_bps = max(0.0, _safe_float(self.exec_slippage_ema_bps, 0.0))
        edge_delta = 0.0
        quality_delta = 0.0
        conf_delta = 0.0
        risk_mult = 1.0
        debounce_delta = 0
        label = "normal"
        if slip_bps >= 12.0:
            edge_delta += 0.00040
            quality_delta += 0.012
            conf_delta += 0.008
            risk_mult *= 0.84
            debounce_delta += 1
            label = "slippage_guard"
        elif slip_bps >= 7.0:
            edge_delta += 0.00022
            quality_delta += 0.007
            risk_mult *= 0.92
            label = "slippage_watch"
        if api_errors >= 2:
            edge_delta += 0.00032
            quality_delta += 0.010
            conf_delta += 0.008
            risk_mult *= 0.88
            debounce_delta += 1
            label = "api_unstable"
        if logic_errors >= 2 or slow_cycles >= 2:
            edge_delta += 0.00024
            quality_delta += 0.008
            conf_delta += 0.006
            risk_mult *= 0.90
            debounce_delta += 1
            label = "runtime_degraded"
        return {
            "edge_delta": float(_clamp(edge_delta, 0.0, 0.0012)),
            "quality_delta": float(_clamp(quality_delta, 0.0, 0.03)),
            "conf_delta": float(_clamp(conf_delta, 0.0, 0.02)),
            "risk_mult": float(_clamp(risk_mult, 0.70, 1.0)),
            "debounce_delta": int(max(0, min(2, debounce_delta))),
            "label": label,
        }

    def _symbol_execution_bias(self, symbol: str) -> Dict[str, float]:
        sym = str(symbol or "").upper().strip()
        if not sym:
            return {
                "edge_delta": 0.0,
                "risk_mult": 1.0,
                "tp_mult": 1.0,
                "ts_mult": 1.0,
                "sl_mult": 1.0,
                "label": "none",
            }
        slip_bps = max(0.0, _safe_float(self.symbol_exec_slippage_ema_bps.get(sym, 0.0), 0.0))
        exec_quality = _clamp(_safe_float(self.symbol_exec_quality_ema.get(sym, 0.50), 0.50), 0.0, 1.0)
        edge_delta = 0.0
        risk_mult = 1.0
        tp_mult = 1.0
        ts_mult = 1.0
        sl_mult = 1.0
        label = "normal"
        if slip_bps >= 10.0:
            edge_delta += 0.00034
            risk_mult *= 0.88
            tp_mult *= 0.93
            ts_mult *= 0.90
            sl_mult *= 0.97
            label = "symbol_slippage_guard"
        elif slip_bps >= 6.0:
            edge_delta += 0.00016
            risk_mult *= 0.94
            tp_mult *= 0.97
            ts_mult *= 0.95
            label = "symbol_slippage_watch"
        if exec_quality <= 0.34:
            edge_delta += 0.00030
            risk_mult *= 0.86
            tp_mult *= 0.92
            ts_mult *= 0.88
            sl_mult *= 0.96
            label = "symbol_exec_degraded"
        elif exec_quality >= 0.66 and slip_bps <= 4.0:
            edge_delta -= 0.00012
            risk_mult *= 1.04
            tp_mult *= 1.02
            ts_mult *= 1.01
            label = "symbol_exec_clean"
        return {
            "edge_delta": float(_clamp(edge_delta, -0.0002, 0.0010)),
            "risk_mult": float(_clamp(risk_mult, 0.76, 1.05)),
            "tp_mult": float(_clamp(tp_mult, 0.88, 1.04)),
            "ts_mult": float(_clamp(ts_mult, 0.84, 1.03)),
            "sl_mult": float(_clamp(sl_mult, 0.94, 1.02)),
            "label": label,
            "slippage_bps": float(slip_bps),
            "exec_quality": float(exec_quality),
        }

    def _market_regime_bucket(
        self,
        regime: str,
        flags: set[str] | list[str] | tuple[str, ...] | None = None,
        market_volatility: float = 0.0,
        anomaly_score: float = 0.0,
        momentum_speed: float = 0.0,
    ) -> str:
        regime_name = str(regime or "flat").strip().lower() or "flat"
        raw_flags = flags if isinstance(flags, (set, list, tuple)) else []
        flag_set = {str(x or "").strip().lower() for x in raw_flags if str(x or "").strip()}
        vol = max(0.0, _safe_float(market_volatility, 0.0))
        anomaly = max(0.0, _safe_float(anomaly_score, 0.0))
        momentum = max(0.0, _safe_float(momentum_speed, 0.0))
        if anomaly >= 0.72 or "dangerous" in flag_set:
            return "danger_anomalous"
        if regime_name == "impulse" or ("volume_spike" in flag_set and momentum >= 0.30):
            return "trend_impulse" if regime_name in {"trend", "impulse"} else "flat_spike"
        if regime_name == "trend":
            if momentum >= 0.22 or vol >= 0.0026:
                return "trend_fast"
            if "low_vol" in flag_set or "low_volatility" in flag_set:
                return "trend_low_vol"
            return "trend_slow"
        if regime_name in {"range", "flat"}:
            if "low_vol" in flag_set or "low_volatility" in flag_set or vol <= 0.0010:
                return "flat_low_vol"
            if "chop" in flag_set or "sideways" in flag_set or momentum <= 0.10:
                return "flat_chop"
            return "flat_balanced"
        if "low_vol" in flag_set or "low_volatility" in flag_set:
            return "neutral_low_vol"
        return f"{regime_name}_base"

    @staticmethod
    def _confidence_bucket(confidence: float) -> str:
        conf = _clamp(_safe_float(confidence, 0.0), 0.0, 1.0)
        if conf >= 0.82:
            return "very_high"
        if conf >= 0.68:
            return "high"
        if conf >= 0.56:
            return "mid"
        if conf >= 0.44:
            return "soft"
        return "low"

    def _update_confidence_calibration(
        self,
        *,
        ai_conf: float,
        realized_score: float,
    ) -> None:
        conf = _clamp(_safe_float(ai_conf, 0.0), 0.0, 1.0)
        score = _clamp(_safe_float(realized_score, 0.5), 0.0, 1.0)
        bucket = self._confidence_bucket(conf)
        alpha = 0.10 if score >= 0.60 or score <= 0.40 else 0.06
        st = dict(self.confidence_calibration_state or {})
        st["overall_ema_score"] = ((1.0 - alpha) * _safe_float(st.get("overall_ema_score"), 0.50)) + (alpha * score)
        st["overall_ema_margin"] = ((1.0 - alpha) * _safe_float(st.get("overall_ema_margin"), 0.0)) + (alpha * (score - conf))
        raw_bucket_stats = st.get("bucket_stats", {})
        bucket_stats = dict(raw_bucket_stats) if isinstance(raw_bucket_stats, dict) else {}
        bucket_state = dict(bucket_stats.get(bucket, {}))
        bucket_state["ema_score"] = ((1.0 - alpha) * _safe_float(bucket_state.get("ema_score"), 0.50)) + (alpha * score)
        bucket_state["ema_margin"] = ((1.0 - alpha) * _safe_float(bucket_state.get("ema_margin"), 0.0)) + (alpha * (score - conf))
        bucket_state["count"] = min(5000.0, _safe_float(bucket_state.get("count"), 0.0) + 1.0)
        bucket_state["updated_ts"] = float(time.time())
        bucket_stats[bucket] = bucket_state
        st["bucket_stats"] = bucket_stats
        st["updated_ts"] = float(time.time())
        self.confidence_calibration_state = st

    def _confidence_calibration_bias(self, ai_conf: float) -> Dict[str, float]:
        conf = _clamp(_safe_float(ai_conf, 0.0), 0.0, 1.0)
        st = dict(self.confidence_calibration_state or {})
        bucket = self._confidence_bucket(conf)
        bucket_stats = st.get("bucket_stats", {})
        bucket_state = dict(bucket_stats.get(bucket, {})) if isinstance(bucket_stats, dict) else {}
        bucket_margin = _safe_float(bucket_state.get("ema_margin"), 0.0)
        bucket_score = _clamp(_safe_float(bucket_state.get("ema_score"), _safe_float(st.get("overall_ema_score"), 0.50)), 0.0, 1.0)
        bucket_count = _safe_float(bucket_state.get("count"), 0.0)
        strength = _clamp(bucket_count / 36.0, 0.0, 1.0)
        edge_delta = 0.0
        quality_delta = 0.0
        conf_delta = 0.0
        if bucket_margin <= -0.06 and conf >= 0.56:
            edge_delta += 0.00022 * max(0.35, strength)
            quality_delta += 0.007 * max(0.35, strength)
            conf_delta += 0.010 * max(0.35, strength)
        elif bucket_margin >= 0.05 and bucket_score >= 0.58:
            edge_delta -= 0.00014 * max(0.30, strength)
            quality_delta -= 0.005 * max(0.30, strength)
            conf_delta -= 0.008 * max(0.30, strength)
        return {
            "edge_delta": float(_clamp(edge_delta, -0.00022, 0.00036)),
            "quality_delta": float(_clamp(quality_delta, -0.008, 0.012)),
            "conf_delta": float(_clamp(conf_delta, -0.012, 0.016)),
            "bucket": bucket,
            "score": float(bucket_score),
            "margin": float(bucket_margin),
        }

    def _missed_opportunity_keys(self, symbol: str, regime_bucket: str) -> list[str]:
        sym = str(symbol or "").upper().strip()
        bucket = str(regime_bucket or "flat_balanced").strip().upper()
        keys: list[str] = []
        if sym and bucket:
            keys.append(f"{sym}|{bucket}")
        if bucket:
            keys.append(bucket)
        return keys

    def _register_missed_opportunity_candidate(
        self,
        *,
        symbol: str,
        regime_bucket: str,
        event: str,
        ai_action: str,
        ai_conf: float,
        ai_quality: float,
        expected_edge_pct: float,
        min_expected_edge_pct: float,
        price: float,
        latest_candle_ts: int,
        buy_block_reasons: list[str],
    ) -> None:
        evt = str(event or "").strip().lower()
        action = str(ai_action or "").strip().upper()
        sym = str(symbol or "").upper().strip()
        px = max(0.0, _safe_float(price, 0.0))
        if evt not in {"entry_blocked", "no_trade_signal", "training_no_signal", "risk_guard_blocked"}:
            return
        if not sym or px <= 0 or action not in {"LONG", "SHORT"}:
            return
        conf = _clamp(_safe_float(ai_conf, 0.0), 0.0, 1.0)
        quality = _clamp(_safe_float(ai_quality, 0.0), 0.0, 1.0)
        edge_now = _safe_float(expected_edge_pct, 0.0)
        edge_floor = max(1e-9, _safe_float(min_expected_edge_pct, 0.0))
        if conf < 0.54 or quality < 0.50:
            return
        if edge_now < (edge_floor * 0.60):
            return
        reasons = [str(x or "").strip().lower() for x in buy_block_reasons if str(x or "").strip()]
        if reasons and all("market_data" in x for x in reasons):
            return
        self.pending_opportunity_setups.append({
            "symbol": sym,
            "bucket": str(regime_bucket or "flat_balanced").strip().upper(),
            "action": action,
            "entry_price": float(px),
            "entry_edge": float(edge_now),
            "entry_floor": float(edge_floor),
            "entry_conf": float(conf),
            "entry_quality": float(quality),
            "entry_candle_ts": int(latest_candle_ts),
            "created_ts": float(time.time()),
            "expiry_sec": float(780.0 if (self.config.DRY_RUN and self.config.AI_TRAINING_MODE) else 1020.0),
            "reasons": reasons[:5],
        })

    def _refresh_missed_opportunities(self, symbol: str, current_price: float, latest_candle_ts: int) -> None:
        if not self.pending_opportunity_setups:
            return
        sym = str(symbol or "").upper().strip()
        px = max(0.0, _safe_float(current_price, 0.0))
        if not sym or px <= 0:
            return
        now_ts = time.time()
        remaining: deque[Dict[str, object]] = deque(maxlen=self.pending_opportunity_setups.maxlen)
        while self.pending_opportunity_setups:
            item = self.pending_opportunity_setups.popleft()
            if not isinstance(item, dict):
                continue
            item_sym = str(item.get("symbol", "")).upper().strip()
            if item_sym != sym:
                if (now_ts - _safe_float(item.get("created_ts"), now_ts)) < _safe_float(item.get("expiry_sec"), 900.0):
                    remaining.append(item)
                continue
            age_sec = max(0.0, now_ts - _safe_float(item.get("created_ts"), now_ts))
            candles_elapsed = self._candles_since(int(latest_candle_ts), int(_safe_float(item.get("entry_candle_ts"), 0.0)), self.config.TIMEFRAME)
            if age_sec < _safe_float(item.get("expiry_sec"), 900.0) and candles_elapsed < 6:
                remaining.append(item)
                continue
            entry_price = max(1e-9, _safe_float(item.get("entry_price"), 0.0))
            move_pct = (px / entry_price) - 1.0
            if str(item.get("action", "")).upper() == "SHORT":
                move_pct *= -1.0
            threshold = max(0.0012, min(0.0080, max(
                _safe_float(item.get("entry_edge"), 0.0) * 0.70,
                _safe_float(item.get("entry_floor"), 0.0) * 0.85,
            )))
            good = 1.0 if move_pct >= threshold else 0.0
            bad = 1.0 if move_pct <= (-threshold * 0.60) else 0.0
            alpha = 0.14 if (self.config.DRY_RUN and self.config.AI_TRAINING_MODE) else 0.11
            for key in self._missed_opportunity_keys(sym, str(item.get("bucket", ""))):
                st = dict(self.missed_opportunity_feedback.get(key, {}))
                st["ema_good"] = ((1.0 - alpha) * _safe_float(st.get("ema_good"), 0.5)) + (alpha * good)
                st["ema_bad"] = ((1.0 - alpha) * _safe_float(st.get("ema_bad"), 0.5)) + (alpha * bad)
                st["ema_move"] = ((1.0 - alpha) * _safe_float(st.get("ema_move"), 0.0)) + (alpha * move_pct)
                st["count"] = min(5000.0, _safe_float(st.get("count"), 0.0) + 1.0)
                st["updated_ts"] = float(now_ts)
                self.missed_opportunity_feedback[key] = st
            self._update_skip_learning_feedback(
                good_skip=good,
                bad_skip=bad,
                move_pct=move_pct,
            )
        self.pending_opportunity_setups = remaining

    def _missed_opportunity_bias(self, symbol: str, regime_bucket: str) -> Dict[str, float]:
        ema_good = 0.5
        ema_bad = 0.5
        ema_move = 0.0
        count = 0.0
        for key in self._missed_opportunity_keys(symbol, regime_bucket):
            st = dict(self.missed_opportunity_feedback.get(key, {}))
            if not st:
                continue
            weight = 1.0 if "|" in key else 0.55
            ema_good += (_safe_float(st.get("ema_good"), 0.5) - 0.5) * weight
            ema_bad += (_safe_float(st.get("ema_bad"), 0.5) - 0.5) * weight
            ema_move += _safe_float(st.get("ema_move"), 0.0) * weight
            count += _safe_float(st.get("count"), 0.0) * weight
        strength = _clamp(count / 28.0, 0.0, 1.0)
        edge_delta = 0.0
        quality_delta = 0.0
        risk_mult = 1.0
        if ema_good >= 0.58 and ema_bad <= 0.46 and ema_move >= 0.0008:
            edge_delta -= 0.00016 * max(0.35, strength)
            quality_delta -= 0.005 * max(0.35, strength)
            risk_mult *= 1.02
        elif ema_bad >= 0.58 and ema_good <= 0.46:
            edge_delta += 0.00022 * max(0.35, strength)
            quality_delta += 0.007 * max(0.35, strength)
            risk_mult *= 0.94
        return {
            "edge_delta": float(_clamp(edge_delta, -0.00022, 0.00032)),
            "quality_delta": float(_clamp(quality_delta, -0.008, 0.010)),
            "risk_mult": float(_clamp(risk_mult, 0.90, 1.03)),
            "score": float(_clamp((ema_good - ema_bad) + (ema_move * 22.0), -1.0, 1.0)),
            "count": float(count),
        }

    def _anti_fragile_mode(self, symbol: str, regime_bucket: str) -> Dict[str, float]:
        exec_stress = self._execution_stress_mode()
        missed_bias = self._missed_opportunity_bias(symbol, regime_bucket)
        symbol_exec = self._symbol_execution_bias(symbol)
        shared_bias = self._shared_learning_bias()
        stress_score = _safe_float(exec_stress.get("score"), 0.0)
        missed_score = _safe_float(missed_bias.get("score"), 0.0)
        exec_quality = _safe_float(symbol_exec.get("exec_quality"), 0.5)
        mode_score = 0.0
        if stress_score >= 0.24:
            mode_score += stress_score * 0.55
        if exec_quality <= 0.38:
            mode_score += 0.20
        if missed_score <= -0.10:
            mode_score += min(0.18, abs(missed_score) * 0.18)
        if _safe_float(shared_bias.get("score"), 0.5) <= 0.42:
            mode_score += 0.12
        mode_score = _clamp(mode_score, 0.0, 1.0)
        if mode_score < 0.20:
            if missed_score >= 0.12 and exec_quality >= 0.58 and stress_score < 0.12:
                return {
                    "edge_delta": -0.00010,
                    "quality_delta": -0.004,
                    "conf_delta": -0.003,
                    "risk_mult": 1.02,
                    "tp_mult": 1.02,
                    "ts_mult": 1.01,
                    "sl_mult": 1.00,
                    "label": "resilient",
                    "score": 0.12,
                }
            return {
                "edge_delta": 0.0,
                "quality_delta": 0.0,
                "conf_delta": 0.0,
                "risk_mult": 1.0,
                "tp_mult": 1.0,
                "ts_mult": 1.0,
                "sl_mult": 1.0,
                "label": "neutral",
                "score": float(mode_score),
            }
        return {
            "edge_delta": float(_clamp(mode_score * 0.0008, 0.0, 0.0010)),
            "quality_delta": float(_clamp(mode_score * 0.015, 0.0, 0.018)),
            "conf_delta": float(_clamp(mode_score * 0.012, 0.0, 0.015)),
            "risk_mult": float(_clamp(1.0 - (mode_score * 0.20), 0.78, 1.0)),
            "tp_mult": float(_clamp(1.0 - (mode_score * 0.14), 0.86, 1.0)),
            "ts_mult": float(_clamp(1.0 - (mode_score * 0.10), 0.88, 1.0)),
            "sl_mult": float(_clamp(1.0 - (mode_score * 0.05), 0.94, 1.0)),
            "label": "anti_fragile_guard",
            "score": float(mode_score),
        }

    def _update_shared_learning_feedback(
        self,
        *,
        ai_quality: float,
        ai_conf: float,
        expected_edge_pct: float,
        min_expected_edge_pct: float,
        decision_score: float,
        trade_score: Optional[float] = None,
    ) -> None:
        st = dict(self.shared_learning_feedback or {})
        source_training = bool(self.config.DRY_RUN and self.config.AI_TRAINING_MODE)
        alpha = 0.045 if source_training else 0.070
        pass_now = 1.0 if expected_edge_pct >= min_expected_edge_pct else 0.0
        edge_margin = float(expected_edge_pct - min_expected_edge_pct)
        st["ema_pass"] = ((1.0 - alpha) * _safe_float(st.get("ema_pass"), 0.25)) + (alpha * pass_now)
        st["ema_quality"] = ((1.0 - alpha) * _safe_float(st.get("ema_quality"), 0.50)) + (alpha * _clamp(ai_quality, 0.0, 1.0))
        st["ema_conf"] = ((1.0 - alpha) * _safe_float(st.get("ema_conf"), 0.50)) + (alpha * _clamp(ai_conf, 0.0, 1.0))
        st["ema_edge_margin"] = ((1.0 - alpha) * _safe_float(st.get("ema_edge_margin"), 0.0)) + (alpha * edge_margin)
        st["ema_cycle_score"] = ((1.0 - alpha) * _safe_float(st.get("ema_cycle_score"), 0.50)) + (alpha * _clamp(decision_score, 0.0, 1.0))
        if trade_score is not None:
            st["ema_trade_score"] = ((1.0 - alpha) * _safe_float(st.get("ema_trade_score"), 0.50)) + (alpha * _clamp(trade_score, 0.0, 1.0))
        if source_training:
            st["training_weight"] = min(5000.0, _safe_float(st.get("training_weight"), 0.0) + 1.0)
        else:
            st["live_weight"] = min(5000.0, _safe_float(st.get("live_weight"), 0.0) + 1.0)
        st["updated_ts"] = float(time.time())
        self.shared_learning_feedback = st

    def _update_skip_learning_feedback(
        self,
        *,
        good_skip: float,
        bad_skip: float,
        move_pct: float,
    ) -> None:
        st = dict(self.shared_learning_feedback or {})
        alpha = 0.050 if (self.config.DRY_RUN and self.config.AI_TRAINING_MODE) else 0.040
        st["ema_good_skip"] = ((1.0 - alpha) * _safe_float(st.get("ema_good_skip"), 0.50)) + (alpha * _clamp(good_skip, 0.0, 1.0))
        st["ema_bad_skip"] = ((1.0 - alpha) * _safe_float(st.get("ema_bad_skip"), 0.50)) + (alpha * _clamp(bad_skip, 0.0, 1.0))
        st["ema_skip_move"] = ((1.0 - alpha) * _safe_float(st.get("ema_skip_move"), 0.0)) + (alpha * float(move_pct))
        st["updated_ts"] = float(time.time())
        self.shared_learning_feedback = st

    def _shared_learning_bias(self) -> Dict[str, float]:
        st = dict(self.shared_learning_feedback or {})
        ema_pass = _clamp(_safe_float(st.get("ema_pass"), 0.25), 0.0, 1.0)
        ema_quality = _clamp(_safe_float(st.get("ema_quality"), 0.50), 0.0, 1.0)
        ema_conf = _clamp(_safe_float(st.get("ema_conf"), 0.50), 0.0, 1.0)
        ema_edge_margin = _safe_float(st.get("ema_edge_margin"), 0.0)
        ema_trade_score = _clamp(_safe_float(st.get("ema_trade_score"), 0.50), 0.0, 1.0)
        ema_cycle_score = _clamp(_safe_float(st.get("ema_cycle_score"), 0.50), 0.0, 1.0)
        ema_good_skip = _clamp(_safe_float(st.get("ema_good_skip"), 0.50), 0.0, 1.0)
        ema_bad_skip = _clamp(_safe_float(st.get("ema_bad_skip"), 0.50), 0.0, 1.0)
        ema_skip_move = _safe_float(st.get("ema_skip_move"), 0.0)
        training_weight = _safe_float(st.get("training_weight"), 0.0)
        live_weight = _safe_float(st.get("live_weight"), 0.0)
        skip_balance = _clamp((ema_good_skip - ema_bad_skip) + (ema_skip_move * 18.0), -1.0, 1.0)
        combined_score = (ema_trade_score * 0.50) + (ema_cycle_score * 0.34) + ((_clamp(skip_balance, -1.0, 1.0) + 1.0) * 0.08)
        live_dominant = live_weight >= max(8.0, training_weight * 0.20)
        edge_delta = 0.0
        quality_delta = 0.0
        conf_delta = 0.0
        if ema_pass <= 0.18 and combined_score <= 0.42:
            edge_delta += 0.00022
            quality_delta += 0.008
            conf_delta += 0.007
        elif ema_pass >= 0.44 and ema_edge_margin >= 0.0008 and combined_score >= 0.60:
            edge_delta -= 0.00018
            quality_delta -= 0.007
            conf_delta -= 0.006
        if ema_quality <= 0.42:
            quality_delta += 0.006
        elif ema_quality >= 0.64 and ema_conf >= 0.64:
            quality_delta -= 0.004
            conf_delta -= 0.004
        if skip_balance <= -0.18:
            edge_delta += 0.00016
            quality_delta += 0.005
        elif skip_balance >= 0.16 and ema_trade_score >= 0.52:
            edge_delta -= 0.00014
            quality_delta -= 0.004
        if not live_dominant:
            edge_delta *= 0.82
            quality_delta *= 0.82
            conf_delta *= 0.82
        return {
            "edge_delta": float(_clamp(edge_delta, -0.00035, 0.00045)),
            "quality_delta": float(_clamp(quality_delta, -0.012, 0.016)),
            "conf_delta": float(_clamp(conf_delta, -0.012, 0.016)),
            "score": float(_clamp(combined_score, 0.0, 1.0)),
            "skip_balance": float(skip_balance),
            "label": ("live_weighted" if live_dominant else "training_seeded"),
        }

    def _execution_stress_mode(self) -> Dict[str, float]:
        recent = list(self.recent_cycle_feedback)[-48:]
        slow_cycles = int(max(0, self._slow_cycle_streak))
        api_errors = int(_safe_float(self.guard_state.get("consecutive_api_errors"), 0.0))
        logic_errors = int(_safe_float(self.guard_state.get("consecutive_logic_errors"), 0.0))
        slip_bps = max(0.0, _safe_float(self.exec_slippage_ema_bps, 0.0))
        if not recent and api_errors <= 0 and logic_errors <= 0 and slip_bps < 6.0:
            return {"edge_delta": 0.0, "quality_delta": 0.0, "conf_delta": 0.0, "risk_mult": 1.0, "label": "normal", "score": 0.0}
        total = float(max(1, len(recent)))
        stale_rate = sum(1.0 for x in recent if bool(x.get("market_data_stale", False))) / total
        blocked_rate = sum(1.0 for x in recent if str(x.get("event", "")).lower() in {"entry_blocked", "risk_guard_blocked", "cooldown"}) / total
        decision_avg = sum(_safe_float(x.get("decision_score"), 0.5) for x in recent) / total
        stress_score = 0.0
        if stale_rate >= 0.16:
            stress_score += 0.24
        if blocked_rate >= 0.70:
            stress_score += 0.22
        if decision_avg <= 0.40:
            stress_score += 0.18
        if slow_cycles >= 2:
            stress_score += 0.20
        if api_errors >= 2:
            stress_score += 0.22
        if logic_errors >= 2:
            stress_score += 0.18
        if slip_bps >= 8.0:
            stress_score += min(0.22, (slip_bps - 8.0) * 0.018)
        stress_score = _clamp(stress_score, 0.0, 1.0)
        if stress_score < 0.22:
            return {"edge_delta": 0.0, "quality_delta": 0.0, "conf_delta": 0.0, "risk_mult": 1.0, "label": "normal", "score": float(stress_score)}
        return {
            "edge_delta": float(_clamp(stress_score * 0.0010, 0.0, 0.0012)),
            "quality_delta": float(_clamp(stress_score * 0.020, 0.0, 0.022)),
            "conf_delta": float(_clamp(stress_score * 0.016, 0.0, 0.018)),
            "risk_mult": float(_clamp(1.0 - (stress_score * 0.22), 0.74, 1.0)),
            "label": ("stress_guard" if stress_score >= 0.50 else "stress_watch"),
            "score": float(stress_score),
        }

    def _adaptive_symbol_market_cooldown(self, symbol: str, regime: str = "") -> Dict[str, float]:
        sym = str(symbol or "").upper().strip()
        regime_key = str(regime or "").strip().lower()
        if not sym:
            return {"seconds": 0.0, "label": "none", "score": 0.0}
        items = [
            x for x in list(self.recent_cycle_feedback)[-28:]
            if isinstance(x, dict) and str(x.get("symbol", "")).strip().upper() == sym
        ]
        if regime_key:
            regime_items = [x for x in items if str(x.get("regime", "")).strip().lower() == regime_key]
            if len(regime_items) >= 6:
                items = regime_items
        if len(items) < 6:
            return {"seconds": 0.0, "label": "none", "score": 0.0}
        total = float(len(items))
        blocked = sum(1.0 for x in items if str(x.get("event", "")).lower() in {"entry_blocked", "risk_guard_blocked", "cooldown"})
        trades = sum(1.0 for x in items if str(x.get("event", "")).lower() in {"buy", "sell", "training_buy_signal", "training_sell_signal", "partial_sell"})
        stale = sum(1.0 for x in items if bool(x.get("market_data_stale", False)))
        no_trade = sum(1.0 for x in items if str(x.get("event", "")).lower() in {"no_trade_signal", "training_no_signal"})
        decision_avg = float(sum(_safe_float(x.get("decision_score"), 0.5) for x in items) / max(1.0, total))
        blocked_rate = blocked / total
        stale_rate = stale / total
        no_trade_rate = no_trade / total
        trade_rate = trades / total
        loss_streak = int(_safe_float(self.symbol_loss_streak.get(sym, 0.0), 0.0))
        exec_bias = self._symbol_execution_bias(sym)
        weak_exec = (
            _safe_float(exec_bias.get("exec_quality"), 0.5) <= 0.36
            or _safe_float(exec_bias.get("slippage_bps"), 0.0) >= 8.0
        )
        seconds = 0.0
        score = 0.0
        label = "none"
        if blocked_rate >= 0.74 and decision_avg <= 0.42 and trade_rate <= 0.12:
            seconds += 35.0
            score += 0.38
            label = "blocked_window"
        if no_trade_rate >= 0.82 and decision_avg <= 0.40:
            seconds += 18.0
            score += 0.22
            label = "dead_window"
        if stale_rate >= 0.18:
            seconds += 14.0
            score += 0.14
            label = "stale_window"
        if loss_streak >= 2:
            seconds += min(45.0, 10.0 + (loss_streak * 7.0))
            score += 0.18
            label = "loss_streak_window"
        if weak_exec:
            seconds += 18.0
            score += 0.16
            label = "execution_window"
        if regime_key in {"flat", "range"} and decision_avg <= 0.36:
            seconds += 12.0
            score += 0.10
        if self.config.DRY_RUN and self.config.AI_TRAINING_MODE:
            seconds *= 0.55
        return {
            "seconds": float(_clamp(seconds, 0.0, 90.0)),
            "label": label,
            "score": float(_clamp(score, 0.0, 1.0)),
        }

    def _score_cycle_decision(
        self,
        *,
        event: str,
        buy_block_reasons: list[str],
        ai_quality: float,
        ai_conf: float,
        expected_edge_pct: float,
        min_expected_edge_pct: float,
        market_data_stale: bool,
    ) -> float:
        evt = str(event or "").strip().lower()
        reasons = [str(x or "").strip().lower() for x in buy_block_reasons if str(x or "").strip()]
        quality = float(_clamp(ai_quality, 0.0, 1.0))
        conf = float(_clamp(ai_conf, 0.0, 1.0))
        edge_ratio = 0.0
        if min_expected_edge_pct > 0:
            edge_ratio = float(_clamp(expected_edge_pct / max(1e-9, min_expected_edge_pct), -2.0, 2.0))
        if market_data_stale or evt == "market_data_unavailable":
            return 0.10
        if evt in {"buy", "training_buy_signal"}:
            return float(_clamp(0.50 + (quality * 0.22) + (conf * 0.18) + (max(0.0, edge_ratio) * 0.10), 0.0, 1.0))
        if evt in {"sell", "training_sell_signal", "partial_sell"}:
            return float(_clamp(0.56 + (quality * 0.12) + (max(0.0, edge_ratio) * 0.08), 0.0, 1.0))
        if evt == "risk_guard_blocked":
            return 0.52
        if evt == "entry_blocked":
            score = 0.48
            if any("expected_edge" in r or "edge" in r for r in reasons):
                score += 0.20 if expected_edge_pct < min_expected_edge_pct else 0.08
            if any("quality" in r for r in reasons):
                score += 0.16 if quality < 0.55 else 0.06
            if any("confidence" in r or "conf" in r for r in reasons):
                score += 0.10 if conf < 0.55 else 0.04
            return float(_clamp(score, 0.0, 1.0))
        if evt in {"no_trade_signal", "training_no_signal"}:
            if expected_edge_pct < min_expected_edge_pct or quality < 0.45:
                return float(_clamp(0.58 + (quality * 0.06), 0.0, 1.0))
            return 0.42
        if evt == "cooldown":
            return 0.55
        return 0.45

    def _update_cycle_feedback(
        self,
        *,
        symbol: str,
        regime: str,
        regime_bucket: str = "",
        event: str,
        buy_block_reasons: list[str],
        ai_quality: float,
        ai_conf: float,
        expected_edge_pct: float,
        min_expected_edge_pct: float,
        market_data_stale: bool,
    ) -> None:
        decision_score = self._score_cycle_decision(
            event=event,
            buy_block_reasons=buy_block_reasons,
            ai_quality=ai_quality,
            ai_conf=ai_conf,
            expected_edge_pct=expected_edge_pct,
            min_expected_edge_pct=min_expected_edge_pct,
            market_data_stale=market_data_stale,
        )
        item = {
            "symbol": str(symbol or "").upper().strip(),
            "regime": str(regime or "flat").strip().lower(),
            "regime_bucket": str(regime_bucket or "").strip().lower(),
            "event": str(event or "").strip().lower(),
            "buy_block_reasons": [str(x or "").strip().lower() for x in buy_block_reasons if str(x or "").strip()],
            "ai_quality": float(_clamp(ai_quality, 0.0, 1.0)),
            "ai_conf": float(_clamp(ai_conf, 0.0, 1.0)),
            "expected_edge_pct": float(expected_edge_pct),
            "min_expected_edge_pct": float(min_expected_edge_pct),
            "market_data_stale": bool(market_data_stale),
            "decision_score": float(decision_score),
            "ts": float(time.time()),
        }
        self.recent_cycle_feedback.append(item)
        self.recent_decision_scores.append(float(decision_score))
        self._update_shared_learning_feedback(
            ai_quality=ai_quality,
            ai_conf=ai_conf,
            expected_edge_pct=expected_edge_pct,
            min_expected_edge_pct=min_expected_edge_pct,
            decision_score=decision_score,
        )
        self._update_confidence_calibration(
            ai_conf=ai_conf,
            realized_score=decision_score,
        )
        sym_key = f"{str(symbol or '').upper().strip()}|{str(regime or 'flat').strip().lower()}"
        if sym_key and "|" in sym_key:
            sym_state = self.symbol_regime_trade_feedback.get(sym_key, {})
            alpha = 0.18 if self.config.DRY_RUN else 0.14
            self.symbol_regime_trade_feedback[sym_key] = {
                "ema_pnl": float(_safe_float(sym_state.get("ema_pnl"), 0.0)),
                "ema_win": float(_safe_float(sym_state.get("ema_win"), 0.5)),
                "ema_decision": float(((1.0 - alpha) * _safe_float(sym_state.get("ema_decision"), 0.5)) + (alpha * decision_score)),
                "count": float(max(_safe_float(sym_state.get("count"), 0.0), 0.0)),
                "updated_ts": float(time.time()),
            }

    def _cycle_adaptive_bias(self, symbol: str, regime: str) -> Dict[str, float]:
        regime_key = str(regime or "flat").strip().lower()
        symbol_key = str(symbol or "").upper().strip()
        if not self.recent_cycle_feedback:
            return {"edge_delta": 0.0, "quality_delta": 0.0, "conf_delta": 0.0, "debounce_delta": 0}
        items = [
            x for x in self.recent_cycle_feedback
            if isinstance(x, dict)
            and str(x.get("regime", "")).strip().lower() == regime_key
            and (not symbol_key or str(x.get("symbol", "")).strip().upper() == symbol_key)
        ]
        if len(items) < 8:
            items = [
                x for x in self.recent_cycle_feedback
                if isinstance(x, dict) and str(x.get("regime", "")).strip().lower() == regime_key
            ]
        if not items:
            return {"edge_delta": 0.0, "quality_delta": 0.0, "conf_delta": 0.0, "debounce_delta": 0}
        total = float(len(items))
        blocked = sum(1.0 for x in items if str(x.get("event", "")).lower() in {"entry_blocked", "risk_guard_blocked"})
        market_unavail = sum(1.0 for x in items if str(x.get("event", "")).lower() == "market_data_unavailable")
        weak_edge = sum(1.0 for x in items if any("expected_edge" in r or "edge" in r for r in x.get("buy_block_reasons", [])))
        weak_quality = sum(1.0 for x in items if any("quality" in r for r in x.get("buy_block_reasons", [])))
        stale = sum(1.0 for x in items if bool(x.get("market_data_stale", False)))
        trades = sum(1.0 for x in items if str(x.get("event", "")).lower() in {"buy", "sell", "training_buy_signal", "training_sell_signal", "partial_sell"})
        blocked_rate = blocked / total
        market_bad_rate = (market_unavail + stale) / total
        trade_rate = trades / total
        edge_delta = 0.0
        quality_delta = 0.0
        conf_delta = 0.0
        debounce_delta = 0
        if market_bad_rate >= 0.28:
            debounce_delta += 1
            conf_delta += 0.01
        if blocked_rate >= 0.72 and trade_rate < 0.08:
            if weak_edge >= weak_quality:
                edge_delta -= 0.00035
            else:
                quality_delta -= 0.010
                conf_delta -= 0.008
        elif blocked_rate <= 0.35 and trade_rate >= 0.10:
            edge_delta += 0.00010
        return {
            "edge_delta": float(_clamp(edge_delta, -0.0008, 0.0004)),
            "quality_delta": float(_clamp(quality_delta, -0.02, 0.015)),
            "conf_delta": float(_clamp(conf_delta, -0.02, 0.015)),
            "debounce_delta": int(max(-1, min(2, debounce_delta))),
        }

    def _refresh_confirmed_exchange_state(self, symbol: str, price_hint: float = 0.0) -> None:
        if self.config.DRY_RUN:
            return
        try:
            pos = self.trader.get_position(symbol)
        except Exception as exc:
            logging.info("Post-trade sync skipped for %s: %s", symbol, exc)
            return
        exchange_usdt_free = max(0.0, _safe_float(pos.get("usdt_free"), 0.0))
        exchange_base_free = max(0.0, _safe_float(pos.get("base_free"), 0.0))
        balance_source = str(pos.get("balance_source", "unknown") or "unknown").strip().lower()
        px = max(0.0, _safe_float(price_hint, 0.0))
        if px <= 0:
            try:
                px = max(0.0, _safe_float(self.trader.last_price(symbol, force_refresh=True), 0.0))
            except Exception:
                px = 0.0
        equity_usdt = exchange_usdt_free + (exchange_base_free * px if px > 0 else 0.0)
        if balance_source in {"exchange", "cache_exchange"}:
            self._last_confirmed_exchange_usdt_free = exchange_usdt_free
            self._last_confirmed_exchange_equity_usdt = max(0.0, equity_usdt)
            self._last_confirmed_exchange_balance_source = balance_source

    def _loss_streak_risk_scale(self) -> float:
        if self.loss_streak_live <= 0:
            return 1.0
        # 1,2,3+ losses -> softer size.
        return _clamp(1.0 - (0.14 * min(self.loss_streak_live, 4)), 0.45, 1.0)

    def _param_f(self, key: str, fallback: float) -> float:
        return _safe_float(self.runtime_params.get(key), fallback)

    def _param_i(self, key: str, fallback: int) -> int:
        return int(_safe_float(self.runtime_params.get(key), float(fallback)))

    def _classify_loss_case(self, pnl_pct: float, entry_quality: float, market_ctx: dict[str, object], spread_like: float) -> str:
        if pnl_pct >= 0:
            return ""
        regime = str(market_ctx.get("regime", "flat") or "flat")
        trend_strength = _safe_float(market_ctx.get("trend_strength"), 0.0)
        impulse_score = _safe_float(market_ctx.get("impulse_score"), 0.0)
        if regime == "flat":
            return "entry_in_sideways"
        if impulse_score > 2.2 and entry_quality < 0.45:
            return "entry_against_impulse"
        if trend_strength > 0.005 and entry_quality < 0.50:
            return "late_entry"
        if entry_quality < 0.35:
            return "early_entry"
        if spread_like > 0.01:
            return "low_liquidity_entry"
        return "uncategorized_loss"

    @staticmethod
    def _atr_pct(ohlcv: list[list[float]], period: int = 14) -> float:
        if len(ohlcv) < max(2, period + 1):
            return 0.0
        true_ranges: list[float] = []
        prev_close = _safe_float(ohlcv[0][4], 0.0)
        for candle in ohlcv[1:]:
            if len(candle) < 5:
                continue
            high = _safe_float(candle[2], 0.0)
            low = _safe_float(candle[3], 0.0)
            close = _safe_float(candle[4], 0.0)
            if high <= 0 or low <= 0 or close <= 0 or prev_close <= 0:
                prev_close = close if close > 0 else prev_close
                continue
            tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
            true_ranges.append(max(0.0, tr))
            prev_close = close
        if len(true_ranges) < period:
            return 0.0
        atr = float(np.mean(true_ranges[-period:]))
        last_close = _safe_float(ohlcv[-1][4], 0.0)
        if atr <= 0 or last_close <= 0:
            return 0.0
        return atr / last_close

    def _dynamic_time_stop_candles(self, active_risk: dict, ai_signal: Optional[AISignal], atr_pct: float) -> int:
        base = max(1, self.config.POSITION_TIME_STOP_CANDLES)
        min_c = max(1, self._param_i("TIME_STOP_MIN_CANDLES", self.config.TIME_STOP_MIN_CANDLES))
        max_c = max(min_c, self._param_i("TIME_STOP_MAX_CANDLES", self.config.TIME_STOP_MAX_CANDLES))
        if atr_pct <= 0:
            return int(_clamp(base, min_c, max_c))
        vol_ref = max(1e-6, self._param_f("TIME_STOP_VOL_REF", self.config.TIME_STOP_VOL_REF))
        vol_scale = _clamp(vol_ref / max(1e-6, atr_pct), 0.6, 3.0)
        conf = 0.5
        if ai_signal is not None:
            conf_base = ai_signal.calibrated_confidence if ai_signal.calibrated_confidence > 0 else ai_signal.confidence
            conf = _clamp(conf_base, 0.0, 1.0)
        strength_boost = _lerp(0.9, 1.25, conf)
        regime = str(self.runtime_market_ctx.get("regime", "flat") or "flat").lower().strip()
        bias = self._regime_trade_bias(regime)
        quality = self._quality_for_entry(ai_signal)
        quality_mult = _lerp(0.90, 1.18, quality)
        if regime in {"flat", "range"}:
            quality_mult *= 0.94
        dynamic = int(base * vol_scale * strength_boost * quality_mult * _safe_float(bias.get("time_mult"), 1.0))
        return int(_clamp(dynamic, min_c, max_c))

    def _apply_open_position_tp_adaptation(
        self,
        *,
        symbol: str,
        has_open_position: bool,
        current_price: float,
        cycle_equity_usdt: float,
        active_risk: dict,
        market_ctx: dict[str, object],
        ai_signal: Optional[AISignal],
        momentum_speed: float,
        anomaly_score: float,
    ) -> tuple[dict[str, float], dict[str, object]]:
        """
        Adaptive TP/TS squeeze for already opened position.
        Does not change entry logic; only adjusts exit aggressiveness when market weakens.
        """
        out = {
            "applied": False,
            "profile": "none",
            "score": 0.0,
            "tp_mult": 1.0,
            "ts_mult": 1.0,
            "reason": "",
            "symbol": symbol,
        }
        if not has_open_position:
            return dict(active_risk), out

        risk = dict(active_risk)
        tp_old = max(0.0, _safe_float(risk.get("take_profit"), 0.0))
        ts_old = max(0.0, _safe_float(risk.get("trailing_stop"), 0.0))
        sl_now = max(0.0, _safe_float(risk.get("stop_loss"), 0.0))
        if tp_old <= 0 or ts_old <= 0 or sl_now <= 0:
            return risk, out

        regime = str(market_ctx.get("regime", "flat") or "flat").lower().strip()
        flags_raw = market_ctx.get("flags", [])
        flags = {str(x).lower().strip() for x in flags_raw} if isinstance(flags_raw, list) else set()
        regime_bucket = self._market_regime_bucket(
            regime,
            flags,
            market_volatility=_safe_float(market_ctx.get("market_volatility"), 0.0),
            anomaly_score=anomaly_score,
            momentum_speed=momentum_speed,
        )
        ai_quality = self._quality_for_entry(ai_signal)
        soft_q = self._param_f("AI_ENTRY_SOFT_QUALITY", self.config.AI_ENTRY_SOFT_QUALITY)

        weak_score = 0.0
        reasons: list[str] = []
        if regime == "flat":
            weak_score += 0.40
            reasons.append("flat")
        if "low_vol" in flags or "low_volatility" in flags:
            weak_score += 0.28
            reasons.append("low_vol")
        if momentum_speed < 0.12:
            weak_score += 0.18
            reasons.append("weak_momentum")
        if ai_quality < soft_q:
            weak_score += 0.16
            reasons.append("quality_soft")
        if anomaly_score >= 0.60:
            weak_score += 0.10
            reasons.append("anomaly")
        if regime in {"trend", "impulse"}:
            weak_score -= 0.30
        weak_score = _clamp(weak_score, 0.0, 1.0)
        if weak_score < 0.45:
            weak_score = max(weak_score, 0.0)

        # Stronger weakness -> stronger TP squeeze and tighter trailing.
        tp_mult = _lerp(0.90, 0.42, weak_score)
        ts_mult = _lerp(0.96, 0.68, weak_score)  # smaller trailing pct = tighter protection
        symbol_exec_bias = self._symbol_execution_bias(symbol)
        if _safe_float(symbol_exec_bias.get("risk_mult"), 1.0) < 0.995:
            tp_mult *= _safe_float(symbol_exec_bias.get("tp_mult"), 1.0)
            ts_mult *= _safe_float(symbol_exec_bias.get("ts_mult"), 1.0)
            reasons.append(str(symbol_exec_bias.get("label", "exec_bias")))

        # Tiny balance mode: bring TP closer to real reachable range.
        eq = max(0.0, _safe_float(cycle_equity_usdt, 0.0))
        tiny_balance_score = 0.0
        if eq > 0:
            if eq <= 3.5:
                tiny_balance_score = 1.0
            elif eq <= 7.0:
                tiny_balance_score = 0.72
            elif eq <= 15.0:
                tiny_balance_score = 0.38
        if tiny_balance_score > 0 and weak_score >= 0.35:
            tp_mult *= _lerp(0.92, 0.72, tiny_balance_score)
            reasons.append("tiny_balance")

        mem = dict(self.position_memory.get(str(symbol or "").upper().strip(), {}))
        entry_px = max(0.0, _safe_float(mem.get("entry_price"), 0.0))
        peak_px = max(0.0, _safe_float(mem.get("peak_price"), entry_px))
        candles_in_position = int(_safe_float(mem.get("candles_in_position"), 0.0))
        candles_since_peak = int(_safe_float(mem.get("candles_since_peak"), 0.0))
        current_px = max(0.0, _safe_float(current_price, 0.0))
        if entry_px > 0 and current_px > 0 and tp_old > 0:
            tp_target_price = entry_px * (1.0 + tp_old)
            denom = max(1e-9, tp_target_price - entry_px)
            tp_progress = _clamp((current_px - entry_px) / denom, -1.0, 2.0)
            stall_ratio = (candles_since_peak / float(max(1, candles_in_position))) if candles_in_position > 0 else 0.0
            anti_fragile = self._anti_fragile_mode(symbol, regime_bucket)
            if candles_in_position >= 4 and tp_progress < 0.34 and stall_ratio >= 0.42:
                weak_score = _clamp(weak_score + 0.16, 0.0, 1.0)
                tp_mult *= _clamp(_safe_float(anti_fragile.get("tp_mult"), 1.0), 0.86, 1.0)
                ts_mult *= _clamp(_safe_float(anti_fragile.get("ts_mult"), 1.0), 0.88, 1.0)
                reasons.append("tp_probability_drop")
            elif tp_progress >= 0.68 and momentum_speed >= 0.22 and regime_bucket in {"trend_fast", "trend_impulse"}:
                tp_mult *= 1.03
                ts_mult *= 1.01
                reasons.append("tp_probability_hold")

        if weak_score < 0.45 and not any(x in {"tp_probability_drop", "tp_probability_hold"} for x in reasons):
            return risk, out

        fee_and_slippage = 2.0 * (max(0.0, self.config.AI_FEE_BPS) + max(0.0, self.config.AI_SLIPPAGE_BPS)) / 10_000.0
        tp_floor = max(self.config.AI_TAKE_PROFIT_MIN, fee_and_slippage + 0.0015)
        min_rr_floor = max(1.02, min(1.35, self._param_f("AI_MIN_RR", self.config.AI_MIN_RR) * 0.72))
        tp_rr_floor = sl_now * min_rr_floor
        tp_new = max(tp_floor, tp_rr_floor, tp_old * tp_mult)
        # In weak market cap TP from above too, otherwise target can still stay unrealistically far.
        rr_cap = _lerp(1.45, 1.08, weak_score)
        if tiny_balance_score > 0:
            rr_cap *= _lerp(0.90, 0.74, tiny_balance_score)
        tp_cap_floor = max(tp_floor * 1.10, sl_now * 0.90)
        tp_cap = max(tp_cap_floor, sl_now * rr_cap)
        tp_new = min(tp_new, tp_cap)
        tp_new = min(tp_new, tp_old)

        ts_new = max(self.config.AI_TRAILING_STOP_MIN, ts_old * ts_mult)
        ts_new = min(ts_new, ts_old)

        if tp_new >= (tp_old - 1e-9) and ts_new >= (ts_old - 1e-9):
            return risk, out

        risk["take_profit"] = tp_new
        risk["trailing_stop"] = ts_new
        out = {
            "applied": True,
            "profile": "weak_market_tp_squeeze",
            "score": round(float(weak_score), 4),
            "tp_mult": round(float(tp_mult), 4),
            "ts_mult": round(float(ts_mult), 4),
            "tp_from": float(tp_old),
            "tp_to": float(tp_new),
            "ts_from": float(ts_old),
            "ts_to": float(ts_new),
            "reason": ",".join(reasons) if reasons else "weak_market",
            "symbol": symbol,
            "regime_bucket": regime_bucket,
            "tiny_balance_score": round(float(tiny_balance_score), 4),
            "cycle_equity_usdt": float(eq),
            "symbol_exec_bias": dict(symbol_exec_bias),
        }
        return risk, out

    def _apply_entry_tp_reachability_cap(
        self,
        *,
        symbol: str,
        has_open_position: bool,
        active_risk: dict,
        market_ctx: dict[str, object],
        ai_signal: Optional[AISignal],
        atr_pct: float,
        cycle_equity_usdt: float,
    ) -> tuple[dict[str, float], dict[str, object]]:
        """
        Entry-side TP realism cap.
        Prevents optimistic TP targets in weak/flat markets where the signal can be strong
        but realized move is usually too short to reach a distant target.
        """
        out = {
            "applied": False,
            "profile": "none",
            "tp_from": 0.0,
            "tp_to": 0.0,
            "cap_rr": 0.0,
            "cap_atr": 0.0,
            "cap_hard": 0.0,
            "reason": "",
            "symbol": symbol,
        }
        risk = dict(active_risk)
        if has_open_position:
            return risk, out

        tp_old = max(0.0, _safe_float(risk.get("take_profit"), 0.0))
        sl_now = max(0.0, _safe_float(risk.get("stop_loss"), 0.0))
        if tp_old <= 0.0 or sl_now <= 0.0:
            return risk, out

        regime = str(market_ctx.get("regime", "flat") or "flat").lower().strip()
        flags_raw = market_ctx.get("flags", [])
        flags = {str(x).lower().strip() for x in flags_raw} if isinstance(flags_raw, list) else set()
        market_volatility = max(0.0, _safe_float(market_ctx.get("market_volatility"), 0.0))
        momentum_speed = max(0.0, _safe_float(market_ctx.get("momentum_speed"), 0.0))
        impulse_score = max(0.0, _safe_float(market_ctx.get("impulse_score"), 0.0))
        anomaly_score = max(0.0, _safe_float(market_ctx.get("anomaly_score"), 0.0))

        ai_quality = self._quality_for_entry(ai_signal)
        conf_base = 0.5
        if ai_signal is not None:
            conf_base = ai_signal.calibrated_confidence if ai_signal.calibrated_confidence > 0 else ai_signal.confidence
        ai_conf = _clamp(conf_base, 0.0, 1.0)
        signal_strength = _clamp((ai_quality * 0.56) + (ai_conf * 0.44), 0.0, 1.0)

        weak_market = (
            regime in {"flat", "range"}
            or ("low_vol" in flags)
            or ("low_volatility" in flags)
        )
        strong_impulse = (
            regime in {"trend", "impulse"}
            and impulse_score >= 1.6
            and momentum_speed >= 0.18
            and ("low_vol" not in flags)
            and ("low_volatility" not in flags)
        )
        tiny_balance_score = 0.0
        eq = max(0.0, _safe_float(cycle_equity_usdt, 0.0))
        if eq > 0:
            if eq <= 3.5:
                tiny_balance_score = 1.0
            elif eq <= 7.0:
                tiny_balance_score = 0.70
            elif eq <= 15.0:
                tiny_balance_score = 0.35

        fee_and_slippage = 2.0 * (max(0.0, self.config.AI_FEE_BPS) + max(0.0, self.config.AI_SLIPPAGE_BPS)) / 10_000.0
        tp_floor = max(self.config.AI_TAKE_PROFIT_MIN, fee_and_slippage + 0.0013)

        if weak_market and not strong_impulse:
            rr_cap_mult = _lerp(1.04, 1.26, signal_strength)
            atr_cap_mult = _lerp(0.72, 1.12, signal_strength)
            hard_cap = _lerp(0.0052, 0.0092, signal_strength)
            if momentum_speed < 0.12:
                hard_cap *= 0.92
            if anomaly_score >= 0.55:
                hard_cap *= 0.94
        else:
            rr_cap_mult = _lerp(1.18, 1.85, signal_strength)
            atr_cap_mult = _lerp(1.05, 1.70, signal_strength)
            hard_cap = _lerp(0.0085, 0.0180, signal_strength)
            if strong_impulse:
                hard_cap *= 1.08

        if tiny_balance_score > 0:
            shrink = _lerp(0.95, 0.78, tiny_balance_score)
            rr_cap_mult *= shrink
            atr_cap_mult *= shrink
            hard_cap *= shrink

        if market_volatility > 0 and weak_market and not strong_impulse:
            hard_cap = min(hard_cap, max(tp_floor, market_volatility * 10.0))

        cap_rr = max(tp_floor, sl_now * rr_cap_mult)
        cap_atr = max(tp_floor, atr_pct * atr_cap_mult) if atr_pct > 0 else tp_old
        cap_hard = max(tp_floor, min(self.config.AI_TAKE_PROFIT_MAX, hard_cap))
        tp_cap = max(tp_floor, min(tp_old, cap_rr, cap_atr, cap_hard))

        if tp_cap >= (tp_old - 1e-9):
            return risk, out

        risk["take_profit"] = max(tp_floor, tp_cap)
        out = {
            "applied": True,
            "profile": "entry_reachability_cap",
            "tp_from": float(tp_old),
            "tp_to": float(risk["take_profit"]),
            "cap_rr": float(cap_rr),
            "cap_atr": float(cap_atr),
            "cap_hard": float(cap_hard),
            "reason": "weak_market" if weak_market and not strong_impulse else "market_reachability",
            "symbol": symbol,
            "signal_strength": float(signal_strength),
            "tiny_balance_score": float(tiny_balance_score),
            "strong_impulse": bool(strong_impulse),
        }
        return risk, out

    def _expected_edge_pct(self, active_risk: dict, ai_quality: float = 0.0) -> float:
        gross_tp = max(0.0, _safe_float(active_risk.get("take_profit"), 0.0))
        gross_sl = max(0.0, _safe_float(active_risk.get("stop_loss"), 0.0))
        fee_and_slippage = 2.0 * (
            max(0.0, self.config.AI_FEE_BPS) + max(0.0, self.config.AI_SLIPPAGE_BPS)
        ) / 10_000.0
        # Extra execution reserve to avoid optimistic EV on tiny balances and market orders.
        exec_reserve = max(0.0006, min(0.0020, max(0.0, self.exec_slippage_ema_bps) / 10_000.0))
        q = _clamp(_safe_float(ai_quality, 0.0), 0.0, 1.0)
        # Conservative win-probability prior; do not trust raw quality as direct probability.
        win_prob = _clamp(0.45 + (0.35 * q), 0.45, 0.80)
        loss_prob = 1.0 - win_prob
        edge = (win_prob * gross_tp) - (loss_prob * gross_sl) - fee_and_slippage - exec_reserve

        regime = str(self.runtime_market_ctx.get("regime", "flat") or "flat").lower().strip()
        raw_flags = self.runtime_market_ctx.get("flags", [])
        flags = {str(x).lower().strip() for x in raw_flags} if isinstance(raw_flags, list) else set()
        if regime in {"flat", "range"}:
            edge -= 0.0006
        if "low_vol" in flags or "low_volatility" in flags:
            edge -= 0.00045
        return edge

    def _dynamic_min_expected_edge_pct(
        self,
        symbol: str,
        active_risk: dict,
        market_volatility: float,
        micro_noise: float,
        equity_usdt: float,
    ) -> float:
        """
        Dynamic entry floor based on effective transaction costs and current market noise.
        Keeps no-trade zone around near-zero expected edge to avoid fee churn.
        """
        base_cfg = max(0.0, _safe_float(self._param_f("MIN_EXPECTED_EDGE_PCT", self.config.MIN_EXPECTED_EDGE_PCT), self.config.MIN_EXPECTED_EDGE_PCT))
        fee_and_slippage = 2.0 * (
            max(0.0, self.config.AI_FEE_BPS) + max(0.0, self.config.AI_SLIPPAGE_BPS)
        ) / 10_000.0
        exec_reserve = max(0.0003, min(0.0028, max(0.0, self.exec_slippage_ema_bps) / 10_000.0))
        atr_pct = max(0.0, _safe_float(active_risk.get("stop_loss"), 0.0))
        spread_proxy = max(0.0002, min(0.0024, (atr_pct * 0.12)))
        noise_proxy = max(0.0, min(0.0018, (max(0.0, float(market_volatility)) * 0.18) + (max(0.0, float(micro_noise)) * 0.0009)))

        regime = str(self.runtime_market_ctx.get("regime", "flat") or "flat").lower().strip()
        raw_flags = self.runtime_market_ctx.get("flags", [])
        flags = {str(x).lower().strip() for x in raw_flags} if isinstance(raw_flags, list) else set()
        weak_market = regime in {"flat", "range"} or ("low_vol" in flags) or ("low_volatility" in flags)
        tiny_balance = equity_usdt > 0 and equity_usdt <= 5.0
        # Autopilot should avoid over-strict manual edge floor on tiny balance.
        if self.config.AUTO_PILOT_MODE:
            if weak_market and tiny_balance:
                base_cfg = min(base_cfg, 0.0018)
            elif weak_market:
                base_cfg = min(base_cfg, 0.0022)
            elif tiny_balance:
                base_cfg = min(base_cfg, 0.0021)

        safety_buffer = 0.0007 if ((not self.config.DRY_RUN) and (not self.config.AI_TRAINING_MODE)) else 0.00045
        if weak_market:
            safety_buffer = max(0.00035, safety_buffer - 0.00012)
        if tiny_balance:
            safety_buffer = max(0.00030, safety_buffer - 0.00010)

        dynamic_floor = fee_and_slippage + exec_reserve + spread_proxy + noise_proxy + safety_buffer
        if self.config.DRY_RUN and self.config.AI_TRAINING_MODE:
            # Paper mode: keep floor realistic, but allow more signal collection.
            dynamic_floor *= 0.72
        elif (not self.config.DRY_RUN) and (not self.config.AI_TRAINING_MODE):
            dynamic_floor *= 0.88

        low_bound = 0.0010 if (self.config.DRY_RUN and self.config.AI_TRAINING_MODE) else 0.0014
        if tiny_balance:
            low_bound = min(low_bound, 0.0012)
        high_bound = 0.0080
        dynamic_floor = max(low_bound, min(high_bound, dynamic_floor))
        self.runtime_edge_floor = {
            "symbol": symbol,
            "mode": "training" if (self.config.DRY_RUN and self.config.AI_TRAINING_MODE) else ("live" if (not self.config.DRY_RUN and not self.config.AI_TRAINING_MODE) else "custom"),
            "base_cfg": float(base_cfg),
            "fee_and_slippage": float(fee_and_slippage),
            "exec_reserve": float(exec_reserve),
            "spread_proxy": float(spread_proxy),
            "noise_proxy": float(noise_proxy),
            "safety_buffer": float(safety_buffer),
            "dynamic_floor": float(dynamic_floor),
            "low_bound": float(low_bound),
            "high_bound": float(high_bound),
            "regime": str(regime),
            "regime_flags": sorted(list(flags)),
            "regime_edge_mult": float(self._regime_edge_multiplier(regime, flags, bool(self.runtime_market_ctx.get("dangerous", False)))),
            "regime_edge_state": dict(self.regime_edge_autotune.get(str(regime), {})),
        }
        # Use higher of configured and dynamic floor unless configured value is clearly too strict for weak market.
        if weak_market and tiny_balance:
            return max(low_bound, min(dynamic_floor, max(low_bound, base_cfg * 0.90)))
        return max(base_cfg, dynamic_floor)

    def _quality_for_entry(self, ai_signal: Optional[AISignal]) -> float:
        if ai_signal is None:
            return 0.0
        quality = _safe_float(ai_signal.trade_quality, 0.0)
        if quality > 0:
            return _clamp(quality, 0.0, 1.0)
        conf_base = ai_signal.calibrated_confidence if ai_signal.calibrated_confidence > 0 else ai_signal.confidence
        return _clamp(conf_base, 0.0, 1.0)

    def _get_active_risk_profile(self, symbol: str, ai_signal: Optional[AISignal], ohlcv: list[list[float]]) -> dict:
        risk_profile = {
            "fraction": self.config.SPOT_BALANCE_RISK_FRACTION,
            "stop_loss": self.config.SPOT_STOP_LOSS_PCT,
            "take_profit": self.config.SPOT_TAKE_PROFIT_PCT,
            "trailing_stop": self.config.SPOT_TRAILING_STOP_PCT,
        }

        if self.config.AI_AUTO_RISK and ai_signal:
            closes = np.array([c[4] for c in ohlcv])
            if len(closes) > 1:
                log_returns = np.log(closes[1:] / closes[:-1])
                vol = np.std(log_returns)
            else:
                vol = 0.0

            conf_base = ai_signal.calibrated_confidence if ai_signal.calibrated_confidence > 0 else ai_signal.confidence
            conf = _clamp(conf_base, 0.0, 1.0)
            score = _clamp(abs(ai_signal.score), 0.0, 1.0)
            uncertainty = _clamp(ai_signal.uncertainty if ai_signal.uncertainty > 0 else 0.0, 0.0, 1.0)
            wf_hit_rate = _clamp(ai_signal.walkforward_hit_rate if ai_signal.walkforward_hit_rate > 0 else 0.5, 0.0, 1.0)
            vol_norm = _clamp(vol / 0.01, 0.0, 1.0)  # Normalize by 1% hourly vol
            strength = _clamp((0.45 * conf) + (0.25 * score) + (0.20 * (1.0 - uncertainty)) + (0.10 * (1.0 - vol_norm)), 0.0, 1.0)
            
            risk_profile["fraction"] = _lerp(self.config.AI_RISK_FRACTION_MIN, self.config.AI_RISK_FRACTION_MAX, strength)
            risk_profile["stop_loss"] = _lerp(self.config.AI_STOP_LOSS_MIN, self.config.AI_STOP_LOSS_MAX, vol_norm)
            risk_profile["take_profit"] = _lerp(self.config.AI_TAKE_PROFIT_MIN, self.config.AI_TAKE_PROFIT_MAX, strength)
            risk_profile["trailing_stop"] = _lerp(self.config.AI_TRAILING_STOP_MIN, self.config.AI_TRAILING_STOP_MAX, strength)
            atr_pct = self._atr_pct(ohlcv)
            if atr_pct > 0:
                atr_stop_mult = max(0.1, self._param_f("AI_ATR_STOP_MULT", self.config.AI_ATR_STOP_MULT))
                atr_take_mult = max(atr_stop_mult, self._param_f("AI_ATR_TAKE_PROFIT_MULT", self.config.AI_ATR_TAKE_PROFIT_MULT))
                atr_trail_mult = max(0.1, self._param_f("AI_ATR_TRAILING_MULT", self.config.AI_ATR_TRAILING_MULT))
                atr_stop = _clamp(atr_pct * atr_stop_mult, self.config.AI_STOP_LOSS_MIN, self.config.AI_STOP_LOSS_MAX)
                atr_take = _clamp(
                    atr_pct * atr_take_mult,
                    self.config.AI_TAKE_PROFIT_MIN,
                    self.config.AI_TAKE_PROFIT_MAX,
                )
                atr_trailing = _clamp(
                    atr_pct * atr_trail_mult,
                    self.config.AI_TRAILING_STOP_MIN,
                    self.config.AI_TRAILING_STOP_MAX,
                )
                risk_profile["stop_loss"] = max(risk_profile["stop_loss"], atr_stop)
                risk_profile["take_profit"] = max(risk_profile["take_profit"], atr_take)
                risk_profile["trailing_stop"] = max(risk_profile["trailing_stop"], atr_trailing)

            health_degraded = (
                ai_signal.model_health == "degraded"
                or wf_hit_rate < self.config.AI_HEALTH_MIN_WF_HIT_RATE
                or uncertainty > self.config.AI_UNCERTAINTY_HIGH
            )
            if health_degraded:
                downshift = _clamp(self.config.AI_RISK_DOWNSHIFT_FACTOR, 0.1, 1.0)
                risk_profile["fraction"] = max(self.config.AI_RISK_FRACTION_MIN, risk_profile["fraction"] * downshift)
                risk_profile["take_profit"] = max(self.config.AI_TAKE_PROFIT_MIN, risk_profile["take_profit"] * (0.85 + (0.15 * downshift)))
                risk_profile["trailing_stop"] = max(self.config.AI_TRAILING_STOP_MIN, risk_profile["trailing_stop"] * 0.90)
            entry_quality = self._quality_for_entry(ai_signal)
            soft_quality = self._param_f("AI_ENTRY_SOFT_QUALITY", self.config.AI_ENTRY_SOFT_QUALITY)
            if entry_quality < soft_quality:
                penalty = _clamp(self._param_f("AI_LOW_QUALITY_RISK_FACTOR", self.config.AI_LOW_QUALITY_RISK_FACTOR), 0.10, 1.0)
                risk_profile["fraction"] = max(self.config.AI_RISK_FRACTION_MIN, risk_profile["fraction"] * penalty)
            min_rr = self._param_f("AI_MIN_RR", self.config.AI_MIN_RR)
            risk_profile["take_profit"] = max(
                risk_profile["take_profit"],
                min(self.config.AI_TAKE_PROFIT_MAX, risk_profile["stop_loss"] * min_rr),
            )

        execution_profile = self._execution_profile_bias()
        symbol_exec_bias = self._symbol_execution_bias(symbol)
        regime = str(self.runtime_market_ctx.get("regime", "flat") or "flat").lower().strip()
        flags_raw = self.runtime_market_ctx.get("flags", [])
        flags = {str(x).lower().strip() for x in flags_raw} if isinstance(flags_raw, list) else set()
        if _safe_float(execution_profile.get("risk_mult"), 1.0) < 0.999:
            risk_profile["fraction"] = max(
                self.config.AI_RISK_FRACTION_MIN,
                risk_profile["fraction"] * _safe_float(execution_profile.get("risk_mult"), 1.0),
            )
            if _safe_float(execution_profile.get("edge_delta"), 0.0) >= 0.0003:
                risk_profile["take_profit"] = max(
                    self.config.AI_TAKE_PROFIT_MIN,
                    risk_profile["take_profit"] * 0.92,
                )
                risk_profile["trailing_stop"] = max(
                    self.config.AI_TRAILING_STOP_MIN,
                    risk_profile["trailing_stop"] * 0.88,
                )
                risk_profile["stop_loss"] = max(
                    self.config.AI_STOP_LOSS_MIN,
                    min(self.config.AI_STOP_LOSS_MAX, risk_profile["stop_loss"] * 0.96),
                )
        if _safe_float(symbol_exec_bias.get("risk_mult"), 1.0) < 0.999 or _safe_float(symbol_exec_bias.get("edge_delta"), 0.0) > 0:
            risk_profile["fraction"] = max(
                self.config.AI_RISK_FRACTION_MIN,
                risk_profile["fraction"] * _safe_float(symbol_exec_bias.get("risk_mult"), 1.0),
            )
            risk_profile["take_profit"] = max(
                self.config.AI_TAKE_PROFIT_MIN,
                risk_profile["take_profit"] * _safe_float(symbol_exec_bias.get("tp_mult"), 1.0),
            )
            risk_profile["trailing_stop"] = max(
                self.config.AI_TRAILING_STOP_MIN,
                risk_profile["trailing_stop"] * _safe_float(symbol_exec_bias.get("ts_mult"), 1.0),
            )
            risk_profile["stop_loss"] = max(
                self.config.AI_STOP_LOSS_MIN,
                min(
                    self.config.AI_STOP_LOSS_MAX,
                    risk_profile["stop_loss"] * _safe_float(symbol_exec_bias.get("sl_mult"), 1.0),
                ),
            )
        if regime in {"flat", "range"} and ("low_vol" in flags or "low_volatility" in flags):
            risk_profile["take_profit"] = max(
                self.config.AI_TAKE_PROFIT_MIN,
                risk_profile["take_profit"] * 0.96,
            )
            risk_profile["trailing_stop"] = max(
                self.config.AI_TRAILING_STOP_MIN,
                risk_profile["trailing_stop"] * 0.97,
            )
            if self.loss_streak_live >= 2:
                risk_profile["take_profit"] = max(
                    self.config.AI_TAKE_PROFIT_MIN,
                    risk_profile["take_profit"] * 0.90,
                )
                risk_profile["trailing_stop"] = max(
                    self.config.AI_TRAILING_STOP_MIN,
                    risk_profile["trailing_stop"] * 0.93,
                )
        risk_profile["stop_loss"] = _clamp(
            risk_profile["stop_loss"],
            self.config.AI_STOP_LOSS_MIN,
            self.config.AI_STOP_LOSS_MAX,
        )
        risk_profile["take_profit"] = _clamp(
            risk_profile["take_profit"],
            self.config.AI_TAKE_PROFIT_MIN,
            self.config.AI_TAKE_PROFIT_MAX,
        )
        risk_profile["trailing_stop"] = _clamp(
            risk_profile["trailing_stop"],
            self.config.AI_TRAILING_STOP_MIN,
            self.config.AI_TRAILING_STOP_MAX,
        )

        return risk_profile

    @staticmethod
    def _latest_candle_ts(ohlcv: list[list[float]]) -> int:
        if not ohlcv:
            return 0
        try:
            return int(_safe_float(ohlcv[-1][0], 0.0))
        except Exception:
            return 0

    @staticmethod
    def _candles_since(now_ts: int, prev_ts: int, timeframe: str = "1m") -> int:
        if now_ts <= 0 or prev_ts <= 0 or now_ts < prev_ts:
            return 0
        step_ms = 60_000
        tf = (timeframe or "1m").lower().strip()
        if tf.endswith("m"):
            step_ms = max(60_000, int(_safe_float(tf[:-1], 1.0) * 60_000))
        elif tf.endswith("h"):
            step_ms = max(60_000, int(_safe_float(tf[:-1], 1.0) * 3_600_000))
        elif tf.endswith("d"):
            step_ms = max(60_000, int(_safe_float(tf[:-1], 1.0) * 86_400_000))
        return max(0, int((now_ts - prev_ts) / step_ms))

    def _build_smart_stagnation_exit_plan(
        self,
        *,
        pnl: float,
        active_risk: dict,
        dynamic_time_stop_candles: int,
        candles_in_pos: int,
        candles_since_peak: int,
        ai_signal: Optional[AISignal],
    ) -> Dict[str, object]:
        regime = str(self.runtime_market_ctx.get("regime", "flat")).lower().strip()
        raw_flags = self.runtime_market_ctx.get("flags", [])
        flags = {str(x).lower().strip() for x in raw_flags} if isinstance(raw_flags, list) else set()

        quality = self._quality_for_entry(ai_signal)
        soft_quality = self._param_f("AI_ENTRY_SOFT_QUALITY", self.config.AI_ENTRY_SOFT_QUALITY)
        weak_quality = quality > 0 and quality < soft_quality
        ai_short_bias = bool(ai_signal and str(ai_signal.action).upper() == "SHORT")

        hold_gate = max(6, int(max(1, dynamic_time_stop_candles) * 0.55))
        hold_ratio = candles_in_pos / float(max(1, dynamic_time_stop_candles))
        reached_hold_gate = candles_in_pos >= hold_gate
        stalled_from_peak = candles_since_peak >= max(4, self.config.POSITION_STALL_CANDLES // 2)

        flat_like = regime in {"flat", "range"} or bool(
            {"low_vol", "low_volatility", "chop", "sideways"} & flags
        )
        weak_market = flat_like or hold_ratio >= 0.95
        weak_signal = weak_quality or ai_short_bias
        regime_bias = self._regime_trade_bias(regime)

        sl_pct = max(0.0, _safe_float(active_risk.get("stop_loss"), 0.0))
        factor = 0.16
        if weak_market:
            factor += 0.04
        if weak_quality:
            factor += 0.03
        if hold_ratio >= 0.80:
            factor += 0.04
        if hold_ratio >= 1.00:
            factor += 0.05
        factor += max(0.0, _safe_float(regime_bias.get("exit_bias"), 0.0)) * 0.20
        max_cap = max(0.0018, sl_pct * 0.45)
        loss_limit = _clamp(sl_pct * factor, 0.0010, max_cap)
        if flat_like and quality >= max(0.52, soft_quality + 0.04):
            loss_limit = min(max_cap, loss_limit * 1.12)
        if _safe_float(regime_bias.get("exit_bias"), 0.0) >= 0.08:
            stalled_from_peak = stalled_from_peak or (candles_since_peak >= max(3, self.config.POSITION_STALL_CANDLES // 3))

        should_exit = bool(
            pnl < 0
            and reached_hold_gate
            and stalled_from_peak
            and weak_market
            and weak_signal
            and abs(pnl) >= loss_limit
        )
        return {
            "enabled": True,
            "should_exit": should_exit,
            "loss_limit_pct": loss_limit,
            "hold_ratio": hold_ratio,
            "weak_market": weak_market,
            "weak_signal": weak_signal,
            "quality": quality,
            "soft_quality": soft_quality,
            "regime": regime,
            "flags": sorted(flags),
            "candles_in_pos": candles_in_pos,
            "candles_since_peak": candles_since_peak,
        }

    def _manage_position_state(
        self,
        symbol: str,
        price: float,
        base_free: float,
        active_risk: dict,
        dynamic_time_stop_candles: int,
        latest_candle_ts: int,
        ai_signal: Optional[AISignal],
        position_side: str = "LONG",
    ) -> Tuple[float, float, float, str, int, int]:
        state = self.spot_state.setdefault(symbol, {"entry_price": 0.0, "peak_price": 0.0, "entry_candle_ts": 0.0, "peak_candle_ts": 0.0})
        entry_price, peak_price, pnl, sell_reason = 0.0, 0.0, 0.0, ""
        candles_in_pos, candles_since_peak = 0, 0
        self.runtime_smart_exit = {}
        side = "SHORT" if str(position_side).upper() == "SHORT" else "LONG"
        if base_free > 0:
            if state["entry_price"] == 0:
                state["entry_price"] = price
            # Defensive guard: corrupted/stale entry can appear after abrupt symbol switches.
            # If entry is wildly off current symbol price, normalize to current price.
            entry_raw = _safe_float(state.get("entry_price"), 0.0)
            if entry_raw > 0 and price > 0:
                ratio = entry_raw / price
                if ratio > 5.0 or ratio < 0.2:
                    logging.warning(
                        "[TRADE] POSITION_STATE_NORMALIZE | symbol=%s | entry_raw=%.6f | price=%.6f | ratio=%.3f",
                        symbol,
                        entry_raw,
                        price,
                        ratio,
                    )
                    state["entry_price"] = price
                    state["peak_price"] = price
                    if latest_candle_ts > 0:
                        state["entry_candle_ts"] = float(latest_candle_ts)
                        state["peak_candle_ts"] = float(latest_candle_ts)
            if _safe_float(state.get("entry_candle_ts"), 0.0) <= 0 and latest_candle_ts > 0:
                state["entry_candle_ts"] = float(latest_candle_ts)
            if side == "SHORT":
                if _safe_float(state.get("peak_price"), 0.0) <= 0:
                    state["peak_price"] = price
                else:
                    state["peak_price"] = min(_safe_float(state.get("peak_price"), price), price)
            else:
                state["peak_price"] = max(state["peak_price"], price)
            if _safe_float(state.get("peak_candle_ts"), 0.0) <= 0 and latest_candle_ts > 0:
                state["peak_candle_ts"] = float(latest_candle_ts)
            if side == "SHORT":
                if price <= _safe_float(state.get("peak_price"), price) and latest_candle_ts > 0:
                    state["peak_candle_ts"] = float(latest_candle_ts)
            else:
                if price >= _safe_float(state.get("peak_price"), 0.0) and latest_candle_ts > 0:
                    state["peak_candle_ts"] = float(latest_candle_ts)
            entry_price, peak_price = state["entry_price"], state["peak_price"]
            pnl = -_pnl_pct(entry_price, price) if side == "SHORT" else _pnl_pct(entry_price, price)
            entry_ts = int(_safe_float(state.get("entry_candle_ts"), 0.0))
            peak_ts = int(_safe_float(state.get("peak_candle_ts"), 0.0))
            candles_in_pos = self._candles_since(latest_candle_ts, entry_ts, self.config.TIMEFRAME)
            candles_since_peak = self._candles_since(latest_candle_ts, peak_ts, self.config.TIMEFRAME)
            smart_exit_plan = self._build_smart_stagnation_exit_plan(
                pnl=pnl,
                active_risk=active_risk,
                dynamic_time_stop_candles=dynamic_time_stop_candles,
                candles_in_pos=candles_in_pos,
                candles_since_peak=candles_since_peak,
                ai_signal=ai_signal,
            )
            self.runtime_smart_exit = dict(smart_exit_plan)

            if pnl <= -active_risk["stop_loss"]:
                sell_reason = "stop_loss"
            elif pnl >= active_risk["take_profit"]:
                sell_reason = "take_profit"
            elif (
                ((side == "LONG" and state["peak_price"] > state["entry_price"]) or (side == "SHORT" and state["peak_price"] < state["entry_price"]))
                and pnl >= self._param_f("TRAILING_ACTIVATION_PCT", self.config.TRAILING_ACTIVATION_PCT)
                and (
                    (side == "LONG" and price < state["peak_price"] * (1 - active_risk["trailing_stop"]))
                    or (side == "SHORT" and price > state["peak_price"] * (1 + active_risk["trailing_stop"]))
                )
            ):
                sell_reason = "trailing_stop"
            elif (
                candles_since_peak >= self.config.POSITION_STALL_CANDLES
                and pnl >= self.config.POSITION_STALL_MIN_PNL_PCT
                and (
                    (side == "LONG" and price <= state["peak_price"] * (1.0 - self.config.POSITION_STALL_PULLBACK_PCT))
                    or (side == "SHORT" and price >= state["peak_price"] * (1.0 + self.config.POSITION_STALL_PULLBACK_PCT))
                )
            ):
                sell_reason = "stall_exit"
            elif bool(smart_exit_plan.get("should_exit", False)):
                sell_reason = "smart_stagnation_exit"
            elif (
                candles_in_pos >= max(1, dynamic_time_stop_candles)
                and pnl > 0
                and pnl < max(self.config.POSITION_STALL_MIN_PNL_PCT, active_risk["take_profit"] * 0.35)
            ):
                sell_reason = "time_stop"
            elif (
                ai_signal is not None
                and ai_signal.trade_quality > 0
                and ai_signal.trade_quality < self.config.AI_EXIT_QUALITY_DROP
                and pnl > 0
                and candles_in_pos >= 3
            ):
                sell_reason = "ai_quality_fade"
            
        else:
            state["entry_price"], state["peak_price"] = 0.0, 0.0
            state["entry_candle_ts"], state["peak_candle_ts"] = 0.0, 0.0
            self.runtime_smart_exit = {}
        return entry_price, peak_price, pnl, sell_reason, candles_in_pos, candles_since_peak

    @staticmethod
    def _risk_levels(entry_price: float, peak_price: float, active_risk: dict, position_side: str = "LONG") -> Tuple[float, float, float]:
        if entry_price <= 0:
            return 0.0, 0.0, 0.0
        side = "SHORT" if str(position_side).upper() == "SHORT" else "LONG"
        if side == "SHORT":
            sl = entry_price * (1.0 + _safe_float(active_risk.get("stop_loss"), 0.0))
            tp = entry_price * (1.0 - _safe_float(active_risk.get("take_profit"), 0.0))
            ts = peak_price * (1.0 + _safe_float(active_risk.get("trailing_stop"), 0.0)) if peak_price < entry_price else 0.0
        else:
            sl = entry_price * (1.0 - _safe_float(active_risk.get("stop_loss"), 0.0))
            tp = entry_price * (1.0 + _safe_float(active_risk.get("take_profit"), 0.0))
            ts = peak_price * (1.0 - _safe_float(active_risk.get("trailing_stop"), 0.0)) if peak_price > entry_price else 0.0
        return sl, tp, ts

    def _compute_spot_buy_usdt(self, usdt_free: float, risk_fraction: float) -> float:
        spendable = max(0.0, usdt_free * 0.98)
        if spendable <= 0:
            return 0.0

        # For tiny balances, use almost all available funds to avoid dust loops.
        tiny_balance_threshold = max(self.config.SPOT_MIN_BUY_USDT * 3.0, 5.0)
        if spendable <= tiny_balance_threshold:
            return spendable

        if not self.config.DYNAMIC_POSITION_SIZE:
            base_amount = max(self.config.SPOT_MIN_BUY_USDT, usdt_free * 0.5)
        else:
            adaptive_fraction = _clamp(risk_fraction, 0.05, 1.0)
            base_amount = max(self.config.SPOT_MIN_BUY_USDT, usdt_free * adaptive_fraction)

        # Optional cap: if max<=0, cap is disabled.
        cap = self.config.SPOT_MAX_BUY_USDT
        if cap > 0:
            base_amount = min(base_amount, cap)

        if self.config.SPOT_USE_FULL_BALANCE:
            # "Full balance" keeps behavior aggressive, but still AI-adaptive:
            # use at least computed adaptive amount and at most full spendable.
            return min(spendable, max(base_amount, spendable * 0.75))

        return min(base_amount, spendable)

    def _required_topup_usdt(self, symbol: str, base_free: float, price: float) -> float:
        if base_free <= 0 or price <= 0:
            return 0.0
        min_cost = self.trader.market_min_cost(symbol)
        if min_cost <= 0:
            return 0.0
        current_notional = base_free * price
        if current_notional >= min_cost:
            return 0.0
        # Small buffer reduces repeated near-threshold failures due to price movement/fees.
        target_notional = min_cost * 1.05
        return max(0.0, target_notional - current_notional)
    
    def _is_cooldown(self) -> bool:
        if self.config.AI_TRAINING_MODE or not self.last_trade_ts:
            return False
        return (time.time() - self.last_trade_ts) < self.config.TRADE_COOLDOWN_SECONDS

    def _cooldown_remaining_sec(self) -> int:
        if self.config.AI_TRAINING_MODE or not self.last_trade_ts:
            return 0
        remaining = int(max(0.0, self.config.TRADE_COOLDOWN_SECONDS - (time.time() - self.last_trade_ts)))
        return remaining

    def _set_symbol_internal_cooldown(self, symbol: str, seconds: float) -> None:
        sym = str(symbol or "").upper().strip()
        if not sym:
            return
        sec = max(0.0, float(seconds))
        if sec <= 0:
            self.symbol_internal_cooldown_until_ts.pop(sym, None)
            return
        self.symbol_internal_cooldown_until_ts[sym] = time.time() + sec

    def _symbol_internal_cooldown_remaining_sec(self, symbol: str) -> int:
        sym = str(symbol or "").upper().strip()
        if not sym:
            return 0
        until_ts = float(self.symbol_internal_cooldown_until_ts.get(sym, 0.0) or 0.0)
        if until_ts <= 0:
            return 0
        remaining = int(max(0.0, until_ts - time.time()))
        if remaining <= 0:
            self.symbol_internal_cooldown_until_ts.pop(sym, None)
            return 0
        return remaining

    def _is_symbol_quarantined(self, symbol: str) -> tuple[bool, int]:
        sym = str(symbol or "").upper().strip()
        if not sym:
            return False, 0
        until_ts = _safe_float(self.symbol_quarantine_until_ts.get(sym), 0.0)
        if until_ts <= 0:
            self._symbol_quarantine_log_ts.pop(sym, None)
            return False, 0
        remaining = int(max(0.0, until_ts - time.time()))
        if remaining <= 0:
            self.symbol_quarantine_until_ts.pop(sym, None)
            self._symbol_quarantine_log_ts.pop(sym, None)
            return False, 0
        return True, remaining

    def _set_symbol_quarantine(self, symbol: str, minutes: int, reason: str) -> None:
        sym = str(symbol or "").upper().strip()
        if not sym:
            return
        mins = max(1, int(minutes))
        until_ts = time.time() + (mins * 60)
        prev_until = _safe_float(self.symbol_quarantine_until_ts.get(sym), 0.0)
        if prev_until > until_ts:
            return
        self.symbol_quarantine_until_ts[sym] = until_ts
        self._symbol_quarantine_log_ts[sym] = 0.0
        logging.warning(
            "Pair quarantine enabled | symbol=%s | minutes=%d | reason=%s",
            sym,
            mins,
            str(reason or "loss_streak"),
        )

    def _pretrade_execution_guard(
        self,
        *,
        symbol: str,
        side: str,
        notional_usdt: float,
        price_hint: float,
        ai_quality: float,
        expected_edge_pct: float,
    ) -> tuple[bool, str]:
        sym = str(symbol or "").upper().strip()
        trade_side = str(side or "buy").strip().lower()
        if not sym:
            return False, "invalid_symbol"
        px = max(0.0, _safe_float(price_hint, 0.0))
        try:
            live_px = max(0.0, _safe_float(self.trader.last_price(sym, force_refresh=True), 0.0))
        except Exception:
            live_px = 0.0
        if live_px > 0:
            px = live_px
        if px <= 0:
            return False, "market_data_unavailable"
        if trade_side == "buy":
            min_cost = max(0.0, _safe_float(self.trader.market_min_cost(sym), 0.0))
            if min_cost > 0 and notional_usdt < (min_cost * 1.01):
                return False, "below_min_notional"
            if not self.config.DRY_RUN:
                try:
                    pos = self.trader.get_position(sym)
                    usdt_free = max(0.0, _safe_float(pos.get("usdt_free"), 0.0))
                    if usdt_free > 0 and notional_usdt > (usdt_free * 1.02):
                        return False, "balance_changed"
                except Exception:
                    pass
        regime = str(self.runtime_market_ctx.get("regime", "flat") or "flat").lower().strip()
        bias = self._regime_trade_bias(regime)
        if trade_side == "buy":
            if expected_edge_pct <= 0 and ai_quality < 0.60:
                return False, "weak_edge_quality_combo"
            if _safe_float(bias.get("exit_bias"), 0.0) >= 0.12 and expected_edge_pct < 0.0022:
                return False, "regime_degraded_for_entries"
        return True, ""

    def _runtime_degradation_recover(self, *, event: str, symbol: str, market_data_stale: bool) -> Dict[str, object]:
        event_norm = str(event or "").strip().lower()
        out = {
            "triggered": False,
            "action": "",
            "reason": "",
        }
        cycle_errors = int(_safe_float(self.guard_state.get("consecutive_cycle_errors"), 0.0))
        logic_errors = int(_safe_float(self.guard_state.get("consecutive_logic_errors"), 0.0))
        api_errors = int(_safe_float(self.guard_state.get("consecutive_api_errors"), 0.0))
        should_reset_symbol = False
        if market_data_stale and (cycle_errors >= 2 or api_errors >= 2):
            should_reset_symbol = True
            out["reason"] = "stale_market_data"
        elif event_norm == "market_data_unavailable" and cycle_errors >= 2:
            should_reset_symbol = True
            out["reason"] = "repeated_market_data_unavailable"
        elif event_norm in {"entry_blocked", "risk_guard_blocked"} and logic_errors >= max(3, int(self.config.MAX_CYCLE_ERRORS)):
            should_reset_symbol = True
            out["reason"] = "repeated_entry_degradation"
        if should_reset_symbol and self.config.AUTO_SYMBOL_SELECTION and not self._last_has_open_position:
            self.active_symbol = None
            self._last_symbol_scan_ts = 0.0
            out["triggered"] = True
            out["action"] = "symbol_rescan"
        return out

    def _cycle_runtime_watchdog(self, *, cycle_index: int, cycle_elapsed_sec: float, last_event: str) -> Dict[str, object]:
        out = {
            "triggered": False,
            "action": "",
            "reason": "",
            "event": str(last_event or ""),
        }
        threshold_sec = max(
            18.0,
            float(self._next_cycle_sleep_seconds()) * 4.0,
            float(self.config.CYCLE_IO_TIMEOUT_SEC) * 4.0,
            float(self.config.ORDER_CONFIRM_TOTAL_TIMEOUT_SEC) * 2.2,
        )
        if cycle_elapsed_sec <= threshold_sec:
            self._slow_cycle_streak = max(0, int(self._slow_cycle_streak) - 1)
            return out
        self._slow_cycle_streak = min(12, int(self._slow_cycle_streak) + 1)
        if self._slow_cycle_streak < 2:
            return out
        now_ts = time.time()
        has_open_position = bool(getattr(self, "_last_has_open_position", False))
        event_norm = str(last_event or "").strip().lower()
        if has_open_position and self.active_symbol:
            self._refresh_confirmed_exchange_state(self.active_symbol)
            action = "position_resync"
            reason = "slow_cycle_with_open_position"
        else:
            self.active_symbol = None
            self._last_symbol_scan_ts = 0.0
            self._cycle_compute_cache.clear()
            self.current_cycle_trade_meta.clear()
            action = "soft_runtime_reset"
            reason = "slow_cycle_degraded"
        event_sig = f"{action}:{event_norm}:{int(cycle_elapsed_sec)}"
        should_emit = (
            event_sig != self._last_cycle_watchdog_event
            or (now_ts - float(self._last_cycle_watchdog_log_ts)) >= 45.0
        )
        self._last_cycle_watchdog_event = event_sig
        self._last_cycle_watchdog_log_ts = now_ts
        if not should_emit:
            return out
        out["triggered"] = True
        out["action"] = action
        out["reason"] = f"{reason}:streak={int(self._slow_cycle_streak)}:threshold={threshold_sec:.1f}s"
        if event_norm == "cycle_busy":
            out["reason"] += ":busy"
        return out

    def _reconcile_post_order_position(
        self,
        *,
        symbol: str,
        side: str,
        pre_base_free: float,
        requested_base: float,
        quote_amount: float,
        executed_price: float,
    ) -> Dict[str, float]:
        out = {
            "confirmed_qty": 0.0,
            "confirmed_quote": max(0.0, _safe_float(quote_amount, 0.0)),
            "confirmed_price": max(0.0, _safe_float(executed_price, 0.0)),
            "post_base_free": max(0.0, _safe_float(pre_base_free, 0.0)),
        }
        if self.config.DRY_RUN:
            return out
        side_norm = str(side or "").strip().lower()
        if side_norm not in {"buy", "sell"}:
            return out
        try:
            pos = self.trader.get_position(symbol)
        except Exception as exc:
            self.trader._record_api_error(f"post_order_reconcile_{side_norm}", exc)
            return out
        post_base = max(0.0, _safe_float(pos.get("base_free"), pre_base_free))
        out["post_base_free"] = post_base
        if side_norm == "buy":
            confirmed_qty = max(0.0, post_base - max(0.0, _safe_float(pre_base_free, 0.0)))
        else:
            confirmed_qty = max(0.0, max(0.0, _safe_float(pre_base_free, 0.0)) - post_base)
        if confirmed_qty <= 0 and side_norm == "buy":
            confirmed_qty = max(0.0, _safe_float(requested_base, 0.0))
        out["confirmed_qty"] = confirmed_qty
        price_ref = max(0.0, _safe_float(executed_price, 0.0))
        if price_ref <= 0:
            try:
                price_ref = max(0.0, _safe_float(self.trader.last_price(symbol, force_refresh=True), 0.0))
            except Exception:
                price_ref = 0.0
        if confirmed_qty > 0:
            if out["confirmed_quote"] <= 0 and price_ref > 0:
                out["confirmed_quote"] = confirmed_qty * price_ref
            if price_ref <= 0 and out["confirmed_quote"] > 0:
                price_ref = out["confirmed_quote"] / max(1e-12, confirmed_qty)
        out["confirmed_price"] = max(0.0, price_ref)
        return out

    def _mark_closed_trade_outcome(self, symbol: str, reason: str, pnl_net_pct: float, side: str = "LONG") -> None:
        sym = str(symbol or "").upper().strip()
        if not sym:
            return
        close_reason = str(reason or "").strip().lower()
        side_norm = "SHORT" if str(side or "").upper() == "SHORT" else "LONG"

        if pnl_net_pct < 0:
            streak = int(self.symbol_loss_streak.get(sym, 0)) + 1
            self.symbol_loss_streak[sym] = min(12, streak)
        else:
            self.symbol_loss_streak[sym] = max(0, int(self.symbol_loss_streak.get(sym, 0)) - 1)

        if close_reason == "stop_loss":
            self.side_stoploss_streak[side_norm] = min(12, int(self.side_stoploss_streak.get(side_norm, 0)) + 1)
        else:
            self.side_stoploss_streak[side_norm] = max(0, int(self.side_stoploss_streak.get(side_norm, 0)) - 1)

        regime = str(self.runtime_market_ctx.get("regime", "flat") or "flat").lower().strip()
        regime_bias = self._regime_trade_bias(regime)
        if pnl_net_pct < 0 and _safe_float(regime_bias.get("exit_bias"), 0.0) >= 0.10:
            self.symbol_loss_streak[sym] = min(12, int(self.symbol_loss_streak.get(sym, 0)) + 1)

        max_pair_streak = max(2, int(self.config.PAIR_QUARANTINE_LOSS_STREAK))
        if int(self.symbol_loss_streak.get(sym, 0)) >= max_pair_streak:
            extra_minutes = 0
            if pnl_net_pct < 0 and close_reason in {"stop_loss", "smart_stagnation_exit", "stall_exit", "ai_quality_fade"}:
                extra_minutes += 8
            if _safe_float(regime_bias.get("exit_bias"), 0.0) >= 0.10:
                extra_minutes += 6
            self._set_symbol_quarantine(
                sym,
                max(10, int(self.config.PAIR_QUARANTINE_MINUTES) + extra_minutes),
                reason=f"loss_streak_{int(self.symbol_loss_streak.get(sym, 0))}:{regime}",
            )
            # Keep counter below trigger to avoid re-trigger every close event.
            self.symbol_loss_streak[sym] = max_pair_streak - 1

    def _maybe_force_exit_on_slippage(self, symbol: str, entry_slippage_bps: float) -> None:
        sym = str(symbol or "").upper().strip()
        if not sym:
            return
        threshold = max(0.0, _safe_float(self.config.MAX_ENTRY_SLIPPAGE_BPS, 0.0))
        if threshold <= 0:
            return
        if entry_slippage_bps <= threshold:
            return
        self.force_exit_reasons[sym] = "slippage_guard"
        logging.warning(
            "Entry slippage guard armed | symbol=%s | slippage=%.2f bps | max=%.2f bps",
            sym,
            float(entry_slippage_bps),
            float(threshold),
        )

    def _consume_forced_exit(self, symbol: str) -> str:
        sym = str(symbol or "").upper().strip()
        if not sym:
            return ""
        return str(self.force_exit_reasons.pop(sym, "") or "")

    def _can_afford_buy(self, usdt_free, spot_buy_usdt) -> bool:
        return usdt_free >= spot_buy_usdt and spot_buy_usdt >= self.config.SPOT_MIN_BUY_USDT

    def _confirm_buy_position(
        self,
        symbol: str,
        pre_base_free: float,
        *,
        retries: Optional[int] = None,
        wait_sec: Optional[float] = None,
    ) -> Tuple[bool, float]:
        attempts = max(1, int(retries if retries is not None else self.config.ORDER_CONFIRM_RETRIES))
        pause = max(0.15, float(wait_sec if wait_sec is not None else self.config.ORDER_CONFIRM_POLL_SEC))
        total_timeout = max(1.0, float(self.config.ORDER_CONFIRM_TOTAL_TIMEOUT_SEC))
        deadline = time.time() + total_timeout
        last_base = max(0.0, pre_base_free)
        tolerance = 1e-10
        checks = 0
        while checks < attempts or time.time() < deadline:
            checks += 1
            if checks > 1:
                time.sleep(pause)
            try:
                pos = self.trader.get_position(symbol)
                last_base = max(0.0, _safe_float(pos.get("base_free"), 0.0))
                if last_base > (pre_base_free + tolerance):
                    return True, last_base
            except Exception as exc:
                self.trader._record_api_error("order_confirm_buy_position", exc)  # best-effort telemetry
        return False, last_base

    def _confirm_sell_position(
        self,
        symbol: str,
        pre_base_free: float,
        *,
        retries: Optional[int] = None,
        wait_sec: Optional[float] = None,
    ) -> Tuple[bool, float]:
        attempts = max(1, int(retries if retries is not None else self.config.ORDER_CONFIRM_RETRIES))
        pause = max(0.15, float(wait_sec if wait_sec is not None else self.config.ORDER_CONFIRM_POLL_SEC))
        total_timeout = max(1.0, float(self.config.ORDER_CONFIRM_TOTAL_TIMEOUT_SEC))
        deadline = time.time() + total_timeout
        last_base = max(0.0, pre_base_free)
        # Accept confirmation if position reduced significantly (partial fills can happen).
        target_base = pre_base_free * 0.90
        checks = 0
        while checks < attempts or time.time() < deadline:
            checks += 1
            if checks > 1:
                time.sleep(pause)
            try:
                pos = self.trader.get_position(symbol)
                last_base = max(0.0, _safe_float(pos.get("base_free"), 0.0))
                if last_base <= target_base or (pre_base_free > 0 and last_base < 1e-10):
                    return True, last_base
            except Exception as exc:
                self.trader._record_api_error("order_confirm_sell_position", exc)  # best-effort telemetry
        return False, last_base
        
    def _execute_futures_entry(
        self,
        symbol: str,
        usdt_cost: float,
        price: float,
        candle_ts: int,
        active_risk: dict,
        *,
        entry_side: str,
        reason: str = "signal",
    ) -> bool:
        side = "SHORT" if str(entry_side).upper() == "SHORT" else "LONG"
        logging.info("[TRADE] FUTURES_OPEN_START | symbol=%s | side=%s | margin=%.4f USDT | reason=%s", symbol, side, usdt_cost, reason)
        if self.config.DRY_RUN:
            logging.info("FUTURES dry-run open skipped (execution disabled).")
            return False
        try:
            order = self.trader.place_futures_open(symbol, side, usdt_cost)
            filled = _safe_float(order.get("filled"), 0.0) if isinstance(order, dict) else 0.0
            avg_price = (
                _safe_float(order.get("average"), 0.0) or _safe_float(order.get("price"), 0.0)
                if isinstance(order, dict)
                else 0.0
            )
            executed_price = avg_price if avg_price > 0 else price
            qty = max(0.0, filled)
            state = self.spot_state.setdefault(symbol, {"entry_price": 0.0, "peak_price": 0.0, "entry_candle_ts": 0.0, "peak_candle_ts": 0.0})
            state["entry_price"] = executed_price
            state["peak_price"] = executed_price
            state["entry_candle_ts"] = float(candle_ts)
            state["peak_candle_ts"] = float(candle_ts)
            state["position_side"] = side
            self._lock_position_memory(symbol=symbol, entry_price=executed_price, peak_price=executed_price, candle_ts=candle_ts, active_risk=active_risk, source=f"futures_{reason}", position_side=side)
            self.last_trade_ts = time.time()
            self.active_symbol = symbol
            self._record_order(order_type="buy", symbol=symbol, reason=f"futures_open_{side.lower()}:{reason}", quote_amount=_safe_float(order.get("cost"), 0.0) if isinstance(order, dict) else usdt_cost, base_amount=qty, order=order if isinstance(order, dict) else None)
            logging.info("[TRADE] FUTURES_OPEN | symbol=%s | side=%s | entry=%.6f | qty=%.8f", symbol, side, executed_price, qty)
            return True
        except Exception as exc:
            logging.warning("FUTURES open failed for %s (%s): %s", symbol, side, exc)
            return False

    def _execute_futures_close(self, symbol: str, contracts: float, position_side: str, reason: str, price: float) -> bool:
        side = "SHORT" if str(position_side).upper() == "SHORT" else "LONG"
        logging.info("[TRADE] FUTURES_CLOSE_START | symbol=%s | side=%s | qty=%.8f | reason=%s", symbol, side, contracts, reason)
        entry_snapshot = _safe_float(self.spot_state.get(symbol, {}).get("entry_price"), 0.0)
        if self.config.DRY_RUN:
            logging.info("FUTURES dry-run close skipped (execution disabled).")
            return False
        try:
            order = self.trader.place_futures_close(symbol, side, contracts)
            avg_price = (
                _safe_float(order.get("average"), 0.0) or _safe_float(order.get("price"), 0.0)
                if isinstance(order, dict)
                else 0.0
            )
            exit_price = avg_price if avg_price > 0 else price
            pnl_pct_preview = (_pnl_pct(entry_snapshot, exit_price) if side == "LONG" else -_pnl_pct(entry_snapshot, exit_price)) if entry_snapshot > 0 and exit_price > 0 else 0.0
            self._update_internal_learning(symbol, pnl_pct_preview)
            self._mark_closed_trade_outcome(symbol, reason, pnl_pct_preview, side=side)
            self._record_order(order_type="sell", symbol=symbol, reason=f"futures_close_{side.lower()}:{reason}", base_amount=contracts, quote_amount=_safe_float(order.get("cost"), 0.0) if isinstance(order, dict) else 0.0, order=order if isinstance(order, dict) else None)
            self._clear_position_memory(symbol)
            self.last_trade_ts = time.time()
            self.active_symbol = None
            logging.info("[TRADE] FUTURES_CLOSE | symbol=%s | side=%s | entry=%.6f | exit=%.6f | pnl=%+.2f%% | reason=%s", symbol, side, entry_snapshot, exit_price, pnl_pct_preview * 100.0, reason)
            return True
        except Exception as exc:
            logging.warning("FUTURES close failed for %s (%s): %s", symbol, side, exc)
            return False

    def _execute_buy(self, symbol: str, usdt_cost: float, price: float, candle_ts: int, active_risk: dict, reason: str = "signal") -> bool:
        # Defensive resync: avoid symbol/price mismatch on fast symbol switches.
        try:
            live_ref_price = _safe_float(self.trader.last_price(symbol, force_refresh=True), 0.0)
        except Exception:
            live_ref_price = 0.0
        candle_ref_price = 0.0
        try:
            ohlcv_ref = self.trader.fetch_ohlcv(symbol, "1m", limit=3, force_refresh=True)
            if isinstance(ohlcv_ref, list) and ohlcv_ref:
                candle_ref_price = _safe_float(ohlcv_ref[-1][4], 0.0)
        except Exception:
            candle_ref_price = 0.0
        if live_ref_price > 0 and candle_ref_price > 0:
            refs_drift_bps = abs((live_ref_price / candle_ref_price) - 1.0) * 10_000.0
            if refs_drift_bps > 3000.0:
                logging.warning(
                    "[TRADE] BUY_REF_MISMATCH | symbol=%s | ticker=%.6f | candle=%.6f | drift=%.2f bps",
                    symbol,
                    live_ref_price,
                    candle_ref_price,
                    refs_drift_bps,
                )
                if self.config.AI_TRAINING_MODE and self.config.DRY_RUN:
                    # In paper mode, a large ticker/candle desync may corrupt virtual balance accounting.
                    logging.warning(
                        "[TRADE] BUY_BLOCKED | symbol=%s | reason=paper_price_desync | drift=%.2f bps",
                        symbol,
                        refs_drift_bps,
                    )
                    self._register_api_soft_guard_if_needed("paper_price_desync:buy")
                    return False
                live_ref_price = candle_ref_price
        if live_ref_price <= 0 and candle_ref_price > 0:
            live_ref_price = candle_ref_price
        if live_ref_price > 0 and _safe_float(price, 0.0) > 0:
            drift_bps = abs((_safe_float(price, 0.0) / live_ref_price) - 1.0) * 10_000.0
            if drift_bps > 3500.0:
                logging.warning(
                    "[TRADE] BUY_PRICE_RESYNC | symbol=%s | stale_price=%.6f | live_price=%.6f | drift=%.2f bps",
                    symbol,
                    _safe_float(price, 0.0),
                    live_ref_price,
                    drift_bps,
                )
                price = live_ref_price
        elif _safe_float(price, 0.0) <= 0 and live_ref_price > 0:
            price = live_ref_price

        if live_ref_price <= 0:
            logging.warning("[TRADE] BUY_BLOCKED | symbol=%s | reason=market_data_unavailable:no_ref_price", symbol)
            self._register_api_soft_guard_if_needed("market_data_unavailable:buy:no_ref_price")
            return False
        if _safe_float(price, 0.0) <= 0:
            logging.warning("[TRADE] BUY_BLOCKED | symbol=%s | reason=market_data_unavailable", symbol)
            self._register_api_soft_guard_if_needed("market_data_unavailable:buy")
            return False
        logging.info("[TRADE] BUY_START | symbol=%s | budget=%.4f USDT | reason=%s", symbol, usdt_cost, reason)
        base_amount = 0.0
        quote_amount = usdt_cost
        executed_price = price
        pre_base_free = 0.0
        confirmed_delta_base = 0.0

        if self.config.AI_TRAINING_MODE and self.config.DRY_RUN:
            usdt_before = _safe_float(self.paper_usdt_free, 0.0)
            usdt_spent = min(usdt_cost, usdt_before)
            if usdt_spent <= 0 or price <= 0:
                logging.info("Paper BUY skipped: insufficient paper USDT or invalid price.")
                return False
            base_amount = usdt_spent / price
            self.paper_usdt_free = max(0.0, usdt_before - usdt_spent)
            self.paper_positions[symbol] = _safe_float(self.paper_positions.get(symbol, 0.0), 0.0) + base_amount
            self.active_symbol = symbol
            logging.info(
                "[TRADE] BUY_FILL | mode=paper | symbol=%s | spent=%.4f USDT | qty=%.10f | usdt_left=%.4f",
                symbol,
                usdt_spent,
                base_amount,
                _safe_float(self.paper_usdt_free, 0.0),
            )
            self._save_paper_state()
            quote_amount = usdt_spent

        order = None
        if not self.config.DRY_RUN:
            try:
                pre_pos = self.trader.get_position(symbol)
                pre_base_free = max(0.0, _safe_float(pre_pos.get("base_free"), 0.0))
                order = self.trader.place_spot_buy(symbol, usdt_cost)
                if isinstance(order, dict):
                    logging.info(
                        "[TRADE] BUY_ORDER | symbol=%s | id=%s | status=%s | avg=%.6f | filled=%.10f | cost=%.4f",
                        symbol,
                        str(order.get("id", "") or ""),
                        str(order.get("status", "") or "-"),
                        _safe_float(order.get("average"), 0.0) or _safe_float(order.get("price"), 0.0),
                        _safe_float(order.get("filled"), 0.0),
                        _safe_float(order.get("cost"), 0.0),
                    )
                base_amount = _safe_float(order.get("filled"), 0.0) if isinstance(order, dict) else 0.0
                if isinstance(order, dict):
                    quote_amount = _safe_float(order.get("cost"), quote_amount)
                if isinstance(order, dict):
                    order_px = _safe_float(order.get("average"), 0.0) or _safe_float(order.get("price"), 0.0)
                    if order_px > 0:
                        executed_price = order_px
                adapter_confirm = None
                try:
                    adapter_confirm = self.trader.confirm_buy_position(symbol, pre_base_free)
                except Exception:
                    adapter_confirm = None
                if (
                    isinstance(adapter_confirm, tuple)
                    and len(adapter_confirm) == 2
                ):
                    confirmed = bool(adapter_confirm[0])
                    confirmed_base = max(0.0, _safe_float(adapter_confirm[1], 0.0))
                else:
                    confirmed, confirmed_base = self._confirm_buy_position(symbol, pre_base_free)
                if confirmed:
                    confirmed_delta_base = max(0.0, confirmed_base - pre_base_free)
                    # Prefer confirmed balance delta over raw exchange "filled" when available.
                    if confirmed_delta_base > 0:
                        base_amount = confirmed_delta_base
                else:
                    logging.warning(
                        "BUY confirm timeout: could not verify position growth for %s after %d attempts.",
                        symbol,
                        max(1, int(self.config.ORDER_CONFIRM_RETRIES)),
                    )
                    self._register_api_soft_guard_if_needed("order_confirm_timeout:buy")
                    return False
                reconcile = self._reconcile_post_order_position(
                    symbol=symbol,
                    side="buy",
                    pre_base_free=pre_base_free,
                    requested_base=base_amount if base_amount > 0 else (quote_amount / max(1e-12, price)),
                    quote_amount=quote_amount,
                    executed_price=executed_price,
                )
                reconciled_qty = max(0.0, _safe_float(reconcile.get("confirmed_qty"), 0.0))
                reconciled_quote = max(0.0, _safe_float(reconcile.get("confirmed_quote"), 0.0))
                reconciled_price = max(0.0, _safe_float(reconcile.get("confirmed_price"), 0.0))
                if reconciled_qty > 0:
                    base_amount = reconciled_qty
                if reconciled_quote > 0:
                    if quote_amount <= 0:
                        quote_amount = reconciled_quote
                    elif abs((reconciled_quote / max(1e-12, quote_amount)) - 1.0) > 0.35:
                        quote_amount = reconciled_quote
                if reconciled_price > 0:
                    executed_price = reconciled_price
                if executed_price <= 0:
                    executed_price = self.trader.last_price(symbol, force_refresh=True)
                # Sanity guard for malformed fill prices from exchange/gateway.
                live_ref_now = 0.0
                try:
                    live_ref_now = _safe_float(self.trader.last_price(symbol, force_refresh=True), 0.0)
                except Exception:
                    live_ref_now = 0.0
                market_ref_price = live_ref_now if live_ref_now > 0 else price
                if market_ref_price <= 0:
                    logging.warning("[TRADE] BUY_SANITY_GUARD | symbol=%s | reason=no_market_reference_price", symbol)
                    self._register_api_soft_guard_if_needed("buy_sanity_guard:no_market_ref")
                    return False
                if executed_price > 0 and market_ref_price > 0:
                    fill_slippage_bps = abs((executed_price / market_ref_price) - 1.0) * 10_000.0
                    hard_sanity_bps = max(1500.0, float(self.config.MAX_ENTRY_SLIPPAGE_BPS) * 8.0)
                    if fill_slippage_bps > hard_sanity_bps:
                        logging.warning(
                            "[TRADE] BUY_SANITY_GUARD | symbol=%s | fill_price=%.6f | market_ref=%.6f | slippage=%.2f bps > %.2f bps",
                            symbol,
                            executed_price,
                            market_ref_price,
                            fill_slippage_bps,
                            hard_sanity_bps,
                        )
                        if confirmed_delta_base <= 0:
                            self._register_api_soft_guard_if_needed("buy_sanity_guard:no_confirm")
                            return False
                        # Keep trade only with corrected reference price and confirmed qty.
                        executed_price = market_ref_price
                        if quote_amount > 0 and base_amount <= 0:
                            base_amount = max(0.0, quote_amount / executed_price)
                # Notional sanity: filled qty must match spent quote in reasonable bounds.
                if base_amount > 0 and market_ref_price > 0:
                    target_spend = quote_amount if quote_amount > 0 else usdt_cost
                    implied_spend = base_amount * market_ref_price
                    if target_spend > 0:
                        notional_err = abs((implied_spend / target_spend) - 1.0)
                        if notional_err > 0.35:
                            normalized_qty = max(0.0, target_spend / market_ref_price)
                            logging.warning(
                                "[TRADE] BUY_NOTIONAL_NORMALIZE | symbol=%s | qty_raw=%.10f | qty_norm=%.10f | implied=%.4f | target=%.4f | err=%.2f%%",
                                symbol,
                                base_amount,
                                normalized_qty,
                                implied_spend,
                                target_spend,
                                notional_err * 100.0,
                            )
                            base_amount = normalized_qty
                self.active_symbol = symbol
                self._buy_settle_until_ts[_symbol_key(symbol)] = (
                    time.time() + max(4.0, float(self.config.ORDER_CONFIRM_TOTAL_TIMEOUT_SEC))
                )
            except Exception as exc:
                logging.warning("BUY execution failed for %s: %s", symbol, exc)
                return False
        state = self.spot_state.setdefault(symbol, {"entry_price": 0.0, "peak_price": 0.0})
        state["entry_price"] = executed_price if executed_price > 0 else state.get("entry_price", 0.0)
        state["peak_price"] = max(_safe_float(state.get("peak_price"), 0.0), _safe_float(state.get("entry_price"), 0.0))
        self._lock_position_memory(
            symbol=symbol,
            entry_price=_safe_float(state.get("entry_price"), 0.0),
            peak_price=_safe_float(state.get("peak_price"), 0.0),
            candle_ts=candle_ts,
            active_risk=active_risk,
            source=str(reason),
        )
        sl, tp, ts = self._risk_levels(_safe_float(state.get("entry_price"), 0.0), _safe_float(state.get("peak_price"), 0.0), active_risk)
        entry_slippage_bps = 0.0
        if price > 0 and executed_price > 0:
            entry_slippage_bps = abs((executed_price / price) - 1.0) * 10_000.0
        logging.info(
            "[TRADE] OPEN | symbol=%s | entry=%.6f | qty=%.10f | spent=%.4f USDT | TP=%.6f (+%.2f%%) | SL=%.6f (-%.2f%%) | TS=%.2f%% | reason=%s",
            symbol,
            _safe_float(state.get("entry_price"), 0.0),
            base_amount,
            quote_amount,
            tp,
            _safe_float(active_risk.get("take_profit"), 0.0) * 100.0,
            sl,
            _safe_float(active_risk.get("stop_loss"), 0.0) * 100.0,
            _safe_float(active_risk.get("trailing_stop"), 0.0) * 100.0,
            reason,
        )
        if entry_slippage_bps > 0:
            self._update_internal_learning(symbol, 0.0, entry_slippage_bps=entry_slippage_bps, touch_outcome=False)
        self._maybe_force_exit_on_slippage(symbol, entry_slippage_bps)
        self._record_order(order_type="buy", symbol=symbol, reason=reason, quote_amount=quote_amount, base_amount=base_amount, order=order)
        if not self.config.DRY_RUN:
            cycle_meta = dict(self.current_cycle_trade_meta) if isinstance(self.current_cycle_trade_meta, dict) else {}
            self.real_trade_entry_meta[symbol.upper()] = {
                "entry_ts_utc": datetime.now(timezone.utc).isoformat(),
                "entry_price": _safe_float(state.get("entry_price"), executed_price),
                "entry_quality": _safe_float(cycle_meta.get("entry_quality"), 0.0),
                "ai_confidence": _safe_float(cycle_meta.get("ai_confidence"), 0.0),
                "expected_edge_pct": _safe_float(cycle_meta.get("expected_edge_pct"), 0.0),
                "dynamic_time_stop_candles": int(_safe_float(cycle_meta.get("dynamic_time_stop_candles"), 0.0)),
                "atr_pct": _safe_float(cycle_meta.get("atr_pct"), 0.0),
                "market_ctx": cycle_meta.get("market_ctx", {}),
                "runtime_params": cycle_meta.get("runtime_params", {}),
                "active_risk": cycle_meta.get("active_risk", dict(active_risk)),
            }
            self._refresh_confirmed_exchange_state(symbol, executed_price)
        self.last_trade_ts = time.time()
        return True

    def _execute_sell(self, symbol: str, base_amount: float, reason: str, price: float) -> bool:
        try:
            live_ref_price = _safe_float(self.trader.last_price(symbol, force_refresh=True), 0.0)
        except Exception:
            live_ref_price = 0.0
        candle_ref_price = 0.0
        try:
            ohlcv_ref = self.trader.fetch_ohlcv(symbol, "1m", limit=3, force_refresh=True)
            if isinstance(ohlcv_ref, list) and ohlcv_ref:
                candle_ref_price = _safe_float(ohlcv_ref[-1][4], 0.0)
        except Exception:
            candle_ref_price = 0.0
        if live_ref_price > 0 and candle_ref_price > 0:
            refs_drift_bps = abs((live_ref_price / candle_ref_price) - 1.0) * 10_000.0
            if refs_drift_bps > 3000.0:
                logging.warning(
                    "[TRADE] SELL_REF_MISMATCH | symbol=%s | ticker=%.6f | candle=%.6f | drift=%.2f bps",
                    symbol,
                    live_ref_price,
                    candle_ref_price,
                    refs_drift_bps,
                )
                live_ref_price = candle_ref_price
        if live_ref_price <= 0 and candle_ref_price > 0:
            live_ref_price = candle_ref_price
        if live_ref_price > 0 and _safe_float(price, 0.0) > 0:
            drift_bps = abs((_safe_float(price, 0.0) / live_ref_price) - 1.0) * 10_000.0
            if drift_bps > 1200.0:
                logging.warning(
                    "[TRADE] SELL_PRICE_RESYNC | symbol=%s | stale_price=%.6f | live_price=%.6f | drift=%.2f bps",
                    symbol,
                    _safe_float(price, 0.0),
                    live_ref_price,
                    drift_bps,
                )
                price = live_ref_price
        elif _safe_float(price, 0.0) <= 0 and live_ref_price > 0:
            price = live_ref_price
        if live_ref_price <= 0:
            logging.warning("[TRADE] SELL_BLOCKED | symbol=%s | reason=market_data_unavailable:no_ref_price", symbol)
            self._register_api_soft_guard_if_needed("market_data_unavailable:sell:no_ref_price")
            return False
        entry_snapshot = _safe_float(self.spot_state.get(symbol, {}).get("entry_price"), 0.0)
        if entry_snapshot <= 0:
            entry_snapshot = _safe_float(self.position_memory.get(symbol.upper(), {}).get("entry_price"), 0.0)
        logging.info("[TRADE] SELL_START | symbol=%s | qty=%.10f | reason=%s", symbol, base_amount, reason)
        quote_amount = 0.0
        pre_base_free = max(0.0, base_amount)

        if self.config.AI_TRAINING_MODE and self.config.DRY_RUN:
            base_before = _safe_float(self.paper_positions.get(symbol, 0.0), 0.0)
            base_to_sell = min(base_amount, base_before)
            if base_to_sell <= 0 or price <= 0:
                logging.info("Paper SELL skipped: no paper position or invalid price.")
                return False
            quote_amount = base_to_sell * price if price > 0 else 0.0
            remaining = max(0.0, base_before - base_to_sell)
            self.paper_positions[symbol] = remaining
            if remaining <= 1e-12:
                self.paper_positions.pop(symbol, None)
            self.paper_usdt_free = _safe_float(self.paper_usdt_free, 0.0) + quote_amount
            logging.info(
                "[TRADE] SELL_FILL | mode=paper | symbol=%s | qty=%.10f | got=%.4f USDT | usdt_now=%.4f",
                symbol,
                base_to_sell,
                quote_amount,
                _safe_float(self.paper_usdt_free, 0.0),
            )
            self._save_paper_state()
            base_amount = base_to_sell
            if not self._paper_has_open_position():
                self.active_symbol = None

        order = None
        if not self.config.DRY_RUN:
            try:
                pre_pos = self.trader.get_position(symbol)
                pre_base_free = max(0.0, _safe_float(pre_pos.get("base_free"), base_amount))
                order = self.trader.place_spot_sell(symbol, base_amount)
                if isinstance(order, dict):
                    logging.info(
                        "[TRADE] SELL_ORDER | symbol=%s | id=%s | status=%s | avg=%.6f | filled=%.10f | cost=%.4f",
                        symbol,
                        str(order.get("id", "") or ""),
                        str(order.get("status", "") or "-"),
                        _safe_float(order.get("average"), 0.0) or _safe_float(order.get("price"), 0.0),
                        _safe_float(order.get("filled"), 0.0),
                        _safe_float(order.get("cost"), 0.0),
                    )
                quote_amount = _safe_float(order.get("cost"), 0.0) if isinstance(order, dict) else 0.0
                if quote_amount <= 0:
                    exec_px = 0.0
                    if isinstance(order, dict):
                        exec_px = _safe_float(order.get("average"), 0.0) or _safe_float(order.get("price"), 0.0)
                    if exec_px <= 0:
                        exec_px = self.trader.last_price(symbol, force_refresh=True)
                    if exec_px > 0 and base_amount > 0:
                        quote_amount = base_amount * exec_px
                adapter_confirm = None
                try:
                    adapter_confirm = self.trader.confirm_sell_position(symbol, pre_base_free)
                except Exception:
                    adapter_confirm = None
                if (
                    isinstance(adapter_confirm, tuple)
                    and len(adapter_confirm) == 2
                ):
                    confirmed = bool(adapter_confirm[0])
                    post_base = max(0.0, _safe_float(adapter_confirm[1], 0.0))
                else:
                    confirmed, post_base = self._confirm_sell_position(symbol, pre_base_free)
                if not confirmed:
                    logging.warning(
                        "SELL confirm timeout: could not verify position decrease for %s after %d attempts.",
                        symbol,
                        max(1, int(self.config.ORDER_CONFIRM_RETRIES)),
                    )
                    if quote_amount <= 0 and base_amount > 0:
                        self._register_api_soft_guard_if_needed("order_confirm_timeout:sell")
                        return False
                else:
                    sold_confirmed = max(0.0, pre_base_free - post_base)
                    if sold_confirmed > 0 and sold_confirmed < base_amount:
                        base_amount = sold_confirmed
                reconcile = self._reconcile_post_order_position(
                    symbol=symbol,
                    side="sell",
                    pre_base_free=pre_base_free,
                    requested_base=base_amount,
                    quote_amount=quote_amount,
                    executed_price=price,
                )
                reconciled_qty = max(0.0, _safe_float(reconcile.get("confirmed_qty"), 0.0))
                reconciled_quote = max(0.0, _safe_float(reconcile.get("confirmed_quote"), 0.0))
                reconciled_price = max(0.0, _safe_float(reconcile.get("confirmed_price"), 0.0))
                if reconciled_qty > 0:
                    base_amount = reconciled_qty
                if reconciled_quote > 0:
                    if quote_amount <= 0:
                        quote_amount = reconciled_quote
                    elif abs((reconciled_quote / max(1e-12, quote_amount)) - 1.0) > 0.35:
                        quote_amount = reconciled_quote
                if reconciled_price > 0:
                    price = reconciled_price
                self.active_symbol = None
            except Exception as exc:
                logging.warning("SELL execution failed for %s: %s", symbol, exc)
                return False
        pnl_pct_preview = _pnl_pct(entry_snapshot, price) if entry_snapshot > 0 and price > 0 else 0.0
        pnl_net_preview = pnl_pct_preview - (
            2.0
            * max(0.0, _safe_float(self.config.AI_FEE_BPS, 0.0) + _safe_float(self.config.AI_SLIPPAGE_BPS, 0.0))
            / 10_000.0
        )
        self._update_internal_learning(symbol, pnl_net_preview)
        self._mark_closed_trade_outcome(symbol, reason, pnl_net_preview, side="LONG")
        logging.info(
            "[TRADE] CLOSE | symbol=%s | entry=%.6f | exit=%.6f | pnl=%+.2f%% | qty=%.10f | got=%.4f USDT | reason=%s",
            symbol,
            entry_snapshot,
            price,
            pnl_pct_preview * 100.0,
            base_amount,
            quote_amount,
            reason,
        )
        self._record_order(order_type="sell", symbol=symbol, reason=reason, base_amount=base_amount, quote_amount=quote_amount, order=order)
        if not self.config.DRY_RUN:
            entry_meta = self.real_trade_entry_meta.pop(symbol.upper(), None)
            if isinstance(entry_meta, dict):
                entry_price = _safe_float(entry_meta.get("entry_price"), 0.0)
                pnl_pct = _pnl_pct(entry_price, price) if entry_price > 0 else 0.0
                roundtrip_cost_pct = (
                    2.0
                    * max(0.0, _safe_float(self.config.AI_FEE_BPS, 0.0) + _safe_float(self.config.AI_SLIPPAGE_BPS, 0.0))
                    / 10_000.0
                )
                pnl_pct_net = pnl_pct - roundtrip_cost_pct
                entry_ts_utc = str(entry_meta.get("entry_ts_utc", "") or "")
                duration_sec = 0.0
                try:
                    if entry_ts_utc:
                        duration_sec = max(
                            0.0,
                            datetime.now(timezone.utc).timestamp()
                            - datetime.fromisoformat(entry_ts_utc.replace("Z", "+00:00")).timestamp(),
                        )
                except Exception:
                    duration_sec = 0.0
                market_ctx = entry_meta.get("market_ctx", {})
                regime = str(market_ctx.get("regime", "flat")) if isinstance(market_ctx, dict) else "flat"
                spread_like = _safe_float(entry_meta.get("atr_pct"), 0.0)
                loss_class = self._classify_loss_case(
                    pnl_pct=pnl_pct_net,
                    entry_quality=_safe_float(entry_meta.get("entry_quality"), 0.0),
                    market_ctx=market_ctx if isinstance(market_ctx, dict) else {},
                    spread_like=spread_like,
                )
                trade_record = {
                    "entry_ts_utc": entry_ts_utc,
                    "exit_ts_utc": datetime.now(timezone.utc).isoformat(),
                    "symbol": symbol,
                    "entry_price": entry_price,
                    "exit_price": price,
                    "pnl_pct": pnl_pct_net,
                    "pnl_pct_gross": pnl_pct,
                    "cost_roundtrip_pct": roundtrip_cost_pct,
                    "duration_sec": duration_sec,
                    "exit_reason": reason,
                    "entry_quality": _safe_float(entry_meta.get("entry_quality"), 0.0),
                    "ai_confidence": _safe_float(entry_meta.get("ai_confidence"), 0.0),
                    "entry_edge": _safe_float(entry_meta.get("expected_edge_pct"), 0.0),
                    "dynamic_time_stop_candles": int(_safe_float(entry_meta.get("dynamic_time_stop_candles"), 0.0)),
                    "atr_pct": _safe_float(entry_meta.get("atr_pct"), 0.0),
                    "regime": regime,
                    "loss_class": loss_class,
                    "market_ctx": market_ctx,
                    "runtime_params": entry_meta.get("runtime_params", {}),
                    "active_risk": entry_meta.get("active_risk", {}),
                    "quote_amount": quote_amount,
                    "base_amount": base_amount,
                }
                self._update_regime_trade_feedback(
                    symbol=symbol,
                    regime=regime,
                    pnl_pct_net=pnl_pct_net,
                    entry_quality=_safe_float(entry_meta.get("entry_quality"), 0.0),
                    entry_edge=_safe_float(entry_meta.get("expected_edge_pct"), 0.0),
                    exit_reason=reason,
                    duration_sec=duration_sec,
                )
                self.runtime_adaptation = self.adaptive_agent.on_real_trade_closed(trade_record)
                # Refresh baseline adaptive params after learning update.
                self.runtime_params = self.adaptive_agent.current_base_params()
                if self.ai_engine is not None:
                    diag = self.ai_engine.diagnostics()
                    evolve_res = self.model_evolver.on_real_trade_closed(
                        wf_hit_rate=_safe_float(diag.get("wf_hit_rate"), 0.0),
                        hit_rate=_safe_float(diag.get("hit_rate"), 0.0),
                        model_health=str(diag.get("model_health", "unknown") or "unknown"),
                    )
                    if isinstance(evolve_res, dict) and bool(evolve_res.get("restored_best", False)):
                        try:
                            self.ai_engine.reload_models()
                            logging.info(
                                "AI model restored to best snapshot: %s",
                                str(evolve_res.get("restored_model_id", "") or "unknown"),
                            )
                        except Exception as exc:
                            logging.warning("AI model restore reload failed: %s", exc)
            self._refresh_confirmed_exchange_state(symbol, price)
        self._clear_position_memory(symbol)
        self.last_trade_ts = time.time()
        return True

    def _execute_partial_sell(self, symbol: str, base_amount: float, reason: str, price: float) -> bool:
        if base_amount <= 0:
            return False
        if self.config.DRY_RUN:
            return False
        min_cost = max(0.0, _safe_float(self.trader.market_min_cost(symbol), 0.0))
        px = price if price > 0 else self.trader.last_price(symbol)
        can_sell = self.trader.can_sell_position(symbol, base_amount, px)
        if (min_cost > 0 and px > 0 and (base_amount * px) < (min_cost * 1.001)) or (not can_sell):
            now_ts = time.time()
            key = symbol.upper()
            if now_ts >= _safe_float(self._partial_sell_skip_until_ts.get(key), 0.0):
                logging.info(
                    "PARTIAL SELL skipped for %s: amount below exchange minimum (need>=%.4f USDT).",
                    symbol,
                    min_cost,
                )
                self._partial_sell_skip_until_ts[key] = now_ts + 45.0
            return False
        logging.info("Выполняется PARTIAL SELL: %.10f %s, причина: %s", base_amount, symbol, reason)
        quote_amount = 0.0
        try:
            pre_pos = self.trader.get_position(symbol)
            pre_base_free = max(0.0, _safe_float(pre_pos.get("base_free"), base_amount))
            order = self.trader.place_spot_sell(symbol, base_amount)
            quote_amount = _safe_float(order.get("cost"), 0.0) if isinstance(order, dict) else 0.0
            confirmed, post_base = self._confirm_sell_position(
                symbol,
                pre_base_free,
                retries=max(1, min(2, int(self.config.ORDER_CONFIRM_RETRIES))),
                wait_sec=max(0.15, float(self.config.ORDER_CONFIRM_POLL_SEC) * 0.8),
            )
            if confirmed:
                sold_confirmed = max(0.0, pre_base_free - post_base)
                if sold_confirmed > 0:
                    base_amount = sold_confirmed
            reconcile = self._reconcile_post_order_position(
                symbol=symbol,
                side="sell",
                pre_base_free=pre_base_free,
                requested_base=base_amount,
                quote_amount=quote_amount,
                executed_price=px,
            )
            reconciled_qty = max(0.0, _safe_float(reconcile.get("confirmed_qty"), 0.0))
            reconciled_quote = max(0.0, _safe_float(reconcile.get("confirmed_quote"), 0.0))
            if reconciled_qty > 0:
                base_amount = reconciled_qty
            if reconciled_quote > 0 and (
                quote_amount <= 0 or abs((reconciled_quote / max(1e-12, quote_amount)) - 1.0) > 0.35
            ):
                quote_amount = reconciled_quote
            self._refresh_confirmed_exchange_state(symbol, px)
            self._record_order(order_type="sell", symbol=symbol, reason=reason, base_amount=base_amount, quote_amount=quote_amount, order=order)
            self.last_trade_ts = time.time()
            return True
        except Exception as exc:
            now_ts = time.time()
            key = symbol.upper()
            if now_ts >= _safe_float(self._partial_sell_warn_until_ts.get(key), 0.0):
                logging.warning("PARTIAL SELL failed for %s: %s", symbol, exc)
                self._partial_sell_warn_until_ts[key] = now_ts + 45.0
            # Avoid repeated failures on the same dust amount in fast loops.
            self._partial_sell_skip_until_ts[key] = now_ts + 45.0
            return False

    def _record_order(
        self,
        order_type: str,
        symbol: str,
        reason: str,
        quote_amount: float = 0.0,
        base_amount: float = 0.0,
        order: Optional[dict] = None,
    ) -> None:
        order_id = ""
        if isinstance(order, dict):
            order_id = str(order.get("id", "") or "")
        self.recent_orders.appendleft({
            "ts_utc": datetime.now(timezone.utc).isoformat(),
            "type": order_type,
            "symbol": symbol,
            "reason": reason,
            "quote_amount": quote_amount,
            "base_amount": base_amount,
            "order_id": order_id,
            "dry_run": self.config.DRY_RUN,
        })
        self._register_trade_execution()

    @staticmethod
    def _market_volatility(ohlcv: list[list[float]]) -> float:
        closes = np.array([_safe_float(c[4], 0.0) for c in ohlcv if len(c) > 4], dtype=float)
        if len(closes) < 2:
            return 0.0
        valid = closes > 0
        if not np.all(valid):
            closes = closes[valid]
        if len(closes) < 2:
            return 0.0
        log_returns = np.log(closes[1:] / closes[:-1])
        return float(np.std(log_returns))

    def _ai_payload(self, ai_signal: Optional[AISignal]) -> dict[str, object]:
        if ai_signal is None:
            return {
                "enabled": bool(self.ai_engine),
                "action": "HOLD",
                "confidence": 0.0,
                "calibrated_confidence": 0.0,
                "score": 0.0,
                "regime": "unknown",
                "uncertainty": 1.0,
                "trade_quality": 0.0,
                "trade_filter_pass": False,
                "walkforward_hit_rate": 0.0,
                "model_health": "unknown",
                "reason": "ai_disabled_or_unavailable",
            }
        return {
            "enabled": True,
            "action": ai_signal.action,
            "confidence": ai_signal.confidence,
            "calibrated_confidence": ai_signal.calibrated_confidence,
            "score": ai_signal.score,
            "regime": ai_signal.regime,
            "uncertainty": ai_signal.uncertainty,
            "trade_quality": ai_signal.trade_quality,
            "trade_filter_pass": ai_signal.trade_filter_pass,
            "walkforward_hit_rate": ai_signal.walkforward_hit_rate,
            "model_health": ai_signal.model_health,
                "reason": ai_signal.reason,
        }

    @staticmethod
    def _human_guard_reason(reason: str) -> str:
        mapping = {
            "api_unstable_soft_guard": "Серия API-ошибок: бот временно на паузе.",
            "too_many_cycle_errors": "Слишком много ошибок подряд: временная пауза.",
            "max_trades_per_day": "Достигнут лимит сделок на день.",
            "daily_drawdown_usdt_limit": "Достигнут дневной лимит просадки (USDT).",
            "daily_drawdown_pct_limit": "Достигнут дневной лимит просадки (%).",
            "market_volatility_anomaly": "Аномальная волатильность: вход временно приостановлен.",
            "price_jump_anomaly": "Резкий скачок цены: вход временно приостановлен.",
            "side_stoploss_streak_guard_long": "Серия стоп-лоссов по LONG: временная пауза.",
            "side_stoploss_streak_guard_short": "Серия стоп-лоссов по SHORT: временная пауза.",
        }
        return _repair_mojibake_ru(mapping.get(str(reason or "").strip(), str(reason or "").strip()))


    def _write_live_status(self, payload: dict[str, object]) -> None:
        try:
            incoming = dict(payload or {})
            out = dict(self._last_live_status_payload)
            prev_symbol = str(out.get("symbol", "") or "").strip()
            prev_price = _safe_float(out.get("price"), 0.0)
            prev_sma = _safe_float(out.get("sma"), 0.0)
            prev_deviation = _safe_float(out.get("deviation"), 0.0)
            prev_expected_edge_pct = _safe_float(out.get("expected_edge_pct"), 0.0)
            prev_min_expected_edge_pct = _safe_float(out.get("min_expected_edge_pct"), 0.0)
            prev_ai_entry_quality = _safe_float(out.get("ai_entry_quality"), 0.0)
            prev_ai_entry_min_quality = _safe_float(out.get("ai_entry_min_quality"), 0.0)
            prev_human_reason = str(out.get("human_reason", "") or "")
            prev_has_open_position = bool(out.get("has_open_position", False))
            out.update(incoming)
            incoming_event = str(incoming.get("event", "") or "").strip().lower()
            incoming_symbol_raw = str(incoming.get("symbol", "") or "").strip()
            # Keep UI symbol stable while an open position is being managed.
            # This prevents pair flicker on intermediate cycle events.
            if (
                prev_has_open_position
                and prev_symbol
                and incoming_symbol_raw
                and (_symbol_key(incoming_symbol_raw) != _symbol_key(prev_symbol))
                and incoming_event in {
                    "cycle_busy",
                    "training_no_signal",
                    "no_trade_signal",
                    "entry_blocked",
                    "cooldown",
                }
            ):
                out["source_symbol"] = incoming_symbol_raw
                out["symbol"] = prev_symbol
            # Keep symbol/price consistency for intermediate heartbeat events.
            incoming_symbol = str(incoming.get("symbol", "") or "").strip()
            if "symbol" in out:
                incoming_symbol = str(out.get("symbol", "") or "").strip()
            symbol_changed = bool(incoming_symbol and prev_symbol and (_symbol_key(incoming_symbol) != _symbol_key(prev_symbol)))
            if incoming_symbol and not incoming.get("source_symbol"):
                out["source_symbol"] = incoming_symbol
            if symbol_changed:
                # Hard-reset symbol-scoped sections on pair switch.
                out["base_currency"] = ""
                out["base_free"] = 0.0
                out["entry_price"] = 0.0
                out["peak_price"] = 0.0
                out["pnl_pct"] = 0.0
                out["expected_edge_pct"] = 0.0
                out["min_expected_edge_pct"] = 0.0
                out["ai_entry_quality"] = 0.0
                out["ai_entry_min_quality"] = 0.0
                out["ai_quality_ok"] = False
                out["expected_edge_ok"] = False
                out["edge_floor_components"] = {}
                out["buy_block_reasons"] = []
                out["tp_adaptation"] = {}
                out["smart_exit"] = {}
                out["chart"] = {"candles": []}
                out.pop("cycle_snapshot", None)
            if str(incoming.get("event", "")).strip().lower() == "cycle_busy":
                src_symbol = str(out.get("source_symbol", "") or "").strip()
                out_symbol = str(out.get("symbol", "") or "").strip()
                src_key = _symbol_key(src_symbol)
                out_key = _symbol_key(out_symbol)
                has_pos_busy = bool(incoming.get("has_open_position", out.get("has_open_position", False)))
                if (not symbol_changed) and (not src_key or src_key == out_key):
                    if _safe_float(incoming.get("price"), 0.0) <= 0.0 and prev_price > 0.0:
                        out["price"] = prev_price
                    if _safe_float(incoming.get("sma"), 0.0) <= 0.0 and prev_sma > 0.0:
                        out["sma"] = prev_sma
                    if abs(_safe_float(incoming.get("deviation"), 0.0)) <= 0.0 and abs(prev_deviation) > 0.0:
                        out["deviation"] = prev_deviation
                    if _safe_float(incoming.get("expected_edge_pct"), 0.0) == 0.0 and prev_expected_edge_pct != 0.0:
                        out["expected_edge_pct"] = prev_expected_edge_pct
                    if _safe_float(incoming.get("min_expected_edge_pct"), 0.0) == 0.0 and prev_min_expected_edge_pct > 0.0:
                        out["min_expected_edge_pct"] = prev_min_expected_edge_pct
                    if _safe_float(incoming.get("ai_entry_quality"), 0.0) == 0.0 and prev_ai_entry_quality > 0.0:
                        out["ai_entry_quality"] = prev_ai_entry_quality
                    if _safe_float(incoming.get("ai_entry_min_quality"), 0.0) == 0.0 and prev_ai_entry_min_quality > 0.0:
                        out["ai_entry_min_quality"] = prev_ai_entry_min_quality
                elif symbol_changed:
                    out["price"] = 0.0
                    out["sma"] = 0.0
                    out["deviation"] = 0.0
                if src_key and out_key and src_key != out_key:
                    # Do not leak stale quote from previous symbol while new symbol is being prepared.
                    out["price"] = _safe_float(incoming.get("price"), 0.0)
                    out["sma"] = _safe_float(incoming.get("sma"), 0.0)
                    out["deviation"] = _safe_float(incoming.get("deviation"), 0.0)
                # Also neutralize heavy snapshot fields during busy heartbeat to avoid stale confusion in GUI.
                if "cycle_snapshot" in out:
                    out.pop("cycle_snapshot", None)
                # Reset symbol-scoped fields only when preparing a different symbol and no active position.
                # During open position we keep live fields stable to avoid UI flicker.
                if (not has_pos_busy) and src_key and out_key and src_key != out_key:
                    out["base_currency"] = ""
                    out["base_free"] = 0.0
                    out["entry_price"] = 0.0
                    out["peak_price"] = 0.0
                    out["pnl_pct"] = 0.0
                    out["expected_edge_pct"] = 0.0
                    out["min_expected_edge_pct"] = 0.0
                    out["ai_entry_quality"] = 0.0
                    out["ai_entry_min_quality"] = 0.0
                    out["ai_quality_ok"] = False
                    out["expected_edge_ok"] = False
                    out["edge_floor_components"] = {}
                    out["buy_block_reasons"] = []
                    out["tp_adaptation"] = {}
                    out["smart_exit"] = {}
                    out["chart"] = {"candles": []}
            if "event" in incoming and str(incoming.get("event", "")) != "error" and "error" not in incoming:
                out.pop("error", None)
            if "event" in incoming and "sleep_remaining_sec" not in incoming:
                out.pop("sleep_remaining_sec", None)
            mode = self.config.BOT_MODE
            if mode not in {"training", "live"}:
                mode = "training" if self.config.AI_TRAINING_MODE else ("live" if not self.config.DRY_RUN else "custom")
            now_utc = datetime.now(timezone.utc).isoformat()
            out["bot_mode"] = mode
            out["dry_run"] = bool(self.config.DRY_RUN)
            out["ai_training_mode"] = bool(self.config.AI_TRAINING_MODE)
            out["auto_pilot_mode"] = bool(self.config.AUTO_PILOT_MODE)
            out["use_ai_signal"] = bool(self.config.USE_AI_SIGNAL)
            out["risk_guard_enabled"] = bool(self.config.RISK_GUARD_ENABLED)
            out["live_trading_enabled"] = bool((not self.config.DRY_RUN) and (not self.config.AI_TRAINING_MODE))
            out["trade_market"] = str(self.config.TRADE_MARKET)
            out["engine_backend"] = self.engine_backend
            try:
                if hasattr(self.trader, "backend_status"):
                    bstat = self.trader.backend_status()
                    if isinstance(bstat, dict):
                        out["engine_backend_status"] = bstat
            except Exception:
                pass
            out["market_regime"] = str(self.runtime_market_ctx.get("regime", "flat"))
            out["market_flags"] = self.runtime_market_ctx.get("flags", [])
            out["market_dangerous"] = bool(self.runtime_market_ctx.get("dangerous", False))
            out["adaptive_runtime_params"] = dict(self.runtime_params)
            out["adaptive_last_event"] = dict(self.runtime_adaptation)
            out["advisory"] = dict(self.runtime_advisory) if isinstance(self.runtime_advisory, dict) else {}
            if self.advisory_provider is not None:
                out["advisory_provider_status"] = self.advisory_provider.status()
            out["bot_pid"] = os.getpid()
            out["bot_host"] = socket.gethostname()
            # Backward-compat for GUI/live readers that still use legacy writer fields.
            out["writer_pid"] = out["bot_pid"]
            out["writer_host"] = out["bot_host"]
            out["session_id"] = self._run_session_id
            out["status"] = "running"
            # Heartbeat freshness must always move forward, even for merged payloads.
            out["status_write_ts_utc"] = now_utc
            out["ts_utc"] = now_utc
            human_reason = _repair_mojibake_ru(build_human_reason(out))
            human_reason = _repair_mojibake_ru(human_reason)
            if _is_mojibake_ru(human_reason):
                if bool(out.get("has_open_position", False)):
                    sell_reason = str(out.get("sell_reason", "") or "").strip()
                    if sell_reason:
                        human_reason = f"Позиция открыта: ожидается условие выхода ({self._human_sell_reason(sell_reason)})."
                    else:
                        human_reason = "Позиция открыта: бот сопровождает сделку."
                elif str(out.get("event", "") or "").strip().lower() in {"entry_blocked", "risk_guard_blocked"}:
                    human_reason = "Условия входа не выполнены."
                elif str(out.get("event", "") or "").strip().lower() == "market_data_unavailable":
                    human_reason = "Нет корректных рыночных данных: бот временно пропускает вход."
                else:
                    human_reason = "Подходящих условий для сделки пока нет."
            if _is_mojibake_ru(human_reason) and prev_human_reason and not _is_mojibake_ru(prev_human_reason):
                human_reason = prev_human_reason
            out["human_reason"] = human_reason
            out = _repair_payload_text_ru(out)
            path = Path(self.config.BOT_LIVE_STATUS_PATH)
            if not path.is_absolute():
                path = APP_BASE_DIR / path
            path.parent.mkdir(parents=True, exist_ok=True)
            lock_path = path.with_suffix(path.suffix + ".lock")
            with _JsonFileLock(lock_path, timeout_sec=1.5, stale_sec=30.0) as lock:
                if not lock._acquired:
                    return
                # stale-writer guard: skip overwrite if a live/fresh external writer owns this status file.
                if path.exists():
                    try:
                        existing = json.loads(path.read_text(encoding="utf-8-sig"))
                        existing_pid = int(_safe_float(existing.get("bot_pid"), 0.0))
                        existing_ts = _iso_to_ts(existing.get("status_write_ts_utc") or existing.get("ts_utc"))
                        existing_age = max(0.0, time.time() - existing_ts) if existing_ts > 0 else 9999.0
                        owner_pid = 0
                        try:
                            proc_lock_path = Path(self.config.BOT_PROCESS_LOCK_PATH)
                            if not proc_lock_path.is_absolute():
                                proc_lock_path = APP_BASE_DIR / proc_lock_path
                            if proc_lock_path.exists():
                                lock_payload = json.loads(proc_lock_path.read_text(encoding="utf-8-sig"))
                                owner_pid = int(_safe_float(lock_payload.get("pid"), 0.0))
                        except Exception:
                            owner_pid = 0
                        if (
                            existing_pid > 0
                            and existing_pid != os.getpid()
                            and _pid_alive(existing_pid)
                            and owner_pid != os.getpid()
                            and existing_age <= max(10.0, float(self.config.SLEEP_SECONDS) * 3.0)
                        ):
                            return
                    except Exception:
                        pass
                _write_json_atomic(path, out)
                self._last_live_status_payload = dict(out)
                if "has_open_position" in out:
                    self._last_has_open_position = bool(out.get("has_open_position", False))
                if "event" in out:
                    self._last_cycle_event = str(out.get("event", self._last_cycle_event))
        except Exception as exc:
            # Live status is observability only and must not break trading.
            now_ts = time.time()
            last_warn_ts = _safe_float(getattr(self, "_last_live_status_write_error_ts", 0.0), 0.0)
            if now_ts - last_warn_ts >= 60.0:
                logging.warning("Live status write failed: %s", exc)
                self._last_live_status_write_error_ts = now_ts
            return

    def _setup_signal_handlers(self):
        signal.signal(signal.SIGINT, self._request_stop)
        signal.signal(signal.SIGTERM, self._request_stop)
        if hasattr(signal, "SIGBREAK"):
            signal.signal(signal.SIGBREAK, self._request_stop)
            
    def _next_cycle_sleep_seconds(self) -> float:
        base = max(1.0, float(self.config.SLEEP_SECONDS))
        if not self.config.LIVE_FAST_LOOP:
            return base
        # Live mode: react faster when there is an open position or cooldown.
        if self._last_has_open_position or self._cooldown_remaining_sec() > 0:
            target = min(base, float(self.config.LIVE_ACTIVE_SLEEP_SEC))
        else:
            target = min(base, float(self.config.LIVE_IDLE_SLEEP_SEC))

        # Auto backoff when API errors accumulate: keeps bot responsive but safer for rate-limit/network issues.
        now_ts = time.time()
        window_sec = max(30, int(self.config.API_SOFT_GUARD_WINDOW_SEC))
        while self.api_error_ts and (now_ts - self.api_error_ts[0]) > window_sec:
            self.api_error_ts.popleft()
        api_err_count = len(self.api_error_ts)
        max_errors = max(2, int(self.config.API_SOFT_GUARD_MAX_ERRORS))
        if api_err_count > 0:
            pressure = _clamp(api_err_count / float(max_errors), 0.0, 1.0)
            target *= (1.0 + (1.8 * pressure))

        lock_until = _safe_float(self.guard_state.get("lock_until_ts"), 0.0)
        lock_reason = str(self.guard_state.get("lock_reason", "") or "")
        if lock_until > now_ts and lock_reason == "api_unstable_soft_guard":
            target = max(target, min(base, float(self.config.LIVE_IDLE_SLEEP_SEC) * 2.0))
        return min(base, max(0.8, target))

    def _sleep_with_countdown(self, seconds: float):
        total = max(0.0, float(seconds))
        end_time = time.time() + total
        heartbeat_sec = float(self.config.LIVE_HEARTBEAT_SEC)
        next_hb = time.time() + heartbeat_sec
        while time.time() < end_time and not self.stop_requested:
            now = time.time()
            if (
                heartbeat_sec > 0
                and now >= next_hb
                and self._last_live_status_payload
            ):
                remaining = max(0.0, end_time - now)
                self._write_live_status(
                    {
                        "sleep_remaining_sec": round(remaining, 2),
                        "cooldown_remaining_sec": self._cooldown_remaining_sec(),
                    }
                )
                next_hb = now + heartbeat_sec
            time.sleep(min(0.25, max(0.05, end_time - now)))


def setup_logging(config: Config):
    prune_stats = _auto_prune_runtime_logs(config)
    log_path = Path(config.BOT_LOG_PATH)
    if not log_path.is_absolute():
        log_path = APP_BASE_DIR / log_path
    log_path.parent.mkdir(parents=True, exist_ok=True)
    audit_path = Path(config.AUDIT_LOG_PATH)
    if not audit_path.is_absolute():
        audit_path = APP_BASE_DIR / audit_path
    audit_path.parent.mkdir(parents=True, exist_ok=True)

    # Lightweight log rotation keeps runtime logs fast to read in GUI/live monitor.
    try:
        _rotate_runtime_file(log_path, max_bytes=5 * 1024 * 1024, keep_archives=25)
        _rotate_runtime_file(audit_path, max_bytes=12 * 1024 * 1024, keep_archives=18)
    except Exception:
        # Rotation issues must not block bot start.
        pass

    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    file_handler = logging.FileHandler(log_path, encoding="utf-8-sig")
    file_handler.setFormatter(formatter)
    logging.basicConfig(
        level=logging.INFO,
        handlers=[stream_handler, file_handler],
        force=True,
    )
    try:
        flt = _MojibakeLogFilter()
        root = logging.getLogger()
        for h in root.handlers:
            h.addFilter(flt)
    except Exception:
        pass
    try:
        # Repair mojibake at record creation time to catch early/bare loggers too.
        old_factory = logging.getLogRecordFactory()

        def _record_factory(*args: object, **kwargs: object) -> logging.LogRecord:
            record = old_factory(*args, **kwargs)
            try:
                if isinstance(record.msg, str):
                    record.msg = _repair_mojibake_ru(record.msg)
                if getattr(record, "args", None):
                    fixed_args = []
                    for a in record.args:
                        fixed_args.append(_repair_mojibake_ru(a) if isinstance(a, str) else a)
                    record.args = tuple(fixed_args)
            except Exception:
                pass
            return record

        logging.setLogRecordFactory(_record_factory)
    except Exception:
        pass
    try:
        bot_removed, bot_total = prune_stats.get("bot_log", (0, 0))
        audit_removed, audit_total = prune_stats.get("audit_log", (0, 0))
        if bot_removed > 0 or audit_removed > 0:
            logging.info(
                "Auto-prune old records: bot_log removed=%d/%d | trade_audit removed=%d/%d",
                int(bot_removed),
                int(bot_total),
                int(audit_removed),
                int(audit_total),
            )
        fixed_lines, total_lines = _normalize_runtime_log_mojibake(log_path)
        if fixed_lines > 0:
            logging.info(
                "UTF-8 normalize: repaired garbled runtime log lines=%d/%d",
                int(fixed_lines),
                int(total_lines),
            )
    except Exception:
        pass

def main():
    """Main entry point."""
    _force_utf8_stdio()
    load_env_layers(APP_BASE_DIR, override=False)
    config = Config()
    setup_logging(config)
    adaptive_state_path = Path(config.AI_ADAPTIVE_STATE_PATH)
    if not adaptive_state_path.is_absolute():
        adaptive_state_path = APP_BASE_DIR / adaptive_state_path
    adaptive_trades_path = Path(config.AI_ADAPTIVE_TRADES_PATH)
    if not adaptive_trades_path.is_absolute():
        adaptive_trades_path = APP_BASE_DIR / adaptive_trades_path
    model_registry_path = Path(config.AI_MODEL_REGISTRY_PATH)
    if not model_registry_path.is_absolute():
        model_registry_path = APP_BASE_DIR / model_registry_path
    telemetry_path = Path(config.BOT_TELEMETRY_PATH)
    if not telemetry_path.is_absolute():
        telemetry_path = APP_BASE_DIR / telemetry_path
    adaptive_agent = AdaptiveAgent(
        state_path=adaptive_state_path,
        trade_log_path=adaptive_trades_path,
        base_params={
            "AI_ENTRY_MIN_QUALITY": config.AI_ENTRY_MIN_QUALITY,
            "AI_ENTRY_SOFT_QUALITY": config.AI_ENTRY_SOFT_QUALITY,
            "AI_LOW_QUALITY_RISK_FACTOR": config.AI_LOW_QUALITY_RISK_FACTOR,
            "MIN_EXPECTED_EDGE_PCT": config.MIN_EXPECTED_EDGE_PCT,
            "AI_ATR_STOP_MULT": config.AI_ATR_STOP_MULT,
            "AI_ATR_TAKE_PROFIT_MULT": config.AI_ATR_TAKE_PROFIT_MULT,
            "AI_ATR_TRAILING_MULT": config.AI_ATR_TRAILING_MULT,
            "TIME_STOP_MIN_CANDLES": float(config.TIME_STOP_MIN_CANDLES),
            "TIME_STOP_MAX_CANDLES": float(config.TIME_STOP_MAX_CANDLES),
            "TIME_STOP_VOL_REF": config.TIME_STOP_VOL_REF,
            "TRAILING_ACTIVATION_PCT": config.TRAILING_ACTIVATION_PCT,
            "AI_MIN_RR": config.AI_MIN_RR,
        },
    )
    market_ctx_engine = MarketContextEngine(
        cache_ttl_sec=max(15, config.SLEEP_SECONDS),
        telemetry_path=telemetry_path,
        use_okx_data=config.AI_USE_OKX_MARKET_DATA,
        use_global_market_data=config.AI_USE_GLOBAL_MARKET_DATA,
    )
    overlay_engine = PositionOverlayEngine()
    advisory_provider = AdvisoryProvider(
        enabled=bool(config.ADVISORY_PROVIDER_ENABLED),
        name=config.ADVISORY_PROVIDER_NAME,
        url=config.ADVISORY_PROVIDER_URL,
        timeout_sec=float(config.ADVISORY_PROVIDER_TIMEOUT_SEC),
        ttl_sec=float(config.ADVISORY_PROVIDER_TTL_SEC),
    )
    model_path_obj = Path(config.AI_MODEL_PATH)
    if not model_path_obj.is_absolute():
        model_path_obj = APP_BASE_DIR / model_path_obj
    model_evolver = ModelEvolver(model_path=model_path_obj, registry_path=model_registry_path, min_trade_gap=8)
    
    ai_engine = AISignalEngine(
        use_internet_data=config.AI_USE_INTERNET_DATA,
        online_learning_enabled=True,
        model_path=config.AI_MODEL_PATH,
        training_log_path=config.AI_TRAINING_LOG_PATH,
        status_path=config.AI_STATUS_PATH,
        learning_rate=config.AI_LEARNING_RATE,
        horizon_candles=config.AI_HORIZON_CANDLES,
        label_threshold=config.AI_LABEL_THRESHOLD,
        fee_bps=float(os.getenv("AI_FEE_BPS", "10.0")),
        slippage_bps=float(os.getenv("AI_SLIPPAGE_BPS", "6.0")),
        walkforward_window=int(os.getenv("AI_WALKFORWARD_WINDOW", "300")),
        replay_size=int(os.getenv("AI_REPLAY_SIZE", "800")),
    ) if config.USE_AI_SIGNAL else None

    process_lock_path = Path(config.BOT_PROCESS_LOCK_PATH)
    if not process_lock_path.is_absolute():
        process_lock_path = APP_BASE_DIR / process_lock_path
    process_lock = _ProcessPidLock(process_lock_path)
    if not process_lock.acquire():
        logging.error("Another bot process is already running. Stop it before starting a new one.")
        return

    if not config.DRY_RUN:
        api_key = str(os.getenv("MEXC_API_KEY") or "").strip()
        api_secret = str(os.getenv("MEXC_API_SECRET") or "").strip()
        if not api_key or not api_secret:
            logging.error("Live mode requires MEXC_API_KEY and MEXC_API_SECRET.")
            process_lock.release()
            return

    trader_native: Optional[Trader] = None
    trader_backend: Any = None
    try:
        trader_native = Trader(config)
        trader_backend = build_engine_adapter(config, trader_native)
        logging.info(
            "Execution backend active: %s (mode=%s, market=%s)",
            str(getattr(trader_backend, "backend_name", "python")),
            config.BOT_MODE or ("training" if config.AI_TRAINING_MODE else ("live" if not config.DRY_RUN else "custom")),
            config.TRADE_MARKET,
        )
        bot = Bot(
            config,
            trader_backend,
            ai_engine,
            adaptive_agent,
            market_ctx_engine,
            overlay_engine,
            model_evolver,
            advisory_provider=advisory_provider,
        )
        bot.run()
    except RuntimeError as exc:
        logging.error(f"Initialization failed: {exc}")
    except KeyboardInterrupt:
        logging.info("Interrupted by user. Shutdown completed.")
    except Exception:
        logging.exception("Unhandled exception")
    finally:
        if trader_native is not None:
            try:
                trader_native.shutdown()
            except Exception:
                pass
        process_lock.release()

if __name__ == "__main__":
    main()

