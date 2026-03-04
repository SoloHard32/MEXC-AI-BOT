from __future__ import annotations

from collections import Counter, deque
import json
import os
import queue
import signal
import shutil
import subprocess
import sys
import threading
import time
import zipfile
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from PySide6.QtCore import QObject, QTimer, QUrl, Signal, Slot
from PySide6.QtWidgets import QApplication, QFileDialog, QMainWindow, QMessageBox, QDialog, QTextEdit, QVBoxLayout
from PySide6.QtWebChannel import QWebChannel
from PySide6.QtWebEngineWidgets import QWebEngineView

from live_reason_mapper import build_human_reason

BASE_DIR = Path(__file__).resolve().parent
BOT_PATH = BASE_DIR / "mexc_bot.py"
ENGINE_SERVICE_PATH = BASE_DIR / "rust_engine_service.py"
ADVISORY_MOCK_SERVICE_PATH = BASE_DIR / "advisory_mock_server.py"
SETTINGS_PATH = BASE_DIR / "gui_settings.json"
ENV_PATH = BASE_DIR / ".env"
LIVE_PATH = BASE_DIR / "logs" / "runtime" / "bot_live_status.json"
AI_PATH = BASE_DIR / "logs" / "training" / "ai_status.json"
LOG_PATH = BASE_DIR / "logs" / "runtime" / "mexc_bot.log"
TELEMETRY_PATH = BASE_DIR / "logs" / "runtime" / "bot_telemetry.json"
REPORT_PATH = BASE_DIR / "logs" / "reports" / "health_report.json"
SESSION_REPORT_PATH = BASE_DIR / "logs" / "reports" / "session_report.json"
REPORTS_DIR = BASE_DIR / "logs" / "reports"
RUNTIME_DIR = BASE_DIR / "logs" / "runtime"
TRAINING_DIR = BASE_DIR / "logs" / "training"
UI_INDEX = BASE_DIR / "ui_dashboard" / "index.html"
AUDIT_PATH = BASE_DIR / "logs" / "runtime" / "trade_audit.jsonl"


def _hide_windows_console() -> None:
    """Hide parent console window for GUI launch on Windows."""
    if os.name != "nt":
        return
    try:
        import ctypes  # local import to avoid non-Windows dependency surface

        hwnd = ctypes.windll.kernel32.GetConsoleWindow()
        if hwnd:
            # SW_HIDE = 0
            ctypes.windll.user32.ShowWindow(hwnd, 0)
    except Exception:
        # Non-fatal: GUI should still start if hide call is unavailable.
        pass


def _safe_float(v: Any, d: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return d


def _safe_int(v: Any, d: int = 0) -> int:
    try:
        return int(float(v))
    except Exception:
        return d


def _read_json(path: Path) -> dict[str, Any]:
    try:
        if not path.exists():
            return {}
        raw = path.read_text(encoding="utf-8-sig", errors="replace")
        data = json.loads(raw) if raw.strip() else {}
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(payload, ensure_ascii=False, indent=2)
    tmp = path.with_suffix(path.suffix + f".tmp.{os.getpid()}.{threading.get_ident()}")
    tmp.write_text(text, encoding="utf-8")
    try:
        tmp.replace(path)
    except Exception:
        path.write_text(text, encoding="utf-8")
        try:
            tmp.unlink(missing_ok=True)
        except Exception:
            pass


def _iso_to_ts(value: object) -> float:
    raw = str(value or "").strip()
    if not raw:
        return 0.0
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).timestamp()
    except Exception:
        return 0.0


def _tail_text_lines(path: Path, max_lines: int = 500, max_bytes: int = 300_000) -> list[str]:
    if not path.exists():
        return []
    try:
        size = path.stat().st_size
        read_from = max(0, size - max_bytes)
        with path.open("rb") as f:
            if read_from > 0:
                f.seek(read_from)
            raw = f.read()
        text = raw.decode("utf-8-sig", errors="replace")
        lines = text.splitlines()
        if read_from > 0 and lines:
            # First line can be cut when reading tail from arbitrary offset.
            lines = lines[1:]
        return [_repair_mojibake_ru(ln) for ln in lines[-max_lines:]]
    except Exception:
        return []


def _read_env_file(path: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    if not path.exists():
        return out
    try:
        for line in path.read_text(encoding="utf-8-sig", errors="replace").splitlines():
            s = line.strip()
            if not s or s.startswith("#") or "=" not in s:
                continue
            k, v = s.split("=", 1)
            out[k.strip()] = v.strip()
    except Exception:
        return {}
    return out


def _fmt_price(v: float) -> str:
    av = abs(v)
    if av >= 1000:
        return f"{v:.2f}"
    if av >= 100:
        return f"{v:.3f}"
    if av >= 1:
        return f"{v:.4f}"
    return f"{v:.6f}"


def _sym(s: str) -> str:
    raw = str(s or "").strip().upper()
    if "/" in raw:
        return raw
    if raw.endswith("USDT") and len(raw) > 4:
        return raw[:-4] + "/USDT"
    return raw


def _repair_mojibake_ru(text: object) -> str:
    raw = str(text or "")
    if not raw:
        return raw
    markers = (
        "Р°", "Рё", "Рѕ", "Рµ", "РЅ", "Рї", "Рґ", "Р»", "Рє",
        "С‚", "СЏ", "С…", "Сѓ", "С€", "СЂ", "СЃ", "СЌ",
        "Ð", "Ñ",
    )
    if not any(m in raw for m in markers):
        return raw
    try:
        fixed = raw.encode("cp1251", errors="strict").decode("utf-8", errors="strict")
        if fixed and not (("Р" in fixed and "С" in fixed) or ("Ð" in fixed and "Ñ" in fixed)):
            return fixed
    except Exception:
        pass
    try:
        fixed2 = raw.encode("latin1", errors="strict").decode("utf-8", errors="strict")
        if fixed2 and not (("Р" in fixed2 and "С" in fixed2) or ("Ð" in fixed2 and "Ñ" in fixed2)):
            return fixed2
    except Exception:
        pass
    return raw


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


class BotBridge(QObject):
    stateChanged = Signal(str)
    toast = Signal(str, str)

    def __init__(self, parent_window: "BotWebWindow") -> None:
        super().__init__()
        self.win = parent_window

    @Slot()
    def startBot(self) -> None:
        self.win.start_bot()

    @Slot()
    def stopBot(self) -> None:
        self.win.stop_bot()

    @Slot()
    def restartBot(self) -> None:
        self.win.restart_bot()

    @Slot()
    def saveSettings(self) -> None:
        self.win.save_settings()

    @Slot(str)
    def setMode(self, mode: str) -> None:
        self.win.set_mode(mode)

    @Slot()
    def refreshHealth(self) -> None:
        self.win.refresh_health_report()

    @Slot()
    def openHealth(self) -> None:
        self.win.open_health_report()

    @Slot()
    def openReportsDir(self) -> None:
        self.win.open_reports_dir()

    @Slot()
    def clearLog(self) -> None:
        self.win.clear_log_view()

    @Slot()
    def openLogFile(self) -> None:
        self.win.open_log_file()

    @Slot()
    def safeReset(self) -> None:
        self.win.safe_reset_runtime_cache()

    @Slot()
    def exportAi(self) -> None:
        self.win.export_ai_bundle()

    @Slot()
    def importAi(self) -> None:
        self.win.import_ai_bundle()

    @Slot()
    def refreshBalance(self) -> None:
        self.win.manual_refresh_balance()

    @Slot()
    def openLiveWindow(self) -> None:
        self.win.open_live_window()

    @Slot(result=str)
    def getApiSettings(self) -> str:
        return self.win.get_api_settings_json()

    @Slot(str, str)
    def saveApiSettings(self, api_key: str, api_secret: str) -> None:
        self.win.save_api_settings(api_key, api_secret)

    @Slot(result=str)
    def getAdvancedSettings(self) -> str:
        return self.win.get_advanced_settings_json()

    @Slot(str)
    def saveAdvancedSettings(self, payload: str) -> None:
        self.win.save_advanced_settings_json(payload)

    @Slot(str, result=str)
    def testAdvisorySettings(self, payload: str) -> str:
        return self.win.test_advisory_settings_json(payload)

    @Slot(result=str)
    def getUiState(self) -> str:
        return self.win.get_ui_state_json()

    @Slot(str)
    def saveUiState(self, payload: str) -> None:
        self.win.save_ui_state_json(payload)


class LiveMonitorDialog(QDialog):
    def __init__(self, parent: QMainWindow | None = None) -> None:
        super().__init__(parent)
        self.setWindowTitle("Live мониторинг")
        self.resize(980, 700)
        lay = QVBoxLayout(self)
        self.text = QTextEdit()
        self.text.setReadOnly(True)
        lay.addWidget(self.text)


class BotWebWindow(QMainWindow):
    def __init__(self) -> None:
        super().__init__()
        self.setWindowTitle("MEXC AI BOT v1.0")
        self.resize(1660, 900)

        self.process: subprocess.Popen | None = None
        self.engine_process: subprocess.Popen | None = None
        self.advisory_process: subprocess.Popen | None = None
        self.log_queue: queue.Queue[str] = queue.Queue()
        self.log_lines: list[str] = []
        self._log_all: deque[str] = deque(maxlen=1800)
        self._log_trade: deque[str] = deque(maxlen=700)
        self._log_signal: deque[str] = deque(maxlen=700)
        self._log_error: deque[str] = deque(maxlen=900)
        self.mode = "training"
        self.runtime_mode: str | None = None
        self._last_live_cycle_index = -1
        self._last_live_write_ts = 0.0
        self._cycle_interval_ema_sec = 0.0
        self._cycle_ema_pid = 0
        self._cycle_ema_session = ""
        self._last_chart_symbol = ""
        self._last_chart_candles: list[dict[str, Any]] = []
        self._audit_cache_mtime = 0.0
        self._audit_cache: list[dict[str, Any]] = []
        self._audit_summary_cache_key: tuple[str, int, float] | None = None
        self._audit_summary_cache_ts: float = 0.0
        self._audit_summary_cache_value: dict[str, object] = {}
        self._ui_state: dict[str, Any] = {}
        self.live_dialog: LiveMonitorDialog | None = None
        self._proc_lock = threading.RLock()
        self._proc_op_busy = False
        self._proc_op_name = ""
        self._proc_op_started_ts = 0.0
        self._proc_timeout_warn_ts = 0.0
        self._watchdog_busy_count = 0
        self._watchdog_last_restart_ts = 0.0
        self._watchdog_last_status_ts = ""
        self._watchdog_same_status_count = 0
        self._watchdog_last_warn_ts = 0.0
        self._last_bot_start_ts = 0.0
        self._last_session_report_ts = 0.0
        self._last_dup_kill_ts = 0.0

        self.view = QWebEngineView(self)
        # Lock UI scale to a fixed value so layout always opens in the same visual size.
        self.view.setZoomFactor(1.0)
        self.setCentralWidget(self.view)

        self.bridge = BotBridge(self)
        self.channel = QWebChannel(self.view.page())
        self.channel.registerObject("botBridge", self.bridge)
        self.view.page().setWebChannel(self.channel)

        self._load_settings()
        self._ensure_ui_files()
        self.view.setUrl(QUrl.fromLocalFile(str(UI_INDEX)))

        self.timer = QTimer(self)
        self.timer.timeout.connect(self._refresh_runtime)
        self.timer.start(450)
        self._runtime_timer_interval_ms = 450

        self.log_timer = QTimer(self)
        self.log_timer.timeout.connect(self._drain_log_queue)
        self.log_timer.start(220)
        self._log_timer_interval_ms = 220

        self.proc_watchdog_timer = QTimer(self)
        self.proc_watchdog_timer.timeout.connect(self._check_proc_op_timeout)
        self.proc_watchdog_timer.start(1000)

    def _write_gui_live_status(self, *, event_name: str, note: str = "") -> None:
        """
        Fast GUI-side status marker to keep UI consistent between process transitions.
        Does not override full bot runtime payload when bot writes normally.
        """
        try:
            payload = _read_json(LIVE_PATH)
            now_iso = datetime.now(timezone.utc).isoformat()
            payload["status_write_ts_utc"] = now_iso
            payload["ts_utc"] = now_iso
            payload["event"] = str(event_name or "gui_state")
            payload["human_reason"] = str(note or "")
            payload["bot_mode"] = str(self.runtime_mode or self.mode or "training")
            payload["gui_pid"] = int(os.getpid())
            with self._proc_lock:
                running = bool(self.process and self.process.poll() is None)
                payload["bot_pid"] = int(self.process.pid) if running and self.process else 0
                payload["writer_pid"] = int(payload["bot_pid"])
                payload["writer_host"] = str(payload.get("bot_host") or os.getenv("COMPUTERNAME") or "")
            payload["status"] = "running" if running else "stopped"
            event_key = str(event_name or "").strip().lower()
            # On GUI-side lifecycle events, clear stale runtime fields until bot publishes fresh cycle data.
            if event_key in {"gui_starting", "gui_started", "gui_restart"}:
                payload["symbol"] = "-"
                payload["price"] = 0.0
                payload["sma"] = 0.0
                payload["deviation"] = 0.0
                payload["cycle_index"] = 0
                payload["cycle_elapsed_ms"] = 0.0
                payload["market_fetch_ms"] = 0.0
                payload["features_ms"] = 0.0
                payload["signal_ms"] = 0.0
                payload["order_ms"] = 0.0
                payload["market_data_stale"] = False
                payload["market_data_stale_sec"] = 0.0
                payload["cooldown_remaining_sec"] = 0
                payload["sleep_remaining_sec"] = 0.0
                payload["has_open_position"] = False
                payload["position_side"] = ""
                payload["position_entry"] = 0.0
                payload["position_pnl_pct"] = 0.0
                payload["recent_orders"] = []
            if not running:
                payload["symbol"] = "-"
                payload["price"] = 0.0
                payload["sma"] = 0.0
                payload["deviation"] = 0.0
                payload["cycle_elapsed_ms"] = 0.0
                payload["market_data_stale"] = False
                payload["market_data_stale_sec"] = 0.0
                payload["cooldown_remaining_sec"] = 0
                payload["sleep_remaining_sec"] = 0.0
                payload["has_open_position"] = False
                payload["position_side"] = ""
                payload["position_entry"] = 0.0
                payload["position_pnl_pct"] = 0.0
                payload["recent_orders"] = []
            _write_json_atomic(LIVE_PATH, payload)
        except Exception:
            return

    def _begin_proc_op(self, op_name: str = "op") -> bool:
        with self._proc_lock:
            if self._proc_op_busy:
                return False
            self._proc_op_busy = True
            self._proc_op_name = str(op_name or "op")
            self._proc_op_started_ts = time.time()
            return True

    def _end_proc_op(self) -> None:
        with self._proc_lock:
            self._proc_op_busy = False
            self._proc_op_name = ""
            self._proc_op_started_ts = 0.0
            self._proc_timeout_warn_ts = 0.0

    def _run_proc_op_async(self, fn: Any, op_name: str = "op") -> None:
        if not self._begin_proc_op(op_name):
            self._emit_toast("Операция уже выполняется...", "warn")
            return

        def _worker() -> None:
            try:
                fn()
            finally:
                self._end_proc_op()

        threading.Thread(target=_worker, daemon=True).start()

    def _check_proc_op_timeout(self) -> None:
        with self._proc_lock:
            busy = self._proc_op_busy
            op_name = self._proc_op_name
            started = self._proc_op_started_ts
        if not busy or started <= 0:
            return
        elapsed = time.time() - started
        if elapsed < 18.0:
            return
        if (time.time() - self._proc_timeout_warn_ts) >= 8.0:
            self._append_log(f"[GUI-WEB] Операция '{op_name}' выполняется дольше обычного ({elapsed:.1f}s)")
            self._proc_timeout_warn_ts = time.time()
        if elapsed >= 45.0 and (time.time() - self._proc_timeout_warn_ts) >= 10.0:
            self._append_log(
                f"[GUI-WEB] Операция '{op_name}' все еще выполняется ({elapsed:.1f}s). "
                "Ожидаю завершения, чтобы избежать гонок start/stop."
            )
            self._proc_timeout_warn_ts = time.time()

    def _list_foreign_bot_pids(self, exclude_pid: int | None = None) -> list[int]:
        """
        Return running python process IDs that execute this exact BOT_PATH script,
        excluding current GUI process and optional exclude_pid.
        """
        target = str(BOT_PATH.resolve()).lower().replace("/", "\\")
        me = os.getpid()
        skip: set[int] = {me}
        if exclude_pid and exclude_pid > 0:
            skip.add(int(exclude_pid))
        found: list[int] = []
        try:
            if os.name == "nt":
                ps_cmd = (
                    "Get-CimInstance Win32_Process | "
                    "Where-Object { $_.Name -match '^python(\\\\.exe)?$' } | "
                    "Select-Object ProcessId,CommandLine | ConvertTo-Json -Compress"
                )
                cp = subprocess.run(
                    ["powershell", "-NoProfile", "-Command", ps_cmd],
                    check=False,
                    capture_output=True,
                    text=True,
                    encoding="utf-8",
                    errors="replace",
                )
                raw = (cp.stdout or "").strip()
                if not raw:
                    return []
                data = json.loads(raw)
                rows = data if isinstance(data, list) else [data]
                for row in rows:
                    if not isinstance(row, dict):
                        continue
                    pid = _safe_int(row.get("ProcessId"), 0)
                    if pid <= 0 or pid in skip:
                        continue
                    cmd = str(row.get("CommandLine", "") or "").lower().replace("/", "\\")
                    if target in cmd and "mexc_bot_gui" not in cmd:
                        found.append(pid)
            else:
                cp = subprocess.run(
                    ["ps", "-eo", "pid,args"],
                    check=False,
                    capture_output=True,
                    text=True,
                    encoding="utf-8",
                    errors="replace",
                )
                for ln in (cp.stdout or "").splitlines():
                    s = ln.strip()
                    if not s:
                        continue
                    parts = s.split(None, 1)
                    if len(parts) < 2:
                        continue
                    pid = _safe_int(parts[0], 0)
                    cmd = parts[1].lower()
                    if pid <= 0 or pid in skip:
                        continue
                    if str(BOT_PATH.resolve()).lower() in cmd and "mexc_bot_gui" not in cmd:
                        found.append(pid)
        except Exception:
            return []
        uniq: list[int] = []
        seen: set[int] = set()
        for pid in found:
            if pid not in seen:
                seen.add(pid)
                uniq.append(pid)
        return uniq

    def _kill_pid_hard(self, pid: int) -> bool:
        if pid <= 0 or pid == os.getpid():
            return False
        # Graceful first.
        try:
            os.kill(pid, signal.SIGTERM)
        except Exception:
            pass
        t0 = time.time()
        while (time.time() - t0) < 1.2:
            if not _pid_alive(pid):
                return True
            time.sleep(0.05)
        if os.name == "nt":
            try:
                subprocess.run(
                    ["taskkill", "/PID", str(pid), "/T", "/F"],
                    check=False,
                    capture_output=True,
                    text=True,
                )
            except Exception:
                pass
        else:
            try:
                os.kill(pid, signal.SIGKILL)
            except Exception:
                pass
        return not _pid_alive(pid)

    def _stop_foreign_bot_processes(self, keep_pid: int | None = None) -> int:
        killed = 0
        for pid in self._list_foreign_bot_pids(exclude_pid=keep_pid):
            if self._kill_pid_hard(pid):
                killed += 1
        return killed

    def _enforce_single_bot_process(self, force: bool = False) -> int:
        now_ts = time.time()
        if (not force) and (now_ts - self._last_dup_kill_ts) < 5.0:
            return 0
        with self._proc_lock:
            keep_pid = int(self.process.pid) if (self.process and self.process.poll() is None) else None
        killed = self._stop_foreign_bot_processes(keep_pid=keep_pid)
        self._last_dup_kill_ts = now_ts
        if killed > 0:
            self._append_log(f"[GUI-WEB] Auto-guard: остановлено лишних процессов бота: {killed}")
        return killed

    def _ensure_ui_files(self) -> None:
        ui_dir = BASE_DIR / "ui_dashboard"
        ui_dir.mkdir(parents=True, exist_ok=True)
        if not UI_INDEX.exists():
            raise FileNotFoundError(f"UI file not found: {UI_INDEX}")

    def _emit_toast(self, text: str, level: str = "info") -> None:
        self.bridge.toast.emit(text, level)

    def _append_log(self, line: str) -> None:
        if not line:
            return
        line = _repair_mojibake_ru(line)
        self.log_lines.append(line)
        if len(self.log_lines) > 3500:
            self.log_lines = self.log_lines[-3500:]
        s = str(line)
        sl = s.lower()
        self._log_all.append(s)
        if any(k in sl for k in ("[trade]", "buy", "sell", "close", "opened", "позиц")):
            self._log_trade.append(s)
        if any(k in sl for k in ("signal", "quality", "edge", "ai", "entry", "exit")):
            self._log_signal.append(s)
        if any(k in sl for k in ("error", "warning", "exception", "traceback", "timeout", "fail")):
            self._log_error.append(s)

    def _load_settings(self) -> None:
        cfg = _read_json(SETTINGS_PATH)
        mode = str(cfg.get("BOT_MODE", "training")).strip().lower()
        self.mode = mode if mode in {"training", "live"} else "training"
        ui_state = cfg.get("WEB_UI_STATE", {})
        self._ui_state = ui_state if isinstance(ui_state, dict) else {}
        ui_lang = str(cfg.get("UI_LANGUAGE", self._ui_state.get("lang", "ru"))).strip().lower()
        if ui_lang not in {"ru", "en"}:
            ui_lang = "ru"
        self._ui_state["lang"] = ui_lang

    def set_mode(self, mode: str) -> None:
        m = str(mode).strip().lower()
        if m not in {"training", "live"}:
            return
        if self.process and self.process.poll() is None:
            self._append_log("[GUI-WEB] Смена режима недоступна во время работы. Сначала остановите бота.")
            self._emit_toast("Остановите бота перед сменой режима", "warn")
            return
        self.mode = m
        self._append_log(f"[GUI-WEB] Выбран режим: {self.mode}")

    def save_settings(self) -> None:
        cfg = _read_json(SETTINGS_PATH)
        cfg["BOT_MODE"] = self.mode
        try:
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_dir = REPORTS_DIR / "backups"
            backup_dir.mkdir(parents=True, exist_ok=True)
            if SETTINGS_PATH.exists():
                shutil.copy2(SETTINGS_PATH, backup_dir / f"gui_settings_{ts}.json.bak")
            if ENV_PATH.exists():
                shutil.copy2(ENV_PATH, backup_dir / f".env_{ts}.bak")
            SETTINGS_PATH.write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")
            self._append_log("[GUI-WEB] Настройки сохранены")
            self._emit_toast("Настройки сохранены", "ok")
        except Exception as exc:
            self._append_log(f"[GUI-WEB] Ошибка сохранения: {exc}")
            self._emit_toast(f"Ошибка сохранения: {exc}", "err")

    def _bot_env(self) -> dict[str, str]:
        env = os.environ.copy()
        cfg = _read_json(SETTINGS_PATH)
        env_file = _read_env_file(ENV_PATH)
        for k, v in env_file.items():
            env[k] = v
        for k, v in cfg.items():
            env[k] = "true" if isinstance(v, bool) and v else "false" if isinstance(v, bool) else str(v)

        env["BOT_MODE"] = self.mode
        # Safe execution policy:
        # - allow engine gateway in training for staged validation
        # - live mode may use gateway when ENABLE_GATEWAY_IN_LIVE=true
        gateway_requested = str(env.get("ENGINE_EXECUTION_GATEWAY", "false")).strip().lower() in {"1", "true", "yes", "on"}
        training_gateway = str(env.get("ENABLE_GATEWAY_IN_TRAINING", "true")).strip().lower() in {"1", "true", "yes", "on"}
        live_gateway = str(env.get("ENABLE_GATEWAY_IN_LIVE", "true")).strip().lower() in {"1", "true", "yes", "on"}
        strict_requested = str(env.get("ENGINE_STRICT", "false")).strip().lower() in {"1", "true", "yes", "on"}
        training_strict = str(env.get("ENABLE_STRICT_IN_TRAINING", "true")).strip().lower() in {"1", "true", "yes", "on"}
        live_strict = str(env.get("ENABLE_STRICT_IN_LIVE", "false")).strip().lower() in {"1", "true", "yes", "on"}
        auto_live_strict = str(env.get("AUTO_ENABLE_LIVE_STRICT", "true")).strip().lower() in {"1", "true", "yes", "on"}
        if self.mode == "training":
            env["ENGINE_EXECUTION_GATEWAY"] = "true" if (gateway_requested and training_gateway) else "false"
            env["ENGINE_STRICT"] = "true" if (strict_requested and training_strict) else "false"
        else:
            env["ENGINE_EXECUTION_GATEWAY"] = "true" if (gateway_requested and live_gateway) else "false"
            strict_allowed = bool(strict_requested and live_strict)
            if strict_allowed and auto_live_strict:
                strict_allowed = self._live_backend_ready_for_strict()
            env["ENGINE_STRICT"] = "true" if strict_allowed else "false"

        if self.mode == "training":
            env["DRY_RUN"] = "true"
            env["AI_TRAINING_MODE"] = "true"
        else:
            env["DRY_RUN"] = "false"
            env["AI_TRAINING_MODE"] = "false"
        env["PYTHONIOENCODING"] = "utf-8"
        env["PYTHONUTF8"] = "1"
        return env

    def _live_backend_ready_for_strict(self) -> bool:
        live = _read_json(LIVE_PATH)
        b = live.get("engine_backend_status", {}) if isinstance(live.get("engine_backend_status"), dict) else {}
        if not b:
            # Boot case: no status yet, rely on direct engine health probe.
            return self._engine_health_ok(timeout_sec=0.25)
        if bool(b.get("ready_for_live_strict", False)):
            return True
        # Conservative fallback heuristic when old status format is present.
        healthy = bool(b.get("healthy", False))
        degraded = bool(b.get("exec_degraded", False))
        health_fails = _safe_int(b.get("health_fail_count"), 0)
        fallback_orders = _safe_int(b.get("fallback_order_count"), 0)
        return bool(healthy and (not degraded) and health_fails == 0 and fallback_orders == 0)

    def _engine_health_ok(self, timeout_sec: float = 0.35) -> bool:
        url = "http://127.0.0.1:17890/health"
        try:
            req = urllib.request.Request(url, method="GET")
            with urllib.request.urlopen(req, timeout=max(0.1, float(timeout_sec))) as resp:
                raw = resp.read().decode("utf-8", errors="replace")
            data = json.loads(raw) if raw.strip() else {}
            return bool(isinstance(data, dict) and data.get("ok", False))
        except Exception:
            return False

    def _ensure_engine_service_running(self) -> None:
        env_file = _read_env_file(ENV_PATH)
        cfg = _read_json(SETTINGS_PATH)
        backend = str(cfg.get("ENGINE_BACKEND", env_file.get("ENGINE_BACKEND", "python")) or "python").strip().lower()
        auto_start = str(cfg.get("AUTO_START_ENGINE_SERVICE", env_file.get("AUTO_START_ENGINE_SERVICE", "true")) or "true").strip().lower() in {"1", "true", "yes", "on"}
        if backend == "python" or not auto_start:
            return
        if self._engine_health_ok():
            return
        if not ENGINE_SERVICE_PATH.exists():
            self._append_log("[GUI-WEB] rust_engine_service.py не найден, backend fallback на python")
            return
        try:
            flags = 0
            startupinfo = None
            if os.name == "nt":
                flags |= getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
                flags |= getattr(subprocess, "CREATE_NO_WINDOW", 0)
                startupinfo = subprocess.STARTUPINFO()
                startupinfo.dwFlags |= getattr(subprocess, "STARTF_USESHOWWINDOW", 0)
                startupinfo.wShowWindow = getattr(subprocess, "SW_HIDE", 0)
            proc = subprocess.Popen(
                [sys.executable, "-u", str(ENGINE_SERVICE_PATH)],
                cwd=str(BASE_DIR),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
                errors="replace",
                bufsize=1,
                creationflags=flags,
                startupinfo=startupinfo,
            )
            self.engine_process = proc
            ok = False
            for _ in range(25):
                if self._engine_health_ok(timeout_sec=0.25):
                    ok = True
                    break
                time.sleep(0.12)
            if ok:
                self._append_log(f"[GUI-WEB] Engine service запущен (PID {proc.pid})")
            else:
                self._append_log("[GUI-WEB] Engine service не подтвердил health, продолжим с fallback.")
        except Exception as exc:
            self._append_log(f"[GUI-WEB] Ошибка запуска engine service: {exc}")

    def _stop_engine_service_if_owned(self) -> None:
        proc = self.engine_process
        if not proc:
            return
        try:
            if proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=1.5)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait(timeout=1.0)
        except Exception:
            pass
        finally:
            self.engine_process = None

    def _advisory_health_ok(self, url: str, timeout_sec: float = 0.25) -> bool:
        try:
            base = str(url or "").strip()
            if not base:
                return False
            parsed = urllib.parse.urlparse(base)
            if not parsed.scheme or not parsed.netloc:
                return False
            health_url = urllib.parse.urlunparse((parsed.scheme, parsed.netloc, "/health", "", "", ""))
            req = urllib.request.Request(health_url, method="GET")
            with urllib.request.urlopen(req, timeout=max(0.1, float(timeout_sec))) as resp:
                raw = resp.read().decode("utf-8", errors="replace")
            data = json.loads(raw) if raw.strip() else {}
            return bool(isinstance(data, dict) and data.get("ok", False))
        except Exception:
            return False

    def _ensure_advisory_service_running(self, env: dict[str, str]) -> None:
        enabled = str(env.get("ADVISORY_PROVIDER_ENABLED", "false")).strip().lower() in {"1", "true", "yes", "on"}
        if not enabled:
            return
        url = str(env.get("ADVISORY_PROVIDER_URL", "") or "").strip()
        if not url:
            return
        auto_start = str(env.get("AUTO_START_ADVISORY_MOCK", "true")).strip().lower() in {"1", "true", "yes", "on"}
        if not auto_start:
            return
        try:
            parsed = urllib.parse.urlparse(url)
            host = (parsed.hostname or "").strip().lower()
            port = int(parsed.port or (443 if parsed.scheme == "https" else 80))
        except Exception:
            return
        if host not in {"127.0.0.1", "localhost"}:
            return
        if self._advisory_health_ok(url, timeout_sec=0.25):
            return
        if not ADVISORY_MOCK_SERVICE_PATH.exists():
            self._append_log("[GUI-WEB] advisory_mock_server.py не найден, advisory URL локальный, но mock не запущен.")
            return
        try:
            flags = 0
            startupinfo = None
            if os.name == "nt":
                flags |= getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
                flags |= getattr(subprocess, "CREATE_NO_WINDOW", 0)
                startupinfo = subprocess.STARTUPINFO()
                startupinfo.dwFlags |= getattr(subprocess, "STARTF_USESHOWWINDOW", 0)
                startupinfo.wShowWindow = getattr(subprocess, "SW_HIDE", 0)
            proc = subprocess.Popen(
                [sys.executable, "-u", str(ADVISORY_MOCK_SERVICE_PATH), "--host", host, "--port", str(port)],
                cwd=str(BASE_DIR),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
                errors="replace",
                bufsize=1,
                creationflags=flags,
                startupinfo=startupinfo,
            )
            self.advisory_process = proc
            ok = False
            for _ in range(25):
                if self._advisory_health_ok(url, timeout_sec=0.25):
                    ok = True
                    break
                time.sleep(0.12)
            if ok:
                self._append_log(f"[GUI-WEB] Advisory mock service запущен (PID {proc.pid}) | {host}:{port}")
            else:
                self._append_log("[GUI-WEB] Advisory mock service не подтвердил health.")
        except Exception as exc:
            self._append_log(f"[GUI-WEB] Ошибка запуска advisory mock service: {exc}")

    def _stop_advisory_service_if_owned(self) -> None:
        proc = self.advisory_process
        if not proc:
            return
        try:
            if proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=1.5)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait(timeout=1.0)
        except Exception:
            pass
        finally:
            self.advisory_process = None

    def start_bot(self) -> None:
        self._run_proc_op_async(self._start_bot_impl, "start")

    def _start_bot_impl(self) -> None:
        with self._proc_lock:
            running = self.process and self.process.poll() is None
        if running:
            self._emit_toast("Бот уже запущен", "warn")
            return
        if not BOT_PATH.exists():
            self._emit_toast(f"Не найден {BOT_PATH}", "err")
            return
        try:
            if self.mode == "training":
                cfg = _read_json(SETTINGS_PATH)
                env_vals = _read_env_file(ENV_PATH)
                paper_start = _safe_float(cfg.get("PAPER_START_USDT", env_vals.get("PAPER_START_USDT", "100.0")), 100.0)
                self._apply_paper_balance_now(paper_start, force=True)
            self._ensure_engine_service_running()
            killed = self._stop_foreign_bot_processes()
            if killed > 0:
                self._append_log(f"[GUI-WEB] Остановлено сторонних процессов бота: {killed}")
            bot_env = self._bot_env()
            self._ensure_advisory_service_running(bot_env)
            flags = 0
            startupinfo = None
            if os.name == "nt":
                flags |= getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
                flags |= getattr(subprocess, "CREATE_NO_WINDOW", 0)
                startupinfo = subprocess.STARTUPINFO()
                startupinfo.dwFlags |= getattr(subprocess, "STARTF_USESHOWWINDOW", 0)
                startupinfo.wShowWindow = getattr(subprocess, "SW_HIDE", 0)
            proc = subprocess.Popen(
                [sys.executable, "-u", str(BOT_PATH)],
                cwd=str(BASE_DIR),
                env=bot_env,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
                errors="replace",
                bufsize=1,
                creationflags=flags,
                startupinfo=startupinfo,
            )
            with self._proc_lock:
                self.process = proc
                self.runtime_mode = self.mode
            self._write_gui_live_status(event_name="gui_starting", note="Запуск процесса...")
            self._last_live_cycle_index = -1
            self._last_live_write_ts = 0.0
            self._cycle_interval_ema_sec = 0.0
            self._cycle_ema_pid = int(proc.pid)
            self._cycle_ema_session = ""
            self._append_log(f"[GUI-WEB] Бот запущен (PID {proc.pid}) | mode={self.mode}")
            self._append_log(
                f"[GUI-WEB] Engine backend={bot_env.get('ENGINE_BACKEND','python')} | "
                f"gateway={bot_env.get('ENGINE_EXECUTION_GATEWAY','false')} | "
                f"strict={bot_env.get('ENGINE_STRICT','false')}"
            )
            self._emit_toast("Бот запущен", "ok")
            self._write_gui_live_status(event_name="gui_started", note="Бот запущен из GUI.")
            self._last_bot_start_ts = time.time()
            self._watchdog_busy_count = 0
            self._watchdog_same_status_count = 0
            self._watchdog_last_status_ts = ""
            threading.Thread(target=self._read_process_output, daemon=True).start()
            self._stop_foreign_bot_processes(keep_pid=int(proc.pid))
            self._enforce_single_bot_process(force=True)
        except Exception as exc:
            self._append_log(f"[GUI-WEB] Ошибка запуска: {exc}")
            self._emit_toast(f"Ошибка запуска: {exc}", "err")

    def stop_bot(self) -> None:
        self._run_proc_op_async(self._stop_bot_impl, "stop")

    def _stop_bot_impl(self) -> None:
        with self._proc_lock:
            proc = self.process
        if not proc or proc.poll() is not None:
            self._append_log("[GUI-WEB] Бот не запущен")
            self._write_gui_live_status(event_name="gui_stopped", note="Бот не запущен.")
            return
        try:
            self._write_gui_live_status(event_name="gui_stopping", note="Остановка процесса...")
            soft_sent = False
            if os.name == "nt":
                try:
                    proc.send_signal(signal.CTRL_BREAK_EVENT)
                    soft_sent = True
                    self._append_log("[GUI-WEB] Отправлен мягкий сигнал остановки (CTRL_BREAK)")
                except Exception:
                    soft_sent = False
            if soft_sent:
                try:
                    proc.wait(timeout=6.0)
                except subprocess.TimeoutExpired:
                    self._append_log("[GUI-WEB] Мягкая остановка не успела, завершаю процесс")
            if proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=3.0)
                except subprocess.TimeoutExpired:
                    try:
                        proc.kill()
                        proc.wait(timeout=2.0)
                    except Exception:
                        pass
            if proc.poll() is None:
                self._kill_pid_hard(int(proc.pid))
            self._append_log("[GUI-WEB] Бот остановлен")
            self._emit_toast("Бот остановлен", "ok")
            self._write_gui_live_status(event_name="gui_stopped", note="Бот остановлен.")
            self._watchdog_busy_count = 0
            self._watchdog_same_status_count = 0
            self._watchdog_last_status_ts = ""
        except Exception as exc:
            self._append_log(f"[GUI-WEB] Ошибка остановки: {exc}")
            self._emit_toast(f"Ошибка остановки: {exc}", "err")
        finally:
            with self._proc_lock:
                self.runtime_mode = None
                if self.process and self.process.poll() is not None:
                    self.process = None
            self._stop_advisory_service_if_owned()
            self._write_gui_live_status(event_name="gui_stopped", note="Бот остановлен.")

    def restart_bot(self) -> None:
        self._run_proc_op_async(self._restart_bot_impl, "restart")

    def _restart_bot_impl(self) -> None:
        try:
            self._append_log("[GUI-WEB] Перезапуск бота...")
            self._emit_toast("Перезапуск бота...", "warn")
            self._write_gui_live_status(event_name="gui_restart", note="Перезапуск бота...")
            self.save_settings()
            self._stop_bot_impl()
            killed = self._stop_foreign_bot_processes()
            if killed > 0:
                self._append_log(f"[GUI-WEB] Остановлено сторонних процессов перед перезапуском: {killed}")
            self._start_bot_impl()
        except Exception as exc:
            self._append_log(f"[GUI-WEB] Ошибка перезапуска бота: {exc}")
            self._emit_toast(f"Ошибка перезапуска: {exc}", "err")

    def _read_process_output(self) -> None:
        if not self.process or not self.process.stdout:
            return
        try:
            for line in self.process.stdout:
                self.log_queue.put(line.rstrip("\n"))
        except Exception as exc:
            self.log_queue.put(f"[GUI-WEB] Ошибка чтения: {exc}")

    def _drain_log_queue(self) -> None:
        drained = 0
        drain_limit = 120
        try:
            while drained < drain_limit:
                self._append_log(self.log_queue.get_nowait())
                drained += 1
        except queue.Empty:
            pass
        if self.process and self.process.poll() is not None:
            self._append_log(f"[GUI-WEB] Бот завершился с кодом {self.process.returncode}")
            self.process = None
            self.runtime_mode = None

    def closeEvent(self, event) -> None:  # type: ignore[override]
        try:
            if self._begin_proc_op():
                try:
                    self._stop_bot_impl()
                finally:
                    self._end_proc_op()
            if self.process and self.process.poll() is None:
                # Short grace wait in case stop initiated async cleanup in child.
                t0 = time.time()
                while self.process and self.process.poll() is None and (time.time() - t0) < 3.0:
                    QApplication.processEvents()
                    time.sleep(0.05)
            # Hard safety: on full GUI close, stop any remaining bot workers from this bot folder.
            killed = self._stop_foreign_bot_processes()
            if killed > 0:
                self._append_log(f"[GUI-WEB] При закрытии остановлено процессов бота: {killed}")
            self._stop_advisory_service_if_owned()
            self._stop_engine_service_if_owned()
            self._autocleanup_runtime_logs()
        except Exception:
            pass
        super().closeEvent(event)

    def _autocleanup_runtime_logs(self) -> None:
        try:
            archive_dir = RUNTIME_DIR / "archive"
            archive_dir.mkdir(parents=True, exist_ok=True)
            self._rotate_and_trim_text_file(
                LOG_PATH,
                archive_dir=archive_dir,
                archive_prefix="mexc_bot",
                rotate_size_bytes=2 * 1024 * 1024,
                keep_lines=2500,
                keep_archives=10,
            )
            self._rotate_and_trim_text_file(
                AUDIT_PATH,
                archive_dir=archive_dir,
                archive_prefix="trade_audit",
                rotate_size_bytes=4 * 1024 * 1024,
                keep_lines=6000,
                keep_archives=12,
            )
            self._append_log("[GUI-WEB] Авто-подчистка логов выполнена")
        except Exception as exc:
            self._append_log(f"[GUI-WEB] Авто-подчистка логов: {exc}")

    def _rotate_and_trim_text_file(
        self,
        path: Path,
        *,
        archive_dir: Path,
        archive_prefix: str,
        rotate_size_bytes: int,
        keep_lines: int,
        keep_archives: int,
    ) -> None:
        if not path.exists():
            return
        try:
            size = path.stat().st_size
        except Exception:
            size = 0

        if size >= max(128 * 1024, int(rotate_size_bytes)):
            ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            archived = archive_dir / f"{archive_prefix}_{ts}{path.suffix or '.log'}"
            try:
                path.replace(archived)
            except Exception:
                archived = None
            source_for_trim = archived if archived and archived.exists() else path
            lines = _tail_text_lines(source_for_trim, max_lines=max(200, keep_lines), max_bytes=max(500_000, rotate_size_bytes))
            if lines:
                path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        else:
            lines = _tail_text_lines(path, max_lines=max(200, keep_lines), max_bytes=max(300_000, size))
            if lines:
                path.write_text("\n".join(lines) + "\n", encoding="utf-8")

        try:
            archives = sorted(
                archive_dir.glob(f"{archive_prefix}_*{path.suffix or '.log'}"),
                key=lambda p: p.stat().st_mtime,
                reverse=True,
            )
            for old in archives[max(1, keep_archives):]:
                try:
                    old.unlink()
                except Exception:
                    pass
        except Exception:
            pass

    def refresh_health_report(self, quiet: bool = False) -> None:
        live = _read_json(LIVE_PATH)
        ai = _read_json(AI_PATH)
        rg = live.get("risk_guard", {}) if isinstance(live.get("risk_guard"), dict) else {}
        stale_age = _safe_float(live.get("status_age_sec"), 0.0)
        if stale_age <= 0:
            ts = str(live.get("status_write_ts_utc") or live.get("ts_utc") or "")
            if ts:
                try:
                    stale_age = max(0.0, datetime.now(timezone.utc).timestamp() - datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp())
                except Exception:
                    stale_age = 0.0
        recommendations: list[str] = []
        advisory = live.get("advisory", {}) if isinstance(live.get("advisory"), dict) else {}
        advisory_status = (
            live.get("advisory_provider_status", {})
            if isinstance(live.get("advisory_provider_status"), dict)
            else {}
        )
        if not _read_env_file(ENV_PATH).get("MEXC_API_KEY"):
            recommendations.append("API ключ не задан в .env.")
        if stale_age > 30:
            recommendations.append(f"Live-статус устарел ({stale_age:.1f} сек).")
        if _safe_int(rg.get("api_error_count_window")) >= max(2, _safe_int(rg.get("api_soft_guard_max_errors")) - 1):
            recommendations.append("Слишком частые API ошибки: возможны пропуски циклов.")
        if bool(advisory.get("enabled", False)) and not str(advisory_status.get("url", "") or "").strip():
            recommendations.append("Advisory включен, но URL не задан.")
        if bool(advisory.get("enabled", False)) and _safe_int(advisory_status.get("err_count"), 0) >= 3:
            recommendations.append("Внешний advisory часто недоступен: временно работает только внутренний AI.")
        if not recommendations:
            recommendations.append("Критичных отклонений не обнаружено.")

        session_report = self._build_session_report(live)

        report = {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "live": live,
            "ai": {"ai_status_path": str(AI_PATH), "ai_status": ai},
            "health_flags": {
                "live_file_present": LIVE_PATH.exists(),
                "position_state_present": (RUNTIME_DIR / "position_state.json").exists() or (RUNTIME_DIR / "position_state.json.bak").exists(),
                "bot_log_present": LOG_PATH.exists(),
                "ai_status_present": AI_PATH.exists(),
                "live_stale": stale_age > 30,
            },
            "last_trade_summary": (live.get("recent_orders") or [{}])[0] if isinstance(live.get("recent_orders"), list) and live.get("recent_orders") else {},
            "session_report_path": str(SESSION_REPORT_PATH),
            "session_summary": {
                "warnings": _safe_int(session_report.get("log_counts", {}).get("warnings"), 0),
                "errors": _safe_int(session_report.get("log_counts", {}).get("errors"), 0),
                "tracebacks": _safe_int(session_report.get("log_counts", {}).get("tracebacks"), 0),
                "top_block_reason": (
                    session_report.get("audit", {}).get("top_buy_block_reasons", [{}])[0].get("reason", "-")
                    if isinstance(session_report.get("audit", {}).get("top_buy_block_reasons"), list) and session_report.get("audit", {}).get("top_buy_block_reasons")
                    else "-"
                ),
            },
            "log_tail": self.log_lines[-300:],
            "recommendations": recommendations,
        }
        try:
            REPORTS_DIR.mkdir(parents=True, exist_ok=True)
            REPORT_PATH.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
            SESSION_REPORT_PATH.write_text(json.dumps(session_report, ensure_ascii=False, indent=2), encoding="utf-8")
            if not quiet:
                self._append_log(f"[GUI-WEB] Health report обновлен: {REPORT_PATH}")
                self._append_log(f"[GUI-WEB] Session report обновлен: {SESSION_REPORT_PATH}")
                self._emit_toast("Health report обновлен", "ok")
        except Exception as exc:
            if not quiet:
                self._append_log(f"[GUI-WEB] Ошибка health report: {exc}")
                self._emit_toast(f"Ошибка report: {exc}", "err")

    def _build_session_report(self, live: dict[str, Any]) -> dict[str, Any]:
        now_utc = datetime.now(timezone.utc).isoformat()
        runtime_lines = _tail_text_lines(LOG_PATH, max_lines=12000, max_bytes=2_500_000)
        start_idx = -1
        desired_dry = bool(live.get("dry_run", False))
        desired_train = bool(live.get("ai_training_mode", False))
        dry_token = f"DRY_RUN={'True' if desired_dry else 'False'}"
        train_token = f"AI_TRAINING_MODE={'True' if desired_train else 'False'}"
        # Prefer session start that matches currently running mode from live status.
        for i in range(len(runtime_lines) - 1, -1, -1):
            ln = runtime_lines[i]
            if dry_token in ln and train_token in ln:
                start_idx = i
                break
        # Fallback to generic latest start marker.
        if start_idx < 0:
            for i in range(len(runtime_lines) - 1, -1, -1):
                ln = runtime_lines[i]
                if "DRY_RUN=" in ln and "AI_TRAINING_MODE=" in ln:
                    start_idx = i
                    break
        session_lines = runtime_lines[start_idx:] if start_idx >= 0 else runtime_lines
        start_line = runtime_lines[start_idx] if start_idx >= 0 else ""

        warnings = 0
        errors = 0
        tracebacks = 0
        warn_counter: Counter[str] = Counter()
        err_counter: Counter[str] = Counter()
        for ln in session_lines:
            s = str(ln or "").strip()
            up = s.upper()
            if "WARNING" in up:
                warnings += 1
                warn_counter[s] += 1
            if "ERROR" in up:
                errors += 1
                err_counter[s] += 1
            if "TRACEBACK" in up:
                tracebacks += 1

        start_ts = 0.0
        if start_line:
            try:
                ts_raw = start_line.split(" | ", 1)[0].strip()
                start_ts = datetime.strptime(ts_raw, "%Y-%m-%d %H:%M:%S,%f").replace(tzinfo=timezone.utc).timestamp()
            except Exception:
                start_ts = 0.0

        cur_session_id = str(live.get("session_id", "") or "")
        cur_pid = _safe_int(live.get("bot_pid"), 0)
        audit_lines = _tail_text_lines(AUDIT_PATH, max_lines=20000, max_bytes=4_000_000)
        audit_events: list[dict[str, Any]] = []
        for ln in audit_lines:
            try:
                obj = json.loads(ln)
            except Exception:
                continue
            if not isinstance(obj, dict):
                continue
            keep = True
            if cur_session_id:
                keep = str(obj.get("session_id", "") or "") == cur_session_id
            elif cur_pid > 0:
                keep = _safe_int(obj.get("bot_pid"), 0) == cur_pid
            elif start_ts > 0:
                try:
                    ots = datetime.fromisoformat(str(obj.get("ts_utc", "")).replace("Z", "+00:00")).timestamp()
                    keep = ots >= start_ts
                except Exception:
                    keep = True
            if keep:
                audit_events.append(obj)

        event_counter: Counter[str] = Counter()
        block_counter: Counter[str] = Counter()
        exit_counter: Counter[str] = Counter()
        for ev in audit_events:
            event_counter[str(ev.get("event", "") or "-")] += 1
            for r in (ev.get("buy_block_reasons") or []):
                rr = str(r or "").strip()
                if rr:
                    block_counter[rr] += 1
            sr = str(ev.get("sell_reason", "") or "").strip()
            if sr:
                exit_counter[sr] += 1

        backend = live.get("engine_backend_status", {}) if isinstance(live.get("engine_backend_status"), dict) else {}
        phase = str(backend.get("rollout_phase", "-") or "-")
        strict = bool(backend.get("strict_mode", False))
        healthy = bool(backend.get("healthy", False))
        ready = bool(backend.get("ready_for_live_strict", False))
        rec = str(backend.get("recommended_live_mode", "-") or "-").strip().lower()
        degraded = bool(backend.get("exec_degraded", False))
        fb_order = _safe_int(backend.get("fallback_order_count"), 0)
        health_fail = _safe_int(backend.get("health_fail_count"), 0)
        strict_fallback = bool(backend.get("live_strict_fallback_triggered", False))
        mode_now = str(live.get("bot_mode", "") or "").strip().lower()

        decision = "observe"
        rationale = "Недостаточно данных для фазового решения."
        if mode_now == "live":
            if strict and (degraded or strict_fallback or not healthy or fb_order > 0 or health_fail > 0):
                decision = "rollback_soft"
                rationale = "Strict-live нестабилен: зафиксирована деградация/ошибки backend."
            elif (not strict) and ready and rec == "strict" and healthy and (not degraded):
                decision = "promote_strict"
                rationale = "Backend стабилен и готов к strict-live."
            elif strict and healthy and (not degraded) and fb_order == 0 and health_fail == 0:
                decision = "keep_strict"
                rationale = "Strict-live работает стабильно."
            else:
                decision = "keep_soft"
                rationale = "Soft-live пока безопаснее для текущего состояния backend."
        else:
            if strict and healthy and not degraded:
                decision = "training_strict_ok"
                rationale = "Training strict стабилен, можно использовать для валидации gateway."
            elif strict and (degraded or not healthy):
                decision = "training_strict_unstable"
                rationale = "Training strict нестабилен, нужно устранить деградацию backend."
            else:
                decision = "training_soft"
                rationale = "Training запущен в soft-профиле."

        return {
            "generated_at_utc": now_utc,
            "session_start_line": start_line,
            "session_detected": bool(start_line),
            "line_count_session": len(session_lines),
            "log_counts": {
                "warnings": warnings,
                "errors": errors,
                "tracebacks": tracebacks,
            },
            "top_warnings": [{"line": k, "count": v} for k, v in warn_counter.most_common(10)],
            "top_errors": [{"line": k, "count": v} for k, v in err_counter.most_common(10)],
            "audit": {
                "events_count": len(audit_events),
                "top_events": [{"event": k, "count": v} for k, v in event_counter.most_common(12)],
                "top_buy_block_reasons": [{"reason": k, "count": v} for k, v in block_counter.most_common(12)],
                "top_exit_reasons": [{"reason": k, "count": v} for k, v in exit_counter.most_common(12)],
            },
            "backend": backend,
            "phase_assessment": {
                "phase": phase,
                "mode": mode_now,
                "strict": strict,
                "healthy": healthy,
                "ready_for_live_strict": ready,
                "recommended_live_mode": rec or "-",
                "exec_degraded": degraded,
                "fallback_order_count": fb_order,
                "health_fail_count": health_fail,
                "strict_fallback_triggered": strict_fallback,
                "decision": decision,
                "rationale": rationale,
            },
            "live_meta": {
                "mode": live.get("bot_mode", ""),
                "dry_run": bool(live.get("dry_run", False)),
                "session_id": cur_session_id,
                "bot_pid": cur_pid,
                "event": live.get("event", ""),
                "cycle_index": _safe_int(live.get("cycle_index"), 0),
                "symbol": live.get("symbol", ""),
            },
        }

    def open_health_report(self) -> None:
        if not REPORT_PATH.exists():
            self._emit_toast("health_report.json еще не создан", "warn")
            return
        try:
            os.startfile(str(REPORT_PATH))  # type: ignore[attr-defined]
        except Exception:
            QMessageBox.information(self, "Report", str(REPORT_PATH))

    def open_reports_dir(self) -> None:
        REPORTS_DIR.mkdir(parents=True, exist_ok=True)
        try:
            os.startfile(str(REPORTS_DIR))  # type: ignore[attr-defined]
        except Exception:
            QMessageBox.information(self, "Reports", str(REPORTS_DIR))

    def clear_log_view(self) -> None:
        self.log_lines.clear()
        self._log_all.clear()
        self._log_trade.clear()
        self._log_signal.clear()
        self._log_error.clear()
        self._append_log("[GUI-WEB] Локальный вывод очищен")

    def open_log_file(self) -> None:
        if not LOG_PATH.exists():
            self._emit_toast("Файл runtime лога не найден", "warn")
            return
        try:
            os.startfile(str(LOG_PATH))  # type: ignore[attr-defined]
        except Exception:
            QMessageBox.information(self, "Логи", str(LOG_PATH))

    def safe_reset_runtime_cache(self) -> None:
        runtime_files = [
            RUNTIME_DIR / "bot_live_status.json",
            RUNTIME_DIR / "bot_process.lock",
            RUNTIME_DIR / "bot_telemetry.json",
            RUNTIME_DIR / "position_state.json",
            RUNTIME_DIR / "position_state.json.bak",
        ]
        removed = 0
        for p in runtime_files:
            try:
                if p.exists():
                    p.unlink()
                    removed += 1
            except Exception:
                pass
        self._append_log(f"[GUI-WEB] Безопасный reset: удалено runtime-файлов: {removed}")
        self._emit_toast(f"Безопасный reset: {removed} файлов", "ok")

    def export_ai_bundle(self) -> None:
        try:
            REPORTS_DIR.mkdir(parents=True, exist_ok=True)
            default_name = f"ai_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip"
            target, _ = QFileDialog.getSaveFileName(self, "Экспорт AI", str(REPORTS_DIR / default_name), "ZIP (*.zip)")
            if not target:
                return
            with zipfile.ZipFile(target, "w", compression=zipfile.ZIP_DEFLATED) as zf:
                for p in [
                    TRAINING_DIR / "ai_model.json",
                    TRAINING_DIR / "ai_model_m2.json",
                    TRAINING_DIR / "ai_model_m3.json",
                    TRAINING_DIR / "ai_status.json",
                    TRAINING_DIR / "adaptive_state.json",
                    TRAINING_DIR / "ai_training_log.csv",
                ]:
                    if p.exists():
                        zf.write(p, arcname=p.name)
            self._append_log(f"[GUI-WEB] Экспорт AI выполнен: {target}")
            self._emit_toast("Экспорт AI выполнен", "ok")
        except Exception as exc:
            self._append_log(f"[GUI-WEB] Ошибка экспорта AI: {exc}")
            self._emit_toast(f"Ошибка экспорта: {exc}", "err")

    def import_ai_bundle(self) -> None:
        try:
            source, _ = QFileDialog.getOpenFileName(self, "Импорт AI", str(REPORTS_DIR), "ZIP (*.zip)")
            if not source:
                return
            TRAINING_DIR.mkdir(parents=True, exist_ok=True)
            with zipfile.ZipFile(source, "r") as zf:
                for name in zf.namelist():
                    base = Path(name).name
                    if not base:
                        continue
                    if base.endswith((".json", ".csv")):
                        (TRAINING_DIR / base).write_bytes(zf.read(name))
            self._append_log(f"[GUI-WEB] Импорт AI выполнен: {source}")
            self._emit_toast("Импорт AI выполнен", "ok")
        except Exception as exc:
            self._append_log(f"[GUI-WEB] Ошибка импорта AI: {exc}")
            self._emit_toast(f"Ошибка импорта: {exc}", "err")

    def manual_refresh_balance(self) -> None:
        self._refresh_runtime()

    def open_live_window(self) -> None:
        if self.live_dialog is None:
            self.live_dialog = LiveMonitorDialog(self)
        self._refresh_live_dialog()
        self.live_dialog.show()
        self.live_dialog.raise_()
        self.live_dialog.activateWindow()

    def _refresh_live_dialog(self) -> None:
        if self.live_dialog is None or not self.live_dialog.isVisible():
            return
        live = _read_json(LIVE_PATH)
        header = "=== LIVE MONITOR ==="
        payload = json.dumps(live, ensure_ascii=False, indent=2) if live else "Live-статус пока не записан."
        tail = "\n".join(self.log_lines[-30:])
        self.live_dialog.text.setPlainText(f"{header}\n\n{payload}\n\n=== LOG TAIL ===\n{tail}")

    def get_api_settings_json(self) -> str:
        env = _read_env_file(ENV_PATH)
        cfg = _read_json(SETTINGS_PATH)
        paper_start = str(cfg.get("PAPER_START_USDT", env.get("PAPER_START_USDT", "100.000000")) or "100.000000").strip()
        ui_lang = str(cfg.get("UI_LANGUAGE", self._ui_state.get("lang", "ru"))).strip().lower()
        if ui_lang not in {"ru", "en"}:
            ui_lang = "ru"
        payload = {
            "api_key": env.get("MEXC_API_KEY", ""),
            "api_secret": env.get("MEXC_API_SECRET", ""),
            "advisory_enabled": str(env.get("ADVISORY_PROVIDER_ENABLED", "false")).strip().lower() in {"1", "true", "yes", "on"},
            "advisory_name": env.get("ADVISORY_PROVIDER_NAME", "re7labs"),
            "advisory_url": env.get("ADVISORY_PROVIDER_URL", ""),
            "advisory_timeout_sec": env.get("ADVISORY_PROVIDER_TIMEOUT_SEC", "0.35"),
            "advisory_ttl_sec": env.get("ADVISORY_PROVIDER_TTL_SEC", "10.0"),
            "advisory_weight_training": env.get("ADVISORY_PROVIDER_WEIGHT_TRAINING", env.get("ADVISORY_PROVIDER_WEIGHT", "0.15")),
            "advisory_weight_live": env.get("ADVISORY_PROVIDER_WEIGHT_LIVE", env.get("ADVISORY_PROVIDER_WEIGHT", "0.22")),
            "paper_start_usdt": paper_start,
            "paper_apply_now": False,
            "ui_language": ui_lang,
        }
        return json.dumps(payload, ensure_ascii=False)

    def save_api_settings(self, api_key: str, api_secret: str) -> None:
        key = str(api_key or "").strip()
        sec = str(api_secret or "").strip()
        try:
            self._upsert_env_values(
                {
                    "MEXC_API_KEY": key,
                    "MEXC_API_SECRET": sec,
                }
            )
            self._append_log("[GUI-WEB] API ключи сохранены в .env")
            self._emit_toast("API ключи сохранены", "ok")
        except Exception as exc:
            self._append_log(f"[GUI-WEB] Ошибка сохранения API: {exc}")
            self._emit_toast(f"Ошибка сохранения API: {exc}", "err")

    def get_advanced_settings_json(self) -> str:
        return self.get_api_settings_json()

    def save_advanced_settings_json(self, payload: str) -> None:
        try:
            data = json.loads(str(payload or "{}"))
        except Exception:
            data = {}
        if not isinstance(data, dict):
            data = {}
        api_key = str(data.get("api_key", "") or "").strip()
        api_secret = str(data.get("api_secret", "") or "").strip()
        advisory_enabled = bool(data.get("advisory_enabled", False))
        advisory_name = str(data.get("advisory_name", "re7labs") or "re7labs").strip() or "re7labs"
        advisory_url = str(data.get("advisory_url", "") or "").strip()
        advisory_timeout_sec = max(0.10, _safe_float(data.get("advisory_timeout_sec"), 0.35))
        advisory_ttl_sec = max(1.0, _safe_float(data.get("advisory_ttl_sec"), 10.0))
        advisory_weight_training = max(0.0, min(0.35, _safe_float(data.get("advisory_weight_training"), 0.15)))
        advisory_weight_live = max(0.0, min(0.35, _safe_float(data.get("advisory_weight_live"), 0.22)))
        paper_start_usdt = max(1.0, min(1_000_000.0, _safe_float(data.get("paper_start_usdt"), 100.0)))
        paper_apply_now = bool(data.get("paper_apply_now", False))
        ui_language = str(data.get("ui_language", self._ui_state.get("lang", "ru"))).strip().lower()
        if ui_language not in {"ru", "en"}:
            ui_language = "ru"
        try:
            self._upsert_env_values(
                {
                    "MEXC_API_KEY": api_key,
                    "MEXC_API_SECRET": api_secret,
                    "ADVISORY_PROVIDER_ENABLED": "true" if advisory_enabled else "false",
                    "ADVISORY_PROVIDER_NAME": advisory_name,
                    "ADVISORY_PROVIDER_URL": advisory_url,
                    "ADVISORY_PROVIDER_TIMEOUT_SEC": f"{advisory_timeout_sec:.2f}",
                    "ADVISORY_PROVIDER_TTL_SEC": f"{advisory_ttl_sec:.2f}",
                    "ADVISORY_PROVIDER_WEIGHT_TRAINING": f"{advisory_weight_training:.2f}",
                    "ADVISORY_PROVIDER_WEIGHT_LIVE": f"{advisory_weight_live:.2f}",
                    "PAPER_START_USDT": f"{paper_start_usdt:.6f}",
                }
            )
            cfg = _read_json(SETTINGS_PATH)
            cfg["PAPER_START_USDT"] = f"{paper_start_usdt:.6f}"
            cfg["UI_LANGUAGE"] = ui_language
            web_ui_state = cfg.get("WEB_UI_STATE", {})
            if not isinstance(web_ui_state, dict):
                web_ui_state = {}
            web_ui_state["lang"] = ui_language
            cfg["WEB_UI_STATE"] = web_ui_state
            self._ui_state = dict(web_ui_state)
            SETTINGS_PATH.write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")
            if paper_apply_now:
                self._apply_paper_balance_now(paper_start_usdt)
            self._append_log("[GUI-WEB] Расширенные настройки (API + advisory + paper) сохранены")
            self._emit_toast("Расширенные настройки сохранены", "ok")
        except Exception as exc:
            self._append_log(f"[GUI-WEB] Ошибка сохранения расширенных настроек: {exc}")
            self._emit_toast(f"Ошибка сохранения: {exc}", "err")

    def _apply_paper_balance_now(self, amount: float, force: bool = False) -> None:
        with self._proc_lock:
            running = bool(self.process and self.process.poll() is None)
        if running and (not force):
            self._append_log("[GUI-WEB] Paper баланс не применен: сначала остановите бота.")
            return
        if self.mode != "training" and (not force):
            self._append_log("[GUI-WEB] Paper баланс применяется только в режиме training.")
            return
        path = RUNTIME_DIR / "paper_state.json"
        payload = {
            "ts_utc": datetime.now(timezone.utc).isoformat(),
            "paper_usdt_free": max(0.0, float(amount)),
            "paper_positions": {},
        }
        _write_json_atomic(path, payload)
        self._append_log(f"[GUI-WEB] Paper баланс применен: {amount:.2f} USDT (позиции сброшены).")

    def test_advisory_settings_json(self, payload: str) -> str:
        try:
            data = json.loads(str(payload or "{}"))
        except Exception:
            data = {}
        if not isinstance(data, dict):
            data = {}
        enabled = bool(data.get("advisory_enabled", False))
        if not enabled:
            return json.dumps({"ok": True, "message": "advisory выключен"}, ensure_ascii=False)
        url = str(data.get("advisory_url", "") or "").strip()
        if not url:
            return json.dumps({"ok": False, "message": "URL advisory не задан"}, ensure_ascii=False)
        timeout_sec = max(0.1, _safe_float(data.get("advisory_timeout_sec"), 0.35))
        try:
            parsed = urllib.parse.urlparse(url)
            host = (parsed.hostname or "").strip().lower()
            if host in {"127.0.0.1", "localhost"}:
                env_probe = {
                    "ADVISORY_PROVIDER_ENABLED": "true",
                    "ADVISORY_PROVIDER_URL": url,
                    "AUTO_START_ADVISORY_MOCK": "true",
                }
                self._ensure_advisory_service_running(env_probe)
        except Exception:
            pass

        t0 = time.perf_counter()
        health_ok = self._advisory_health_ok(url, timeout_sec=timeout_sec)
        latency_ms = round((time.perf_counter() - t0) * 1000.0, 1)
        if not health_ok:
            return json.dumps(
                {"ok": False, "message": f"advisory health недоступен ({latency_ms:.1f}ms)"},
                ensure_ascii=False,
            )
        try:
            base = str(url or "").strip()
            sep = "&" if "?" in base else "?"
            probe_url = (
                f"{base}{sep}symbol=SOL/USDT&timeframe=1m&mode={self.mode}&regime=flat&source=gui_test"
            )
            t1 = time.perf_counter()
            req = urllib.request.Request(probe_url, method="GET")
            with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
                raw = resp.read().decode("utf-8", errors="replace")
            payload_resp = json.loads(raw) if raw.strip() else {}
            sig = payload_resp.get("signal", {}) if isinstance(payload_resp, dict) else {}
            action = str(sig.get("action", "HOLD") or "HOLD")
            conf = round(_safe_float(sig.get("confidence"), 0.0), 3)
            probe_ms = round((time.perf_counter() - t1) * 1000.0, 1)
            return json.dumps(
                {
                    "ok": True,
                    "message": f"advisory OK | action={action} conf={conf} | health={latency_ms:.1f}ms probe={probe_ms:.1f}ms",
                },
                ensure_ascii=False,
            )
        except Exception as exc:
            return json.dumps({"ok": False, "message": f"advisory probe error: {exc}"}, ensure_ascii=False)

    def _upsert_env_values(self, values: dict[str, str]) -> None:
        lines: list[str] = []
        if ENV_PATH.exists():
            lines = ENV_PATH.read_text(encoding="utf-8-sig", errors="replace").splitlines()
        out: list[str] = []
        seen: set[str] = set()
        for ln in lines:
            s = ln.strip()
            if not s or s.startswith("#") or "=" not in s:
                out.append(ln)
                continue
            k, _ = s.split("=", 1)
            key = str(k or "").strip()
            if key in values:
                out.append(f"{key}={values[key]}")
                seen.add(key)
            else:
                out.append(ln)
        for k, v in values.items():
            if k not in seen:
                out.append(f"{k}={v}")
        ENV_PATH.write_text("\n".join(out).rstrip() + "\n", encoding="utf-8")

    def get_ui_state_json(self) -> str:
        return json.dumps(self._ui_state if isinstance(self._ui_state, dict) else {}, ensure_ascii=False)

    def save_ui_state_json(self, payload: str) -> None:
        try:
            data = json.loads(str(payload or "{}"))
            if not isinstance(data, dict):
                return
            current = self._ui_state if isinstance(self._ui_state, dict) else {}
            merged = dict(current)
            for k, v in data.items():
                if isinstance(v, (dict, list, str, int, float, bool)) or v is None:
                    merged[k] = v
            self._ui_state = merged
            cfg = _read_json(SETTINGS_PATH)
            cfg["WEB_UI_STATE"] = self._ui_state
            SETTINGS_PATH.write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception:
            return

    def _build_state(self) -> dict[str, Any]:
        live = _read_json(LIVE_PATH)
        ai = _read_json(AI_PATH)

        ts = str(live.get("status_write_ts_utc") or live.get("ts_utc") or "")
        age = 9999.0
        if ts:
            try:
                age = max(0.0, datetime.now(timezone.utc).timestamp() - datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp())
            except Exception:
                pass
        running = self.process is not None and self.process.poll() is None
        if not running:
            # Do not show stale trading context when bot is not running.
            live = {
                **live,
                "event": "stopped",
                "symbol": "-",
                "price": 0.0,
                "usdt_free": 0.0,
                "equity_usdt": 0.0,
                "base_free": 0.0,
                "expected_edge_pct": 0.0,
                "ai_entry_quality": 0.0,
                "has_open_position": False,
                "human_reason": "Бот не запущен",
                "recent_orders": [],
                "cooldown_remaining_sec": 0,
                "sleep_remaining_sec": 0,
            }
        if not running:
            connection = "idle"
        elif not ts:
            connection = "sync"
        else:
            connection = "connected" if age < 20 else "stale" if age < 90 else "disconnected"
        cycle_idx = _safe_int(live.get("cycle_index"), 0)
        writer_pid = _safe_int(live.get("bot_pid"), 0)
        writer_session = str(live.get("session_id", "") or "")
        current_pid = int(self.process.pid) if (self.process and self.process.poll() is None) else 0
        live_write_ts = _iso_to_ts(live.get("status_write_ts_utc") or live.get("ts_utc"))
        if current_pid <= 0:
            self._cycle_ema_pid = 0
            self._cycle_ema_session = ""
            self._last_live_cycle_index = -1
            self._last_live_write_ts = 0.0
            self._cycle_interval_ema_sec = 0.0
        elif writer_pid > 0 and (writer_pid != self._cycle_ema_pid or (writer_session and writer_session != self._cycle_ema_session)):
            self._cycle_ema_pid = writer_pid
            self._cycle_ema_session = writer_session
            self._last_live_cycle_index = -1
            self._last_live_write_ts = 0.0
            self._cycle_interval_ema_sec = 0.0
        if live_write_ts > 0 and cycle_idx >= 0:
            if self._last_live_cycle_index >= 0 and cycle_idx != self._last_live_cycle_index and self._last_live_write_ts > 0:
                interval_sec = max(0.0, live_write_ts - self._last_live_write_ts)
                if interval_sec > 0:
                    if self._cycle_interval_ema_sec <= 0:
                        self._cycle_interval_ema_sec = interval_sec
                    else:
                        self._cycle_interval_ema_sec = (self._cycle_interval_ema_sec * 0.75) + (interval_sec * 0.25)
            self._last_live_cycle_index = cycle_idx
            self._last_live_write_ts = live_write_ts
        live_mode_raw = str(live.get("bot_mode", "")).strip().lower()
        live_mode = live_mode_raw if live_mode_raw in {"training", "live"} else None
        effective_mode = (self.runtime_mode or live_mode or self.mode) if running else self.mode

        rg = live.get("risk_guard", {}) if isinstance(live.get("risk_guard"), dict) else {}
        has_open_position_live = bool(live.get("has_open_position", False))
        event_raw = str(live.get("event", "-") or "-").strip()
        event_norm = event_raw.lower()
        status_reason = (
            "Бот не запущен"
            if not running
            else _repair_mojibake_ru(build_human_reason(live) or str(live.get("human_reason", "-")) or "-")
        )
        status_event = event_raw
        if running and has_open_position_live and event_norm == "cycle_busy":
            # During active position, cycle_busy is a heartbeat; keep stable user-facing text.
            status_event = "position_manage"
            status_reason = "Позиция открыта: бот сопровождает сделку."
        ai_payload = live.get("ai", {}) if isinstance(live.get("ai"), dict) else {}
        engine_backend = str(live.get("engine_backend", "python") or "python")
        engine_backend_status = (
            live.get("engine_backend_status", {})
            if isinstance(live.get("engine_backend_status"), dict)
            else {}
        )
        flags = live.get("market_flags", []) if isinstance(live.get("market_flags"), list) else []
        orders = live.get("recent_orders", []) if isinstance(live.get("recent_orders"), list) else []
        # When bot process is not running, do not show stale order list from previous session.
        if not running:
            orders = []
        telemetry_live = live.get("telemetry", {}) if isinstance(live.get("telemetry"), dict) else {}
        advisory = live.get("advisory", {}) if isinstance(live.get("advisory"), dict) else {}
        advisory_status = (
            live.get("advisory_provider_status", {})
            if isinstance(live.get("advisory_provider_status"), dict)
            else {}
        )
        chart_live = live.get("chart", {}) if isinstance(live.get("chart"), dict) else {}
        chart_candles = chart_live.get("candles", []) if isinstance(chart_live.get("candles", []), list) else []
        chart_symbol = _sym(str(live.get("symbol", "-")))
        if chart_symbol != self._last_chart_symbol:
            self._last_chart_candles = []
            self._last_chart_symbol = chart_symbol
        if chart_candles:
            self._last_chart_candles = chart_candles[-120:]
        elif self._last_chart_candles:
            chart_candles = list(self._last_chart_candles)
        else:
            chart_candles = []
            self._last_chart_candles = []
        telemetry = telemetry_live
        audit_summary = self._audit_summary(effective_mode, window=180)
        session_report = _read_json(SESSION_REPORT_PATH)
        session_log = session_report.get("log_counts", {}) if isinstance(session_report.get("log_counts"), dict) else {}
        session_audit = session_report.get("audit", {}) if isinstance(session_report.get("audit"), dict) else {}
        top_block_reason = "-"
        top_warning = "-"
        try:
            arr = session_audit.get("top_buy_block_reasons", [])
            if isinstance(arr, list) and arr:
                top_block_reason = str((arr[0] or {}).get("reason", "-"))
        except Exception:
            top_block_reason = "-"
        try:
            arrw = session_report.get("top_warnings", [])
            if isinstance(arrw, list) and arrw:
                top_warning = str((arrw[0] or {}).get("line", "-"))
        except Exception:
            top_warning = "-"

        status_age_sec = max(0.0, age if ts else 0.0)
        market_age_sec = max(0.0, _safe_float(live.get("market_data_stale_sec"), 0.0))
        cooldown_sec = max(0, _safe_int(live.get("cooldown_remaining_sec"), 0))
        cycle_elapsed_ms = max(0.0, _safe_float(live.get("cycle_elapsed_ms"), 0.0))
        cycle_ms_est = cycle_elapsed_ms if cycle_elapsed_ms > 0 else max(0.0, self._cycle_interval_ema_sec * 1000.0)
        api_err_count = max(0, _safe_int(rg.get("api_error_count_window"), 0))
        api_err_max = max(1, _safe_int(rg.get("api_soft_guard_max_errors"), 5))
        market_stale = bool(live.get("market_data_stale", False))
        cycle_busy = str(live.get("event", "")).strip().lower() == "cycle_busy"

        cycle_health = 100.0
        if running:
            age_penalty = min(55.0, status_age_sec * 2.0)
            if cycle_ms_est > 0:
                lag_ratio = status_age_sec / max(1e-9, (cycle_ms_est / 1000.0) * 2.2)
                age_penalty = min(70.0, age_penalty + (max(0.0, lag_ratio - 1.0) * 25.0))
            api_penalty = min(35.0, (api_err_count / float(api_err_max)) * 35.0)
            stale_penalty = 20.0 if market_stale else 0.0
            busy_bonus = -6.0 if cycle_busy else 0.0
            cycle_health = max(0.0, min(100.0, 100.0 - age_penalty - api_penalty - stale_penalty + busy_bonus))
        else:
            cycle_health = 0.0

        pass_pct = float(audit_summary.get("entry_pass_pct", 0.0))
        block_pct = float(audit_summary.get("entry_block_pct", 0.0))
        entries_opened = max(0.0, _safe_float(audit_summary.get("entries_opened"), 0.0))
        entries_blocked = max(0.0, _safe_float(audit_summary.get("entries_blocked"), 0.0))
        entry_total = entries_opened + entries_blocked
        anomaly_penalty = min(20.0, _safe_float(live.get("anomaly_score"), 0.0) * 25.0)

        # Decision quality should remain informative even when strict filters block most entries.
        ai_quality_pct = _safe_float(live.get("ai_entry_quality"), 0.0) * 100.0
        expected_edge_pct = _safe_float(live.get("expected_edge_pct"), 0.0)
        min_edge_pct = max(1e-9, _safe_float(live.get("min_expected_edge_pct"), 0.0))
        edge_ratio_score = max(0.0, min(130.0, (expected_edge_pct / min_edge_pct) * 100.0)) if min_edge_pct > 0 else 0.0
        if entry_total >= 8:
            decision_health = (
                (pass_pct * 0.52)
                + ((100.0 - block_pct) * 0.18)
                + (ai_quality_pct * 0.20)
                + (edge_ratio_score * 0.10)
                - anomaly_penalty
            )
        else:
            # Warm-up: rely more on signal quality/edge than on sparse entry stats.
            decision_health = (ai_quality_pct * 0.72) + (edge_ratio_score * 0.18) + (10.0 * 0.10) - anomaly_penalty
        if running and entry_total <= 0:
            decision_health = max(18.0, decision_health)
        decision_health = max(0.0, min(100.0, decision_health))

        warnings: list[str] = []
        if bool(rg.get("locked", False)):
            warnings.append(f"Guard lock: {str(rg.get('lock_reason', 'unknown'))}")
        if bool(live.get("market_data_stale", False)):
            warnings.append("Рыночные данные устарели")
        if not bool(live.get("expected_edge_ok", True)):
            warnings.append("Ожидаемое преимущество ниже порога")
        if bool(live.get("short_model_weak", False)):
            warnings.append("short_model_weak=true (для spot это инфо)")
        if engine_backend != "python" and not bool(engine_backend_status.get("healthy", False)):
            warnings.append("Внешний движок недоступен: используется python fallback.")
        if bool(engine_backend_status.get("exec_degraded", False)):
            left_sec = _safe_float(engine_backend_status.get("exec_degraded_left_sec"), 0.0)
            warnings.append(f"Gateway временно в деградации: fallback на python ({left_sec:.0f}s).")
        if bool(engine_backend_status.get("live_strict_fallback_triggered", False)):
            warnings.append("Live strict автоматически откатан в soft из-за проблем backend.")
        if running and cycle_health < 35:
            warnings.append("Цикл обновляется нестабильно (высокая задержка телеметрии).")
        if effective_mode == "training" and not bool(live.get("exchange_balance_trusted", False)):
            warnings.append("Реальный MEXC баланс временно не подтвержден (показано последнее валидное значение).")
        if bool(advisory.get("enabled", False)) and not bool(advisory.get("ok", False)):
            mute_for = _safe_float(advisory_status.get("mute_for_sec"), 0.0)
            if mute_for > 0:
                warnings.append(f"Advisory временно muted ({mute_for:.0f}s), используется только внутренний AI.")
            elif _safe_int(advisory_status.get("err_count"), 0) > 0:
                warnings.append("Advisory недоступен, используется только внутренний AI.")

        rollout_phase = str(engine_backend_status.get("rollout_phase", "-") or "-")
        ready_live_strict = bool(engine_backend_status.get("ready_for_live_strict", False))
        rec_live_mode = str(engine_backend_status.get("recommended_live_mode", "-") or "-")
        strict_mode_on = bool(engine_backend_status.get("strict_mode", False))
        if effective_mode == "live":
            if (not strict_mode_on) and ready_live_strict:
                warnings.append("Backend готов: можно перейти на strict live.")
            if strict_mode_on and rec_live_mode == "soft":
                warnings.append("Рекомендуется soft режим live: backend нестабилен для strict.")
        ui_lang = str(self._ui_state.get("lang", "ru")).strip().lower()
        if ui_lang not in {"ru", "en"}:
            ui_lang = "ru"

        labels_real = _safe_int(ai.get("total_labels"))
        labels_observed = _safe_int(ai.get("observed_samples_total"))
        runtime_dry_run = bool(ai.get("runtime_dry_run", False))
        labels_live = labels_observed if runtime_dry_run else labels_real

        state = {
            "header": {
                "title": "MEXC AI BOT v1.0",
                "connection": connection,
                "connection_text": (
                    "Подключено" if connection == "connected"
                    else "С задержкой" if connection == "stale"
                    else "Синхронизация" if connection == "sync"
                    else "Ожидание"
                ),
                "ts": ts or "-",
                "mode": effective_mode,
                "lang": ui_lang,
                "market": str(live.get("trade_market", "spot")).upper(),
                "backend": str(live.get("engine_backend", "python") or "python"),
                "pid": int(self.process.pid) if self.process else _safe_int(live.get("bot_pid"), 0),
                "advisory_enabled": bool(advisory.get("enabled", False)),
                "advisory_ok": bool(advisory.get("ok", False)),
                "advisory_muted": (_safe_float(advisory_status.get("mute_for_sec"), 0.0) > 0.0),
                "advisory_status_text": (
                    "Advisory: ON"
                    if bool(advisory.get("enabled", False)) and bool(advisory.get("ok", False))
                    else (
                        "Advisory: MUTED"
                        if bool(advisory.get("enabled", False)) and (_safe_float(advisory_status.get("mute_for_sec"), 0.0) > 0.0)
                        else (
                            "Advisory: OFF"
                            if bool(advisory.get("enabled", False))
                            else "Advisory: DISABLED"
                        )
                    )
                ),
            },
            "banner": "AI обучение активно. DRY RUN: реальные ордера не отправляются." if effective_mode == "training" else "Боевой режим LIVE: реальные ордера разрешены.",
            "controls": {"mode": effective_mode, "running": running},
            "proc": {
                "busy": bool(self._proc_op_busy),
                "op": str(self._proc_op_name or ""),
                "elapsed_sec": round(max(0.0, time.time() - self._proc_op_started_ts), 1) if self._proc_op_busy and self._proc_op_started_ts > 0 else 0.0,
            },
            "status": {
                "status": "Работает" if self.process and self.process.poll() is None else "Остановлен",
                "pid": int(self.process.pid) if self.process else _safe_int(live.get("bot_pid"), 0),
                "cycle": _safe_int(live.get("cycle_index"), 0),
                "reason": status_reason,
                "symbol": _sym(str(live.get("symbol", "-"))),
                "event": status_event,
            },
            "balance": {
                "exchange_usdt": round(
                    _safe_float(
                        live.get(
                            "exchange_usdt_free",
                            _safe_float(live.get("usdt_free"), 0.0) if effective_mode == "live" else 0.0,
                        )
                    ),
                    2,
                ),
                "paper_usdt": round(
                    _safe_float(
                        live.get(
                            "paper_usdt_free",
                            _safe_float(live.get("usdt_free"), 0.0) if effective_mode == "training" else 0.0,
                        )
                    ),
                    2,
                ),
                "pnl_pct": round(_safe_float(live.get("pnl_pct")) * 100.0, 2),
                "symbol": _sym(str(live.get("symbol", "-"))),
                "price": _fmt_price(_safe_float(live.get("price"))),
                "exchange_balance_source": str(live.get("exchange_balance_source", "unknown") or "unknown"),
                "exchange_balance_trusted": bool(live.get("exchange_balance_trusted", False)),
            },
            "ai_signal": {
                "action": str(ai_payload.get("action", "-")),
                "conf": round(_safe_float(ai_payload.get("confidence")), 2),
                "quality": round(_safe_float(live.get("ai_entry_quality")), 2),
                "min_quality": round(_safe_float(live.get("ai_entry_min_quality"), 0.5), 2),
                "edge_pct": round(_safe_float(live.get("expected_edge_pct")) * 100.0, 2),
                "min_edge_pct": round(_safe_float(live.get("min_expected_edge_pct")) * 100.0, 2),
                "health": str(ai_payload.get("model_health", live.get("model_health", "-"))),
                "drift": round(_safe_float(live.get("drift_score")), 2),
                "drift_active": bool(live.get("drift_active", False)),
            },
            "training": {
                "model": str(ai.get("model_status", "-")),
                "online": bool(ai.get("online_learning_effective", False)),
                "labels": labels_live,
                "labels_real": labels_real,
                "samples": labels_observed,
                "labels_mode": ("observed" if runtime_dry_run else "real"),
                "hit_rate": round(_safe_float(ai.get("hit_rate")) * 100.0, 2),
                "wf": round(_safe_float(ai.get("walkforward_hit_rate")) * 100.0, 2),
                "last_update": str(ai.get("last_training_update", "-")),
                "last_observation_update": str(ai.get("last_observation_update", "-")),
            },
            "market": {
                "regime": str(live.get("market_regime", "-")),
                "flags": flags,
                "anomaly": round(_safe_float(live.get("anomaly_score")), 2),
                "data_stale": bool(live.get("market_data_stale", False)),
                "volatility": round(_safe_float(live.get("market_volatility")), 4),
            },
            "guard": {
                "locked": bool(rg.get("locked", False)),
                "reason": str(rg.get("lock_reason", "-")) or "-",
                "api_errors": _safe_int(rg.get("api_error_count_window")),
                "api_max": _safe_int(rg.get("api_soft_guard_max_errors")),
                "trades_day": _safe_int(rg.get("trade_count")),
                "trades_max": _safe_int(rg.get("max_trades_per_day")),
                "drawdown": round(_safe_float(rg.get("drawdown_pct")), 2),
            },
            "session": {
                "warnings": _safe_int(session_log.get("warnings"), 0),
                "errors": _safe_int(session_log.get("errors"), 0),
                "tracebacks": _safe_int(session_log.get("tracebacks"), 0),
                "top_block_reason": top_block_reason,
                "top_warning": top_warning,
                "audit_events": _safe_int(session_audit.get("events_count"), 0),
                "report_ts": str(session_report.get("generated_at_utc", "-")),
            },
            "ops": {
                "cycle_ms_est": round(cycle_ms_est, 0),
                "status_age_sec": round(status_age_sec, 1),
                "market_age_sec": round(market_age_sec, 1),
                "cooldown_sec": cooldown_sec,
                "sleep_remaining_sec": round(max(0.0, _safe_float(live.get("sleep_remaining_sec"), 0.0)), 1),
                "entry_pass_pct": round(pass_pct, 1),
                "entry_block_pct": round(block_pct, 1),
                "top_block_reason": str(audit_summary.get("top_block_reason", "-")),
                "top_exit_reason": str(audit_summary.get("top_exit_reason", "-")),
                "sample_size": _safe_int(audit_summary.get("sample_size"), 0),
                "api_err": api_err_count,
                "api_err_max": api_err_max,
                "cycle_health_score": round(cycle_health, 1),
                "decision_health_score": round(decision_health, 1),
                "exec_slippage_bps": round(_safe_float(live.get("exec_slippage_ema_bps"), 0.0), 2),
                "loss_streak_live": _safe_int(live.get("loss_streak_live"), 0),
                "position_risk_source": str(live.get("position_risk_source", "-") or "-"),
                "entry_policy_primary_reason": str(live.get("entry_policy_primary_reason", "") or "-"),
                "engine_backend": engine_backend,
                "engine_healthy": bool(engine_backend_status.get("healthy", engine_backend == "python")),
                "engine_mode": str(engine_backend_status.get("mode", "native") or "native"),
                "engine_exec_gateway": bool(engine_backend_status.get("exec_gateway", False)),
                "engine_strict_mode": bool(engine_backend_status.get("strict_mode", False)),
                "engine_rollout_phase": rollout_phase,
                "engine_ready_live_strict": ready_live_strict,
                "engine_recommended_live_mode": rec_live_mode,
                "engine_live_strict_fallback_triggered": bool(engine_backend_status.get("live_strict_fallback_triggered", False)),
                "engine_exec_degraded": bool(engine_backend_status.get("exec_degraded", False)),
                "engine_exec_degraded_left_sec": round(_safe_float(engine_backend_status.get("exec_degraded_left_sec"), 0.0), 1),
                "engine_health_latency_ms": round(_safe_float(engine_backend_status.get("health_latency_ms"), 0.0), 1),
                "engine_snapshot_latency_ms": round(_safe_float(engine_backend_status.get("snapshot_latency_ms"), 0.0), 1),
                "engine_order_latency_ms": round(_safe_float(engine_backend_status.get("order_latency_ms"), 0.0), 1),
                "engine_fallback_snapshot_count": _safe_int(engine_backend_status.get("fallback_snapshot_count"), 0),
                "engine_fallback_order_count": _safe_int(engine_backend_status.get("fallback_order_count"), 0),
                "engine_health_fail_count": _safe_int(engine_backend_status.get("health_fail_count"), 0),
                "advisory_enabled": bool(advisory.get("enabled", False)),
                "advisory_source": str(advisory.get("source", "") or str(advisory_status.get("name", "-") or "-")),
                "advisory_ok": bool(advisory.get("ok", False)),
                "advisory_action": str(advisory.get("action", "HOLD") or "HOLD"),
                "advisory_confidence": round(_safe_float(advisory.get("confidence"), 0.0), 2),
                "advisory_edge_bias_pct": round(_safe_float(advisory.get("edge_bias_pct"), 0.0) * 100.0, 2),
                "advisory_applied": bool(advisory.get("applied", False)),
                "advisory_err_count": _safe_int(advisory_status.get("err_count"), 0),
                "advisory_ok_count": _safe_int(advisory_status.get("ok_count"), 0),
                "advisory_latency_ms": round(_safe_float(advisory_status.get("last_latency_ms"), 0.0), 1),
                "advisory_mute_sec": round(_safe_float(advisory_status.get("mute_for_sec"), 0.0), 1),
            },
            "warnings": warnings,
            "recent_orders": orders[:30],
            "logs": list(self._log_all)[-700:],
            "log_counts": {
                "all": len(self._log_all),
                "trade": len(self._log_trade),
                "signal": len(self._log_signal),
                "error": len(self._log_error),
            },
            "telemetry": {
                "ai_quality": telemetry.get("ai_quality", []) if isinstance(telemetry.get("ai_quality", []), list) else [],
                "expected_edge": telemetry.get("expected_edge", []) if isinstance(telemetry.get("expected_edge", []), list) else [],
                "backend_health": engine_backend_status.get("health_series", []) if isinstance(engine_backend_status.get("health_series", []), list) else [],
                "backend_snapshot_latency": engine_backend_status.get("snapshot_latency_series", []) if isinstance(engine_backend_status.get("snapshot_latency_series", []), list) else [],
                "backend_order_latency": engine_backend_status.get("order_latency_series", []) if isinstance(engine_backend_status.get("order_latency_series", []), list) else [],
                "candles": chart_candles,
                "symbol": _sym(str(live.get("symbol", "-"))),
                "timeframe": str(getattr(self, "mode", "") or "1m"),
            },
        }
        return state

    def _audit_summary(self, mode: str, window: int = 180) -> dict[str, object]:
        mode_norm = str(mode or "").strip().lower()
        try:
            mtime = AUDIT_PATH.stat().st_mtime if AUDIT_PATH.exists() else 0.0
        except Exception:
            mtime = 0.0
        cache_key = (mode_norm, int(window), float(mtime))
        now_ts = time.time()
        if (
            self._audit_summary_cache_key == cache_key
            and (now_ts - float(self._audit_summary_cache_ts)) <= 1.4
            and isinstance(self._audit_summary_cache_value, dict)
            and self._audit_summary_cache_value
        ):
            return dict(self._audit_summary_cache_value)
        if mtime > 0 and (mtime != self._audit_cache_mtime or not self._audit_cache):
            rows: list[dict[str, Any]] = []
            for ln in _tail_text_lines(AUDIT_PATH, max_lines=max(260, window * 2), max_bytes=400_000):
                s = ln.strip()
                if not s:
                    continue
                try:
                    obj = json.loads(s)
                except Exception:
                    continue
                if isinstance(obj, dict):
                    rows.append(obj)
            self._audit_cache = rows
            self._audit_cache_mtime = mtime
        elif mtime <= 0:
            self._audit_cache = []
            self._audit_cache_mtime = 0.0

        block_counter: Counter[str] = Counter()
        exit_counter: Counter[str] = Counter()
        opened = 0
        blocked = 0
        sample = 0
        for rec in reversed(self._audit_cache):
            rec_mode = str(rec.get("mode", "")).strip().lower()
            if mode_norm and rec_mode and rec_mode != mode_norm:
                continue
            event = str(rec.get("event", "")).strip().lower()
            sample += 1
            if event in {"buy", "training_buy_signal"}:
                opened += 1
            blocked_row = False
            if event in {"entry_blocked", "risk_guard_blocked"}:
                blocked_row = True
            elif event in {"no_trade_signal", "training_no_signal"}:
                reasons = rec.get("buy_block_reasons", [])
                if isinstance(reasons, list) and reasons:
                    blocked_row = True
            if blocked_row:
                blocked += 1
                reasons = rec.get("buy_block_reasons", [])
                if isinstance(reasons, list):
                    for r in reasons:
                        rr = str(r or "").strip()
                        if rr:
                            block_counter[rr] += 1
                guard_reason = str(rec.get("guard_reason", "")).strip()
                if guard_reason:
                    block_counter[f"guard:{guard_reason}"] += 1
            if event in {"sell", "training_sell_signal", "partial_sell"}:
                ex = str(rec.get("sell_reason", "") or rec.get("reason", "") or "exit").strip()
                if ex:
                    exit_counter[ex] += 1
            if sample >= window:
                break

        total_entry = opened + blocked
        pass_pct = (opened / float(total_entry) * 100.0) if total_entry > 0 else 0.0
        block_pct = (blocked / float(total_entry) * 100.0) if total_entry > 0 else 0.0
        top_block = "-"
        top_exit = "-"
        if block_counter:
            bk, bv = block_counter.most_common(1)[0]
            top_block = f"{bk} ({bv})"
        if exit_counter:
            ek, ev = exit_counter.most_common(1)[0]
            top_exit = f"{ek} ({ev})"
        out = {
            "sample_size": sample,
            "entries_opened": opened,
            "entries_blocked": blocked,
            "entry_pass_pct": pass_pct,
            "entry_block_pct": block_pct,
            "top_block_reason": top_block,
            "top_exit_reason": top_exit,
        }
        self._audit_summary_cache_key = cache_key
        self._audit_summary_cache_ts = now_ts
        self._audit_summary_cache_value = dict(out)
        return out

    def _refresh_runtime(self) -> None:
        state = self._build_state()
        self._retune_timers(state)
        try:
            controls = state.get("controls", {}) if isinstance(state.get("controls"), dict) else {}
            if bool(controls.get("running", False)):
                self._enforce_single_bot_process(force=False)
        except Exception:
            pass
        self.bridge.stateChanged.emit(json.dumps(state, ensure_ascii=False))
        self._refresh_live_dialog()
        self._auto_refresh_reports_if_needed(state)
        self._watchdog_tick(state)

    def _retune_timers(self, state: dict[str, Any]) -> None:
        """
        Adaptive GUI polling:
        - running bot: faster UI updates
        - idle/stopped: lighter polling to reduce CPU usage
        """
        try:
            controls = state.get("controls", {}) if isinstance(state.get("controls"), dict) else {}
            running = bool(controls.get("running", False))
            with self._proc_lock:
                proc_busy = bool(self._proc_op_busy)
            target_runtime = 380 if (running or proc_busy) else 900
            target_log = 180 if (running or proc_busy) else 320

            if target_runtime != int(getattr(self, "_runtime_timer_interval_ms", 0)):
                self.timer.setInterval(int(target_runtime))
                self._runtime_timer_interval_ms = int(target_runtime)
            if target_log != int(getattr(self, "_log_timer_interval_ms", 0)):
                self.log_timer.setInterval(int(target_log))
                self._log_timer_interval_ms = int(target_log)
        except Exception:
            return

    def _auto_refresh_reports_if_needed(self, state: dict[str, Any]) -> None:
        try:
            controls = state.get("controls", {}) if isinstance(state.get("controls"), dict) else {}
            running = bool(controls.get("running", False))
            now_ts = time.time()
            # Keep side tabs (session/health derived blocks) reasonably fresh while bot is active.
            if running and (now_ts - self._last_session_report_ts) >= 25.0:
                self.refresh_health_report(quiet=True)
                self._last_session_report_ts = now_ts
        except Exception:
            return

    def _watchdog_tick(self, state: dict[str, Any]) -> None:
        try:
            controls = state.get("controls", {}) if isinstance(state.get("controls"), dict) else {}
            header = state.get("header", {}) if isinstance(state.get("header"), dict) else {}
            status = state.get("status", {}) if isinstance(state.get("status"), dict) else {}
            ops = state.get("ops", {}) if isinstance(state.get("ops"), dict) else {}
            running = bool(controls.get("running", False))
            mode = str(header.get("mode", controls.get("mode", ""))).strip().lower()
            event = str(status.get("event", "")).strip().lower()
            status_age = _safe_float(ops.get("status_age_sec"), 0.0)
            cycle_ms = max(1.0, _safe_float(ops.get("cycle_ms_est"), 0.0))
            market_age_sec = max(0.0, _safe_float(ops.get("market_age_sec"), 0.0))
            api_errors_window = _safe_int(ops.get("api_errors_window"), _safe_int(ops.get("api_err"), 0))
            status_ts = str(header.get("ts", "") or "")
            with self._proc_lock:
                proc_running = bool(self.process and self.process.poll() is None)
                proc_op_busy = bool(self._proc_op_busy)
            if status_ts and status_ts == self._watchdog_last_status_ts:
                self._watchdog_same_status_count += 1
            else:
                self._watchdog_same_status_count = 0
                self._watchdog_last_status_ts = status_ts

            # Watchdog auto-restart is enabled only for LIVE trading mode.
            if mode != "live" or (not running) or (not proc_running) or proc_op_busy:
                self._watchdog_busy_count = 0
                return

            # Grace period after start/restart.
            if self._last_bot_start_ts > 0 and (time.time() - self._last_bot_start_ts) < 120.0:
                self._watchdog_busy_count = 0
                return

            # Conservative stale detection: avoid false restarts on slow API/network periods.
            stale_limit = max(220.0, (cycle_ms / 1000.0) * 14.0, market_age_sec * 1.8)
            stale_busy = (
                running
                and event == "cycle_busy"
                and status_age >= stale_limit
                and self._watchdog_same_status_count >= 8
                and api_errors_window <= 1
            )
            if stale_busy:
                self._watchdog_busy_count += 1
            else:
                self._watchdog_busy_count = 0

            now_ts = time.time()
            if self._watchdog_busy_count < 5:
                return
            if (now_ts - self._watchdog_last_restart_ts) < 360.0:
                return
            if (now_ts - self._watchdog_last_warn_ts) >= 60.0:
                self._append_log(
                    f"[GUI-WEB] Watchdog: подтвержден длительный cycle_busy (age={status_age:.1f}s, limit={stale_limit:.1f}s). Выполняю перезапуск."
                )
                self._watchdog_last_warn_ts = now_ts
            self._watchdog_last_restart_ts = now_ts
            self._watchdog_busy_count = 0
            self._emit_toast("Watchdog: авто-перезапуск бота", "warn")
            self.restart_bot()
        except Exception:
            return


def main() -> int:
    _hide_windows_console()
    app = QApplication(sys.argv)
    win = BotWebWindow()
    win.showMaximized()
    return app.exec()


if __name__ == "__main__":
    raise SystemExit(main())
