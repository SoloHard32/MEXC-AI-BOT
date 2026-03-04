import json
import tempfile
import unittest
from pathlib import Path

try:
    from mexc_bot import _JsonFileLock, _ProcessPidLock, _write_json_atomic
except BaseException as exc:  # pragma: no cover - env dependent
    raise unittest.SkipTest(f"Skip runtime lock tests: cannot import mexc_bot ({exc})")


class RuntimeLockTests(unittest.TestCase):
    def test_process_pid_lock_single_instance(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            lock_path = Path(td) / "bot_process.lock"
            lock_a = _ProcessPidLock(lock_path)
            lock_b = _ProcessPidLock(lock_path)
            self.assertTrue(lock_a.acquire())
            self.assertFalse(lock_b.acquire())
            lock_a.release()
            self.assertTrue(lock_b.acquire())
            lock_b.release()

    def test_json_lock_and_atomic_write(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            data_path = Path(td) / "state.json"
            bak_path = Path(td) / "state.json.bak"
            lock_path = Path(td) / "state.json.lock"

            with _JsonFileLock(lock_path, timeout_sec=0.5, stale_sec=5.0) as lock:
                self.assertTrue(lock._acquired)
                _write_json_atomic(data_path, {"ok": True}, backup_path=bak_path)

            payload = json.loads(data_path.read_text(encoding="utf-8"))
            backup = json.loads(bak_path.read_text(encoding="utf-8"))
            self.assertEqual(payload.get("ok"), True)
            self.assertEqual(backup.get("ok"), True)


if __name__ == "__main__":
    unittest.main()
