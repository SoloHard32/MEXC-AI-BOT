from __future__ import annotations

from dataclasses import dataclass
from typing import Callable


@dataclass
class ExecutionResult:
    ok: bool
    event: str


class ExecutionEngine:
    """Execution wrapper to keep cycle logic clean and testable."""

    @staticmethod
    def run(action: Callable[[], bool], *, ok_event: str, fail_event: str) -> ExecutionResult:
        try:
            ok = bool(action())
            return ExecutionResult(ok=ok, event=(ok_event if ok else fail_event))
        except Exception:
            return ExecutionResult(ok=False, event=fail_event)

