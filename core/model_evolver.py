from __future__ import annotations

import json
import shutil
from datetime import datetime, timezone
from pathlib import Path


def _safe_float(value: object, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


class ModelEvolver:
    """Keeps model snapshots and best-version metadata based on runtime metrics."""

    def __init__(self, model_path: Path, registry_path: Path, min_trade_gap: int = 8) -> None:
        self.model_path = model_path
        self.registry_path = registry_path
        self.min_trade_gap = max(3, int(min_trade_gap))
        self.trade_counter = 0
        self.best_id = ""
        self.best_score = -1.0
        self._last_restore_trade_counter = -10_000
        self.promote_min_wf_hit_rate = 0.54
        self.promote_min_hit_rate = 0.56
        self._load_registry()

    def _load_registry(self) -> None:
        if not self.registry_path.exists():
            return
        try:
            data = json.loads(self.registry_path.read_text(encoding="utf-8-sig"))
            self.best_id = str(data.get("best_id", "") or "")
            self.best_score = _safe_float(data.get("best_score"), -1.0)
        except Exception:
            return

    def _save_registry(self, versions: list[dict[str, object]]) -> None:
        try:
            self.registry_path.parent.mkdir(parents=True, exist_ok=True)
            payload = {
                "ts_utc": datetime.now(timezone.utc).isoformat(),
                "best_id": self.best_id,
                "best_score": self.best_score,
                "versions": versions[-40:],
            }
            tmp = self.registry_path.with_suffix(self.registry_path.suffix + ".tmp")
            tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
            tmp.replace(self.registry_path)
        except Exception:
            return

    def _read_versions(self) -> list[dict[str, object]]:
        if not self.registry_path.exists():
            return []
        try:
            payload = json.loads(self.registry_path.read_text(encoding="utf-8-sig"))
            rows = payload.get("versions", [])
            return rows if isinstance(rows, list) else []
        except Exception:
            return []

    def _find_best_version(self) -> dict[str, object] | None:
        if not self.best_id:
            return None
        versions = self._read_versions()
        for row in reversed(versions):
            if str(row.get("id", "") or "") == self.best_id:
                return row
        return None

    def _restore_best_if_needed(self, current_score: float, model_health: str) -> dict[str, object]:
        """
        Safe auto-rollback:
        - only when model is degraded or score drops clearly below best;
        - never too often (trade-gap cooldown);
        - restore file(s) only, no core logic changes.
        """
        if self.trade_counter - self._last_restore_trade_counter < self.min_trade_gap:
            return {"restored_best": False}
        health = str(model_health or "").strip().lower()
        if health not in {"degraded", "watch"} and current_score >= (self.best_score - 0.02):
            return {"restored_best": False}
        if self.best_score < 0 or self.best_score <= (current_score + 0.035):
            return {"restored_best": False}
        best = self._find_best_version()
        if not isinstance(best, dict):
            return {"restored_best": False}
        best_path = Path(str(best.get("path", "") or ""))
        if not best_path.exists():
            return {"restored_best": False}
        try:
            self.model_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(best_path, self.model_path)
            # Restore side members if present.
            side_paths = best.get("ensemble_paths", [])
            if isinstance(side_paths, list):
                stem = self.model_path.stem
                suffix = self.model_path.suffix or ".json"
                for tag in ("_m2", "_m3"):
                    dst = self.model_path.with_name(f"{stem}{tag}{suffix}")
                    matched = None
                    for src in side_paths:
                        s = str(src)
                        if s.endswith(f"{tag}{suffix}"):
                            matched = Path(s)
                            break
                    if matched is not None and matched.exists():
                        shutil.copyfile(matched, dst)
            self._last_restore_trade_counter = self.trade_counter
            return {"restored_best": True, "restored_model_id": str(best.get("id", "") or "")}
        except Exception:
            return {"restored_best": False}

    def on_real_trade_closed(self, wf_hit_rate: float, hit_rate: float, model_health: str) -> dict[str, object]:
        self.trade_counter += 1
        score = (0.70 * _safe_float(wf_hit_rate, 0.0)) + (0.30 * _safe_float(hit_rate, 0.0))
        health = str(model_health or "").strip().lower()
        promote_allowed = bool(
            _safe_float(wf_hit_rate, 0.0) >= self.promote_min_wf_hit_rate
            and _safe_float(hit_rate, 0.0) >= self.promote_min_hit_rate
            and health not in {"degraded", "watch", "unknown"}
        )
        restore_result = self._restore_best_if_needed(score, model_health)
        if self.trade_counter % self.min_trade_gap != 0:
            return {"snapshot_created": False, "promote_allowed": promote_allowed, **restore_result}
        if not self.model_path.exists():
            return {"snapshot_created": False, "promote_allowed": promote_allowed, **restore_result}
        versions_dir = self.registry_path.parent / "models"
        versions_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        model_id = f"model_{ts}"
        dst = versions_dir / f"{model_id}.json"
        ensemble_sidecars: list[str] = []
        try:
            shutil.copyfile(self.model_path, dst)
            stem = self.model_path.stem
            suffix = self.model_path.suffix or ".json"
            for tag in ("_m2", "_m3"):
                src_side = self.model_path.with_name(f"{stem}{tag}{suffix}")
                if not src_side.exists():
                    continue
                dst_side = versions_dir / f"{model_id}{tag}{suffix}"
                shutil.copyfile(src_side, dst_side)
                ensemble_sidecars.append(str(dst_side))
        except Exception:
            return {"snapshot_created": False, "promote_allowed": promote_allowed, **restore_result}
        versions: list[dict[str, object]] = []
        if self.registry_path.exists():
            try:
                prev = json.loads(self.registry_path.read_text(encoding="utf-8-sig"))
                versions = prev.get("versions", []) if isinstance(prev.get("versions", []), list) else []
            except Exception:
                versions = []
        versions.append(
            {
                "id": model_id,
                "path": str(dst),
                "ensemble_paths": ensemble_sidecars,
                "score": score,
                "wf_hit_rate": wf_hit_rate,
                "hit_rate": hit_rate,
                "health": model_health,
                "ts_utc": datetime.now(timezone.utc).isoformat(),
            }
        )
        if promote_allowed and score > self.best_score:
            self.best_score = score
            self.best_id = model_id
        self._save_registry(versions)
        return {
            "snapshot_created": True,
            "model_id": model_id,
            "score": score,
            "best_id": self.best_id,
            "promote_allowed": promote_allowed,
            **restore_result,
        }
