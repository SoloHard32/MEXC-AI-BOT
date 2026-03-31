from __future__ import annotations

import os
from pathlib import Path


ENV_FILENAME = ".env"
LOCAL_ENV_FILENAME = ".env.local"


def env_file_paths(base_dir: Path) -> tuple[Path, Path]:
    root = Path(base_dir)
    return root / ENV_FILENAME, root / LOCAL_ENV_FILENAME


def read_env_file(path: Path) -> dict[str, str]:
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


def read_env_layers(base_dir: Path) -> dict[str, str]:
    shared_path, local_path = env_file_paths(base_dir)
    merged = read_env_file(shared_path)
    merged.update(read_env_file(local_path))
    return merged


def load_env_layers(base_dir: Path, *, override: bool = False) -> dict[str, str]:
    merged = read_env_layers(base_dir)
    for k, v in merged.items():
        if override or (k not in os.environ):
            os.environ[k] = v
    return merged


def is_secret_env_key(key: str) -> bool:
    raw = str(key or "").strip().upper()
    if not raw:
        return False
    if raw in {"MEXC_API_KEY", "MEXC_API_SECRET", "OPENAI_API_KEY", "GEMINI_API_KEY"}:
        return True
    return raw.endswith(("_API_KEY", "_API_SECRET", "_TOKEN")) or ("SECRET" in raw)


def _upsert_single_env_file(path: Path, values: dict[str, str]) -> None:
    lines: list[str] = []
    if path.exists():
        lines = path.read_text(encoding="utf-8-sig", errors="replace").splitlines()
    out: list[str] = []
    seen: set[str] = set()
    for ln in lines:
        s = ln.strip()
        if not s or s.startswith("#") or "=" not in s:
            out.append(ln)
            continue
        k, _ = s.split("=", 1)
        env_key = str(k or "").strip()
        if env_key in values:
            out.append(f"{env_key}={values[env_key]}")
            seen.add(env_key)
        else:
            out.append(ln)
    for k, v in values.items():
        if k not in seen:
            out.append(f"{k}={v}")
    path.write_text("\n".join(out).rstrip() + "\n", encoding="utf-8")


def upsert_env_layers(base_dir: Path, values: dict[str, str]) -> None:
    shared_path, local_path = env_file_paths(base_dir)
    shared_values: dict[str, str] = {}
    local_values: dict[str, str] = {}
    for k, v in values.items():
        if is_secret_env_key(k):
            local_values[k] = v
        else:
            shared_values[k] = v
    if shared_values:
        _upsert_single_env_file(shared_path, shared_values)
    if local_values:
        if not local_path.exists():
            local_path.write_text(
                "# Local secrets and machine-specific overrides.\n",
                encoding="utf-8",
            )
        _upsert_single_env_file(local_path, local_values)
