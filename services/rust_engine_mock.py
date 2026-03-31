from __future__ import annotations

import os

from rust_engine_service import main as service_main


def main() -> int:
    # Compatibility launcher for old command name.
    os.environ.setdefault("RUST_ENGINE_ENABLE_EXCHANGE", "false")
    return service_main()


if __name__ == "__main__":
    raise SystemExit(main())
