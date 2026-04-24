from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.wsl_storage import (
    CRITICAL_AVAILABLE_BYTES,
    CRITICAL_USAGE_RATIO,
    WARNING_AVAILABLE_BYTES,
    WARNING_USAGE_RATIO,
    classify_wsl_storage_state,
    parse_wsl_df_output,
)


def verify_df_parser() -> None:
    sample_output = "\n".join(
        [
            "DISTRO=Ubuntu",
            "Filesystem     1B-blocks      Used Available Use% Mounted on",
            "/dev/sdc       26843545600 8589934592 18253611008 32% /",
        ]
    )
    metrics = parse_wsl_df_output(sample_output)
    assert metrics.distro_name == "Ubuntu"
    assert metrics.mount_path == "/"
    assert metrics.total_bytes == 26843545600
    assert metrics.used_bytes == 8589934592
    assert metrics.available_bytes == 18253611008
    assert round(metrics.usage_ratio, 2) == 0.32


def verify_state_classifier() -> None:
    assert classify_wsl_storage_state(
        WARNING_AVAILABLE_BYTES + 1,
        WARNING_USAGE_RATIO - 0.01,
    ) == "safe"
    assert classify_wsl_storage_state(
        WARNING_AVAILABLE_BYTES,
        WARNING_USAGE_RATIO - 0.01,
    ) == "warning"
    assert classify_wsl_storage_state(
        WARNING_AVAILABLE_BYTES + 1,
        WARNING_USAGE_RATIO,
    ) == "warning"
    assert classify_wsl_storage_state(
        CRITICAL_AVAILABLE_BYTES,
        CRITICAL_USAGE_RATIO - 0.01,
    ) == "critical"
    assert classify_wsl_storage_state(
        CRITICAL_AVAILABLE_BYTES + 1,
        CRITICAL_USAGE_RATIO,
    ) == "critical"


def main() -> int:
    verify_df_parser()
    verify_state_classifier()
    print("WSL storage verification passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
