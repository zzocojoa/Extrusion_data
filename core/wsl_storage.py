from __future__ import annotations

import os
import re
import shutil
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Final, Literal, Mapping


StorageState = Literal["safe", "warning", "critical", "error", "unavailable"]
StorageSource = Literal["config_override", "registry"]

WARNING_USAGE_RATIO: Final[float] = 0.80
CRITICAL_USAGE_RATIO: Final[float] = 0.90
WARNING_AVAILABLE_BYTES: Final[int] = 20 * 1024 * 1024 * 1024
CRITICAL_AVAILABLE_BYTES: Final[int] = 10 * 1024 * 1024 * 1024
WSL_REGISTRY_PATH: Final[str] = r"Software\Microsoft\Windows\CurrentVersion\Lxss"
WSL_VHDX_KEY: Final[str] = "WSL_VHDX_PATH"


@dataclass(frozen=True)
class WslStorageIssue:
    code: str
    message: str


@dataclass(frozen=True)
class WslGuestStorageMetrics:
    distro_name: str
    mount_path: str
    total_bytes: int
    used_bytes: int
    available_bytes: int
    usage_ratio: float


@dataclass(frozen=True)
class WslHostStorageMetrics:
    vhdx_path: Path
    file_size_bytes: int
    drive_free_bytes: int
    drive_total_bytes: int
    source: StorageSource


@dataclass(frozen=True)
class WslStorageSnapshot:
    state: StorageState
    guest_metrics: WslGuestStorageMetrics | None
    host_metrics: WslHostStorageMetrics | None
    issues: tuple[WslStorageIssue, ...]
    is_partial: bool
    collected_at: datetime


def parse_wsl_df_output(output_text: str) -> WslGuestStorageMetrics:
    lines = [line.strip() for line in output_text.splitlines() if line.strip()]
    distro_line = next((line for line in lines if line.startswith("DISTRO=")), None)
    if distro_line is None:
        raise ValueError("DISTRO line is missing")

    distro_name = distro_line.split("=", 1)[1].strip()
    if distro_name == "":
        raise ValueError("distro name is empty")

    filesystem_header_index = next(
        (index for index, line in enumerate(lines) if line.startswith("Filesystem")),
        None,
    )
    if filesystem_header_index is None:
        raise ValueError("df header line is missing")
    if filesystem_header_index + 1 >= len(lines):
        raise ValueError("df data line is missing")

    data_line = lines[filesystem_header_index + 1]
    columns = re.split(r"\s+", data_line)
    if len(columns) < 6:
        raise ValueError(f"df data line is malformed: {data_line}")

    total_bytes = int(columns[1])
    used_bytes = int(columns[2])
    available_bytes = int(columns[3])
    usage_ratio = _parse_usage_ratio(columns[4], used_bytes, total_bytes)
    mount_path = columns[5]

    return WslGuestStorageMetrics(
        distro_name=distro_name,
        mount_path=mount_path,
        total_bytes=total_bytes,
        used_bytes=used_bytes,
        available_bytes=available_bytes,
        usage_ratio=usage_ratio,
    )


def classify_wsl_storage_state(
    available_bytes: int,
    usage_ratio: float,
) -> StorageState:
    if available_bytes <= CRITICAL_AVAILABLE_BYTES or usage_ratio >= CRITICAL_USAGE_RATIO:
        return "critical"
    if available_bytes <= WARNING_AVAILABLE_BYTES or usage_ratio >= WARNING_USAGE_RATIO:
        return "warning"
    return "safe"


def collect_wsl_storage_snapshot(config: Mapping[str, str]) -> WslStorageSnapshot:
    guest_metrics, guest_issue, guest_state = _collect_guest_metrics()
    collected_at = datetime.now(timezone.utc)
    if guest_metrics is None:
        issues = tuple(issue for issue in (guest_issue,) if issue is not None)
        return WslStorageSnapshot(
            state=guest_state,
            guest_metrics=None,
            host_metrics=None,
            issues=issues,
            is_partial=False,
            collected_at=collected_at,
        )

    host_metrics, host_issue = _collect_host_metrics(config)
    issues = tuple(issue for issue in (guest_issue, host_issue) if issue is not None)
    return WslStorageSnapshot(
        state=classify_wsl_storage_state(
            guest_metrics.available_bytes,
            guest_metrics.usage_ratio,
        ),
        guest_metrics=guest_metrics,
        host_metrics=host_metrics,
        issues=issues,
        is_partial=host_metrics is None,
        collected_at=collected_at,
    )


def _parse_usage_ratio(raw_value: str, used_bytes: int, total_bytes: int) -> float:
    normalized_value = raw_value.strip()
    if normalized_value.endswith("%"):
        return float(normalized_value[:-1]) / 100.0
    if total_bytes <= 0:
        return 0.0
    return used_bytes / total_bytes


def _collect_guest_metrics() -> tuple[WslGuestStorageMetrics | None, WslStorageIssue | None, StorageState]:
    try:
        completed = subprocess.run(
            [
                "wsl.exe",
                "sh",
                "-lc",
                "printf 'DISTRO=%s\\n' \"$WSL_DISTRO_NAME\"; df -B1 -P /",
            ],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=10,
            check=False,
        )
    except FileNotFoundError:
        return None, WslStorageIssue("wsl_missing", "WSL executable was not found."), "unavailable"
    except subprocess.TimeoutExpired:
        return None, WslStorageIssue("wsl_timeout", "Timed out while reading WSL storage."), "error"
    except Exception as error:
        return None, WslStorageIssue("wsl_read_failed", f"Failed to read WSL storage: {error}"), "error"

    if completed.returncode != 0:
        detail = completed.stderr.strip() or completed.stdout.strip() or "Unknown WSL error"
        return None, WslStorageIssue("wsl_command_failed", detail), "error"

    try:
        return parse_wsl_df_output(completed.stdout), None, "safe"
    except Exception as error:
        return None, WslStorageIssue("wsl_df_parse_failed", f"Failed to parse df output: {error}"), "error"


def _collect_host_metrics(
    config: Mapping[str, str],
) -> tuple[WslHostStorageMetrics | None, WslStorageIssue | None]:
    override_path = config.get(WSL_VHDX_KEY, "").strip()
    if override_path == "":
        return None, WslStorageIssue(
            "vhdx_path_not_configured",
            "Set WSL_VHDX_PATH to read host-side VHDX metrics.",
        )
    return _load_host_metrics_from_override(override_path)


def _load_host_metrics_from_override(
    raw_path: str,
) -> tuple[WslHostStorageMetrics | None, WslStorageIssue | None]:
    vhdx_path = Path(os.path.expandvars(raw_path)).expanduser()
    if not vhdx_path.is_file():
        return None, WslStorageIssue(
            "vhdx_override_not_found",
            f"Configured VHDX path was not found: {vhdx_path}",
        )
    return _load_host_metrics(vhdx_path, "config_override")


def _load_host_metrics(
    vhdx_path: Path,
    source: StorageSource,
) -> tuple[WslHostStorageMetrics | None, WslStorageIssue | None]:
    try:
        resolved_path = vhdx_path.resolve()
        file_size_bytes = resolved_path.stat().st_size
        drive_usage = shutil.disk_usage(str(resolved_path.anchor))
    except Exception as error:
        return None, WslStorageIssue(
            "host_metrics_failed",
            f"Failed to read host storage metrics: {error}",
        )

    return (
        WslHostStorageMetrics(
            vhdx_path=resolved_path,
            file_size_bytes=file_size_bytes,
            drive_free_bytes=drive_usage.free,
            drive_total_bytes=drive_usage.total,
            source=source,
        ),
        None,
    )
