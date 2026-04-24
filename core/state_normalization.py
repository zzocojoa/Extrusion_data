import os
from typing import Any


def normalize_legacy_key(key: str) -> str:
    if "|" not in key:
        return key
    return key.split("|", 1)[0]


def expand_processed_key(key: str) -> set[str]:
    normalized = str(key).strip()
    if normalized == "":
        return set()

    expanded: set[str] = {normalized}
    legacy_key = normalize_legacy_key(normalized)
    if legacy_key != normalized:
        expanded.add(legacy_key)
    return expanded


def normalize_processed_set(values: Any) -> set[str]:
    if not isinstance(values, (list, set, tuple)):
        return set()

    processed: set[str] = set()
    for value in values:
        key = str(value).strip()
        if key != "":
            processed.add(key)
    return processed


def normalize_resume_map(values: Any) -> dict[str, int]:
    if not isinstance(values, dict):
        return {}

    resume: dict[str, int] = {}
    for raw_key, raw_value in values.items():
        key = str(raw_key).strip()
        if key == "":
            continue
        try:
            offset = int(raw_value)
        except Exception:
            continue
        if offset > 0:
            resume[key] = offset
    return resume


def normalize_non_empty_string(value: Any) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    if normalized == "":
        return None
    return normalized


def normalize_positive_float(value: Any) -> float | None:
    try:
        normalized = float(value)
    except Exception:
        return None
    if normalized <= 0:
        return None
    return normalized


def normalize_positive_int(value: Any) -> int | None:
    try:
        normalized = int(value)
    except Exception:
        return None
    if normalized <= 0:
        return None
    return normalized


def normalize_string_map(values: Any) -> dict[str, str] | None:
    if not isinstance(values, dict):
        return None

    normalized_values: dict[str, str] = {}
    for raw_key, raw_value in values.items():
        key = normalize_non_empty_string(raw_key)
        value = normalize_non_empty_string(raw_value)
        if key is None or value is None:
            continue
        normalized_values[key] = value
    return normalized_values


def split_legacy_key(legacy_key: str) -> tuple[str, str]:
    normalized = legacy_key.replace("\\", "/")
    if "/" not in normalized:
        return "", normalized
    folder, filename = normalized.rsplit("/", 1)
    return folder, filename


def build_legacy_file_key(folder: str, filename: str) -> str:
    return f"{folder}/{filename}"


def build_file_state_key(folder: str, filename: str, file_path: str) -> str:
    legacy_key = build_legacy_file_key(folder, filename)
    if file_path.strip() == "":
        return legacy_key
    try:
        stat_result = os.stat(file_path)
    except OSError:
        return legacy_key
    return f"{legacy_key}|size={stat_result.st_size}|mtime_ns={stat_result.st_mtime_ns}"
