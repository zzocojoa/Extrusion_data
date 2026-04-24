import configparser
import os
import sys
from typing import Dict, Tuple, TypedDict
from urllib.parse import urlparse


APP_DIR_NAME = "ExtrusionUploader"
CONFIG_KEYS = (
    "SUPABASE_URL",
    "SUPABASE_ANON_KEY",
    "EDGE_FUNCTION_URL",
    "UI_LANGUAGE",
    "DB_HOST",
    "DB_PORT",
    "DB_USER",
    "DB_PASSWORD",
    "DB_NAME",
    "WSL_VHDX_PATH",
    "LEGACY_CYCLE_MACHINE_ID",
    "PLC_DIR",
    "TEMP_DIR",
    "SMART_SYNC",
    "AUTO_UPLOAD",
    "RANGE_MODE",
    "CUSTOM_DATE",
    "CUSTOM_DATE_START",
    "CUSTOM_DATE_END",
    "MTIME_LAG_MIN",
    "CHECK_LOCK",
)


class ConfigLoadMetadata(TypedDict):
    config_path: str
    env_path: str
    source_by_key: Dict[str, str]


def get_data_dir() -> str:
    """
    Return the directory under APPDATA/home where app state/config is stored.
    """
    appdata = os.getenv("APPDATA") or os.path.expanduser("~")
    d = os.path.join(appdata, APP_DIR_NAME)
    os.makedirs(d, exist_ok=True)
    return d


def resolve_config_paths() -> Tuple[str, str]:
    """
    Compute script-local and AppData config.ini paths.
    """
    # Script directory where the original config.ini may live
    script_dir = os.path.dirname(os.path.abspath(__file__))
    script_cfg = os.path.join(os.path.dirname(script_dir), "config.ini")

    # AppData directory for user-specific config
    app_dir = get_data_dir()
    app_cfg = os.path.join(app_dir, "config.ini")
    return script_cfg, app_cfg


def _load_env_file(path: str) -> Dict[str, str]:
    values: Dict[str, str] = {}
    if not os.path.exists(path):
        return values

    try:
        with open(path, "r", encoding="utf-8-sig") as f:
            lines = f.readlines()
    except Exception:
        with open(path, "r", encoding="cp949", errors="ignore") as f:
            lines = f.readlines()

    for raw_line in lines:
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :].lstrip()
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if key not in CONFIG_KEYS:
            continue
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
            value = value[1:-1]
        values[key] = value

    return values


def _resolve_env_paths(script_cfg: str) -> tuple[str, ...]:
    root_env = os.path.join(os.path.dirname(script_cfg), ".env")
    if not getattr(sys, "frozen", False):
        return (root_env,)

    exe_env = os.path.join(os.path.dirname(os.path.abspath(sys.executable)), ".env")
    if os.path.normcase(exe_env) == os.path.normcase(root_env):
        return (root_env,)
    return (exe_env, root_env)


def _normalize_origin_host(host: str | None) -> str:
    if not host:
        return ""
    normalized = host.lower()
    if normalized in ("localhost", "127.0.0.1", "::1"):
        return "loopback"
    return normalized


def _normalize_origin_port(scheme: str, port: int | None) -> int | None:
    if scheme == "http" and port in (None, 80):
        return 80
    if scheme == "https" and port in (None, 443):
        return 443
    return port


def _parse_origin(url: str) -> tuple[str, str, int | None]:
    parsed = urlparse(url.strip())
    scheme = parsed.scheme.lower()
    return (
        scheme,
        _normalize_origin_host(parsed.hostname),
        _normalize_origin_port(scheme, parsed.port),
    )


def is_edge_url_origin_mismatch(edge_url: str, supabase_url: str) -> bool:
    """
    edge URL과 Supabase URL의 origin이 다른지 비교한다.
    """
    cleaned_edge = edge_url.strip()
    cleaned_base = supabase_url.strip()
    if not cleaned_edge or not cleaned_base:
        return False

    try:
        edge_origin = _parse_origin(cleaned_edge)
        base_origin = _parse_origin(cleaned_base)
    except Exception:
        return False

    if not edge_origin[0] or not base_origin[0]:
        return False

    return edge_origin != base_origin


def load_config_with_sources(path: str | None) -> tuple[Dict[str, str], str, ConfigLoadMetadata]:
    """
    설정을 로드하고 각 키의 최종 적용 source를 함께 반환한다.
    """
    cfg = configparser.ConfigParser()
    cfg.optionxform = str
    defaults: Dict[str, str] = {
        "SUPABASE_URL": "",
        "SUPABASE_ANON_KEY": "",
        "EDGE_FUNCTION_URL": "",
        "UI_LANGUAGE": "ko",
        "DB_HOST": "127.0.0.1",
        "DB_PORT": "",
        "DB_USER": "postgres",
        "DB_PASSWORD": "",
        "DB_NAME": "postgres",
        "WSL_VHDX_PATH": "",
        "LEGACY_CYCLE_MACHINE_ID": "",
        "PLC_DIR": "PLC_data",
        "TEMP_DIR": "Temperature_data",
        "SMART_SYNC": "true",
        "AUTO_UPLOAD": "false",
        "RANGE_MODE": "yesterday",
        "CUSTOM_DATE": "",
        "CUSTOM_DATE_START": "",
        "CUSTOM_DATE_END": "",
        "MTIME_LAG_MIN": "15",
        "CHECK_LOCK": "true",
    }
    source_by_key: Dict[str, str] = {key: "default" for key in CONFIG_KEYS}
    script_cfg, app_cfg = resolve_config_paths()
    chosen = os.path.abspath(path) if path else app_cfg
    if path is None and not os.path.exists(chosen) and os.path.exists(script_cfg):
        try:
            import shutil

            shutil.copyfile(script_cfg, chosen)
        except Exception:
            pass

    if os.path.exists(chosen):
        try:
            cfg.read(chosen, encoding="utf-8-sig")
        except Exception:
            with open(chosen, "r", encoding="cp949", errors="ignore") as f:
                content = f.read()
            cfg.read_string(content if content.strip().startswith("[") else "[app]\n" + content)
        if "app" in cfg:
            for k, v in cfg["app"].items():
                key = k.upper()
                if key in CONFIG_KEYS:
                    defaults[key] = v
                    source_by_key[key] = "config.ini"

    env_source_path = ""
    for env_path in _resolve_env_paths(script_cfg):
        env_cfg = _load_env_file(env_path)
        if not env_cfg:
            continue
        env_source_path = env_path
        for key, value in env_cfg.items():
            defaults[key] = value
            source_by_key[key] = ".env"

    for key in CONFIG_KEYS:
        env_value = os.environ.get(key)
        if env_value is not None:
            defaults[key] = env_value
            source_by_key[key] = "os.environ"

    metadata: ConfigLoadMetadata = {
        "config_path": chosen,
        "env_path": env_source_path,
        "source_by_key": source_by_key,
    }
    return defaults, chosen, metadata


def load_config(path: str | None = None) -> tuple[Dict[str, str], str]:
    """
    기본값, config.ini, .env, 프로세스 환경변수를 합쳐 설정을 로드한다.

    Returns (config_dict, used_path).
    """
    cfg, chosen, _ = load_config_with_sources(path)
    return cfg, chosen


def canonical_edge_url(supabase_url: str) -> str:
    base = supabase_url.strip()
    if not base:
        return ""
    return f"{base.rstrip('/')}/functions/v1/upload-metrics"


def is_local_edge_override(edge_url: str) -> bool:
    normalized = edge_url.strip()
    if not normalized:
        return False

    parsed = urlparse(normalized)
    if parsed.scheme not in ("http", "https"):
        return False

    return parsed.path.rstrip("/") == "/functions/v1/upload-metrics" and parsed.hostname in ("localhost", "127.0.0.1")


def normalize_edge_url(edge_url: str, supabase_url: str) -> str:
    cleaned = edge_url.strip()
    if not cleaned:
        return ""

    if cleaned == canonical_edge_url(supabase_url):
        return ""

    if is_local_edge_override(cleaned):
        return ""

    return cleaned


def compute_edge_url(cfg: Dict[str, str]) -> str:
    """
    Derive the edge function URL from config.
    Prefers EDGE_FUNCTION_URL; falls back to SUPABASE_URL/functions/v1/upload-metrics.
    """
    base = cfg.get("SUPABASE_URL", "") or ""
    edge = normalize_edge_url(cfg.get("EDGE_FUNCTION_URL", "") or "", base)
    if edge:
        return edge
    return canonical_edge_url(base)


def validate_config(cfg: Dict[str, str]) -> tuple[bool, list[str]]:
    """
    Validate minimal config required for upload.
    Returns (ok, missing_keys).
    """
    missing = [k for k in ("SUPABASE_URL", "SUPABASE_ANON_KEY") if not cfg.get(k)]
    if not compute_edge_url(cfg):
        missing.append("EDGE_FUNCTION_URL")
    return len(missing) == 0, missing


def save_config(values: Dict[str, str], path: str | None = None) -> str:
    """
    Save configuration dict to an INI file under AppData (or given path).
    Returns the path used.
    """
    cfg = configparser.ConfigParser()
    cfg.optionxform = str
    cfg["app"] = {k: str(v) for k, v in values.items()}

    _, app_cfg = resolve_config_paths()
    target = path or app_cfg
    os.makedirs(os.path.dirname(target), exist_ok=True)
    # Use utf-8-sig for better BOM tolerance across editors
    with open(target, "w", encoding="utf-8-sig") as f:
        cfg.write(f)
    return target
