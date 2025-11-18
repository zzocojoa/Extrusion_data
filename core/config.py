import configparser
import os
from typing import Tuple, Dict


APP_DIR_NAME = "ExtrusionUploader"


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


def load_config(path: str | None = None) -> tuple[Dict[str, str], str]:
    """
    Load configuration, merging defaults, environment, and INI file.

    Returns (config_dict, used_path).
    """
    cfg = configparser.ConfigParser()
    cfg.optionxform = str
    defaults: Dict[str, str] = {
        "SUPABASE_URL": os.environ.get("SUPABASE_URL", ""),
        "SUPABASE_ANON_KEY": os.environ.get("SUPABASE_ANON_KEY", ""),
        "EDGE_FUNCTION_URL": os.environ.get("EDGE_FUNCTION_URL", ""),
        "PLC_DIR": "PLC_data",
        "TEMP_DIR": "Temperature_data",
        "RANGE_MODE": "yesterday",
        "CUSTOM_DATE": "",
        "MTIME_LAG_MIN": "15",
        "CHECK_LOCK": "true",
    }
    script_cfg, app_cfg = resolve_config_paths()

    # Always prefer AppData config; migrate from script config if needed
    chosen = path or app_cfg
    if not os.path.exists(chosen) and os.path.exists(script_cfg):
        try:
            import shutil

            shutil.copyfile(script_cfg, chosen)
        except Exception:
            pass

    if os.path.exists(chosen):
        # Tolerate BOM (utf-8-sig) and legacy encodings (cp949)
        try:
            cfg.read(chosen, encoding="utf-8-sig")
        except Exception:
            with open(chosen, "r", encoding="cp949", errors="ignore") as f:
                content = f.read()
            cfg.read_string(content if content.strip().startswith("[") else "[app]\n" + content)
        if "app" in cfg:
            for k, v in cfg["app"].items():
                defaults[k.upper()] = v

    return defaults, chosen


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

