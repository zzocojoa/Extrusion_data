import os
import sys

# Add current dir to path
sys.path.append(os.getcwd())

from core import files as core_files
from core.config import load_config

def main():
    cfg, _ = load_config()
    plc_dir = cfg.get('PLC_DIR')
    target_file = 'Factory_Integrated_Log_20251210_000000.csv'
    full_path = os.path.join(plc_dir, target_file)
    
    if os.path.exists(full_path):
        locked = core_files.is_locked(full_path)
        print(f"LOCKED={locked}")
    else:
        print("FILE_NOT_FOUND")

if __name__ == "__main__":
    main()
