import os
import argparse
from typing import List, Tuple, Set

LOG_FILE = "processed_files.log"


def load_lines(path: str) -> List[str]:
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip()]


def detect_locations(name: str) -> Tuple[bool, bool]:
    plc = os.path.exists(os.path.join("PLC_data", name))
    temp = os.path.exists(os.path.join("Temperature_data", name))
    return plc, temp


def normalize_entry(entry: str) -> str:
    # Convert backslashes to forward slashes for consistency
    return entry.replace("\\", "/")


def migrate(entries: List[str]) -> Tuple[List[str], dict]:
    out: List[str] = []
    seen: Set[str] = set()
    stats = {
        "input": len(entries),
        "unchanged": 0,
        "normalized": 0,
        "converted": 0,
        "ambiguous": 0,
        "unresolved": 0,
    }

    for e in entries:
        if not e:
            continue
        if "/" in e or "\\" in e:
            ne = normalize_entry(e)
            if ne not in seen:
                out.append(ne)
                seen.add(ne)
            if ne != e:
                stats["normalized"] += 1
            else:
                stats["unchanged"] += 1
            continue

        # Legacy bare filename: try to map to folder/filename
        plc, temp = detect_locations(e)
        if plc and temp:
            # Record both
            for folder in ("PLC_data", "Temperature_data"):
                ne = f"{folder}/{e}"
                if ne not in seen:
                    out.append(ne)
                    seen.add(ne)
            stats["converted"] += 1
            stats["ambiguous"] += 1
        elif plc:
            ne = f"PLC_data/{e}"
            if ne not in seen:
                out.append(ne)
                seen.add(ne)
            stats["converted"] += 1
        elif temp:
            ne = f"Temperature_data/{e}"
            if ne not in seen:
                out.append(ne)
                seen.add(ne)
            stats["converted"] += 1
        else:
            # Could not determine location; keep as-is
            if e not in seen:
                out.append(e)
                seen.add(e)
            stats["unresolved"] += 1

    return out, stats


def main():
    parser = argparse.ArgumentParser(
        description="Migrate processed_files.log entries to 'folder/filename' format with backup"
    )
    parser.add_argument(
        "--write",
        action="store_true",
        help="Write migrated entries back to processed_files.log (overwrites after creating .bak)",
    )
    parser.add_argument(
        "--log",
        default=LOG_FILE,
        help="Path to processed files log (default: processed_files.log)",
    )
    args = parser.parse_args()

    if not os.path.exists(args.log):
        print(f"No log found at '{args.log}'. Nothing to migrate.")
        return 0

    lines = load_lines(args.log)
    migrated, stats = migrate(lines)

    print("Migration plan:")
    print(f"  Input entries     : {stats['input']}")
    print(f"  Unchanged         : {stats['unchanged']}")
    print(f"  Normalized paths  : {stats['normalized']}")
    print(f"  Converted to new  : {stats['converted']}")
    print(f"  Ambiguous (both)  : {stats['ambiguous']}")
    print(f"  Unresolved (kept) : {stats['unresolved']}")
    print("")

    if args.write:
        # Backup
        bak_path = args.log + ".bak"
        try:
            with open(bak_path, "w", encoding="utf-8") as f:
                f.write("\n".join(lines) + "\n" if lines else "")
            with open(args.log, "w", encoding="utf-8") as f:
                f.write("\n".join(migrated) + "\n" if migrated else "")
            print(f"Wrote backup to '{bak_path}' and updated '{args.log}'.")
        except Exception as e:
            print(f"Failed to write migration: {e}")
            return 1
    else:
        print("Dry-run only. Use --write to apply changes.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

