#!/usr/bin/env bash
set -euo pipefail
SCRIPT=${SCRIPT:-uploader_cli.py}
NAME=${NAME:-ExtrusionUploaderCli}
ICON=${ICON:-assets/app.ico}

if [ -f "venv/bin/activate" ]; then
  source venv/bin/activate
fi

if [ ! -f "$SCRIPT" ]; then
  echo "Script not found: $SCRIPT" >&2
  exit 1
fi

pyinstaller --onefile --noconsole --name "$NAME" --icon "$ICON" --collect-data certifi --collect-data pandas --collect-data numpy "$SCRIPT"

echo "CLI build complete. Output: dist/${NAME}.exe"
