#!/usr/bin/env bash
set -euo pipefail
SPEC=${SPEC:-ExtrusionUploader.spec}
ICON=${ICON:-assets/app.ico}

# Activate venv if present
if [ -f "venv/bin/activate" ]; then
  source venv/bin/activate
fi

if [ ! -f "$SPEC" ]; then
  echo "Spec file not found: $SPEC" >&2
  exit 1
fi

# If a .spec is provided, avoid extra options (PyInstaller rejects --icon with spec)
if echo "$SPEC" | grep -qi '\.spec$'; then
  pyinstaller --clean "$SPEC"
else
  pyinstaller --clean --onefile --noconsole --name ExtrusionUploader --icon "$ICON" "$SPEC"
fi

echo "GUI build complete. Output: dist/ExtrusionUploader.exe"
