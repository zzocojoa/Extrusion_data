#!/bin/bash

set +e

PROJECT_DIR="/mnt/c/Users/user/Documents/GitHub/Extrusion_data"
CONTAINER_NAME="supabase_db_Extrusion_data"
ENV_FILE="$PROJECT_DIR/.env"

if [ -f "$ENV_FILE" ]; then
    set -a
    . "$ENV_FILE"
    set +a
else
    echo "[!] .env file not found at $ENV_FILE"
    exit 1
fi

if [ -z "$DB_PASSWORD" ]; then
    echo "[!] DB_PASSWORD not set in .env"
    exit 1
fi

BACKUP_DIR="${BACKUP_DIR:-$PROJECT_DIR/backups}"
INTERNAL_DB_URL="postgresql://postgres:${DB_PASSWORD}@localhost:5432/postgres"

mkdir -p "$BACKUP_DIR"
DATE=$(date +%Y%m%d_%H%M%S)
FILENAME="backup_${DATE}.sql"
FILEPATH="$BACKUP_DIR/$FILENAME"

echo "[*] Starting backup: $FILENAME"
docker exec "$CONTAINER_NAME" pg_dump --clean --if-exists -n public "$INTERNAL_DB_URL" > "$FILEPATH" 2> "$FILEPATH.log"

EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
  echo "[OK] Backup success: $FILEPATH"
  cp "$FILEPATH" "$BACKUP_DIR/latest.sql"
  rm -f "$FILEPATH.log"

  BACKUP_MIRROR_DIR="${BACKUP_MIRROR_DIR:-}"
  if [ -n "$BACKUP_MIRROR_DIR" ]; then
    echo "[*] Copying mirror backup to $BACKUP_MIRROR_DIR"
    mkdir -p "$BACKUP_MIRROR_DIR"
    cp "$FILEPATH" "$BACKUP_MIRROR_DIR/"
    if [ $? -eq 0 ]; then
      echo "[OK] Copied mirror backup: $BACKUP_MIRROR_DIR/$FILENAME"
    else
      echo "[!] Failed to copy mirror backup"
    fi
  fi
else
  echo "[!] Backup failed. Check log:"
  cat "$FILEPATH.log"
  rm -f "$FILEPATH"
  exit 1
fi
