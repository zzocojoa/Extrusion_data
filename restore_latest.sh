#!/bin/bash

PROJECT_DIR="/mnt/c/Users/user/Documents/GitHub/Extrusion_data"
CONTAINER="supabase_db_Extrusion_data"
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
BACKUP_FILE="$BACKUP_DIR/latest.sql"

if [ ! -f "$BACKUP_FILE" ]; then
  echo "[!] No latest backup found: $BACKUP_FILE"
  exit 1
fi

echo "[*] Restoring database from: $BACKUP_FILE"
echo "[*] Resetting public schema..."
docker exec -e PGPASSWORD="$DB_PASSWORD" "$CONTAINER" psql -U postgres -d postgres -c "DROP SCHEMA IF EXISTS public CASCADE; CREATE SCHEMA public;"

echo "[*] Importing data..."
docker exec -i -e PGPASSWORD="$DB_PASSWORD" "$CONTAINER" psql -U postgres -d postgres < "$BACKUP_FILE"

if [ $? -eq 0 ]; then
  echo "[OK] Restore completed successfully."
else
  echo "[!] Restore failed."
  exit 1
fi
