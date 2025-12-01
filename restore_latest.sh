#!/bin/bash

# === 설정 ===
PROJECT_DIR="/mnt/c/Users/user/Documents/GitHub/Extrusion_data"
BACKUP_FILE="$PROJECT_DIR/backups/latest.sql"

# [중요] 수정된 컨테이너 이름 적용됨
CONTAINER="supabase_db_Extrusion_data"

# 컨테이너 내부 접속용 URL (변경 불필요)
DB_URL="postgresql://postgres:aldmc6061@localhost:5432/postgres"

# === 실행 로직 ===
if [ ! -f "$BACKUP_FILE" ]; then
  echo "[✗] No latest backup found: $BACKUP_FILE"
  exit 1
fi

echo "[*] Restoring database from: $BACKUP_FILE"

# 1. 안전한 초기화: 기존 public 스키마를 삭제하고 다시 만듦 (충돌 방지)
echo "[*] Resetting public schema..."
docker exec "$CONTAINER" psql "$DB_URL" -c "DROP SCHEMA IF EXISTS public CASCADE; CREATE SCHEMA public;"

# 2. 백업 데이터 복구 (Restore)
echo "[*] Importing data..."
docker exec -i "$CONTAINER" psql "$DB_URL" < "$BACKUP_FILE"

if [ $? -eq 0 ]; then
  echo "[✔] Restore completed successfully."
else
  echo "[✗] Restore failed."
  exit 1
fi
