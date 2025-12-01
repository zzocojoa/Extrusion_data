#!/bin/bash
PROJECT_DIR="/mnt/c/Users/user/Documents/GitHub/Extrusion_data"
BACKUP_FILE="$PROJECT_DIR/backups/latest.sql"
CONTAINER="supabase_db_Extrusion_data"
DB_URL="postgresql://postgres:aldmc6061@localhost:5432/postgres"

if [ ! -f "$BACKUP_FILE" ]; then
  echo "[✗] No latest backup found: $BACKUP_FILE"
  exit 1
fi

echo "[*] Restoring database from: $BACKUP_FILE"

# 안전한 초기화: public 스키마만 재생성
docker exec "$CONTAINER" psql "$DB_URL" -c "DROP SCHEMA public CASCADE; CREATE SCHEMA public;"

# 백업 데이터 복구
docker exec -i "$CONTAINER" psql "$DB_URL" < "$BACKUP_FILE"

if [ $? -eq 0 ]; then
  echo "[✔] Restore completed successfully."
else
  echo "[✗] Restore failed."
  exit 1
fi
