#!/bin/bash

# === 설정 ===
# CLI가 외부에서 접속하므로 127.0.0.1:25432 사용
EXTERNAL_DB_URL="postgresql://postgres:aldmc6061@127.0.0.1:25432/postgres"

echo "[*] Checking for schema changes..."

# 1. 변경 사항 감지 및 마이그레이션 파일 생성
TIMESTAMP=$(date +%Y%m%d%H%M%S)
supabase db diff \
  --db-url "$EXTERNAL_DB_URL" \
  --schema public \
  -f "auto_diff_${TIMESTAMP}"

# 파일 생성 여부 확인 (diff 명령 결과가 없으면 파일 생성 안됨)
NEW_FILE=$(ls supabase/migrations/${TIMESTAMP}_auto_diff.sql 2>/dev/null)

if [ -z "$NEW_FILE" ]; then
  echo "[!] No schema changes detected."
else
  echo "[*] New migration created: $NEW_FILE"
  echo "[*] Pushing changes to database..."

  # 2. 변경 사항 DB에 확정 적용
  supabase db push --db-url "$EXTERNAL_DB_URL"

  if [ $? -eq 0 ]; then
    echo "[✔] Migration applied successfully."
  else
    echo "[✗] Migration failed. Check the logs."
    exit 1
  fi
fi
