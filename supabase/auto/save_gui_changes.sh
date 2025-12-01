#!/bin/bash

# === 설정 ===
# 127.0.0.1:25432 (실제 데이터가 있는 DB)
EXTERNAL_DB_URL="postgresql://postgres:aldmc6061@127.0.0.1:25432/postgres"

echo "[*] GUI에서 변경한 내용을 감지하는 중..."

# 현재 시간으로 파일명 생성
TIMESTAMP=$(date +%Y%m%d%H%M%S)

# DB 상태를 읽어서 마이그레이션 파일 생성 (Diff)
supabase db diff \
  --db-url "$EXTERNAL_DB_URL" \
  --schema public \
  -f "gui_update_${TIMESTAMP}"

echo "[✔] 변경 사항이 supabase/migrations/ 폴더에 저장되었습니다!"
