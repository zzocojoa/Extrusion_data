#!/bin/bash

# 에러 발생 시 중단하지 않음 (직접 처리)
set +e

# === 설정 ===
# 프로젝트 루트 경로
PROJECT_DIR="/mnt/c/Users/user/Documents/GitHub/Extrusion_data"
BACKUP_DIR="$PROJECT_DIR/backups"
CONTAINER_NAME="supabase_db_Extrusion_data"
ENV_FILE="$PROJECT_DIR/.env"

# .env 파일 로드
if [ -f "$ENV_FILE" ]; then
    # 주석 제거 및 export
    export $(grep -v '^#' "$ENV_FILE" | xargs)
else
    echo "❌ .env file not found at $ENV_FILE"
    exit 1
fi

# 비밀번호 확인
if [ -z "$DB_PASSWORD" ]; then
    echo "❌ DB_PASSWORD not set in .env"
    exit 1
fi

# 컨테이너 내부에서 접속하므로 localhost:5432 사용
# PGPASSWORD 환경변수를 사용하여 비밀번호 노출 방지 (docker exec 내부는 환경변수 전달이 까다로울 수 있음)
# 가장 안전한 방법은 pg_dump 호출 시 연결 문자열에 비밀번호를 포함하되, 스크립트 변수로 처리
INTERNAL_DB_URL="postgresql://postgres:${DB_PASSWORD}@localhost:5432/postgres"

# === 실행 ===
mkdir -p "$BACKUP_DIR"
DATE=$(date +%Y%m%d_%H%M%S)
FILENAME="backup_${DATE}.sql"
FILEPATH="$BACKUP_DIR/$FILENAME"

echo "[*] Starting backup: $FILENAME"

# Docker 내부 명령어로 덤프 실행
# 주의: 커맨드라인에 비밀번호가 노출될 수 있음. 
# 더 안전한 방법은 docker exec -e PGPASSWORD=... 를 쓰는 것이지만, 
# 여기서는 기존 방식(URL 포함)을 유지하되 변수화함.
docker exec "$CONTAINER_NAME" pg_dump --clean --if-exists -n public "$INTERNAL_DB_URL" > "$FILEPATH" 2> "$FILEPATH.log"

EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
  echo "[✔] Backup Success: $FILEPATH"
  # 최신 백업본 링크 업데이트
  cp "$FILEPATH" "$BACKUP_DIR/latest.sql"
  rm -f "$FILEPATH.log"

  # === USB 백업 복사 (E: 드라이브) ===
  USB_MOUNT="/mnt/e"
  USB_BACKUP_DIR="$USB_MOUNT/backups"

  # /mnt/e 마운트 확인 (간단히 디렉토리 존재 여부 및 쓰기 권한 확인)
  if [ -d "$USB_MOUNT" ] && [ -w "$USB_MOUNT" ]; then
    echo "[*] USB Drive detected at $USB_MOUNT"
    mkdir -p "$USB_BACKUP_DIR"
    cp "$FILEPATH" "$USB_BACKUP_DIR/"
    if [ $? -eq 0 ]; then
      echo "[✔] Copied to USB: $USB_BACKUP_DIR/$FILENAME"
    else
      echo "[!] Failed to copy to USB"
    fi
  else
    echo "[!] USB Drive not mounted or not writable at $USB_MOUNT. Skipping USB copy."
  fi
else
  echo "[✗] Backup Failed. Check log:"
  cat "$FILEPATH.log"
  rm -f "$FILEPATH" # 실패한 파일 삭제
  exit 1
fi

