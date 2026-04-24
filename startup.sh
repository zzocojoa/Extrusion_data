#!/bin/bash

# 에러 발생 시 중단
set -e

PROJECT_DIR="/mnt/c/Users/user/Documents/GitHub/Extrusion_data"
DATA_DRIVE="/mnt/e"  # WSL에서 E드라이브 마운트 포인트 확인 필요 (보통 /mnt/e)
# 혹은 vhdx 마운트 방식에 따라 다를 수 있음. Project_Context.MD에 따르면 "wsl --mount --vhd ... --bare"라고 되어있음.
# bare 마운트 시 /mnt/wsl/... 경로에 생기거나, 직접 마운트해야 함.
# 하지만 기존 스크립트는 그냥 실행했음.
# 여기서는 간단히 프로젝트 디렉토리 존재 여부와 docker 실행 가능 여부만 체크.

echo "[1] 프로젝트 폴더로 이동 중..."
if [ ! -d "$PROJECT_DIR" ]; then
    echo "❌ 프로젝트 디렉토리를 찾을 수 없습니다: $PROJECT_DIR"
    exit 1
fi
cd "$PROJECT_DIR"

echo "[Check] Docker 실행 상태 확인..."
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker가 실행 중이지 않습니다. Docker Desktop을 실행해주세요."
    exit 1
fi

echo "[2] Supabase 실행 중..."
if command -v supabase > /dev/null 2>&1; then
    supabase start || echo "⚠️ Supabase 시작 중 경고 발생 (이미 실행 중일 수 있음)"
else
    echo "⚠️ supabase CLI를 찾지 못했습니다. 기존 로컬 컨테이너를 직접 시작합니다."
    SUPABASE_CONTAINERS=$(docker ps -a --format '{{.Names}}' | grep -E '^(supabase_.*_Extrusion_data|grafana_local)$' || true)
    if [ -z "$SUPABASE_CONTAINERS" ]; then
        echo "❌ supabase CLI가 없고 시작 가능한 로컬 컨테이너도 찾지 못했습니다."
        exit 1
    fi
    echo "$SUPABASE_CONTAINERS" | xargs -r docker start
fi

echo "[3] Grafana 컨테이너 시작 중..."
# 컨테이너 존재 여부 확인
if docker ps -a --format '{{.Names}}' | grep -q "^grafana_local$"; then
    docker start grafana_local
else
    echo "❌ 'grafana_local' 컨테이너가 존재하지 않습니다."
fi

echo "[3-1] Edge Runtime 컨테이너 시작 중..."
# 컨테이너 존재 여부 확인 및 시작
if docker ps -a --format '{{.Names}}' | grep -q "^supabase_edge_runtime_Extrusion_data$"; then
    docker start supabase_edge_runtime_Extrusion_data
else
    echo "❌ 'supabase_edge_runtime_Extrusion_data' 컨테이너가 존재하지 않습니다."
fi

echo "[4] 백업 스케줄러(Cron) 상태 확인..."
if ! service cron status | grep -q "running"; then
  echo " -> Cron 서비스가 꺼져 있어 시작합니다. (sudo 권한 필요)"
  sudo service cron start
else
  echo " -> Cron 서비스가 이미 실행 중입니다."
fi

echo "✅ 모든 시스템이 정상 가동되었습니다!"
