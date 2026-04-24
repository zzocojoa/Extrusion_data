#!/bin/bash

# 오류가 발생하면 즉시 중단한다.
set -euo pipefail

PROJECT_DIR="/mnt/c/Users/user/Documents/GitHub/Extrusion_data"
SUPABASE_EDGE_RUNTIME_CONTAINER="supabase_edge_runtime_Extrusion_data"
SUPABASE_DB_CONTAINER="supabase_db_Extrusion_data"
GRAFANA_CONTAINER_NAME="grafana_local"
GRAFANA_IMAGE="grafana/grafana"
GRAFANA_HOST_PORT="3001"
GRAFANA_CONTAINER_PORT="3000"
GRAFANA_DATA_DIR_PRIMARY="${PROJECT_DIR}/data/grafana_data"
GRAFANA_DATA_DIR_FALLBACK="${PROJECT_DIR}/grafana_data"
GRAFANA_PROVISIONING_DIR="${PROJECT_DIR}/grafana/provisioning"
GRAFANA_DASHBOARDS_DIR="${PROJECT_DIR}/grafana/dashboards"
GRAFANA_VIEW_MIGRATION_PATH="${PROJECT_DIR}/supabase/migrations/20260424000001_create_view_grafana_all_metrics_long.sql"

print_error() {
    local detail
    detail="$1"
    echo "ERROR: ${detail}"
}

ensure_project_dir() {
    echo "[1] 프로젝트 폴더로 이동 중..."
    if [ ! -d "${PROJECT_DIR}" ]; then
        print_error "프로젝트 디렉터리를 찾지 못했습니다. path=${PROJECT_DIR}"
        return 1
    fi
    cd "${PROJECT_DIR}"
}

ensure_docker_ready() {
    echo "[Check] Docker 실행 상태 확인..."
    if ! docker info > /dev/null 2>&1; then
        print_error "Docker Desktop이 실행 중인지 확인해 주세요."
        return 1
    fi
}

resolve_grafana_data_dir() {
    local primary_db_path
    local fallback_db_path
    local primary_db_mtime
    local fallback_db_mtime
    primary_db_path="${GRAFANA_DATA_DIR_PRIMARY}/grafana.db"
    fallback_db_path="${GRAFANA_DATA_DIR_FALLBACK}/grafana.db"

    if [ -f "${primary_db_path}" ] && [ ! -f "${fallback_db_path}" ]; then
        printf '%s\n' "${GRAFANA_DATA_DIR_PRIMARY}"
        return 0
    fi
    if [ ! -f "${primary_db_path}" ] && [ -f "${fallback_db_path}" ]; then
        printf '%s\n' "${GRAFANA_DATA_DIR_FALLBACK}"
        return 0
    fi
    if [ -f "${primary_db_path}" ] && [ -f "${fallback_db_path}" ]; then
        primary_db_mtime="$(stat -c '%Y' "${primary_db_path}")"
        fallback_db_mtime="$(stat -c '%Y' "${fallback_db_path}")"
        if [ "${primary_db_mtime}" -ge "${fallback_db_mtime}" ]; then
            printf '%s\n' "${GRAFANA_DATA_DIR_PRIMARY}"
            return 0
        fi
        printf '%s\n' "${GRAFANA_DATA_DIR_FALLBACK}"
        return 0
    fi
    if [ -d "${GRAFANA_DATA_DIR_PRIMARY}" ]; then
        printf '%s\n' "${GRAFANA_DATA_DIR_PRIMARY}"
        return 0
    fi
    if [ -d "${GRAFANA_DATA_DIR_FALLBACK}" ]; then
        printf '%s\n' "${GRAFANA_DATA_DIR_FALLBACK}"
        return 0
    fi
    mkdir -p "${GRAFANA_DATA_DIR_PRIMARY}"
    printf '%s\n' "${GRAFANA_DATA_DIR_PRIMARY}"
}

ensure_grafana_provisioning_dirs() {
    if [ ! -d "${GRAFANA_PROVISIONING_DIR}/datasources" ]; then
        print_error "Grafana datasource provisioning 경로를 찾지 못했습니다. path=${GRAFANA_PROVISIONING_DIR}/datasources"
        return 1
    fi
    if [ ! -d "${GRAFANA_PROVISIONING_DIR}/dashboards" ]; then
        print_error "Grafana dashboard provisioning 경로를 찾지 못했습니다. path=${GRAFANA_PROVISIONING_DIR}/dashboards"
        return 1
    fi
    if [ ! -d "${GRAFANA_DASHBOARDS_DIR}" ]; then
        print_error "Grafana dashboard 파일 경로를 찾지 못했습니다. path=${GRAFANA_DASHBOARDS_DIR}"
        return 1
    fi
}

resolve_grafana_network() {
    local grafana_network
    grafana_network="$(docker inspect "${SUPABASE_DB_CONTAINER}" --format '{{range $name, $_ := .NetworkSettings.Networks}}{{printf "%s\n" $name}}{{end}}' 2>/dev/null | head -n 1 || true)"
    if [ -z "${grafana_network}" ]; then
        print_error "Grafana 네트워크를 확인하지 못했습니다. base_container=${SUPABASE_DB_CONTAINER}"
        return 1
    fi
    printf '%s\n' "${grafana_network}"
}

does_container_exist() {
    local container_name
    container_name="$1"
    docker ps -a --format '{{.Names}}' | grep -q "^${container_name}$"
}

is_container_running() {
    local container_name
    container_name="$1"
    docker ps --format '{{.Names}}' | grep -q "^${container_name}$"
}

does_container_mount_destination() {
    local container_name
    local destination_path
    container_name="$1"
    destination_path="$2"
    docker inspect "${container_name}" --format '{{range .Mounts}}{{println .Destination}}{{end}}' 2>/dev/null | grep -q "^${destination_path}$"
}

does_container_publish_port() {
    local container_name
    local host_port
    local container_port
    local published_ports
    container_name="$1"
    host_port="$2"
    container_port="$3"
    published_ports="$(docker inspect "${container_name}" --format "{{with index .HostConfig.PortBindings \"${container_port}/tcp\"}}{{range .}}{{println .HostPort}}{{end}}{{end}}" 2>/dev/null || true)"
    printf '%s\n' "${published_ports}" | grep -qx "${host_port}"
}

does_container_join_network() {
    local container_name
    local network_name
    local joined_networks
    container_name="$1"
    network_name="$2"
    joined_networks="$(docker inspect "${container_name}" --format '{{range $name, $_ := .NetworkSettings.Networks}}{{printf "%s\n" $name}}{{end}}' 2>/dev/null || true)"
    printf '%s\n' "${joined_networks}" | grep -qx "${network_name}"
}

ensure_grafana_container_contract() {
    local grafana_network
    grafana_network="$(resolve_grafana_network)"
    if ! does_container_publish_port "${GRAFANA_CONTAINER_NAME}" "${GRAFANA_HOST_PORT}" "${GRAFANA_CONTAINER_PORT}"; then
        print_error "grafana_local 컨테이너 포트 설정이 올바르지 않습니다. expected=${GRAFANA_HOST_PORT}:${GRAFANA_CONTAINER_PORT}"
        return 1
    fi
    if ! does_container_mount_destination "${GRAFANA_CONTAINER_NAME}" "/var/lib/grafana"; then
        print_error "grafana_local 컨테이너 데이터 마운트가 없습니다. expected_destination=/var/lib/grafana"
        return 1
    fi
    if ! does_container_join_network "${GRAFANA_CONTAINER_NAME}" "${grafana_network}"; then
        print_error "grafana_local 컨테이너 네트워크 설정이 올바르지 않습니다. expected_network=${grafana_network}"
        return 1
    fi
}

ensure_grafana_image() {
    local attempt
    attempt=1
    if docker image inspect "${GRAFANA_IMAGE}" > /dev/null 2>&1; then
        return 0
    fi

    while [ "${attempt}" -le 3 ]; do
        echo " -> Grafana 이미지 다운로드 시도 중... attempt=${attempt} image=${GRAFANA_IMAGE}"
        if docker pull "${GRAFANA_IMAGE}"; then
            return 0
        fi
        echo " -> Grafana 이미지 다운로드 재시도 예정"
        attempt=$((attempt + 1))
        sleep 2
    done

    print_error "Grafana 이미지 다운로드에 실패했습니다. image=${GRAFANA_IMAGE}"
    return 1
}

ensure_grafana_port_available() {
    local windows_port_owner
    if is_container_running "${GRAFANA_CONTAINER_NAME}"; then
        return 0
    fi
    if command -v ss > /dev/null 2>&1; then
        if ss -ltnH "( sport = :${GRAFANA_HOST_PORT} )" 2>/dev/null | grep -q .; then
            print_error "Grafana 포트 ${GRAFANA_HOST_PORT} 가 이미 사용 중입니다."
            return 1
        fi
    fi
    if ! command -v powershell.exe > /dev/null 2>&1; then
        return 0
    fi
    windows_port_owner="$(powershell.exe -NoProfile -Command "
        \$connection = Get-NetTCPConnection -State Listen -LocalPort ${GRAFANA_HOST_PORT} -ErrorAction SilentlyContinue | Select-Object -First 1
        if (\$null -eq \$connection) { exit 1 }
        \$process = Get-Process -Id \$connection.OwningProcess -ErrorAction SilentlyContinue | Select-Object -First 1
        if (\$null -eq \$process) {
            Write-Output \"pid=\$($connection.OwningProcess)\"
        } else {
            Write-Output \"pid=\$($process.Id) process=\$($process.ProcessName)\"
        }
    " 2>/dev/null | tr -d '\r' || true)"
    if [ -n "${windows_port_owner}" ]; then
        print_error "Grafana 포트 ${GRAFANA_HOST_PORT} 가 이미 사용 중입니다. ${windows_port_owner}"
        return 1
    fi
}

create_grafana_container() {
    local create_output
    local grafana_data_dir
    local grafana_network
    grafana_data_dir="$(resolve_grafana_data_dir)"
    grafana_network="$(resolve_grafana_network)"

    echo " -> Grafana 컨테이너를 생성합니다. image=${GRAFANA_IMAGE} data_dir=${grafana_data_dir} network=${grafana_network}"
    ensure_grafana_provisioning_dirs
    ensure_grafana_image
    ensure_grafana_port_available

    if ! create_output="$(docker create \
        --name "${GRAFANA_CONTAINER_NAME}" \
        --restart unless-stopped \
        --network "${grafana_network}" \
        -p "${GRAFANA_HOST_PORT}:${GRAFANA_CONTAINER_PORT}" \
        -v "${grafana_data_dir}:/var/lib/grafana" \
        -v "${GRAFANA_PROVISIONING_DIR}:/etc/grafana/provisioning:ro" \
        -v "${GRAFANA_DASHBOARDS_DIR}:/var/lib/grafana/dashboards:ro" \
        "${GRAFANA_IMAGE}" 2>&1 > /dev/null)"; then
        print_error "grafana_local 컨테이너 생성에 실패했습니다. image=${GRAFANA_IMAGE} data_dir=${grafana_data_dir} network=${grafana_network} ${create_output}"
        return 1
    fi
}

ensure_grafana_container() {
    if does_container_exist "${GRAFANA_CONTAINER_NAME}"; then
        return 0
    fi
    create_grafana_container
}

copy_directory_files_to_container() {
    local source_dir
    local container_name
    local destination_dir
    local source_path
    local source_name
    local copy_output
    source_dir="$1"
    container_name="$2"
    destination_dir="$3"

    while IFS= read -r source_path; do
        source_name="$(basename "${source_path}")"
        if ! copy_output="$(docker cp "${source_path}" "${container_name}:${destination_dir}/${source_name}" 2>&1)"; then
            print_error "Grafana provisioning 파일 복사에 실패했습니다. source=${source_path} destination=${destination_dir}/${source_name} ${copy_output}"
            return 1
        fi
    done < <(find "${source_dir}" -maxdepth 1 -type f | sort)
}

sync_grafana_provisioning_into_container() {
    local is_running
    ensure_grafana_provisioning_dirs
    if does_container_mount_destination "${GRAFANA_CONTAINER_NAME}" "/etc/grafana/provisioning" && \
       does_container_mount_destination "${GRAFANA_CONTAINER_NAME}" "/var/lib/grafana/dashboards"; then
        return 0
    fi

    echo " -> 기존 Grafana 컨테이너에 provisioning 파일을 동기화합니다."
    docker exec "${GRAFANA_CONTAINER_NAME}" sh -lc "mkdir -p /etc/grafana/provisioning/datasources /etc/grafana/provisioning/dashboards /var/lib/grafana/dashboards"
    copy_directory_files_to_container "${GRAFANA_PROVISIONING_DIR}/datasources" "${GRAFANA_CONTAINER_NAME}" "/etc/grafana/provisioning/datasources"
    copy_directory_files_to_container "${GRAFANA_PROVISIONING_DIR}/dashboards" "${GRAFANA_CONTAINER_NAME}" "/etc/grafana/provisioning/dashboards"
    copy_directory_files_to_container "${GRAFANA_DASHBOARDS_DIR}" "${GRAFANA_CONTAINER_NAME}" "/var/lib/grafana/dashboards"

    is_running="false"
    if is_container_running "${GRAFANA_CONTAINER_NAME}"; then
        is_running="true"
    fi
    if [ "${is_running}" = "true" ]; then
        echo " -> provisioning 적용을 위해 Grafana 컨테이너를 재시작합니다."
        docker restart "${GRAFANA_CONTAINER_NAME}" > /dev/null
    fi
}

cleanup_legacy_grafana_dashboard_storage() {
    local grafana_data_dir
    local grafana_db_path
    local probe_output
    local cleanup_output
    local was_running
    local stop_output
    local restart_output
    local attempt
    grafana_data_dir="$(resolve_grafana_data_dir)"
    grafana_db_path="${grafana_data_dir}/grafana.db"

    if [ ! -f "${grafana_db_path}" ]; then
        return 0
    fi
    if ! command -v python3 > /dev/null 2>&1; then
        print_error "legacy Grafana dashboard 정리에 필요한 python3 를 찾지 못했습니다."
        return 1
    fi

    attempt=1
    while [ "${attempt}" -le 15 ]; do
        if ! probe_output="$(python3 - "${grafana_db_path}" <<'PY'
import json
import sqlite3
import sys
from pathlib import Path

LEGACY_UID = "adzprvm"
SOURCE_PATH = "/var/lib/grafana/dashboards/extrusion-data-legacy.json"
RESOURCE_GROUP = "dashboard.grafana.app"
RESOURCE_NAME = "dashboards"
MANAGED_BY = "classic-file-provisioning"

db_path = Path(sys.argv[1])
connection = sqlite3.connect(str(db_path))
resource_row = connection.execute(
    'select value from resource where "group" = ? and resource = ? and name = ?',
    (RESOURCE_GROUP, RESOURCE_NAME, LEGACY_UID),
).fetchone()
if resource_row is None:
    print("status=wait reason=resource_missing")
    connection.close()
    raise SystemExit(0)

resource_value = json.loads(resource_row[0])
annotations = resource_value.get("metadata", {}).get("annotations", {})
if annotations.get("grafana.app/managedBy") != MANAGED_BY:
    print("status=skip reason=resource_not_managed")
    connection.close()
    raise SystemExit(0)
if annotations.get("grafana.app/sourcePath") != SOURCE_PATH:
    print("status=skip reason=unexpected_source_path")
    connection.close()
    raise SystemExit(0)

dashboard_row = connection.execute(
    "select id from dashboard where uid = ?",
    (LEGACY_UID,),
).fetchone()
if dashboard_row is None:
    print("status=skip reason=legacy_missing")
    connection.close()
    raise SystemExit(0)

dashboard_id = int(dashboard_row[0])
reference_checks = {
    "dashboard_acl": 'select count(*) from dashboard_acl where dashboard_id = ?',
    "dashboard_provisioning": 'select count(*) from dashboard_provisioning where dashboard_id = ?',
    "dashboard_tag": 'select count(*) from dashboard_tag where dashboard_id = ? or dashboard_uid = ?',
    "star": 'select count(*) from star where dashboard_id = ? or dashboard_uid = ?',
    "annotation": 'select count(*) from annotation where dashboard_id = ? or dashboard_uid = ?',
}
reference_counts = {}
for table_name, query in reference_checks.items():
    if table_name in {"dashboard_tag", "star", "annotation"}:
        count_value = connection.execute(query, (dashboard_id, LEGACY_UID)).fetchone()[0]
    else:
        count_value = connection.execute(query, (dashboard_id,)).fetchone()[0]
    reference_counts[table_name] = int(count_value)

if any(reference_counts.values()):
    summary = ",".join(f"{name}:{value}" for name, value in sorted(reference_counts.items()) if value > 0)
    print(f"status=skip reason=legacy_references dashboard_id={dashboard_id} references={summary}")
    connection.close()
    raise SystemExit(0)

print(f"status=needs_cleanup dashboard_id={dashboard_id}")
connection.close()
PY
)"; then
            print_error "legacy Grafana dashboard 정리 사전 확인에 실패했습니다. db_path=${grafana_db_path}"
            return 1
        fi

        case "${probe_output}" in
            status=wait*)
                sleep 1
                attempt=$((attempt + 1))
                ;;
            status=skip*)
                echo " -> ${probe_output}"
                return 0
                ;;
            status=needs_cleanup*)
                break
                ;;
            *)
                print_error "legacy Grafana dashboard 정리 사전 확인 결과를 해석하지 못했습니다. output=${probe_output}"
                return 1
                ;;
        esac
    done

    if [ "${attempt}" -gt 15 ]; then
        echo " -> status=skip reason=resource_not_ready"
        return 0
    fi

    was_running="false"
    if is_container_running "${GRAFANA_CONTAINER_NAME}"; then
        was_running="true"
        if ! stop_output="$(docker stop "${GRAFANA_CONTAINER_NAME}" 2>&1)"; then
            print_error "legacy Grafana dashboard 정리 전 Grafana 중지에 실패했습니다. ${stop_output}"
            return 1
        fi
    fi

        if ! cleanup_output="$(python3 - "${grafana_db_path}" <<'PY'
import json
import shutil
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

LEGACY_UID = "adzprvm"
SOURCE_PATH = "/var/lib/grafana/dashboards/extrusion-data-legacy.json"
RESOURCE_GROUP = "dashboard.grafana.app"
RESOURCE_NAME = "dashboards"
MANAGED_BY = "classic-file-provisioning"

db_path = Path(sys.argv[1])
connection = sqlite3.connect(str(db_path))
resource_row = connection.execute(
    'select value from resource where "group" = ? and resource = ? and name = ?',
    (RESOURCE_GROUP, RESOURCE_NAME, LEGACY_UID),
).fetchone()
if resource_row is None:
    print("status=wait reason=resource_missing")
    connection.close()
    raise SystemExit(0)

resource_value = json.loads(resource_row[0])
annotations = resource_value.get("metadata", {}).get("annotations", {})
if annotations.get("grafana.app/managedBy") != MANAGED_BY:
    print("status=skip reason=resource_not_managed")
    connection.close()
    raise SystemExit(0)
if annotations.get("grafana.app/sourcePath") != SOURCE_PATH:
    print("status=skip reason=unexpected_source_path")
    connection.close()
    raise SystemExit(0)

dashboard_row = connection.execute(
    "select id from dashboard where uid = ?",
    (LEGACY_UID,),
).fetchone()
if dashboard_row is None:
    print("status=skip reason=legacy_missing")
    connection.close()
    raise SystemExit(0)

dashboard_id = int(dashboard_row[0])
reference_checks = {
    "dashboard_acl": 'select count(*) from dashboard_acl where dashboard_id = ?',
    "dashboard_provisioning": 'select count(*) from dashboard_provisioning where dashboard_id = ?',
    "dashboard_tag": 'select count(*) from dashboard_tag where dashboard_id = ? or dashboard_uid = ?',
    "star": 'select count(*) from star where dashboard_id = ? or dashboard_uid = ?',
    "annotation": 'select count(*) from annotation where dashboard_id = ? or dashboard_uid = ?',
}
reference_counts = {}
for table_name, query in reference_checks.items():
    if table_name in {"dashboard_tag", "star", "annotation"}:
        count_value = connection.execute(query, (dashboard_id, LEGACY_UID)).fetchone()[0]
    else:
        count_value = connection.execute(query, (dashboard_id,)).fetchone()[0]
    reference_counts[table_name] = int(count_value)

if any(reference_counts.values()):
    summary = ",".join(f"{name}:{value}" for name, value in sorted(reference_counts.items()) if value > 0)
    print(f"status=skip reason=legacy_references dashboard_id={dashboard_id} references={summary}")
    connection.close()
    raise SystemExit(0)

version_count = int(
    connection.execute(
        "select count(*) from dashboard_version where dashboard_id = ?",
        (dashboard_id,),
    ).fetchone()[0]
)
backup_dir = db_path.parent / "cleanup_backups"
backup_dir.mkdir(parents=True, exist_ok=True)
backup_timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
backup_path = backup_dir / f"grafana-{LEGACY_UID}-legacy-dashboard-{backup_timestamp}.db"
shutil.copy2(db_path, backup_path)

connection.execute("begin")
connection.execute("delete from dashboard_version where dashboard_id = ?", (dashboard_id,))
connection.execute("delete from dashboard where id = ? and uid = ?", (dashboard_id, LEGACY_UID))
connection.commit()
connection.close()
print(f"status=cleaned dashboard_id={dashboard_id} versions={version_count} backup={backup_path}")
PY
)"; then
            if [ "${was_running}" = "true" ]; then
                docker start "${GRAFANA_CONTAINER_NAME}" > /dev/null || true
            fi
            print_error "legacy Grafana dashboard 정리에 실패했습니다. db_path=${grafana_db_path}"
            return 1
        fi

    if [ "${was_running}" = "true" ]; then
        if ! restart_output="$(docker start "${GRAFANA_CONTAINER_NAME}" 2>&1)"; then
            print_error "legacy Grafana dashboard 정리 후 Grafana 재시작에 실패했습니다. ${restart_output}"
            return 1
        fi
    fi

    case "${cleanup_output}" in
        status=cleaned*)
            echo " -> ${cleanup_output}"
            return 0
            ;;
        status=skip*)
            echo " -> ${cleanup_output}"
            return 0
            ;;
        *)
            print_error "legacy Grafana dashboard 정리 결과를 해석하지 못했습니다. output=${cleanup_output}"
            return 1
            ;;
    esac
}

start_grafana_container() {
    local start_output
    echo "[3] Grafana 컨테이너 시작 중..."

    ensure_grafana_container
    ensure_grafana_container_contract

    if is_container_running "${GRAFANA_CONTAINER_NAME}"; then
        echo " -> Grafana 컨테이너가 이미 실행 중입니다."
        sync_grafana_provisioning_into_container
        cleanup_legacy_grafana_dashboard_storage
        return 0
    fi

    ensure_grafana_port_available

    if ! start_output="$(docker start "${GRAFANA_CONTAINER_NAME}" 2>&1)"; then
        print_error "grafana_local 컨테이너 시작에 실패했습니다. ${start_output}"
        return 1
    fi

    echo " -> ${start_output}"
    sync_grafana_provisioning_into_container
    cleanup_legacy_grafana_dashboard_storage
}

wait_for_supabase_db_ready() {
    local attempt
    attempt=1
    while [ "${attempt}" -le 30 ]; do
        if docker exec "${SUPABASE_DB_CONTAINER}" pg_isready -U postgres > /dev/null 2>&1; then
            return 0
        fi
        attempt=$((attempt + 1))
        sleep 1
    done
    print_error "Supabase DB가 준비되지 않아 Grafana view 마이그레이션을 적용할 수 없습니다. container=${SUPABASE_DB_CONTAINER}"
    return 1
}

apply_grafana_view_migration_with_psql() {
    local migration_output
    echo "[2-1] Grafana view 마이그레이션 적용 중..."
    if [ ! -f "${GRAFANA_VIEW_MIGRATION_PATH}" ]; then
        print_error "Grafana view 마이그레이션 파일이 없습니다. path=${GRAFANA_VIEW_MIGRATION_PATH}"
        return 1
    fi
    if ! migration_output="$(docker exec -i "${SUPABASE_DB_CONTAINER}" psql -v ON_ERROR_STOP=1 -U postgres -d postgres < "${GRAFANA_VIEW_MIGRATION_PATH}" 2>&1)"; then
        print_error "Grafana view 마이그레이션 적용에 실패했습니다. container=${SUPABASE_DB_CONTAINER} path=${GRAFANA_VIEW_MIGRATION_PATH} ${migration_output}"
        return 1
    fi
    if [ -n "${migration_output}" ]; then
        echo "${migration_output}"
    fi
}

start_supabase_runtime() {
    local supabase_containers
    echo "[2] Supabase 시작 중..."
    if command -v supabase > /dev/null 2>&1; then
        supabase start || echo "⚠️ Supabase 시작 중 경고가 발생했습니다. 이미 실행 중일 수 있습니다."
        wait_for_supabase_db_ready
        apply_grafana_view_migration_with_psql
        return 0
    fi

    echo "⚠️ supabase CLI를 찾지 못했습니다. 기존 로컬 컨테이너를 직접 시작합니다."
    supabase_containers="$(docker ps -a --format '{{.Names}}' | grep -E '^supabase_.*_Extrusion_data$' || true)"
    if [ -z "${supabase_containers}" ]; then
        print_error "supabase CLI가 없고 시작 가능한 로컬 Supabase 컨테이너도 찾지 못했습니다."
        return 1
    fi
    echo "${supabase_containers}" | xargs -r docker start
    wait_for_supabase_db_ready
    apply_grafana_view_migration_with_psql
}

start_edge_runtime_container() {
    echo "[3-1] Edge Runtime 컨테이너 시작 중..."
    if does_container_exist "${SUPABASE_EDGE_RUNTIME_CONTAINER}"; then
        docker start "${SUPABASE_EDGE_RUNTIME_CONTAINER}"
        return 0
    fi
    echo "⚠️ '${SUPABASE_EDGE_RUNTIME_CONTAINER}' 컨테이너가 존재하지 않습니다."
}

ensure_cron_running() {
    echo "[4] 백업 스케줄러(Cron) 상태 확인..."
    if ! service cron status | grep -q "running"; then
        echo " -> Cron 서비스가 꺼져 있어 시작합니다. sudo 권한이 필요합니다."
        sudo service cron start
        return 0
    fi
    echo " -> Cron 서비스가 이미 실행 중입니다."
}

ensure_project_dir
ensure_docker_ready
start_supabase_runtime
start_grafana_container
start_edge_runtime_container
ensure_cron_running

echo "✅ 모든 서비스가 정상 가동되었습니다."
