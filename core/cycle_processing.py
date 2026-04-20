from __future__ import annotations

from datetime import datetime, timedelta, timezone
import threading
from typing import Callable, Literal

import pandas as pd
from psycopg2.extras import execute_values

from core.archive_metrics import DbConnectionSettings, create_connection
from core.cycle_operations import LEGACY_ALGORITHM_VERSION, LEGACY_SOURCE_MODE


PRESSURE_THRESHOLD = 30.0
MIN_DURATION = 30.0
MIN_MAX_PRESSURE = 100.0
KST = timezone(timedelta(hours=9))
LegacyIncrementalResult = Literal["completed", "requires_full_backfill"]


class CycleProcessor:
    def __init__(
        self,
        db_settings: DbConnectionSettings,
        machine_id: str,
        log_callback: Callable[[str], None] | None,
        progress_callback: Callable[[float], None] | None,
        source_mode: str,
        algorithm_version: str,
    ):
        self.db_settings = db_settings
        self.machine_id = machine_id.strip()
        self.log_callback = log_callback
        self.progress_callback = progress_callback
        self.source_mode = source_mode
        self.algorithm_version = algorithm_version
        self._stop_event = threading.Event()

    def log(self, message: str) -> None:
        if self.log_callback is not None:
            self.log_callback(message)
            return
        print(message)

    def stop(self) -> None:
        self._stop_event.set()

    def get_db_connection(self):
        return create_connection(self.db_settings)

    def load_work_logs(self, start_from: object) -> pd.DataFrame:
        connection = self.get_db_connection()
        try:
            work_log_frame = pd.read_sql(
                """
                SELECT id AS work_log_id, machine_id, start_time, end_time, die_id
                FROM public.tb_work_log
                WHERE machine_id = %s
                  AND (end_time >= %s OR end_time IS NULL)
                ORDER BY start_time ASC
                """,
                connection,
                params=(self.machine_id, start_from),
            )
        finally:
            connection.close()

        if work_log_frame.empty:
            return work_log_frame

        work_log_frame["start_time"] = pd.to_datetime(work_log_frame["start_time"], utc=True)
        work_log_frame["end_time"] = pd.to_datetime(work_log_frame["end_time"], utc=True)
        work_log_frame["next_start"] = work_log_frame["start_time"].shift(-1)
        work_log_frame["end_time"] = work_log_frame["end_time"].fillna(work_log_frame["next_start"])
        work_log_frame["end_time"] = work_log_frame["end_time"].fillna(pd.Timestamp.now(tz="UTC"))
        return work_log_frame

    def process_chunk(
        self,
        metrics_frame: pd.DataFrame,
        work_log_frame: pd.DataFrame,
    ) -> list[tuple[object, ...]]:
        chunk_frame = metrics_frame.copy()
        chunk_frame["is_active"] = chunk_frame["main_pressure"] > PRESSURE_THRESHOLD
        chunk_frame["active_change"] = chunk_frame["is_active"].astype(int).diff()

        start_indexes = chunk_frame[chunk_frame["active_change"] == 1].index
        end_indexes = chunk_frame[chunk_frame["active_change"] == -1].index

        cycles: list[tuple[object, ...]] = []
        start_pointer = 0
        end_pointer = 0
        while start_pointer < len(start_indexes) and end_pointer < len(end_indexes):
            if self._stop_event.is_set():
                break

            start_index = start_indexes[start_pointer]
            while end_pointer < len(end_indexes) and end_indexes[end_pointer] < start_index:
                end_pointer += 1
            if end_pointer >= len(end_indexes):
                break

            end_index = end_indexes[end_pointer]
            start_time = chunk_frame.loc[start_index, "timestamp"]
            end_time = chunk_frame.loc[end_index, "timestamp"]
            duration = (end_time - start_time).total_seconds()
            cycle_slice = chunk_frame.loc[start_index:end_index]
            max_pressure = cycle_slice["main_pressure"].max()
            production_counter = chunk_frame.loc[end_index, "production_counter"]
            is_valid = bool(duration >= MIN_DURATION and max_pressure >= MIN_MAX_PRESSURE)

            work_log_id: int | None = None
            if not work_log_frame.empty:
                matched_frame = work_log_frame[
                    (work_log_frame["start_time"] <= start_time)
                    & (work_log_frame["end_time"] >= start_time)
                ]
                if not matched_frame.empty:
                    work_log_id = int(matched_frame.iloc[0]["work_log_id"])

            cycles.append(
                (
                    self.machine_id,
                    start_time,
                    end_time,
                    int(production_counter) if pd.notnull(production_counter) else None,
                    work_log_id,
                    float(duration),
                    float(max_pressure),
                    is_valid,
                    False,
                    self.source_mode,
                    self.algorithm_version,
                )
            )
            start_pointer += 1
            end_pointer += 1

        return cycles

    def _upsert_cycles(self, cursor, cycles: list[tuple[object, ...]]) -> None:
        execute_values(
            cursor,
            """
            INSERT INTO public.tb_cycle_log (
                machine_id,
                start_time,
                end_time,
                production_counter,
                work_log_id,
                duration_sec,
                max_pressure,
                is_valid,
                is_test_run,
                source_mode,
                algorithm_version
            )
            VALUES %s
            ON CONFLICT (
                machine_id,
                start_time,
                end_time,
                source_mode,
                algorithm_version
            )
            DO UPDATE
            SET
                production_counter = EXCLUDED.production_counter,
                work_log_id = EXCLUDED.work_log_id,
                duration_sec = EXCLUDED.duration_sec,
                max_pressure = EXCLUDED.max_pressure,
                is_valid = EXCLUDED.is_valid,
                is_test_run = EXCLUDED.is_test_run,
                source_mode = EXCLUDED.source_mode,
                algorithm_version = EXCLUDED.algorithm_version,
                updated_at = now()
            """,
            cycles,
        )

    def update_progress(self, value: float) -> None:
        if self.progress_callback is not None:
            self.progress_callback(value)

    def run_incremental(self) -> LegacyIncrementalResult:
        self._stop_event.clear()
        connection = self.get_db_connection()
        try:
            self.update_progress(0.0)
            with connection.cursor() as cursor:
                self.log("마지막 cycle snapshot 시각 확인 중")
                cursor.execute(
                    """
                    SELECT MAX(end_time)
                    FROM public.tb_cycle_log
                    WHERE machine_id = %s
                      AND source_mode = %s
                      AND algorithm_version = %s
                    """,
                    (
                        self.machine_id,
                        self.source_mode,
                        self.algorithm_version,
                    ),
                )
                last_processed = cursor.fetchone()[0]
                if last_processed is None:
                    self.log("기존 cycle snapshot이 없습니다. 운영 화면에서 full backfill을 먼저 실행하세요.")
                    self.update_progress(1.0)
                    return "requires_full_backfill"

                self.log(f"마지막 처리 시각: {last_processed}")
                self.update_progress(0.2)
                start_time = last_processed - timedelta(minutes=1)

                self.log("작업일보 구간 로딩 중")
                work_log_frame = self.load_work_logs(start_time)
                self.update_progress(0.4)

                self.log("신규 metrics 로딩 중")
                metrics_frame = pd.read_sql(
                    """
                    SELECT "timestamp", main_pressure, production_counter
                    FROM public.all_metrics
                    WHERE "timestamp" >= %s
                    ORDER BY "timestamp" ASC
                    """,
                    connection,
                    params=(start_time,),
                )
                if metrics_frame.empty:
                    self.log("신규 metrics가 없습니다.")
                    self.update_progress(1.0)
                    return "completed"

                metrics_frame["timestamp"] = pd.to_datetime(metrics_frame["timestamp"], utc=True)
                self.log(f"metrics row 수: {len(metrics_frame)}")
                self.update_progress(0.6)

                cycles = self.process_chunk(metrics_frame, work_log_frame)
                if self._stop_event.is_set():
                    self.log("legacy backfill이 중단되었습니다.")
                    return "completed"

                new_cycles = [cycle for cycle in cycles if cycle[2] > last_processed]
                self.update_progress(0.8)
                if not new_cycles:
                    self.log("추가로 저장할 cycle이 없습니다.")
                    self.update_progress(1.0)
                    return "completed"

                self.log(f"legacy cycle upsert 시작: {len(new_cycles)}건")
                self._upsert_cycles(cursor, new_cycles)
                connection.commit()
                self.update_progress(1.0)
                self.log("legacy incremental backfill 완료")
        except Exception as error:
            connection.rollback()
            self.log(f"legacy incremental backfill 오류: {error}")
            raise
        finally:
            connection.close()
        return "completed"

    def run_range(self, mode: str, custom_date: str | None) -> None:
        self._stop_event.clear()
        connection = self.get_db_connection()
        try:
            self.update_progress(0.0)
            with connection.cursor() as cursor:
                now = datetime.now(KST)
                if mode == "all":
                    self.log("legacy full backfill 범위 계산 중")
                    cursor.execute(
                        """
                        SELECT MIN("timestamp"), MAX("timestamp")
                        FROM public.all_metrics
                        """
                    )
                    start_time, end_time = cursor.fetchone()
                    if start_time is None or end_time is None:
                        raise ValueError("all_metrics가 비어 있어 full backfill을 실행할 수 없습니다.")
                elif mode == "today":
                    start_time = now.replace(hour=0, minute=0, second=0, microsecond=0)
                    end_time = now
                elif mode == "yesterday":
                    yesterday = now - timedelta(days=1)
                    start_time = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
                    end_time = yesterday.replace(hour=23, minute=59, second=59)
                elif mode == "custom" and custom_date is not None:
                    start_time = pd.to_datetime(custom_date).tz_localize(KST)
                    end_time = now
                else:
                    raise ValueError("지원하지 않는 legacy backfill 모드입니다.")

                if start_time >= end_time:
                    raise ValueError("legacy backfill 범위가 비어 있습니다.")

                self.log(f"legacy backfill 범위: {start_time} -> {end_time}")
                self.update_progress(0.1)
                work_log_frame = self.load_work_logs(start_time)
                self.update_progress(0.2)

                current_timestamp = start_time
                total_cycles = 0
                chunk_count = 0
                total_seconds = max((end_time - start_time).total_seconds(), 1.0)
                while current_timestamp < end_time:
                    if self._stop_event.is_set():
                        self.log("legacy backfill이 중단되었습니다.")
                        break

                    next_timestamp = min(current_timestamp + timedelta(hours=1), end_time + timedelta(seconds=1))
                    chunk_count += 1
                    self.log(f"legacy chunk {chunk_count}: {current_timestamp} -> {next_timestamp}")
                    chunk_frame = pd.read_sql(
                        """
                        SELECT "timestamp", main_pressure, production_counter
                        FROM public.all_metrics
                        WHERE "timestamp" >= %s
                          AND "timestamp" < %s
                        ORDER BY "timestamp" ASC
                        """,
                        connection,
                        params=(current_timestamp, next_timestamp),
                    )
                    if not chunk_frame.empty:
                        chunk_frame["timestamp"] = pd.to_datetime(chunk_frame["timestamp"], utc=True)
                        cycles = self.process_chunk(chunk_frame, work_log_frame)
                        if cycles:
                            self._upsert_cycles(cursor, cycles)
                            connection.commit()
                            total_cycles += len(cycles)
                            self.log(f"legacy chunk upsert: {len(cycles)}건 (누적 {total_cycles})")

                    elapsed_seconds = (current_timestamp - start_time).total_seconds()
                    self.update_progress(0.2 + (elapsed_seconds / total_seconds) * 0.7)
                    current_timestamp = next_timestamp

                self.update_progress(1.0)
                self.log(f"legacy range backfill 완료: 총 {total_cycles}건")
        except Exception as error:
            connection.rollback()
            self.log(f"legacy range backfill 오류: {error}")
            raise
        finally:
            connection.close()


def build_legacy_cycle_processor(
    db_settings: DbConnectionSettings,
    machine_id: str,
    log_callback: Callable[[str], None] | None,
    progress_callback: Callable[[float], None] | None,
) -> CycleProcessor:
    return CycleProcessor(
        db_settings=db_settings,
        machine_id=machine_id,
        log_callback=log_callback,
        progress_callback=progress_callback,
        source_mode=LEGACY_SOURCE_MODE,
        algorithm_version=LEGACY_ALGORITHM_VERSION,
    )
