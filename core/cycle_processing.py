from __future__ import annotations

from datetime import datetime, timedelta, timezone
import threading
from typing import Callable, Iterable, Literal

import pandas as pd
from psycopg2.extras import execute_values

from core.archive_metrics import DbConnectionSettings, create_connection
from core.cycle_operations import LEGACY_ALGORITHM_VERSION, LEGACY_SOURCE_MODE


PRESSURE_THRESHOLD = 30.0
MIN_DURATION = 30.0
MIN_MAX_PRESSURE = 100.0
KST = timezone(timedelta(hours=9))
METRICS_CHUNK_SIZE = 50000
METRIC_COLUMNS = ["timestamp", "main_pressure", "production_counter"]
LegacyIncrementalResult = Literal["completed", "requires_full_backfill"]
WorkLogLookup = tuple[list[int], list[pd.Timestamp], list[pd.Timestamp]]
OpenCycleState = tuple[pd.Timestamp, float, int | None]


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

    def _build_work_log_lookup(self, work_log_frame: pd.DataFrame) -> WorkLogLookup:
        if work_log_frame.empty:
            return ([], [], [])

        return (
            work_log_frame["work_log_id"].astype(int).tolist(),
            work_log_frame["start_time"].tolist(),
            work_log_frame["end_time"].tolist(),
        )

    def _prepare_metrics_frame(self, metrics_frame: pd.DataFrame) -> pd.DataFrame:
        chunk_frame = metrics_frame.loc[:, METRIC_COLUMNS].copy()
        chunk_frame["timestamp"] = pd.to_datetime(chunk_frame["timestamp"], utc=True)
        if not chunk_frame["timestamp"].is_monotonic_increasing:
            chunk_frame = chunk_frame.sort_values("timestamp", kind="stable")
        chunk_frame = chunk_frame.reset_index(drop=True)
        chunk_frame["is_active"] = chunk_frame["main_pressure"] > PRESSURE_THRESHOLD
        chunk_frame["active_change"] = chunk_frame["is_active"].astype(int).diff()
        return chunk_frame

    def _prepare_incremental_metrics_chunk(self, metrics_chunk: pd.DataFrame) -> pd.DataFrame:
        chunk_frame = metrics_chunk.loc[:, METRIC_COLUMNS].copy()
        chunk_frame["timestamp"] = pd.to_datetime(chunk_frame["timestamp"], utc=True)
        if not chunk_frame["timestamp"].is_monotonic_increasing:
            chunk_frame = chunk_frame.sort_values("timestamp", kind="stable")
        chunk_frame = chunk_frame.reset_index(drop=True)
        chunk_frame["is_active"] = chunk_frame["main_pressure"] > PRESSURE_THRESHOLD
        return chunk_frame

    def _match_work_log_id(
        self,
        start_time: pd.Timestamp,
        work_log_lookup: WorkLogLookup,
        work_log_pointer: int,
    ) -> tuple[int | None, int]:
        work_log_ids, work_log_starts, work_log_ends = work_log_lookup
        while work_log_pointer < len(work_log_ids) and work_log_ends[work_log_pointer] < start_time:
            work_log_pointer += 1

        if (
            work_log_pointer < len(work_log_ids)
            and work_log_starts[work_log_pointer] <= start_time
            and work_log_ends[work_log_pointer] >= start_time
        ):
            return work_log_ids[work_log_pointer], work_log_pointer
        return None, work_log_pointer

    def _process_prepared_chunk(
        self,
        chunk_frame: pd.DataFrame,
        work_log_lookup: WorkLogLookup,
        work_log_pointer: int,
    ) -> tuple[list[tuple[object, ...]], int]:
        if chunk_frame.empty:
            return [], work_log_pointer

        start_positions = chunk_frame.index[chunk_frame["active_change"] == 1].tolist()
        end_positions = chunk_frame.index[chunk_frame["active_change"] == -1].tolist()

        cycles: list[tuple[object, ...]] = []
        start_pointer = 0
        end_pointer = 0
        while start_pointer < len(start_positions) and end_pointer < len(end_positions):
            if self._stop_event.is_set():
                break

            start_position = start_positions[start_pointer]
            while end_pointer < len(end_positions) and end_positions[end_pointer] < start_position:
                end_pointer += 1
            if end_pointer >= len(end_positions):
                break

            end_position = end_positions[end_pointer]
            start_time = chunk_frame.at[start_position, "timestamp"]
            end_time = chunk_frame.at[end_position, "timestamp"]
            duration = (end_time - start_time).total_seconds()
            cycle_slice = chunk_frame.iloc[start_position : end_position + 1]
            max_pressure = cycle_slice["main_pressure"].max()
            production_counter = chunk_frame.at[end_position, "production_counter"]
            is_valid = bool(duration >= MIN_DURATION and max_pressure >= MIN_MAX_PRESSURE)
            work_log_id, work_log_pointer = self._match_work_log_id(
                start_time=start_time,
                work_log_lookup=work_log_lookup,
                work_log_pointer=work_log_pointer,
            )

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

        return cycles, work_log_pointer

    def _process_incremental_chunk(
        self,
        metrics_chunk: pd.DataFrame,
        work_log_lookup: WorkLogLookup,
        work_log_pointer: int,
        previous_is_active: bool | None,
        open_cycle_state: OpenCycleState | None,
    ) -> tuple[list[tuple[object, ...]], int, bool | None, OpenCycleState | None]:
        if metrics_chunk.empty:
            return [], work_log_pointer, previous_is_active, open_cycle_state

        prepared_chunk = self._prepare_incremental_metrics_chunk(metrics_chunk)
        cycles: list[tuple[object, ...]] = []

        for row in prepared_chunk.itertuples(index=False):
            if self._stop_event.is_set():
                break

            timestamp = row.timestamp
            max_pressure = float(row.main_pressure)
            production_counter = int(row.production_counter) if pd.notnull(row.production_counter) else None
            is_active = bool(row.is_active)

            if is_active and previous_is_active is not True:
                work_log_id, work_log_pointer = self._match_work_log_id(
                    start_time=timestamp,
                    work_log_lookup=work_log_lookup,
                    work_log_pointer=work_log_pointer,
                )
                open_cycle_state = (timestamp, max_pressure, work_log_id)
            elif is_active and open_cycle_state is not None:
                open_cycle_state = (
                    open_cycle_state[0],
                    max(open_cycle_state[1], max_pressure),
                    open_cycle_state[2],
                )
            elif not is_active and previous_is_active is True and open_cycle_state is not None:
                start_time, cycle_max_pressure, work_log_id = open_cycle_state
                duration = (timestamp - start_time).total_seconds()
                is_valid = bool(duration >= MIN_DURATION and cycle_max_pressure >= MIN_MAX_PRESSURE)
                cycles.append(
                    (
                        self.machine_id,
                        start_time,
                        timestamp,
                        production_counter,
                        work_log_id,
                        float(duration),
                        float(cycle_max_pressure),
                        is_valid,
                        False,
                        self.source_mode,
                        self.algorithm_version,
                    )
                )
                open_cycle_state = None

            previous_is_active = is_active

        return cycles, work_log_pointer, previous_is_active, open_cycle_state

    def _collect_incremental_cycles(
        self,
        metrics_chunk_iter: Iterable[pd.DataFrame],
        work_log_frame: pd.DataFrame,
    ) -> tuple[list[tuple[object, ...]], int]:
        work_log_lookup = self._build_work_log_lookup(work_log_frame)
        work_log_pointer = 0
        total_metric_rows = 0
        collected_cycles: list[tuple[object, ...]] = []
        previous_is_active: bool | None = None
        open_cycle_state: OpenCycleState | None = None

        for metrics_chunk in metrics_chunk_iter:
            if self._stop_event.is_set():
                break
            if metrics_chunk.empty:
                continue

            total_metric_rows += len(metrics_chunk)
            chunk_cycles, work_log_pointer, previous_is_active, open_cycle_state = self._process_incremental_chunk(
                metrics_chunk=metrics_chunk,
                work_log_lookup=work_log_lookup,
                work_log_pointer=work_log_pointer,
                previous_is_active=previous_is_active,
                open_cycle_state=open_cycle_state,
            )
            collected_cycles.extend(chunk_cycles)

        return collected_cycles, total_metric_rows

    def _upsert_incremental_cycles_by_chunk(
        self,
        cursor: object,
        metrics_chunk_iter: Iterable[pd.DataFrame],
        work_log_frame: pd.DataFrame,
        last_processed: datetime,
    ) -> tuple[int, int]:
        work_log_lookup = self._build_work_log_lookup(work_log_frame)
        work_log_pointer = 0
        total_metric_rows = 0
        total_upserted_cycles = 0
        previous_is_active: bool | None = None
        open_cycle_state: OpenCycleState | None = None

        for metrics_chunk in metrics_chunk_iter:
            if self._stop_event.is_set():
                break
            if metrics_chunk.empty:
                continue

            total_metric_rows += len(metrics_chunk)
            chunk_cycles, work_log_pointer, previous_is_active, open_cycle_state = self._process_incremental_chunk(
                metrics_chunk=metrics_chunk,
                work_log_lookup=work_log_lookup,
                work_log_pointer=work_log_pointer,
                previous_is_active=previous_is_active,
                open_cycle_state=open_cycle_state,
            )
            new_chunk_cycles = [cycle for cycle in chunk_cycles if cycle[2] > last_processed]
            if new_chunk_cycles == []:
                continue

            self._upsert_cycles(cursor, new_chunk_cycles)
            total_upserted_cycles += len(new_chunk_cycles)

        return total_metric_rows, total_upserted_cycles

    def process_chunk(
        self,
        metrics_frame: pd.DataFrame,
        work_log_frame: pd.DataFrame,
    ) -> list[tuple[object, ...]]:
        chunk_frame = self._prepare_metrics_frame(metrics_frame=metrics_frame)
        work_log_lookup = self._build_work_log_lookup(work_log_frame)
        cycles, _ = self._process_prepared_chunk(
            chunk_frame=chunk_frame,
            work_log_lookup=work_log_lookup,
            work_log_pointer=0,
        )
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
                metrics_chunk_iter = pd.read_sql(
                    """
                    SELECT "timestamp", main_pressure, production_counter
                    FROM public.all_metrics
                    WHERE "timestamp" >= %s
                    ORDER BY "timestamp" ASC
                    """,
                    connection,
                    params=(start_time,),
                    chunksize=METRICS_CHUNK_SIZE,
                )
                total_metric_rows, total_upserted_cycles = self._upsert_incremental_cycles_by_chunk(
                    cursor=cursor,
                    metrics_chunk_iter=metrics_chunk_iter,
                    work_log_frame=work_log_frame,
                    last_processed=last_processed,
                )
                if total_metric_rows == 0:
                    self.log("신규 metrics가 없습니다.")
                    self.update_progress(1.0)
                    return "completed"

                self.log(f"metrics row 수: {total_metric_rows}")
                self.update_progress(0.6)
                if self._stop_event.is_set():
                    self.log("legacy backfill이 중단되었습니다.")
                    return "completed"

                self.update_progress(0.8)
                if total_upserted_cycles == 0:
                    self.log("추가로 저장할 cycle이 없습니다.")
                    self.update_progress(1.0)
                    return "completed"

                self.log(f"legacy cycle upsert 시작: {total_upserted_cycles}건")
                connection.commit()
                self.log(f"legacy cycle upsert completed: {total_upserted_cycles}")
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
