from __future__ import annotations

import argparse
from pathlib import Path
import sys

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.training_dataset_v1 import (
    TRAINING_DATASET_V1_COLUMNS,
    build_training_dataset_v1_frame,
    write_training_dataset_v1_parquet,
)


DEFAULT_VERIFY_OUTPUT_FILE = PROJECT_ROOT / "tools" / "_verify_training_dataset_v1.parquet"
CONFLICT_TIMESTAMP = "2026-02-02T08:11:57.843000+09:00"
IDLE_TIMESTAMP = "2026-02-02T07:38:01.414000+09:00"


def log(message: str, level: str = "INFO") -> None:
    print(f"[{level.upper()}] {message}")


def verify_training_dataset_v1(
    training_base_file_path: Path,
    parquet_output_path: Path | None,
) -> int:
    if not training_base_file_path.is_file():
        log(f"training_base parquet를 찾을 수 없습니다: {training_base_file_path}", "ERROR")
        return 1

    try:
        training_base_frame = pd.read_parquet(training_base_file_path)
        training_dataset_v1_frame = build_training_dataset_v1_frame(training_base_frame)
        validation_errors = validate_training_dataset_v1_frame(
            training_base_frame,
            training_dataset_v1_frame,
        )
        if validation_errors:
            for validation_error in validation_errors:
                log(validation_error, "ERROR")
            return 1

        output_path = (
            parquet_output_path if parquet_output_path is not None else DEFAULT_VERIFY_OUTPUT_FILE
        )
        write_training_dataset_v1_parquet(training_dataset_v1_frame, output_path)
        persisted_frame = pd.read_parquet(output_path)
        parquet_errors = validate_parquet_roundtrip(
            training_dataset_v1_frame,
            persisted_frame,
        )
        if parquet_errors:
            for parquet_error in parquet_errors:
                log(parquet_error, "ERROR")
            return 1

        log(f"Rows OK: {len(training_dataset_v1_frame)}")
        log(f"Columns OK: {len(training_dataset_v1_frame.columns)}")
        log(f"Parquet OK: {output_path}")

        if parquet_output_path is None and output_path.exists():
            output_path.unlink()
        return 0
    except Exception as error:
        log(f"Exception: {error}", "ERROR")
        return 1


def validate_training_dataset_v1_frame(
    training_base_frame: pd.DataFrame,
    training_dataset_v1_frame: pd.DataFrame,
) -> list[str]:
    errors: list[str] = []

    if list(training_dataset_v1_frame.columns) != list(TRAINING_DATASET_V1_COLUMNS):
        errors.append("training_dataset_v1 컬럼 집합이 스키마와 일치하지 않습니다.")
    if len(training_dataset_v1_frame) != len(training_base_frame):
        errors.append("training_dataset_v1 행 수가 training_base와 다릅니다.")

    missing_required_columns = [
        column_name
        for column_name in (
            "row_timestamp",
            "spot_temperature",
            "main_pressure",
            "production_count",
            "current_speed",
            "billet_cycle_id",
            "spot_temp_missing",
            "cycle_present_flag",
            "cycle_missing_flag",
            "idle_by_pressure_zero",
            "active_by_pressure_threshold",
            "label_conflict",
            "time_gap_ms",
            "is_partial_row",
            "row_quality_flag",
            "idle_flag",
        )
        if column_name not in training_dataset_v1_frame.columns
    ]
    if missing_required_columns:
        errors.append(
            "training_dataset_v1 필수 컬럼이 없습니다: "
            + ", ".join(missing_required_columns)
        )

    expected_gap_series = (
        pd.to_datetime(training_base_frame["row_timestamp"], errors="coerce")
        .diff()
        .dt.total_seconds()
        .mul(1000)
        .round()
        .astype("Int64")
    )
    actual_gap_series = training_dataset_v1_frame["time_gap_ms"].astype("Int64")
    if not actual_gap_series.equals(expected_gap_series):
        errors.append("time_gap_ms 계산 결과가 training_base timestamp 차이와 다릅니다.")

    if not pd.isna(training_dataset_v1_frame.iloc[0]["time_gap_ms"]):
        errors.append("첫 번째 row의 time_gap_ms가 null이 아닙니다.")

    conflict_row = find_row_by_timestamp(training_dataset_v1_frame, CONFLICT_TIMESTAMP)
    if conflict_row is None:
        errors.append("conflict 기준 행을 찾지 못했습니다.")
    else:
        if not bool(conflict_row["label_conflict"]):
            errors.append("conflict 기준 행의 label_conflict가 True가 아닙니다.")
        if bool(conflict_row["idle_flag"]):
            errors.append("conflict 기준 행이 idle_flag=True로 잘못 확정되었습니다.")
        if not bool(conflict_row["row_quality_flag"]):
            errors.append("conflict 기준 행의 row_quality_flag가 True가 아닙니다.")

    idle_row = find_row_by_timestamp(training_dataset_v1_frame, IDLE_TIMESTAMP)
    if idle_row is None:
        errors.append("idle 기준 행을 찾지 못했습니다.")
    else:
        if not bool(idle_row["spot_temp_missing"]):
            errors.append("idle 기준 행의 spot_temp_missing이 True가 아닙니다.")
        if not bool(idle_row["idle_flag"]):
            errors.append("idle 기준 행의 idle_flag가 True가 아닙니다.")

    return errors


def validate_parquet_roundtrip(
    training_dataset_v1_frame: pd.DataFrame,
    persisted_frame: pd.DataFrame,
) -> list[str]:
    errors: list[str] = []

    if list(persisted_frame.columns) != list(TRAINING_DATASET_V1_COLUMNS):
        errors.append("Parquet roundtrip 이후 컬럼 집합이 스키마와 일치하지 않습니다.")
    if len(persisted_frame) != len(training_dataset_v1_frame):
        errors.append("Parquet roundtrip 이후 행 수가 달라졌습니다.")

    persisted_conflict_row = find_row_by_timestamp(persisted_frame, CONFLICT_TIMESTAMP)
    if persisted_conflict_row is None:
        errors.append("Parquet roundtrip 이후 conflict 기준 행을 찾지 못했습니다.")
    else:
        if bool(persisted_conflict_row["idle_flag"]):
            errors.append("Parquet roundtrip 이후 conflict 행이 idle로 잘못 저장되었습니다.")
        if not bool(persisted_conflict_row["label_conflict"]):
            errors.append("Parquet roundtrip 이후 label_conflict 값이 유지되지 않았습니다.")

    return errors


def find_row_by_timestamp(frame: pd.DataFrame, row_timestamp: str) -> pd.Series | None:
    matched_rows = frame.loc[frame["row_timestamp"] == row_timestamp]
    if matched_rows.empty:
        return None
    return matched_rows.iloc[0]


def main() -> None:
    argument_parser = argparse.ArgumentParser(
        description="Verify training_dataset_v1 regression checks on training_base parquet",
    )
    argument_parser.add_argument(
        "--training-base-file",
        dest="training_base_file",
        required=True,
        help="검증에 사용할 training_base.parquet 경로",
    )
    argument_parser.add_argument(
        "--output",
        dest="output_path",
        default=None,
        help="검증용 training_dataset_v1.parquet 출력 경로",
    )
    parsed_args = argument_parser.parse_args()

    output_path = (
        Path(parsed_args.output_path).resolve()
        if parsed_args.output_path is not None
        else None
    )
    exit_code = verify_training_dataset_v1(
        Path(parsed_args.training_base_file).resolve(),
        output_path,
    )
    raise SystemExit(exit_code)


if __name__ == "__main__":
    main()
