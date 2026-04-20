from __future__ import annotations

import argparse
from pathlib import Path
import sys

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.training_base import TRAINING_BASE_COLUMNS, build_training_base_frame, write_training_base_parquet
from core.transform import build_records_plc


DEFAULT_SAMPLE_FILE = PROJECT_ROOT / "Factory_Integrated_Log_20260202_073801.csv"
DEFAULT_VERIFY_OUTPUT_FILE = PROJECT_ROOT / "tools" / "_verify_training_base.parquet"
MISSING_TEMPERATURE_TIMESTAMP = "2026-02-02T07:38:01.414000+09:00"
LABEL_CONFLICT_TIMESTAMP = "2026-02-02T08:11:57.843000+09:00"
INTEGRATED_BASELINE_TIMESTAMP = "2026-02-02T08:12:26.978000+09:00"


def log(message: str, level: str = "INFO") -> None:
    print(f"[{level.upper()}] {message}")


def verify_training_base(plc_file_path: Path, parquet_output_path: Path | None) -> int:
    if not plc_file_path.is_file():
        log(f"PLC CSV를 찾을 수 없습니다: {plc_file_path}", "ERROR")
        return 1

    try:
        plc_frame = build_records_plc(str(plc_file_path), plc_file_path.name)
        if not hasattr(plc_frame, "empty") or plc_frame.empty:
            log("PLC 변환 결과가 비어 있습니다.", "ERROR")
            return 1

        training_base_frame = build_training_base_frame(plc_frame, None)
        validation_errors = validate_training_base_frame(plc_frame, training_base_frame)
        if validation_errors:
            for validation_error in validation_errors:
                log(validation_error, "ERROR")
            return 1

        output_path = parquet_output_path if parquet_output_path is not None else DEFAULT_VERIFY_OUTPUT_FILE

        write_training_base_parquet(training_base_frame, output_path)
        persisted_frame = pd.read_parquet(output_path)
        parquet_errors = validate_parquet_roundtrip(training_base_frame, persisted_frame)
        if parquet_errors:
            for parquet_error in parquet_errors:
                log(parquet_error, "ERROR")
            return 1

        log(f"Rows OK: {len(training_base_frame)}")
        log(f"Columns OK: {len(training_base_frame.columns)}")
        log(f"Parquet OK: {output_path}")

        if parquet_output_path is None and output_path.exists():
            output_path.unlink()
        return 0
    except Exception as error:
        log(f"Exception: {error}", "ERROR")
        return 1


def validate_training_base_frame(
    plc_frame: pd.DataFrame,
    training_base_frame: pd.DataFrame,
) -> list[str]:
    errors: list[str] = []

    if list(training_base_frame.columns) != list(TRAINING_BASE_COLUMNS):
        errors.append("training_base 컬럼 집합이 스키마와 일치하지 않습니다.")
    if len(training_base_frame) != len(plc_frame):
        errors.append("training_base 행 수가 입력 PLC 행 수와 다릅니다.")

    parsed_row_timestamps = pd.to_datetime(
        training_base_frame["row_timestamp"],
        errors="coerce",
    )
    if bool(parsed_row_timestamps.isna().any()):
        errors.append("row_timestamp에 파싱할 수 없는 값이 있습니다.")

    cycle_present_series = training_base_frame["cycle_present_flag"].astype(bool)
    cycle_missing_series = training_base_frame["cycle_missing_flag"].astype(bool)
    if not cycle_present_series.eq(~cycle_missing_series).all():
        errors.append("cycle_present_flag와 cycle_missing_flag가 보완 관계가 아닙니다.")

    missing_temperature_row = find_row_by_timestamp(
        training_base_frame,
        MISSING_TEMPERATURE_TIMESTAMP,
    )
    if missing_temperature_row is None:
        errors.append("Temperature 결측 기준 행을 찾지 못했습니다.")
    else:
        if not pd.isna(missing_temperature_row["temperature"]):
            errors.append("Temperature 결측 행의 temperature가 결측으로 유지되지 않았습니다.")
        if not bool(missing_temperature_row["spot_temp_missing"]):
            errors.append("Temperature 결측 행의 spot_temp_missing이 True가 아닙니다.")
        if not bool(missing_temperature_row["idle_by_pressure_zero"]):
            errors.append("메인압력 0.0 행의 idle_by_pressure_zero가 True가 아닙니다.")

    label_conflict_row = find_row_by_timestamp(
        training_base_frame,
        LABEL_CONFLICT_TIMESTAMP,
    )
    if label_conflict_row is None:
        errors.append("label_conflict 기준 행을 찾지 못했습니다.")
    else:
        if not pd.isna(label_conflict_row["billet_cycle_id"]):
            errors.append("label_conflict 기준 행의 billet_cycle_id가 비어 있지 않습니다.")
        if float(label_conflict_row["production_counter"]) <= 0.0:
            errors.append("label_conflict 기준 행의 production_counter가 0보다 크지 않습니다.")
        if not bool(label_conflict_row["label_conflict"]):
            errors.append("Billet_CycleID 빈값 + 생산카운터 > 0 행의 label_conflict가 True가 아닙니다.")

    integrated_baseline_row = find_row_by_timestamp(
        training_base_frame,
        INTEGRATED_BASELINE_TIMESTAMP,
    )
    if integrated_baseline_row is None:
        errors.append("integrated log 기본 변환 기준 행을 찾지 못했습니다.")
    else:
        if float(integrated_baseline_row["temperature"]) != 502.5:
            errors.append("integrated log 기본 변환 행의 temperature가 예상값과 다릅니다.")
        if normalize_cycle_id_text(integrated_baseline_row["billet_cycle_id"]) != "1":
            errors.append("integrated log 기본 변환 행의 billet_cycle_id가 예상값과 다릅니다.")
        if not bool(integrated_baseline_row["cycle_present_flag"]):
            errors.append("integrated log 기본 변환 행의 cycle_present_flag가 True가 아닙니다.")
        if bool(integrated_baseline_row["cycle_missing_flag"]):
            errors.append("integrated log 기본 변환 행의 cycle_missing_flag가 True로 잘못 설정되었습니다.")
        if not bool(integrated_baseline_row["active_by_pressure_threshold"]):
            errors.append("메인압력 > 30.0 행의 active_by_pressure_threshold가 True가 아닙니다.")

    return errors


def validate_parquet_roundtrip(
    training_base_frame: pd.DataFrame,
    persisted_frame: pd.DataFrame,
) -> list[str]:
    errors: list[str] = []

    if list(persisted_frame.columns) != list(TRAINING_BASE_COLUMNS):
        errors.append("Parquet roundtrip 이후 컬럼 집합이 스키마와 일치하지 않습니다.")
    if len(persisted_frame) != len(training_base_frame):
        errors.append("Parquet roundtrip 이후 행 수가 달라졌습니다.")

    persisted_missing_row = find_row_by_timestamp(
        persisted_frame,
        MISSING_TEMPERATURE_TIMESTAMP,
    )
    if persisted_missing_row is None:
        errors.append("Parquet roundtrip 이후 Temperature 결측 기준 행을 찾지 못했습니다.")
    else:
        if not bool(persisted_missing_row["spot_temp_missing"]):
            errors.append("Parquet roundtrip 이후 spot_temp_missing 값이 유지되지 않았습니다.")

    persisted_conflict_row = find_row_by_timestamp(
        persisted_frame,
        LABEL_CONFLICT_TIMESTAMP,
    )
    if persisted_conflict_row is None:
        errors.append("Parquet roundtrip 이후 label_conflict 기준 행을 찾지 못했습니다.")
    else:
        if not bool(persisted_conflict_row["label_conflict"]):
            errors.append("Parquet roundtrip 이후 label_conflict 값이 유지되지 않았습니다.")

    return errors


def find_row_by_timestamp(
    frame: pd.DataFrame,
    row_timestamp: str,
) -> pd.Series | None:
    matched_rows = frame.loc[frame["row_timestamp"] == row_timestamp]
    if matched_rows.empty:
        return None
    return matched_rows.iloc[0]


def normalize_cycle_id_text(value: object) -> str:
    if pd.isna(value):
        return ""
    numeric_value = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(numeric_value):
        return str(value).strip()
    return str(int(float(numeric_value)))


def main() -> None:
    argument_parser = argparse.ArgumentParser(
        description="Verify training_base regression cases on a sample integrated log",
    )
    argument_parser.add_argument(
        "--plc-file",
        dest="plc_file",
        default=str(DEFAULT_SAMPLE_FILE),
        help="검증에 사용할 integrated log CSV 경로",
    )
    argument_parser.add_argument(
        "--output",
        dest="output_path",
        default=None,
        help="검증용 parquet 출력 경로",
    )
    parsed_args = argument_parser.parse_args()

    output_path = (
        Path(parsed_args.output_path).resolve()
        if parsed_args.output_path is not None
        else None
    )
    exit_code = verify_training_base(Path(parsed_args.plc_file).resolve(), output_path)
    raise SystemExit(exit_code)


if __name__ == "__main__":
    main()
