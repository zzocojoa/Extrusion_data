from __future__ import annotations

import argparse
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.training_base import (
    TRAINING_BASE_COLUMNS,
    build_training_base_frame,
    write_training_base_parquet,
)
from core.transform import build_records_plc, build_records_temp


def build_training_base(
    plc_file_path: Path,
    output_path: Path,
    filename_hint: str,
    spot_file_path: Path | None,
) -> Path:
    if not plc_file_path.is_file():
        raise FileNotFoundError(f"PLC CSV를 찾을 수 없습니다: {plc_file_path}")
    if spot_file_path is not None and not spot_file_path.is_file():
        raise FileNotFoundError(f"SPOT CSV를 찾을 수 없습니다: {spot_file_path}")

    plc_frame = build_records_plc(str(plc_file_path), filename_hint)
    if not hasattr(plc_frame, "empty") or plc_frame.empty:
        raise ValueError("PLC CSV에서 training_base를 만들 수 있는 데이터가 없습니다.")

    spot_frame = None
    if spot_file_path is not None:
        spot_frame = build_records_temp(str(spot_file_path), spot_file_path.name)
        if not hasattr(spot_frame, "empty") or spot_frame.empty:
            raise ValueError("SPOT CSV에서 사용할 수 있는 temperature 데이터가 없습니다.")

    training_base_frame = build_training_base_frame(plc_frame, spot_frame)
    written_path = write_training_base_parquet(training_base_frame, output_path)

    print(f"[INFO] Wrote training base: {written_path}")
    print(f"[INFO] Rows: {len(training_base_frame)}")
    print(f"[INFO] Columns: {', '.join(TRAINING_BASE_COLUMNS)}")
    return written_path


def main() -> None:
    argument_parser = argparse.ArgumentParser(
        description="Build training_base.parquet from raw CSV files",
    )
    argument_parser.add_argument(
        "--plc-file",
        dest="plc_file",
        required=True,
        help="PLC 또는 Factory Integrated Log CSV 경로",
    )
    argument_parser.add_argument(
        "--output",
        dest="output_path",
        required=True,
        help="출력 parquet 경로",
    )
    argument_parser.add_argument(
        "--filename-hint",
        dest="filename_hint",
        required=False,
        help="파일명 날짜 힌트. 생략 시 PLC 파일명을 사용",
    )
    argument_parser.add_argument(
        "--spot-file",
        dest="spot_file",
        required=False,
        help="SPOT temperature CSV 경로",
    )
    parsed_args = argument_parser.parse_args()

    plc_file_path = Path(parsed_args.plc_file).resolve()
    output_path = Path(parsed_args.output_path).resolve()
    filename_hint = (
        parsed_args.filename_hint
        if parsed_args.filename_hint is not None
        else plc_file_path.name
    )
    spot_file_path = (
        Path(parsed_args.spot_file).resolve()
        if parsed_args.spot_file is not None
        else None
    )
    build_training_base(plc_file_path, output_path, filename_hint, spot_file_path)


if __name__ == "__main__":
    main()
