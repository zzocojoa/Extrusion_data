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


def build_training_dataset_v1(training_base_file_path: Path, output_path: Path) -> Path:
    if not training_base_file_path.is_file():
        raise FileNotFoundError(
            f"training_base parquet를 찾을 수 없습니다: {training_base_file_path}"
        )

    training_base_frame = pd.read_parquet(training_base_file_path)
    training_dataset_v1_frame = build_training_dataset_v1_frame(training_base_frame)
    written_path = write_training_dataset_v1_parquet(training_dataset_v1_frame, output_path)

    print(f"[INFO] Wrote training dataset v1: {written_path}")
    print(f"[INFO] Rows: {len(training_dataset_v1_frame)}")
    print(f"[INFO] Columns: {', '.join(TRAINING_DATASET_V1_COLUMNS)}")
    return written_path


def main() -> None:
    argument_parser = argparse.ArgumentParser(
        description="Build training_dataset_v1.parquet from training_base.parquet",
    )
    argument_parser.add_argument(
        "--training-base-file",
        dest="training_base_file",
        required=True,
        help="입력 training_base.parquet 경로",
    )
    argument_parser.add_argument(
        "--output",
        dest="output_path",
        required=True,
        help="출력 training_dataset_v1.parquet 경로",
    )
    parsed_args = argument_parser.parse_args()

    training_base_file_path = Path(parsed_args.training_base_file).resolve()
    output_path = Path(parsed_args.output_path).resolve()
    build_training_dataset_v1(training_base_file_path, output_path)


if __name__ == "__main__":
    main()
