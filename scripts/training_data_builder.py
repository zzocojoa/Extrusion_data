from __future__ import annotations

import argparse
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.build_training_base import build_training_base
from scripts.build_training_dataset_v1 import build_training_dataset_v1
from scripts.verify_training_base import DEFAULT_SAMPLE_FILE, verify_training_base
from scripts.verify_training_dataset_v1 import verify_training_dataset_v1


def build_base_command(parsed_args: argparse.Namespace) -> int:
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
    return 0


def build_v1_command(parsed_args: argparse.Namespace) -> int:
    training_base_file_path = Path(parsed_args.training_base_file).resolve()
    output_path = Path(parsed_args.output_path).resolve()
    build_training_dataset_v1(training_base_file_path, output_path)
    return 0


def build_all_command(parsed_args: argparse.Namespace) -> int:
    plc_file_path = Path(parsed_args.plc_file).resolve()
    base_output_path = Path(parsed_args.base_output_path).resolve()
    dataset_output_path = Path(parsed_args.dataset_output_path).resolve()
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
    build_training_base(plc_file_path, base_output_path, filename_hint, spot_file_path)
    build_training_dataset_v1(base_output_path, dataset_output_path)
    return 0


def verify_base_command(parsed_args: argparse.Namespace) -> int:
    plc_file_path = Path(parsed_args.plc_file).resolve()
    output_path = (
        Path(parsed_args.output_path).resolve()
        if parsed_args.output_path is not None
        else None
    )
    return verify_training_base(plc_file_path, output_path)


def verify_v1_command(parsed_args: argparse.Namespace) -> int:
    training_base_file_path = Path(parsed_args.training_base_file).resolve()
    output_path = (
        Path(parsed_args.output_path).resolve()
        if parsed_args.output_path is not None
        else None
    )
    return verify_training_dataset_v1(training_base_file_path, output_path)


def build_argument_parser() -> argparse.ArgumentParser:
    argument_parser = argparse.ArgumentParser(
        description="Unified training dataset builder CLI",
    )
    subparsers = argument_parser.add_subparsers(dest="command", required=True)

    build_base_parser = subparsers.add_parser(
        "build-base",
        help="Build training_base.parquet from raw CSV",
    )
    build_base_parser.add_argument("--plc-file", dest="plc_file", required=True)
    build_base_parser.add_argument("--output", dest="output_path", required=True)
    build_base_parser.add_argument("--filename-hint", dest="filename_hint")
    build_base_parser.add_argument("--spot-file", dest="spot_file")
    build_base_parser.set_defaults(handler=build_base_command)

    build_v1_parser = subparsers.add_parser(
        "build-v1",
        help="Build training_dataset_v1.parquet from training_base.parquet",
    )
    build_v1_parser.add_argument(
        "--training-base-file",
        dest="training_base_file",
        required=True,
    )
    build_v1_parser.add_argument("--output", dest="output_path", required=True)
    build_v1_parser.set_defaults(handler=build_v1_command)

    build_all_parser = subparsers.add_parser(
        "build-all",
        help="Build training_base.parquet and training_dataset_v1.parquet in sequence",
    )
    build_all_parser.add_argument("--plc-file", dest="plc_file", required=True)
    build_all_parser.add_argument(
        "--base-output",
        dest="base_output_path",
        required=True,
    )
    build_all_parser.add_argument(
        "--dataset-output",
        dest="dataset_output_path",
        required=True,
    )
    build_all_parser.add_argument("--filename-hint", dest="filename_hint")
    build_all_parser.add_argument("--spot-file", dest="spot_file")
    build_all_parser.set_defaults(handler=build_all_command)

    verify_base_parser = subparsers.add_parser(
        "verify-base",
        help="Verify training_base regression checks",
    )
    verify_base_parser.add_argument(
        "--plc-file",
        dest="plc_file",
        default=str(DEFAULT_SAMPLE_FILE),
    )
    verify_base_parser.add_argument("--output", dest="output_path")
    verify_base_parser.set_defaults(handler=verify_base_command)

    verify_v1_parser = subparsers.add_parser(
        "verify-v1",
        help="Verify training_dataset_v1 regression checks",
    )
    verify_v1_parser.add_argument(
        "--training-base-file",
        dest="training_base_file",
        required=True,
    )
    verify_v1_parser.add_argument("--output", dest="output_path")
    verify_v1_parser.set_defaults(handler=verify_v1_command)
    return argument_parser


def main() -> None:
    argument_parser = build_argument_parser()
    parsed_args = argument_parser.parse_args()
    exit_code = parsed_args.handler(parsed_args)
    raise SystemExit(exit_code)


if __name__ == "__main__":
    main()
