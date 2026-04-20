from __future__ import annotations

import importlib.util
from pathlib import Path
from typing import Final

import pandas as pd


PRESSURE_ACTIVE_THRESHOLD: Final[float] = 30.0
TRAINING_BASE_SCHEMA: Final[tuple[tuple[str, str], ...]] = (
    ("row_timestamp", "string"),
    ("main_pressure", "Float64"),
    ("billet_length", "Float64"),
    ("container_temp_front", "Float64"),
    ("container_temp_rear", "Float64"),
    ("production_counter", "Float64"),
    ("current_speed", "Float64"),
    ("extrusion_end_position", "Float64"),
    ("temperature", "Float64"),
    ("mold_1", "Float64"),
    ("mold_2", "Float64"),
    ("mold_3", "Float64"),
    ("mold_4", "Float64"),
    ("mold_5", "Float64"),
    ("mold_6", "Float64"),
    ("billet_temp", "Float64"),
    ("at_pre", "Float64"),
    ("at_temp", "Float64"),
    ("die_id", "string"),
    ("billet_cycle_id", "string"),
    ("spot_temperature", "Float64"),
    ("spot_temp_missing", "boolean"),
    ("cycle_present_flag", "boolean"),
    ("cycle_missing_flag", "boolean"),
    ("idle_by_pressure_zero", "boolean"),
    ("active_by_pressure_threshold", "boolean"),
    ("label_conflict", "boolean"),
)
TRAINING_BASE_COLUMNS: Final[tuple[str, ...]] = tuple(
    column_name for column_name, _ in TRAINING_BASE_SCHEMA
)
PLC_NUMERIC_COLUMNS: Final[tuple[str, ...]] = (
    "main_pressure",
    "billet_length",
    "container_temp_front",
    "container_temp_rear",
    "production_counter",
    "current_speed",
    "extrusion_end_position",
    "temperature",
    "mold_1",
    "mold_2",
    "mold_3",
    "mold_4",
    "mold_5",
    "mold_6",
    "billet_temp",
    "at_pre",
    "at_temp",
)


def build_training_base_frame(
    plc_frame: pd.DataFrame,
    spot_frame: pd.DataFrame | None,
) -> pd.DataFrame:
    prepared_plc_frame = _prepare_plc_frame(plc_frame)
    prepared_spot_frame = _prepare_spot_frame(spot_frame)
    merged_frame = prepared_plc_frame.merge(
        prepared_spot_frame,
        on="row_timestamp",
        how="left",
        sort=False,
        validate="m:1",
    )
    return _finalize_training_base_frame(merged_frame)


def write_training_base_parquet(
    training_base_frame: pd.DataFrame,
    output_path: Path,
) -> Path:
    parquet_engine = _resolve_parquet_engine()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    training_base_frame.to_parquet(output_path, index=False, engine=parquet_engine)
    return output_path


def _prepare_plc_frame(plc_frame: pd.DataFrame) -> pd.DataFrame:
    if plc_frame.empty:
        raise ValueError("PLC 데이터가 비어 있습니다.")
    if "timestamp" not in plc_frame.columns:
        raise ValueError("PLC 데이터에 timestamp 컬럼이 없습니다.")

    row_timestamp_series = plc_frame["timestamp"].astype("string")
    _validate_timestamp_series(
        row_timestamp_series,
        "PLC 데이터에 해석할 수 없는 timestamp가 있습니다.",
    )

    prepared_frame = plc_frame.copy()
    prepared_frame.insert(0, "row_timestamp", row_timestamp_series)

    for column_name in PLC_NUMERIC_COLUMNS:
        if column_name not in prepared_frame.columns:
            prepared_frame[column_name] = pd.Series(pd.NA, index=prepared_frame.index)
        prepared_frame[column_name] = pd.to_numeric(
            prepared_frame[column_name],
            errors="coerce",
        ).astype("Float64")

    prepared_frame["die_id"] = _coerce_text_series(
        prepared_frame.get("die_id"),
        prepared_frame.index,
    )
    prepared_frame["billet_cycle_id"] = _coerce_text_series(
        prepared_frame.get("billet_cycle_id"),
        prepared_frame.index,
    )

    return prepared_frame


def _prepare_spot_frame(spot_frame: pd.DataFrame | None) -> pd.DataFrame:
    empty_frame = pd.DataFrame(
        {
            "row_timestamp": pd.Series(dtype="string"),
            "spot_temperature": pd.Series(dtype="Float64"),
        }
    )
    if spot_frame is None:
        return empty_frame
    if spot_frame.empty:
        return empty_frame
    if "timestamp" not in spot_frame.columns:
        raise ValueError("SPOT 데이터에 timestamp 컬럼이 없습니다.")
    if "temperature" not in spot_frame.columns:
        raise ValueError("SPOT 데이터에 temperature 컬럼이 없습니다.")

    row_timestamp_series = spot_frame["timestamp"].astype("string")
    _validate_timestamp_series(
        row_timestamp_series,
        "SPOT 데이터에 해석할 수 없는 timestamp가 있습니다.",
    )
    duplicate_mask = row_timestamp_series.duplicated(keep=False)
    if bool(duplicate_mask.any()):
        raise ValueError("SPOT 데이터에 중복 timestamp가 있습니다.")

    return pd.DataFrame(
        {
            "row_timestamp": row_timestamp_series,
            "spot_temperature": pd.to_numeric(
                spot_frame["temperature"],
                errors="coerce",
            ).astype("Float64"),
        }
    )


def _finalize_training_base_frame(merged_frame: pd.DataFrame) -> pd.DataFrame:
    billet_cycle_series = merged_frame["billet_cycle_id"].astype("string")
    cycle_present_series = billet_cycle_series.notna()
    cycle_missing_series = ~cycle_present_series
    main_pressure_series = merged_frame["main_pressure"].astype("Float64")
    production_counter_series = merged_frame["production_counter"].astype("Float64")

    training_base_frame = pd.DataFrame(index=merged_frame.index)
    training_base_frame["row_timestamp"] = merged_frame["row_timestamp"].astype("string")

    for column_name in PLC_NUMERIC_COLUMNS:
        training_base_frame[column_name] = merged_frame[column_name].astype("Float64")

    training_base_frame["die_id"] = merged_frame["die_id"].astype("string")
    training_base_frame["billet_cycle_id"] = billet_cycle_series
    training_base_frame["spot_temperature"] = merged_frame["spot_temperature"].astype(
        "Float64"
    )
    training_base_frame["spot_temp_missing"] = (
        training_base_frame["spot_temperature"].isna().astype("boolean")
    )
    training_base_frame["cycle_present_flag"] = cycle_present_series.astype("boolean")
    training_base_frame["cycle_missing_flag"] = cycle_missing_series.astype("boolean")
    training_base_frame["idle_by_pressure_zero"] = (
        main_pressure_series.eq(0.0).fillna(False).astype("boolean")
    )
    training_base_frame["active_by_pressure_threshold"] = (
        main_pressure_series.gt(PRESSURE_ACTIVE_THRESHOLD)
        .fillna(False)
        .astype("boolean")
    )
    training_base_frame["label_conflict"] = (
        production_counter_series.gt(0.0)
        .fillna(False)
        .astype("boolean")
        & cycle_missing_series.astype("boolean")
    )

    return training_base_frame.loc[:, TRAINING_BASE_COLUMNS]


def _coerce_text_series(
    raw_series: pd.Series | None,
    index: pd.Index,
) -> pd.Series:
    if raw_series is None:
        return pd.Series(pd.NA, index=index, dtype="string")

    text_series = raw_series.astype("string").str.strip()
    empty_mask = text_series.eq("")
    text_series = text_series.mask(empty_mask, pd.NA)
    return text_series.astype("string")


def _validate_timestamp_series(
    timestamp_series: pd.Series,
    error_message: str,
) -> None:
    parsed_series = pd.to_datetime(timestamp_series, errors="coerce")
    if bool(parsed_series.isna().any()):
        raise ValueError(error_message)


def _resolve_parquet_engine() -> str:
    if importlib.util.find_spec("pyarrow") is not None:
        return "pyarrow"
    if importlib.util.find_spec("fastparquet") is not None:
        return "fastparquet"
    raise ModuleNotFoundError(
        "Parquet 엔진이 없습니다. 프로젝트 환경에 pyarrow 또는 fastparquet를 설치하세요."
    )
