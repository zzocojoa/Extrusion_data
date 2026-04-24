from __future__ import annotations

import importlib.util
from pathlib import Path
from typing import Final

import pandas as pd


TRAINING_DATASET_V1_SCHEMA: Final[tuple[tuple[str, str], ...]] = (
    ("row_timestamp", "string"),
    ("spot_temperature", "Float64"),
    ("main_pressure", "Float64"),
    ("billet_length", "Float64"),
    ("container_temp_front", "Float64"),
    ("container_temp_back", "Float64"),
    ("production_count", "Float64"),
    ("current_speed", "Float64"),
    ("end_pos", "Float64"),
    ("integrated_temperature", "Float64"),
    ("mold1", "Float64"),
    ("mold2", "Float64"),
    ("mold3", "Float64"),
    ("mold4", "Float64"),
    ("mold5", "Float64"),
    ("mold6", "Float64"),
    ("billet_temp", "Float64"),
    ("at_pre", "Float64"),
    ("at_temp", "Float64"),
    ("die_id", "string"),
    ("billet_cycle_id", "string"),
    ("spot_temp_missing", "boolean"),
    ("cycle_present_flag", "boolean"),
    ("cycle_missing_flag", "boolean"),
    ("idle_by_pressure_zero", "boolean"),
    ("active_by_pressure_threshold", "boolean"),
    ("label_conflict", "boolean"),
    ("time_gap_ms", "Int64"),
    ("is_partial_row", "boolean"),
    ("row_quality_flag", "boolean"),
    ("idle_flag", "boolean"),
)
TRAINING_DATASET_V1_COLUMNS: Final[tuple[str, ...]] = tuple(
    column_name for column_name, _ in TRAINING_DATASET_V1_SCHEMA
)
REQUIRED_TRAINING_BASE_COLUMNS: Final[tuple[str, ...]] = (
    "row_timestamp",
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
    "die_id",
    "billet_cycle_id",
    "spot_temperature",
    "spot_temp_missing",
    "cycle_present_flag",
    "cycle_missing_flag",
    "idle_by_pressure_zero",
    "active_by_pressure_threshold",
    "label_conflict",
)
NUMERIC_OUTPUT_COLUMNS: Final[tuple[str, ...]] = (
    "spot_temperature",
    "main_pressure",
    "billet_length",
    "container_temp_front",
    "container_temp_back",
    "production_count",
    "current_speed",
    "end_pos",
    "integrated_temperature",
    "mold1",
    "mold2",
    "mold3",
    "mold4",
    "mold5",
    "mold6",
    "billet_temp",
    "at_pre",
    "at_temp",
)
PARTIAL_REQUIRED_COLUMNS: Final[tuple[str, ...]] = (
    "main_pressure",
    "production_count",
    "current_speed",
    "integrated_temperature",
    "die_id",
)


def build_training_dataset_v1_frame(training_base_frame: pd.DataFrame) -> pd.DataFrame:
    validate_training_base_columns(training_base_frame)

    dataset_frame = pd.DataFrame(index=training_base_frame.index)
    dataset_frame["row_timestamp"] = training_base_frame["row_timestamp"].astype("string")
    dataset_frame["spot_temperature"] = to_float_series(training_base_frame["spot_temperature"])
    dataset_frame["main_pressure"] = to_float_series(training_base_frame["main_pressure"])
    dataset_frame["billet_length"] = to_float_series(training_base_frame["billet_length"])
    dataset_frame["container_temp_front"] = to_float_series(
        training_base_frame["container_temp_front"]
    )
    dataset_frame["container_temp_back"] = to_float_series(
        training_base_frame["container_temp_rear"]
    )
    dataset_frame["production_count"] = to_float_series(
        training_base_frame["production_counter"]
    )
    dataset_frame["current_speed"] = to_float_series(training_base_frame["current_speed"])
    dataset_frame["end_pos"] = to_float_series(
        training_base_frame["extrusion_end_position"]
    )
    dataset_frame["integrated_temperature"] = to_float_series(
        training_base_frame["temperature"]
    )
    dataset_frame["mold1"] = to_float_series(training_base_frame["mold_1"])
    dataset_frame["mold2"] = to_float_series(training_base_frame["mold_2"])
    dataset_frame["mold3"] = to_float_series(training_base_frame["mold_3"])
    dataset_frame["mold4"] = to_float_series(training_base_frame["mold_4"])
    dataset_frame["mold5"] = to_float_series(training_base_frame["mold_5"])
    dataset_frame["mold6"] = to_float_series(training_base_frame["mold_6"])
    dataset_frame["billet_temp"] = to_float_series(training_base_frame["billet_temp"])
    dataset_frame["at_pre"] = to_float_series(training_base_frame["at_pre"])
    dataset_frame["at_temp"] = to_float_series(training_base_frame["at_temp"])
    dataset_frame["die_id"] = to_text_series(training_base_frame["die_id"])
    dataset_frame["billet_cycle_id"] = normalize_cycle_id_series(
        training_base_frame["billet_cycle_id"]
    )
    dataset_frame["spot_temp_missing"] = to_boolean_series(
        training_base_frame["spot_temp_missing"]
    )
    dataset_frame["cycle_present_flag"] = to_boolean_series(
        training_base_frame["cycle_present_flag"]
    )
    dataset_frame["cycle_missing_flag"] = to_boolean_series(
        training_base_frame["cycle_missing_flag"]
    )
    dataset_frame["idle_by_pressure_zero"] = to_boolean_series(
        training_base_frame["idle_by_pressure_zero"]
    )
    dataset_frame["active_by_pressure_threshold"] = to_boolean_series(
        training_base_frame["active_by_pressure_threshold"]
    )
    dataset_frame["label_conflict"] = to_boolean_series(
        training_base_frame["label_conflict"]
    )
    dataset_frame["time_gap_ms"] = build_time_gap_series(dataset_frame["row_timestamp"])
    dataset_frame["is_partial_row"] = build_partial_row_series(dataset_frame)
    dataset_frame["row_quality_flag"] = build_row_quality_series(dataset_frame)
    dataset_frame["idle_flag"] = build_idle_series(dataset_frame)
    return dataset_frame.loc[:, TRAINING_DATASET_V1_COLUMNS]


def write_training_dataset_v1_parquet(
    training_dataset_v1_frame: pd.DataFrame,
    output_path: Path,
) -> Path:
    parquet_engine = resolve_parquet_engine()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    training_dataset_v1_frame.to_parquet(output_path, index=False, engine=parquet_engine)
    return output_path


def validate_training_base_columns(training_base_frame: pd.DataFrame) -> None:
    missing_columns = [
        column_name
        for column_name in REQUIRED_TRAINING_BASE_COLUMNS
        if column_name not in training_base_frame.columns
    ]
    if missing_columns:
        raise ValueError(
            f"training_base parquet에 필요한 컬럼이 없습니다: {', '.join(missing_columns)}"
        )


def to_float_series(raw_series: pd.Series) -> pd.Series:
    return pd.to_numeric(raw_series, errors="coerce").astype("Float64")


def to_text_series(raw_series: pd.Series) -> pd.Series:
    text_series = raw_series.astype("string").str.strip()
    return text_series.mask(text_series.eq(""), pd.NA).astype("string")


def to_boolean_series(raw_series: pd.Series) -> pd.Series:
    return raw_series.fillna(False).astype("boolean")


def normalize_cycle_id_series(raw_series: pd.Series) -> pd.Series:
    text_series = to_text_series(raw_series)
    normalized_values: list[str | pd.NAType] = []
    for value in text_series.tolist():
        if value is pd.NA or pd.isna(value):
            normalized_values.append(pd.NA)
            continue
        numeric_value = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
        if pd.isna(numeric_value):
            normalized_values.append(str(value).strip())
            continue
        normalized_values.append(str(int(float(numeric_value))))
    return pd.Series(normalized_values, index=raw_series.index, dtype="string")


def build_time_gap_series(row_timestamp_series: pd.Series) -> pd.Series:
    parsed_timestamps = pd.to_datetime(row_timestamp_series, errors="coerce")
    if bool(parsed_timestamps.isna().any()):
        raise ValueError("training_base parquet에 파싱할 수 없는 row_timestamp가 있습니다.")
    return (
        parsed_timestamps.diff().dt.total_seconds().mul(1000).round().astype("Int64")
    )


def build_partial_row_series(dataset_frame: pd.DataFrame) -> pd.Series:
    partial_mask = pd.Series(False, index=dataset_frame.index, dtype="boolean")
    for column_name in PARTIAL_REQUIRED_COLUMNS:
        partial_mask = partial_mask | dataset_frame[column_name].isna().astype("boolean")
    return partial_mask.astype("boolean")


def build_row_quality_series(dataset_frame: pd.DataFrame) -> pd.Series:
    pressure_positive_mask = dataset_frame["main_pressure"].gt(0.0).fillna(False)
    speed_positive_mask = dataset_frame["current_speed"].gt(0.0).fillna(False)
    missing_cycle_with_activity_mask = (
        dataset_frame["cycle_missing_flag"].astype(bool)
        & (pressure_positive_mask | speed_positive_mask)
    )
    return (
        dataset_frame["label_conflict"].astype(bool)
        | dataset_frame["is_partial_row"].astype(bool)
        | missing_cycle_with_activity_mask
    ).astype("boolean")


def build_idle_series(dataset_frame: pd.DataFrame) -> pd.Series:
    pressure_zero_mask = dataset_frame["main_pressure"].eq(0.0).fillna(False)
    speed_zero_mask = dataset_frame["current_speed"].eq(0.0).fillna(False)
    production_zero_mask = dataset_frame["production_count"].eq(0.0).fillna(False)
    cycle_missing_mask = dataset_frame["cycle_missing_flag"].astype(bool)
    label_conflict_mask = dataset_frame["label_conflict"].astype(bool)
    return (
        pressure_zero_mask
        & speed_zero_mask
        & production_zero_mask
        & cycle_missing_mask
        & ~label_conflict_mask
    ).astype("boolean")


def resolve_parquet_engine() -> str:
    if importlib.util.find_spec("pyarrow") is not None:
        return "pyarrow"
    if importlib.util.find_spec("fastparquet") is not None:
        return "fastparquet"
    raise ModuleNotFoundError(
        "Parquet 엔진이 없습니다. 프로젝트 환경에 pyarrow 또는 fastparquet를 설치하세요."
    )
