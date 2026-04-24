import importlib.util


def resolve_parquet_engine() -> str:
    if importlib.util.find_spec("pyarrow") is not None:
        return "pyarrow"
    if importlib.util.find_spec("fastparquet") is not None:
        return "fastparquet"
    raise ModuleNotFoundError(
        "Parquet 엔진이 없습니다. 프로젝트 환경에 pyarrow 또는 fastparquet를 설치하세요."
    )
