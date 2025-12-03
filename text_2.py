import os
from datetime import datetime, timedelta

import pandas as pd


def clean_column_names(columns):
    """Remove newlines/whitespace from column headers while keeping duplicate suffixes."""
    cleaned = []
    for name in columns:
        name_str = str(name).replace("\n", "").replace("\r", "").strip()
        cleaned.append(name_str)
    return cleaned


def to_numeric(series):
    """Convert a Series with commas to numeric; returns empty series if input missing."""
    if series is None:
        return pd.Series(dtype=float)
    return (
        series.astype(str)
        .str.replace(",", "")
        .str.strip()
        .replace({"": None})
        .pipe(pd.to_numeric, errors="coerce")
    )


def make_timestamp(date_value, start_value, end_value):
    """Build start/end datetimes, handling day rollover and zero-length spans."""
    if pd.isna(date_value) or pd.isna(start_value) or pd.isna(end_value):
        return None, None

    try:
        base_date = datetime.strptime(str(date_value).strip(), "%Y-%m-%d")
    except ValueError:
        return None, None

    def parse_time(value):
        parts = str(value).strip().split(":")
        if len(parts) != 2:
            raise ValueError("Invalid time")
        hour, minute = map(int, parts)
        return hour, minute

    try:
        sh, sm = parse_time(start_value)
        eh, em = parse_time(end_value)
    except Exception:
        return None, None

    start_dt = base_date.replace(hour=sh, minute=sm)
    end_dt = base_date.replace(hour=eh, minute=em)

    if end_dt < start_dt:
        end_dt += timedelta(days=1)
    if start_dt == end_dt:
        end_dt += timedelta(minutes=1)

    return start_dt, end_dt


def pick_column(df, candidates):
    """Return the first existing column name from candidates or None."""
    for name in candidates:
        if name in df.columns:
            return name
    return None


def load_csv(path):
    """
    Load the CSV handling the 2호기 daily report format.
    - Newer file: 3 metadata rows, then header.
    - Older file: header at row index 1.
    """
    for header_row in (3, 1):
        try:
            df = pd.read_csv(path, header=header_row, encoding="utf-8-sig")
            df.columns = clean_column_names(df.columns)
            if "공장" in df.columns:
                return df
        except Exception:
            continue
    raise ValueError("CSV 파일 구조를 인식하지 못했습니다.")


def main():
    current_dir = os.path.dirname(os.path.abspath(__file__))

    candidate_inputs = [
        "압출일보 2호기(251125)csv.csv",
        "2025년 2호기csv.csv",
    ]
    input_file = None
    for cand in candidate_inputs:
        cand_path = os.path.join(current_dir, cand)
        if os.path.exists(cand_path):
            input_file = cand_path
            break

    if not input_file:
        raise FileNotFoundError("입력 CSV 파일을 찾을 수 없습니다.")

    output_file = os.path.join(current_dir, "supabase_upload_final_v3_from_2ho.csv")

    df = load_csv(input_file)

    # Normalize column names that differ between extracts.
    rename_candidates = {
        "525": "출구온도",  # mis-labeled exit temperature column in 251125 file
        "출구온도": "출구온도",  # keep consistent when present
    }
    df = df.rename(columns={k: v for k, v in rename_candidates.items() if k in df.columns})

    # Build machine_id from the known machine (2호기) and the 공장 값 when available.
    df["machine_id"] = df["공장"].apply(
        lambda x: f"2호기({str(x).strip()})" if pd.notna(x) and str(x).strip() else "2호기"
    )

    # Create start/end timestamps.
    df[["start_time", "end_time"]] = df.apply(
        lambda row: pd.Series(make_timestamp(row.get("날짜"), row.get("시작"), row.get("종료"))),
        axis=1,
    )

    # Numeric conversions.
    df["온도"] = to_numeric(df.get("온도"))

    exit_col = pick_column(df, ["출구온도"])
    if exit_col:
        df[exit_col] = to_numeric(df.get(exit_col))

    df["적합수량"] = to_numeric(df.get("적합수량"))
    df["적합중량"] = to_numeric(df.get("적합중량"))
    df["생산성"] = to_numeric(df.get("생산성"))

    selected_columns = [
        "machine_id",
        "start_time",
        "end_time",
        "생산자",
        "DW No.",
        "품명",
        "재질",
        "온도",
        exit_col or "출구온도",
        "적합수량",
        "적합중량",
        "생산성",
    ]

    final_df = df[[col for col in selected_columns if col in df.columns]].copy()
    final_df = final_df.dropna(subset=["start_time", "end_time"])

    final_df = final_df.rename(
        columns={
            "생산자": "worker_name",
            "DW No.": "die_id",
            "품명": "product_name",
            "재질": "alloy_type",
            "온도": "target_billet_temp",
            exit_col or "출구온도": "target_exit_temp",
            "적합수량": "production_qty",
            "적합중량": "production_weight",
            "생산성": "productivity",
        }
    )

    final_df.to_csv(output_file, index=False)

    print("추출 완료")
    print(f"입력 파일: {input_file}")
    print(f"출력 파일: {output_file}")
    print(f"총 행 수: {len(final_df)}")


if __name__ == "__main__":
    main()
