import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone

KST = timezone(timedelta(hours=9))

def clean_column_names(columns):
    """Remove newlines/whitespace from column headers."""
    cleaned = []
    for name in columns:
        name_str = str(name).replace("\n", "").replace("\r", "").strip()
        cleaned.append(name_str)
    return cleaned

def to_numeric(series):
    """Convert a Series with commas to numeric."""
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
    """Build start/end datetimes (KST), handling day rollover."""
    if pd.isna(date_value) or pd.isna(start_value) or pd.isna(end_value):
        return None, None

    try:
        # Handle Excel date (datetime object) or string
        if isinstance(date_value, datetime):
            base_date = date_value
        else:
            # Try parsing string format
            base_date = datetime.strptime(str(date_value).strip(), "%Y-%m-%d")
    except ValueError:
        return None, None

    def parse_time(value):
        # Handle Excel time (time object) or string
        if isinstance(value, (datetime, pd.Timestamp)): 
             return value.hour, value.minute
        
        # Check if it's a time object (datetime.time)
        if hasattr(value, 'hour') and hasattr(value, 'minute'):
             return value.hour, value.minute

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

    # Create aware datetime in KST
    start_dt = base_date.replace(hour=sh, minute=sm, second=0, microsecond=0, tzinfo=KST)
    end_dt = base_date.replace(hour=eh, minute=em, second=0, microsecond=0, tzinfo=KST)

    if end_dt < start_dt:
        end_dt += timedelta(days=1)
    if start_dt == end_dt:
        end_dt += timedelta(minutes=1)

    return start_dt, end_dt

def pick_column(df, candidates):
    for name in candidates:
        if name in df.columns:
            return name
    return None

def parse_work_log_excel(file_path):
    """
    Parse the Work Log Excel file and return a cleaned DataFrame.
    """
    df = None
    # Robust Search: Iterate all sheets and first 20 rows to find "공장"
    found_sheet = None
    found_header_idx = -1
    
    try:
        # Load all sheets to inspect
        xls = pd.ExcelFile(file_path)
        sheet_names = xls.sheet_names
        
        for sheet in sheet_names:
            # Read first 20 rows without header
            df_raw = pd.read_excel(file_path, sheet_name=sheet, header=None, nrows=20)
            
            # Search for "공장" in any cell
            for idx, row in df_raw.iterrows():
                # Convert row to string and check
                row_str = row.astype(str).str.replace(r"\s+", "", regex=True).values
                if any("공장" in str(x) for x in row_str):
                    found_sheet = sheet
                    found_header_idx = idx
                    break
            
            if found_sheet:
                break
                
        if found_sheet:
            # Reload with correct sheet and header
            df = pd.read_excel(file_path, sheet_name=found_sheet, header=found_header_idx)
            df.columns = clean_column_names(df.columns)
        else:
            # Debug info
            debug_msg = f"시트 목록: {sheet_names}"
            raise ValueError(f"모든 시트를 검색했으나 '공장' 헤더를 찾을 수 없습니다. ({debug_msg})")

    except Exception as e:
        raise ValueError(f"파일 읽기 중 오류 발생: {e}")

    if df is None:
        raise ValueError("데이터프레임 생성 실패")

    # Validation: Check for critical columns
    required_cols = ["날짜", "시작", "종료", "공장"]
    # Relaxed check: "공장" might be "공 장" or similar, but we cleaned columns.
    # Let's check again after cleaning.
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"필수 컬럼이 누락되었습니다: {', '.join(missing)} (발견된 컬럼: {list(df.columns)})")

    # Rename columns
    rename_candidates = {
        "525": "출구온도",
        "525.0": "출구온도",
        "출구온도": "출구온도",
        "80": "스트레칭",
        "80.0": "스트레칭",
        "스트레칭": "스트레칭",
    }
    df = df.rename(columns={k: v for k, v in rename_candidates.items() if k in df.columns})

    # Machine ID
    df["machine_id"] = df["공장"].apply(
        lambda x: f"2호기({str(x).strip()})" if pd.notna(x) and str(x).strip() else "2호기"
    )

    # Timestamps
    df[["start_time", "end_time"]] = df.apply(
        lambda row: pd.Series(make_timestamp(row.get("날짜"), row.get("시작"), row.get("종료"))),
        axis=1,
    )

    # Numeric conversions
    df["온도"] = to_numeric(df.get("온도"))
    
    exit_col = pick_column(df, ["출구온도"])
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone

KST = timezone(timedelta(hours=9))

def clean_column_names(columns):
    """Remove newlines/whitespace from column headers."""
    cleaned = []
    for name in columns:
        name_str = str(name).replace("\n", "").replace("\r", "").strip()
        cleaned.append(name_str)
    return cleaned

def to_numeric(series):
    """Convert a Series with commas to numeric."""
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
    """Build start/end datetimes (KST), handling day rollover."""
    if pd.isna(date_value) or pd.isna(start_value) or pd.isna(end_value):
        return None, None

    try:
        # Handle Excel date (datetime object) or string
        if isinstance(date_value, datetime):
            base_date = date_value
        else:
            # Try parsing string format
            base_date = datetime.strptime(str(date_value).strip(), "%Y-%m-%d")
    except ValueError:
        return None, None

    def parse_time(value):
        # Handle Excel time (time object) or string
        if isinstance(value, (datetime, pd.Timestamp)): 
             return value.hour, value.minute
        
        # Check if it's a time object (datetime.time)
        if hasattr(value, 'hour') and hasattr(value, 'minute'):
             return value.hour, value.minute

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

    # Create aware datetime in KST
    start_dt = base_date.replace(hour=sh, minute=sm, second=0, microsecond=0, tzinfo=KST)
    end_dt = base_date.replace(hour=eh, minute=em, second=0, microsecond=0, tzinfo=KST)

    if end_dt < start_dt:
        end_dt += timedelta(days=1)
    if start_dt == end_dt:
        end_dt += timedelta(minutes=1)

    return start_dt, end_dt

def pick_column(df, candidates):
    for name in candidates:
        if name in df.columns:
            return name
    return None

def parse_work_log_excel(file_path):
    """
    Parse the Work Log Excel file and return a cleaned DataFrame.
    """
    df = None
    # Robust Search: Iterate all sheets and first 20 rows to find "공장"
    found_sheet = None
    found_header_idx = -1
    
    try:
        # Load all sheets to inspect
        xls = pd.ExcelFile(file_path)
        sheet_names = xls.sheet_names
        
        for sheet in sheet_names:
            # Read first 20 rows without header
            df_raw = pd.read_excel(file_path, sheet_name=sheet, header=None, nrows=20)
            
            # Search for "공장" in any cell
            for idx, row in df_raw.iterrows():
                # Convert row to string and check
                row_str = row.astype(str).str.replace(r"\s+", "", regex=True).values
                if any("공장" in str(x) for x in row_str):
                    found_sheet = sheet
                    found_header_idx = idx
                    break
            
            if found_sheet:
                break
                
        if found_sheet:
            # Reload with correct sheet and header
            df = pd.read_excel(file_path, sheet_name=found_sheet, header=found_header_idx)
            df.columns = clean_column_names(df.columns)
        else:
            # Debug info
            debug_msg = f"시트 목록: {sheet_names}"
            raise ValueError(f"모든 시트를 검색했으나 '공장' 헤더를 찾을 수 없습니다. ({debug_msg})")

    except Exception as e:
        raise ValueError(f"파일 읽기 중 오류 발생: {e}")

    if df is None:
        raise ValueError("데이터프레임 생성 실패")

    # Validation: Check for critical columns
    required_cols = ["날짜", "시작", "종료", "공장"]
    # Relaxed check: "공장" might be "공 장" or similar, but we cleaned columns.
    # Let's check again after cleaning.
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"필수 컬럼이 누락되었습니다: {', '.join(missing)} (발견된 컬럼: {list(df.columns)})")

    # Rename columns
    rename_candidates = {
        "525": "출구온도",
        "525.0": "출구온도",
        "출구온도": "출구온도",
        "80": "스트레칭",
        "80.0": "스트레칭",
        "스트레칭": "스트레칭",
    }
    df = df.rename(columns={k: v for k, v in rename_candidates.items() if k in df.columns})

    # Machine ID
    df["machine_id"] = df["공장"].apply(
        lambda x: f"2호기({str(x).strip()})" if pd.notna(x) and str(x).strip() else "2호기"
    )

    # Timestamps
    df[["start_time", "end_time"]] = df.apply(
        lambda row: pd.Series(make_timestamp(row.get("날짜"), row.get("시작"), row.get("종료"))),
        axis=1,
    )

    # Numeric conversions
    if "온도" in df.columns:
        df["온도"] = to_numeric(df.get("온도"))
    if "퀜칭온도" in df.columns:
        df["퀜칭온도"] = to_numeric(df.get("퀜칭온도"))
    if "스트레칭" in df.columns:
        df["스트레칭"] = to_numeric(df.get("스트레칭"))
    if "중량" in df.columns:
        df["중량"] = to_numeric(df.get("중량"))
    if "RAM" in df.columns:
        df["RAM"] = to_numeric(df.get("RAM"))
    if "길이" in df.columns:
        df["길이"] = to_numeric(df.get("길이"))
    if "실단중" in df.columns:
        df["실단중"] = to_numeric(df.get("실단중"))
    if "기포" in df.columns:
        df["기포"] = to_numeric(df.get("기포"))
    if "뜯김" in df.columns:
        df["뜯김"] = to_numeric(df.get("뜯김"))
    if "백선/흑선" in df.columns:
        df["백선/흑선"] = to_numeric(df.get("백선/흑선"))
    if "산화물" in df.columns:
        df["산화물"] = to_numeric(df.get("산화물"))
    if "스크래치" in df.columns:
        df["스크래치"] = to_numeric(df.get("스크래치"))
    if "휨" in df.columns:
        df["휨"] = to_numeric(df.get("휨"))
    if "치수" in df.columns:
        df["치수"] = to_numeric(df.get("치수"))
    if "라인" in df.columns:
        df["라인"] = to_numeric(df.get("라인"))
    if "기타" in df.columns:
        df["기타"] = to_numeric(df.get("기타"))
    if "S" in df.columns:
        df["S"] = to_numeric(df.get("S"))
    if "E" in df.columns:
        df["E"] = to_numeric(df.get("E"))

    exit_col = pick_column(df, ["출구온도"])
    if exit_col:
        df[exit_col] = to_numeric(df.get(exit_col))

    # Rounding Rules
    # 1. yield_rate: Keep 1 decimal place
    df["수율"] = to_numeric(df.get("수율")).round(1)
    
    # 2. productivity: Round at 1st decimal -> Integer
    df["생산성"] = to_numeric(df.get("생산성")).round(0)
    
    # 3. production_weight: Round at 1st decimal -> Integer
    df["적합중량"] = to_numeric(df.get("적합중량")).round(0)
    
    df["적합수량"] = to_numeric(df.get("적합수량"))
    
    # # column might be numeric or string, but usually integer. Let's keep as is or numeric?
    # User said it's a number (e.g. 6).
    df["#"] = to_numeric(df.get("#"))

    selected_columns = [
        "machine_id",
        "start_time",
        "end_time",
        "생산자",
        "DW No.",
        # "품명",  <-- Removed
        "재질",
        "LOT",
        "질별",
        "온도",
        "퀜칭온도",
        exit_col or "출구온도",
        "스트레칭",
        "중량",
        "RAM",
        "길이",
        "실단중",
        "적합수량",
        "적합중량",
        "생산성",
        "#",    # New
        "수율",  # New
        "기포",
        "뜯김",
        "백선/흑선",
        "산화물",
        "스크래치",
        "휨",
        "치수",
        "라인",
        "기타",
        "S",
        "E",
        "OP Note (특이사항 입력란)",
    ]

    final_df = df[[col for col in selected_columns if col in df.columns]].copy()
    final_df = final_df.dropna(subset=["start_time", "end_time"])

    final_df = final_df.rename(
        columns={
            "생산자": "worker_name",
            "DW No.": "die_id",
            # "품명": "product_name", <-- Removed
            "재질": "alloy_type",
            "온도": "target_billet_temp",
            exit_col or "출구온도": "target_exit_temp",
            "LOT": "lot",
            "질별": "temper_type",
            "퀜칭온도": "quenching_temp",
            "스트레칭": "stretching",
            "중량": "total_weight",
            "RAM": "ram",
            "길이": "product_length",
            "실단중": "actual_unit_weight",
            "적합수량": "production_qty",
            "적합중량": "production_weight",
            "생산성": "productivity",
            "#": "die_number",      # New
            "수율": "yield_rate",    # New
            "기포": "defect_bubble",
            "뜯김": "defect_tearing",
            "백선/흑선": "defect_white_black_line",
            "산화물": "defect_oxide",
            "스크래치": "defect_scratch",
            "휨": "defect_bend",
            "치수": "defect_dimension",
            "라인": "defect_line",
            "기타": "defect_etc",
            "S": "start_cut",
            "E": "end_cut",
            "OP Note (특이사항 입력란)": "op_note",
        }
    )
    
    # Fix: Ensure integer fields are Int64 (nullable)
    for col in [
        "production_qty",
        "die_number",
        "product_length",
        "defect_bubble",
        "defect_tearing",
        "defect_white_black_line",
        "defect_oxide",
        "defect_scratch",
        "defect_bend",
        "defect_dimension",
        "defect_line",
        "defect_etc",
        "start_cut",
        "end_cut",
    ]:
        if col in final_df.columns:
            final_df[col] = final_df[col].astype("Int64")

    # Fix: Rounding requirements (Integer casting for rounded values)
    if "productivity" in final_df.columns:
        final_df["productivity"] = final_df["productivity"].astype("Int64")
        
    if "production_weight" in final_df.columns:
        final_df["production_weight"] = final_df["production_weight"].astype("Int64")

    return final_df
