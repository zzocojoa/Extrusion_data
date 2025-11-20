from typing import Callable, Optional

import httpx
import numpy as np
import pandas as pd

from .state import set_resume_offset


def upload_via_edge(
    edge_url: str,
    anon_key: str,
    df: pd.DataFrame,
    *,
    log: Callable[[str], None],
    resume_key: Optional[str] = None,
    start_index: int = 0,
    batch_size: int = 500,
    progress_cb=None,
) -> bool:
    """
    Common Edge Function uploader with resume support.
    """
    if df.empty:
        log("    - 유효 데이터 없음(건너뜀)")
        return True

    records = df.replace({np.nan: None}).to_dict(orient="records")
    headers = {"Authorization": f"Bearer {anon_key}", "Content-Type": "application/json"}
    total = len(records)
    start = max(0, min(start_index, total))

    if start > 0:
        log(f"    - 파일 재개 지점: {start}/{total}")
        if progress_cb:
            try:
                progress_cb(start, total)
            except Exception:
                pass

    for i in range(start, total, batch_size):
        batch = records[i : i + batch_size]
        try:
            r = httpx.post(edge_url, json=batch, headers=headers, timeout=30.0)
            if r.status_code >= 300:
                log(f"    업로드 실패 ({r.status_code}): {r.text[:200]}")
                return False
        except Exception as e:
            log(f"    업로드 예외: {e}")
            return False
        if resume_key:
            set_resume_offset(resume_key, min(i + len(batch), total))
        if progress_cb:
            try:
                progress_cb(min(i + len(batch), total), total)
            except Exception:
                pass

    log(f"    {len(records)}건 업로드 완료(Edge)")
    return True
