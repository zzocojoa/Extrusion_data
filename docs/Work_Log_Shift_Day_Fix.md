# tb_work_log 작업일자/주야 기준 정리

## 문제 요약
- 현재 일부 집계가 **자정(00:00) 기준**으로 동작하면서 날짜가 뒤섞임
- 예: 12/17에 00:20 시작 기록은 **실제로 12/18 작업**인데 12/17로 묶여 순서가 꼬임

## 원인
- `date_trunc('day', start_time AT TIME ZONE ...) AT TIME ZONE ...` 같은 **이중 타임존 변환**으로
  날짜 경계가 09:00처럼 밀려 계산됨

## 정리된 기준(고정 규칙)
- **작업일자(work_date)**: KST 캘린더 날짜 기준
- **주/야(shift_type)**: 시간대 기준
  - 08:00~20:00 = 주간(day)
  - 20:00~다음날 08:00 = 야간(night)

추천 계산식
```
work_date = (start_time AT TIME ZONE 'Asia/Seoul')::date
shift_type = CASE
  WHEN (start_time AT TIME ZONE 'Asia/Seoul')::time >= time '08:00'
   AND (start_time AT TIME ZONE 'Asia/Seoul')::time < time '20:00'
  THEN 'day'
  ELSE 'night'
END
```

이렇게 하면:
- 00:20 시작은 **다음날(work_date)**로 정상 분류됨
- 주/야는 시간대 기준으로 일관됨

## 적용 방식
### A안(권장): 조회/대시보드에서 계산 컬럼 사용
- 기존 데이터는 유지하고, 쿼리에서 `work_date/shift_type` 계산

예시
```sql
WITH base AS (
  SELECT
    (start_time AT TIME ZONE 'Asia/Seoul')::date AS work_date,
    CASE
      WHEN (start_time AT TIME ZONE 'Asia/Seoul')::time >= time '08:00'
       AND (start_time AT TIME ZONE 'Asia/Seoul')::time < time '20:00'
      THEN 'day'
      ELSE 'night'
    END AS shift_type,
    *
  FROM public.tb_work_log
)
SELECT work_date, shift_type, die_id, COUNT(*) AS row_count
FROM base
GROUP BY work_date, shift_type, die_id
ORDER BY work_date, shift_type, die_id;
```

### B안: 물리 컬럼 추가 + 백필
- `work_date`, `shift_type` 컬럼을 추가하고 백필
- 이후 업로드 시 자동 계산(트리거) 적용

## 실무 개선 제안(마이그레이션 기준)
### 1) 작업일자/주야 계산 고정
- `work_date`, `shift_type` 생성 컬럼 또는 백필 컬럼 유지

### 2) 중복 방지 키 명확화
- `source_file`, `source_row`, `upload_batch_id` 추가
- 유니크 키 후보: `(machine_id, start_time, end_time, die_id, die_number, lot)`

### 3) 데이터 품질 체크
- `production_weight >= 0`, `productivity >= 0`, 결함 수량 `>= 0`
- `end_time > start_time` 유지

### 4) 인덱스 개선
- `work_date`, `shift_type` 인덱스
- `machine_id + start_time` 또는 `start_time` 단독 인덱스

## 결론
- **KST 날짜 기준으로 작업일자를 계산**하고
- **주/야는 시간대 기준으로만 분리**하면
  00:20과 같은 케이스가 정상적으로 12/18로 분류됩니다.
