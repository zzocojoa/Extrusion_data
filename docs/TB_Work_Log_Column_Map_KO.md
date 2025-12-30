# tb_work_log 영문 컬럼 → XLSM 한글 매핑

기준 파일: `data/raw/압출일보 2호기(251224).xlsm`  
시트: `창녕 압출일보` (헤더 4행 기준)  
파싱 로직: `core/work_log.py`의 `parse_work_log_excel()`

## 컬럼 매핑

| tb_work_log 영문 컬럼 | XLSM 한글 컬럼 | 비고 |
| --- | --- | --- |
| id | - | DB 자동 증가 |
| created_at | - | DB 기본값(now) |
| machine_id | 공장 | `2호기(공장)` 형태로 가공 |
| start_time | 날짜 + 시작 | 날짜 보정 로직 적용 |
| end_time | 날짜 + 종료 | 자정 넘김 보정 적용 |
| die_id | DW No. | 금형 ID |
| worker_name | 생산자 |  |
| alloy_type | 재질 |  |
| target_billet_temp | 온도 | 빌렛 목표온도 |
| target_exit_temp | 525 | 파일 헤더 `525`를 출구온도로 매핑 |
| production_qty | 적합수량 |  |
| production_weight | 적합중량 |  |
| productivity | 생산성 |  |
| lot | LOT | `LOT` 또는 `LOT ` 모두 인식 |
| temper_type | 질별 |  |
| quenching_temp | 퀜칭온도 |  |
| stretching | 80 | 파일 헤더 `80`을 스트레칭으로 매핑 |
| total_weight | 중량 |  |
| ram | RAM |  |
| product_length | 길이 | `RAM`과 `이론단중` 사이의 `길이` 사용 |
| actual_unit_weight | 실단중 |  |
| defect_bubble | 기포 |  |
| defect_tearing | 뜯김 |  |
| defect_white_black_line | 백선/흑선 |  |
| defect_oxide | 산화물 |  |
| defect_scratch | 스크래치 |  |
| defect_bend | 휨 |  |
| defect_dimension | 치수 |  |
| defect_line | 라인 | 251224 파일에는 컬럼 없음(기본 NULL) |
| defect_etc | 기타 |  |
| start_cut | S | 251224 파일에는 컬럼 없음(기본 NULL) |
| end_cut | E | 251224 파일에는 컬럼 없음(기본 NULL) |
| op_note | OP Note (특이사항 입력란) |  |
| die_number | # | 금형 번호 |
| yield_rate | 수율 |  |
| work_date | - | `start_time` 기준 자동 계산(08~20=day) |
| shift_type | - | `start_time` 기준 day/night 계산 |
| source_file | - | 업로드 파일명 |
| source_row | - | 엑셀 행 번호 |
| upload_batch_id | - | 현재 업로드 로직에서 미사용 |
| data_quality_flags | - | 날짜 보정 여부/사유 기록 |

## 참고

- 파일의 `주/야` 컬럼은 **현재 로직에서 사용하지 않으며**, `shift_type`은 `start_time`으로 계산된다.
- `길이.1` 컬럼은 불량 항목 영역의 길이로, `product_length`에 사용하지 않는다.
