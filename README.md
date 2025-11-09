macOS의 경우:

source venv/bin/activate


Windows의 경우 (PowerShell):

.\venv\Scripts\Activate.ps1


2단계: 기존의 잘못된 데이터 삭제 (중요)


이전에 잘못된 날짜(2024년)로 업로드된 PLC 데이터를 삭제해야 합니다.


1. Supabase 프로젝트의 SQL Editor (https://app.supabase.com/project/mmlgifsxqsoophmjinpt/sql/new)로 이동하세요.
2. 아래 코드를 붙여넣고 "RUN"을 클릭합니다.

    DELETE FROM public.all_metrics WHERE device_id = 'extruder_plc';


3단계: 데이터 업로드 스크립트 실행


가상환경이 활성화된 터미널((venv) 표시 확인)에서 아래 명령어를 입력하여 스크립트를 실행합니다. 스크립트가 모든
파일을 읽고 올바른 날짜로 변환하여 Supabase에 업로드할 것입니다.


python process_and_upload.py


supabase DB 데이터 삭제
DELETE FROM public.all_metrics WHERE device_id = 'extruder_plc';
DELETE FROM public.all_metrics WHERE device_id = 'spot_temperature_sensor';
