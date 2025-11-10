macOS??경우:

source venv/bin/activate


Windows??경우 (PowerShell):

.\venv\Scripts\Activate.ps1


2?�계: 기존???�못???�이????�� (중요)


?�전???�못???�짜(2024??�??�로?�된 PLC ?�이?��? ??��?�야 ?�니??


1. Supabase ?�로?�트??SQL Editor (https://app.supabase.com/project/mmlgifsxqsoophmjinpt/sql/new)�??�동?�세??
2. ?�래 코드�?붙여?�고 "RUN"???�릭?�니??

    DELETE FROM public.all_metrics WHERE device_id = 'extruder_plc';


3?�계: ?�이???�로???�크립트 ?�행


가?�환경이 ?�성?�된 ?��???(venv) ?�시 ?�인)?�서 ?�래 명령?��? ?�력?�여 ?�크립트�??�행?�니?? ?�크립트가 모든
?�일???�고 ?�바�??�짜�?변?�하??Supabase???�로?�할 것입?�다.


python process_and_upload.py


supabase DB ?�이????��
DELETE FROM public.all_metrics WHERE device_id = 'extruder_plc';
DELETE FROM public.all_metrics WHERE device_id = 'spot_temperature_sensor';

���� �켱 ����(Edge Function)

1) Edge Function ���� �غ�
   - �Լ� �ڵ�: supabase/functions/upload-metrics/index.ts
   - Supabase ��ú��� �Ǵ� CLI���� �Լ� ��ũ�� ����:
     - SUPABASE_SERVICE_ROLE_KEY = ���� �� Ű(���� ����)
     - SUPABASE_URL = https://<project-ref>.supabase.co
   - ����(��: Supabase ��ú��� Functions �� Deploy �Ǵ� CLI supabase functions deploy upload-metrics)

2) Ŭ���̾�Ʈ(�Ϲ� PC) ����
   - ������: ���� ����(�Ǵ� uploader_edge.py), PLC_data/, Temperature_data/, processed_files.log
   - .env �Ǵ� config�� �Ʒ��� ����(���� Ű�� ���� ����):
     - SUPABASE_URL=https://<project-ref>.supabase.co
     - SUPABASE_ANON_KEY=<anon-public-key>
     - ����: EDGE_FUNCTION_URL=https://<project-ref>.supabase.co/functions/v1/upload-metrics

3) ����(Edge)
   - ����ȯ�� Ȱ��ȭ ��: python uploader_edge.py
   - �Ǵ� PyInstaller�� ���� ���� ���� ���� �� ����Ŭ�� ����

���
- process_and_upload.py�� ���� ȯ��(���� Ű ����)���� ���� DB ���ε������ �����˴ϴ�.
- �Ϲ� ����� PC���� uploader_edge.py ����� �����մϴ�(anon Ű�� �ʿ�, ���� Ű ���� ����).

?�치 �??�행 (권장)

1. 가?�환�??�성
   - Windows (PowerShell):
     `python -m venv venv`
     `.\venv\Scripts\Activate.ps1`
   - macOS/Linux:
     `python3 -m venv venv`
     `source venv/bin/activate`

2. ?�존???�치
   `pip install -r requirements.txt`

3. ?�경 변???�정
   - `.env.example`??복사?�여 `.env` ?�성:
     - Windows: `copy .env.example .env`
     - macOS/Linux: `cp .env.example .env`
   - `.env`??`SUPABASE_URL`, `SUPABASE_SERVICE_KEY` ?�력

4. ?�행
   `python process_and_upload.py`

참고: `.gitignore`??`.env`, `venv/`, `processed_files.log`가 ?��? ?�함?�어 ?�습?�다.


# 빌드하는 방식
# uploader_gui_tk.py pyinstaller
pyinstaller --onefile --noconsole --name ExtrusionUploader --icon assets\app.ico --collect-data certifi --collect-data pandas --collect-data numpy uploader_gui_tk.py

# uploader_cli.py pyinstaller
pyinstaller --onefile --noconsole --name ExtrusionUploaderCli --icon assets\app.ico --collect-data certifi --collect-data pandas --collect-data numpy uploader_cli.py
