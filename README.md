# 설정 (필수)

프로젝트 루트에 `.env` 파일을 생성하고 다음 내용을 설정해야 합니다
(`.env.example` 참고):

```ini
SUPABASE_URL=http://localhost:54321
DB_PASSWORD=your_password
```

http://127.0.0.1:54323

# 가상환경 실행

# macOS

source venv/bin/activate

# Windows

.\venv\Scripts\Activate.ps1

# 실행 방법

## 1. 실행 파일 (권장)

`dist/ExtrusionUploader.exe` 파일을 더블 클릭하여 실행합니다. (별도의 설치나 Python 환경 설정이 필요 없습니다.)

## 2. 소스 코드 실행 (개발용)

# 가상환경 활성화

.\venv\Scripts\Activate.ps1

# 실행

python uploader_gui_tk.py

# 빌드하는 방식

# uploader_gui_tk.py pyinstaller

pyinstaller --onefile --noconsole --name ExtrusionUploader --icon assets\app.ico
--collect-data certifi --collect-data pandas --collect-data numpy
uploader_gui_tk.py pyinstaller --clean ExtrusionUploader.spec

# uploader_cli.py pyinstaller

pyinstaller --onefile --noconsole --name ExtrusionUploaderCli --icon
assets\app.ico --collect-data certifi --collect-data pandas --collect-data numpy

# supabase CLI Data backup(우분투)

1. 경로 이동 cd "/mnt/c/Users/user/Documents/GitHub/Extrusion_data"
2. 백업 진행 ./supabase/auto/backup_daily.sh
3. 결과 확인 ls -lh backups/ (USB 마운트 시 E:\backups 폴더에도 자동 복사됨)

# supabase SQL Editor save

./supabase/auto/save_gui_changes.sh

# Docker container -> root shell 진입

1. Linux Shell (bash/sh) 진입 docker exec -it supabase-db bash
2. PostgreSQL 클라이언트 진입 psql -U postgres -d postgres
3. 이후 권한 명령 실행

```
```
