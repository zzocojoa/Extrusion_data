# 가상환경 실행
# macOS
source venv/bin/activate

# Windows
.\venv\Scripts\Activate.ps1


# 빌드하는 방식
# uploader_gui_tk.py pyinstaller
pyinstaller --onefile --noconsole --name ExtrusionUploader --icon assets\app.ico --collect-data certifi --collect-data pandas --collect-data numpy uploader_gui_tk.py

# uploader_cli.py pyinstaller
pyinstaller --onefile --noconsole --name ExtrusionUploaderCli --icon assets\app.ico --collect-data certifi --collect-data pandas --collect-data numpy uploader_cli.py
