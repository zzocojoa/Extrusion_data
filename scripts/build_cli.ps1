# Build CLI (PyInstaller)
param(
    [string]$Script = "uploader_cli.py",
    [string]$Name = "ExtrusionUploaderCli",
    [string]$Icon = "assets/app.ico"
)

if (Test-Path "venv/Scripts/Activate.ps1") {
    . "venv/Scripts/Activate.ps1"
}

if (-not (Test-Path $Script)) {
    Write-Error "Script not found: $Script"
    exit 1
}

pyinstaller --onefile --noconsole --name $Name --icon $Icon --collect-data certifi --collect-data pandas --collect-data numpy $Script

Write-Host "CLI build complete. Output: dist/${Name}.exe"
