# Build GUI (PyInstaller)
param(
    [string]$Spec = "ExtrusionUploader.spec",
    [string]$Icon = "assets/app.ico"
)

# Activate venv if present
if (Test-Path "venv/Scripts/Activate.ps1") {
    . "venv/Scripts/Activate.ps1"
}

if (-not (Test-Path $Spec)) {
    Write-Error "Spec file not found: $Spec"
    exit 1
}

# If a .spec is provided, don't pass extra options (PyInstaller rejects --icon with spec)
if ($Spec.ToLower().EndsWith(".spec")) {
    pyinstaller --clean $Spec
}
else {
    # Build directly from script with icon/name
    pyinstaller --clean --onefile --noconsole --name ExtrusionUploader --icon $Icon $Spec
}

Write-Host "GUI build complete. Output: dist/ExtrusionUploader.exe"
