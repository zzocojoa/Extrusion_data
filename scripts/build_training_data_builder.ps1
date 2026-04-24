param(
    [string]$Spec = "TrainingDataBuilder.spec"
)

if (Test-Path "venv/Scripts/Activate.ps1") {
    . "venv/Scripts/Activate.ps1"
}

if (-not (Test-Path $Spec)) {
    Write-Error "Spec not found: $Spec"
    exit 1
}

$pyinstallerPath = "venv/Scripts/pyinstaller.exe"
if (-not (Test-Path $pyinstallerPath)) {
    Write-Error "PyInstaller not found: $pyinstallerPath"
    exit 1
}

$appDataRoot = Join-Path (Resolve-Path "tools").Path "_pyinstaller_appdata"
New-Item -ItemType Directory -Force -Path $appDataRoot | Out-Null

$env:PYTHONNOUSERSITE = "1"
$env:APPDATA = $appDataRoot

& $pyinstallerPath --noconfirm $Spec
if ($LASTEXITCODE -ne 0) {
    Write-Error "PyInstaller build failed with exit code $LASTEXITCODE"
    exit $LASTEXITCODE
}

Write-Host "Training builder build complete. Output: dist/TrainingDataBuilder.exe"
