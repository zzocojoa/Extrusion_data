# Build GUI (PyInstaller)
param(
    [string]$Spec = "ExtrusionUploader.spec",
    [string]$Icon = "assets/app.ico"
)

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$specPath = if ([System.IO.Path]::IsPathRooted($Spec)) { $Spec } else { Join-Path $repoRoot $Spec }
$pythonPath = Join-Path $repoRoot "venv/Scripts/python.exe"
$pyinstallerPath = Join-Path $repoRoot "venv/Scripts/pyinstaller.exe"
$i18nCheckerPath = Join-Path $repoRoot "scripts/check_i18n_keys.py"
$i18nDirectoryPath = Join-Path $repoRoot "assets/i18n"
$guiSmokeTestPath = Join-Path $repoRoot "scripts/gui_smoke_test.py"
$stateLockSmokeTestPath = Join-Path $repoRoot "scripts/state_lock_smoke.py"

# Activate venv if present
if (Test-Path (Join-Path $repoRoot "venv/Scripts/Activate.ps1")) {
    . (Join-Path $repoRoot "venv/Scripts/Activate.ps1")
}

if (-not (Test-Path $specPath)) {
    Write-Error "Spec file not found: $specPath"
    exit 1
}

if (-not (Test-Path $pyinstallerPath)) {
    Write-Error "PyInstaller not found: $pyinstallerPath"
    exit 1
}

$appDataRoot = Join-Path (Resolve-Path (Join-Path $repoRoot "tools")).Path "_pyinstaller_appdata"
New-Item -ItemType Directory -Force -Path $appDataRoot | Out-Null

$env:PYTHONNOUSERSITE = "1"
$env:APPDATA = $appDataRoot

if ((Test-Path $i18nCheckerPath) -and (Test-Path $i18nDirectoryPath)) {
    if (-not (Test-Path $pythonPath)) {
        Write-Error "Python interpreter not found for i18n validation: $pythonPath"
        exit 1
    }

    & $pythonPath $i18nCheckerPath --root $repoRoot --locale-dir $i18nDirectoryPath
    if ($LASTEXITCODE -ne 0) {
        Write-Error "i18n key validation failed with exit code $LASTEXITCODE"
        exit $LASTEXITCODE
    }
}

if (Test-Path $guiSmokeTestPath) {
    & $pythonPath $guiSmokeTestPath
    if ($LASTEXITCODE -ne 0) {
        Write-Error "GUI smoke test failed with exit code $LASTEXITCODE"
        exit $LASTEXITCODE
    }
}

if (Test-Path $stateLockSmokeTestPath) {
    & $pythonPath $stateLockSmokeTestPath
    if ($LASTEXITCODE -ne 0) {
        Write-Error "State lock smoke test failed with exit code $LASTEXITCODE"
        exit $LASTEXITCODE
    }
}

# If a .spec is provided, don't pass extra options (PyInstaller rejects --icon with spec)
if ($specPath.ToLower().EndsWith(".spec")) {
    & $pyinstallerPath --clean $specPath
}
else {
    # Build directly from script with icon/name
    & $pyinstallerPath --clean --onefile --noconsole --name ExtrusionUploader --icon $Icon $specPath
}

if ($LASTEXITCODE -ne 0) {
    Write-Error "PyInstaller build failed with exit code $LASTEXITCODE"
    exit $LASTEXITCODE
}

$distExePath = Join-Path $repoRoot "dist/ExtrusionUploader.exe"
if (Test-Path $distExePath) {
    $startupProcess = Start-Process -FilePath $distExePath -WorkingDirectory $repoRoot -PassThru
    Start-Sleep -Seconds 5
    $runningProcess = Get-Process -Id $startupProcess.Id -ErrorAction SilentlyContinue
    if ($null -eq $runningProcess) {
        Write-Error "Built GUI failed startup smoke test."
        exit 1
    }
    Stop-Process -Id $startupProcess.Id -Force
}

Write-Host "GUI build complete. Output: dist/ExtrusionUploader.exe"
