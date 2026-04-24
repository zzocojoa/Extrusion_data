# 사용 전제:
#   예약 실행 시 업로드까지 자동 시작하려면 %APPDATA%\ExtrusionUploader\config.ini
#   또는 .env / os.environ 에 AUTO_UPLOAD=true 가 설정되어 있어야 합니다.
#   AUTO_UPLOAD=false 이면 예약 작업은 GUI만 실행하고 업로드는 시작하지 않습니다.
# 사용 예시:
#   powershell -ExecutionPolicy Bypass -File scripts\install_autorun.ps1 -Mode Daily -ExePath "C:\Path\ExtrusionUploader.exe" -StartTime "01:00" -TaskName "Extrusion Uploader Daily"
#   powershell -ExecutionPolicy Bypass -File scripts\install_autorun.ps1 -Mode OnLogon -ExePath "C:\Path\ExtrusionUploader.exe" -DelayMinutes 1 -TaskName "Extrusion Uploader OnLogon"

param(
  [Parameter(Mandatory=$true)][ValidateSet('Daily','OnLogon')] [string]$Mode,
  [Parameter(Mandatory=$true)] [string]$ExePath,
  [string]$TaskName = 'Extrusion Uploader',
  [string]$Arguments = '',
  [string]$StartTime = '01:00',
  [int]$DelayMinutes = 1
)

if (-not (Test-Path $ExePath)) { throw "ExePath not found: $ExePath" }

$action = New-ScheduledTaskAction -Execute $ExePath -Argument $Arguments

switch ($Mode) {
  'Daily' {
    $trigger = New-ScheduledTaskTrigger -Daily -At $StartTime
  }
  'OnLogon' {
    $ts = New-ScheduledTaskTrigger -AtLogOn
    if ($DelayMinutes -gt 0) {
      $ts.Delay = (New-TimeSpan -Minutes $DelayMinutes)
    }
    $trigger = $ts
  }
}

$settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable
Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $trigger -Settings $settings -Force | Out-Null
Write-Host "Registered task '$TaskName' ($Mode) for $ExePath"
