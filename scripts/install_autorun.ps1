# Usage examples:
#   powershell -ExecutionPolicy Bypass -File scripts\install_autorun.ps1 -Mode Daily -ExePath "C:\Path\ExtrusionUploaderCli.exe" -StartTime "01:00" -TaskName "Extrusion Uploader Daily" -Arguments "--range yesterday --lag 15 --check-lock --quick"
#   powershell -ExecutionPolicy Bypass -File scripts\install_autorun.ps1 -Mode OnLogon -ExePath "C:\Path\ExtrusionUploaderCli.exe" -DelayMinutes 1 -TaskName "Extrusion Uploader OnLogon" -Arguments "--range today --lag 15 --check-lock --quick"

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

