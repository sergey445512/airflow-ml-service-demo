param([string]$RunnerDirectory = 'D:\projects\_tools\actions-runner-ml-demo')
$ErrorActionPreference = 'Stop'
$observer = Join-Path $PSScriptRoot 'observe-actions.ps1'
if (Test-Path -LiteralPath $observer) {
    $activeObserver = Get-CimInstance Win32_Process -Filter "Name = 'powershell.exe'" -ErrorAction SilentlyContinue |
        Where-Object { $_.CommandLine -like '*observe-actions.ps1*' }
    if (-not $activeObserver) {
        Start-Process -FilePath 'powershell.exe' -ArgumentList @('-NoProfile','-ExecutionPolicy','Bypass','-File',"`"$observer`"") -WorkingDirectory $PSScriptRoot -WindowStyle Hidden | Out-Null
        Write-Output 'GitHub Actions observer started for architecture map'
    }
}
if (-not (Test-Path -LiteralPath (Join-Path $RunnerDirectory '.runner'))) {
    throw 'Register the GitHub runner before starting it.'
}
$existing = Get-Process -Name Runner.Listener -ErrorAction SilentlyContinue | Where-Object { $_.Path -eq (Join-Path $RunnerDirectory 'bin\Runner.Listener.exe') }
if ($existing) { Write-Output 'Runner already running'; exit 0 }
$process = Start-Process -FilePath (Join-Path $RunnerDirectory 'run.cmd') -WorkingDirectory $RunnerDirectory -WindowStyle Hidden -PassThru
Write-Output "Runner started, launcher PID $($process.Id). Logs: $RunnerDirectory\_diag"
