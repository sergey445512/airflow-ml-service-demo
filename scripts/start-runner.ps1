param([string]$RunnerDirectory = 'D:\projects\_tools\actions-runner-ml-demo')
$ErrorActionPreference = 'Stop'
if (-not (Test-Path -LiteralPath (Join-Path $RunnerDirectory '.runner'))) {
    throw 'Register the GitHub runner before starting it.'
}
$existing = Get-Process -Name Runner.Listener -ErrorAction SilentlyContinue | Where-Object { $_.Path -eq (Join-Path $RunnerDirectory 'bin\Runner.Listener.exe') }
if ($existing) { Write-Output 'Runner already running'; exit 0 }
$process = Start-Process -FilePath (Join-Path $RunnerDirectory 'run.cmd') -WorkingDirectory $RunnerDirectory -WindowStyle Hidden -PassThru
Write-Output "Runner started, launcher PID $($process.Id). Logs: $RunnerDirectory\_diag"
