param(
    [string]$Repository = 'sergey445512/airflow-ml-service-demo',
    [string]$MapUrl = 'http://127.0.0.1:8090'
)
$ErrorActionPreference = 'Continue'
$gh = 'D:\projects\_tools\github-cli\bin\gh.exe'
if (-not (Test-Path -LiteralPath $gh)) { $gh = 'gh' }
while ($true) {
    try {
        $runs = & $gh run list --repo $Repository --workflow pipeline.yaml --limit 1 --json databaseId | ConvertFrom-Json
        if ($LASTEXITCODE -eq 0 -and $runs.Count -gt 0) {
            $run = & $gh run view $runs[0].databaseId --repo $Repository --json status,conclusion,url,headSha,jobs | ConvertFrom-Json
            if ($LASTEXITCODE -eq 0) {
                $payload = @{run=@{status=$run.status;conclusion=$run.conclusion;url=$run.url;
                    head_sha=$run.headSha;jobs=@($run.jobs | ForEach-Object {
                        @{name=$_.name;status=$_.status;conclusion=$_.conclusion}
                    })}} | ConvertTo-Json -Depth 8 -Compress
                Invoke-RestMethod -Method Post -Uri "$MapUrl/api/observer" -ContentType 'application/json' -Body $payload -TimeoutSec 4 | Out-Null
            }
        }
    } catch { }
    Start-Sleep -Seconds 6
}
