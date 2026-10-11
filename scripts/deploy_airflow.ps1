param(
    [Parameter(Mandatory=$true)][string]$Image,
    [Parameter(Mandatory=$true)][ValidatePattern('^[0-9a-f]{40}$')][string]$Revision,
    [string]$DeploymentRoot = 'D:\projects\_deploy\ml-airflow-demo'
)
$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
function DockerCommand {
    param([string[]]$DockerArgs)
    & docker @DockerArgs
    if ($LASTEXITCODE -ne 0) { throw "docker failed ($LASTEXITCODE): $($DockerArgs -join ' ')" }
}
function ImageOf {
    param([string]$Service)
    $id = & docker ps -q --filter 'label=com.docker.compose.project=ml-airflow-demo' --filter "label=com.docker.compose.service=$Service"
    if ($LASTEXITCODE -ne 0 -or -not $id) { throw "$Service is not running" }
    $image = (& docker inspect $id | ConvertFrom-Json)[0].Config.Image
    if ($LASTEXITCODE -ne 0) { throw "Cannot inspect $Service image" }
    return $image
}

$oldAirflowImage = ImageOf 'airflow'
$platformImage = ImageOf 'mlflow'
$blueImage = ImageOf 'inference-blue'
$greenImage = ImageOf 'inference-green'
$gatewayId = & docker ps -q --filter 'label=com.docker.compose.project=ml-airflow-demo' --filter 'label=com.docker.compose.service=inference'
if ($LASTEXITCODE -ne 0 -or -not $gatewayId) { throw 'Inference gateway is not running' }
$serviceIds = @(& docker ps --format '{{.Names}}={{.ID}}' --filter 'label=com.docker.compose.project=ml-airflow-demo' |
    Where-Object { $_ -notmatch '^ml-airflow-demo-airflow-' } | Sort-Object)
DockerCommand @('pull', $Image)
$details = (& docker image inspect $Image | ConvertFrom-Json)[0]
if ($LASTEXITCODE -ne 0 -or $details.Config.Labels.'org.opencontainers.image.revision' -ne $Revision) {
    throw 'Airflow image revision does not match requested commit'
}
$sourceRoot = Split-Path -Parent $PSScriptRoot
$releaseDir = Join-Path $DeploymentRoot "airflow-releases\$Revision-$(Get-Date -Format 'yyyyMMddTHHmmssfff')"
New-Item -ItemType Directory -Force -Path $releaseDir | Out-Null
Copy-Item -LiteralPath (Join-Path $sourceRoot 'compose.yaml') -Destination $releaseDir
Copy-Item -LiteralPath (Join-Path $sourceRoot 'compose.release.yaml') -Destination $releaseDir
$composeArgs = @('compose', '-p', 'ml-airflow-demo', '-f', (Join-Path $releaseDir 'compose.yaml'), '-f', (Join-Path $releaseDir 'compose.release.yaml'))
$env:PLATFORM_IMAGE = $platformImage
$env:AIRFLOW_IMAGE = $Image
$env:BLUE_IMAGE = $blueImage
$env:GREEN_IMAGE = $greenImage
try {
    DockerCommand ($composeArgs + @('up', '-d', '--no-build', '--no-deps', '--wait', '--wait-timeout', '240', 'airflow'))
    $actual = & docker compose -p ml-airflow-demo -f (Join-Path $releaseDir 'compose.yaml') -f (Join-Path $releaseDir 'compose.release.yaml') exec -T airflow printenv DEMO_CODE_REVISION
    if ($LASTEXITCODE -ne 0 -or $actual.Trim() -ne $Revision) { throw 'Airflow is not running the requested revision' }
    $ready = $false
    for ($attempt = 0; $attempt -lt 30; $attempt++) {
        $dagList = & docker compose -p ml-airflow-demo -f (Join-Path $releaseDir 'compose.yaml') -f (Join-Path $releaseDir 'compose.release.yaml') exec -T airflow airflow dags list --local --output json
        if ($LASTEXITCODE -eq 0) {
            $dags = @(($dagList -join "`n") | ConvertFrom-Json)
            $ids = @($dags | ForEach-Object { $_.dag_id })
            if ('demo_04_train_and_publish' -in $ids -and 'demo_05_weekly_batch_scoring' -in $ids) { $ready = $true; break }
        }
        Start-Sleep -Seconds 4
    }
    if (-not $ready) { throw 'Required DAGs did not appear in released Airflow' }
    $errors = & docker compose -p ml-airflow-demo -f (Join-Path $releaseDir 'compose.yaml') -f (Join-Path $releaseDir 'compose.release.yaml') exec -T airflow airflow dags list-import-errors --local --output json
    if ($LASTEXITCODE -ne 0) { throw 'Cannot inspect Airflow import errors' }
    $importErrors = ($errors -join "`n") | ConvertFrom-Json
    if ($null -ne $importErrors -and @($importErrors).Count -gt 0) { throw 'Airflow has DAG import errors' }
    $after = @(& docker ps --format '{{.Names}}={{.ID}}' --filter 'label=com.docker.compose.project=ml-airflow-demo' |
        Where-Object { $_ -notmatch '^ml-airflow-demo-airflow-' } | Sort-Object)
    if (Compare-Object $serviceIds $after) { throw 'Another service changed during Airflow release' }
    @{ image=$Image; revision=$Revision; deployed_at=(Get-Date).ToUniversalTime().ToString('o') } |
        ConvertTo-Json | Set-Content -LiteralPath (Join-Path $releaseDir 'release.json')
    Write-Output "Airflow DAGs deployed: $Revision"
} catch {
    $failure = $_
    $env:AIRFLOW_IMAGE = $oldAirflowImage
    DockerCommand ($composeArgs + @('up', '-d', '--no-build', '--no-deps', '--wait', '--wait-timeout', '240', 'airflow'))
    throw $failure
}
