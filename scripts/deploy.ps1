param(
    [Parameter(Mandatory=$true)][string]$Image,
    [Parameter(Mandatory=$true)][ValidatePattern('^[0-9a-f]{40}$')][string]$Revision,
    [string]$DeploymentRoot = 'D:\projects\_deploy\ml-airflow-demo',
    [switch]$SkipPull
)
$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
function DockerCommand {
    param([Parameter(ValueFromRemainingArguments=$true)][string[]]$DockerArgs)
    & docker @DockerArgs
    if ($LASTEXITCODE -ne 0) { throw "docker failed ($LASTEXITCODE): $($DockerArgs -join ' ')" }
}
$sourceRoot = Split-Path -Parent $PSScriptRoot
$releaseDir = Join-Path $DeploymentRoot "releases\$Revision"
New-Item -ItemType Directory -Force -Path $releaseDir | Out-Null
$stateFile = Join-Path $DeploymentRoot 'current.json'
$previousFile = Join-Path $releaseDir 'previous.compose.json'
$previousState = $null
$env:DEMO_IMAGE = $Image
if (-not $SkipPull) { DockerCommand pull $Image }
$actualRevision = & docker image inspect $Image --format '{{index .Config.Labels "org.opencontainers.image.revision"}}'
if ($LASTEXITCODE -ne 0 -or $actualRevision.Trim() -ne $Revision) { throw 'Image revision does not match requested commit' }

# Read the current Compose location from Docker, including the original dev setup.
$currentId = & docker ps -q --filter 'label=com.docker.compose.project=ml-airflow-demo' --filter 'label=com.docker.compose.service=airflow'
if ($LASTEXITCODE -ne 0) { throw 'Cannot inspect running deployment' }
if ($currentId) {
    DockerCommand exec $currentId python /opt/demo/scripts/check_idle.py
    $container = (& docker inspect $currentId | ConvertFrom-Json)[0]
    $configPaths = $container.Config.Labels.'com.docker.compose.project.config_files' -split ','
    $oldArgs = @('compose', '-p', 'ml-airflow-demo')
    foreach ($path in $configPaths) { $oldArgs += @('-f', $path) }
    $configJson = & docker @oldArgs config --format json
    if ($LASTEXITCODE -ne 0) { throw 'Cannot capture previous deployment configuration' }
    $previous = $configJson | ConvertFrom-Json
    foreach ($service in @('airflow', 'mlflow', 'inference', 's3-init')) {
        $previous.services.$service.image = $container.Image
        $previous.services.$service.PSObject.Properties.Remove('build')
    }
    [IO.File]::WriteAllText($previousFile, ($previous | ConvertTo-Json -Depth 40))
}
if (Test-Path -LiteralPath $stateFile) { $previousState = Get-Content -LiteralPath $stateFile -Raw }
Copy-Item -LiteralPath (Join-Path $sourceRoot 'compose.yaml') -Destination $releaseDir -Force
Copy-Item -LiteralPath (Join-Path $sourceRoot 'compose.release.yaml') -Destination $releaseDir -Force
$composeArgs = @('compose', '-p', 'ml-airflow-demo', '-f', (Join-Path $releaseDir 'compose.yaml'), '-f', (Join-Path $releaseDir 'compose.release.yaml'))
try {
    DockerCommand @composeArgs up -d --no-build --wait --wait-timeout 240
    DockerCommand @composeArgs exec -T inference python scripts/smoke_release.py --revision $Revision
    $state = @{ image=$Image; revision=$Revision; release_dir=$releaseDir; deployed_at=(Get-Date).ToUniversalTime().ToString('o') }
    [IO.File]::WriteAllText($stateFile, ($state | ConvertTo-Json))
    Write-Output "Deployed $Revision using $Image"
} catch {
    $failure = $_
    if (Test-Path -LiteralPath $previousFile) {
        Write-Warning 'Deployment failed; restoring previous containers. Data volumes are retained.'
        DockerCommand compose -p ml-airflow-demo -f $previousFile up -d --no-build --wait --wait-timeout 240
        if ($previousState) { [IO.File]::WriteAllText($stateFile, $previousState) }
    }
    throw $failure
}
