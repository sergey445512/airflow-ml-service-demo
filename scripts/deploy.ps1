param(
    [Parameter(Mandatory=$true)][string]$Image,
    [Parameter(Mandatory=$true)][ValidatePattern('^[0-9a-f]{40}$')][string]$Revision,
    [string]$DeploymentRoot = 'D:\projects\_deploy\ml-airflow-demo',
    [switch]$SkipPull
)
$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

function DockerCommand {
    param([string[]]$DockerArgs)
    & docker @DockerArgs
    if ($LASTEXITCODE -ne 0) { throw "docker failed ($LASTEXITCODE): $($DockerArgs -join ' ')" }
}

function MapEvent {
    param([string]$Stage)
    try {
        $body = @{ stage=$Stage; revision=$Revision } | ConvertTo-Json -Compress
        Invoke-RestMethod -Method Post -Uri 'http://127.0.0.1:8090/api/deploy-event' -ContentType 'application/json' -Body $body -TimeoutSec 2 | Out-Null
    } catch { } # The observer must never block a release.
}

function PlatformContainers {
    $running = @(& docker ps --format '{{.Names}}={{.ID}}' --filter 'label=com.docker.compose.project=ml-airflow-demo')
    if ($LASTEXITCODE -ne 0) { throw 'Cannot inspect the running platform' }
    return @($running | Where-Object { $_ -notmatch '^ml-airflow-demo-(inference|traffic)-' } | Sort-Object)
}

$sourceRoot = Split-Path -Parent $PSScriptRoot
$releaseDir = Join-Path $DeploymentRoot "releases\$Revision"
if (Test-Path -LiteralPath $releaseDir) {
    # A rollback can deploy the same revision again; keep the earlier release record.
    $releaseDir = Join-Path $DeploymentRoot "releases\$Revision-$(Get-Date -Format 'yyyyMMddTHHmmssfff')"
}
$stateFile = Join-Path $DeploymentRoot 'current.json'
$previousFile = Join-Path $releaseDir 'previous.compose.json'
$previousState = $null

# The platform image is the one already running. A service release never updates it.
$airflowId = & docker ps -q --filter 'label=com.docker.compose.project=ml-airflow-demo' --filter 'label=com.docker.compose.service=airflow'
if ($LASTEXITCODE -ne 0 -or -not $airflowId) { throw 'Airflow platform is not running' }
$platformImage = (& docker inspect $airflowId | ConvertFrom-Json)[0].Config.Image
if ($LASTEXITCODE -ne 0) { throw 'Cannot inspect the platform image' }
$platformRevision = (& docker image inspect $platformImage | ConvertFrom-Json)[0].Config.Labels.'org.opencontainers.image.revision'
if ($LASTEXITCODE -ne 0) { throw 'Cannot inspect the platform revision' }
$platformBefore = @(PlatformContainers)
if ($platformBefore.Count -lt 5) { throw 'Platform services are not all running' }

$env:PLATFORM_IMAGE = $platformImage
MapEvent -Stage 'pull'
if (-not $SkipPull) { DockerCommand -DockerArgs @('pull', $Image) }
$imageDetails = (& docker image inspect $Image | ConvertFrom-Json)[0]
if ($LASTEXITCODE -ne 0) { throw 'Cannot inspect the service image' }
if ($imageDetails.Config.Labels.'org.opencontainers.image.revision' -ne $Revision) {
    throw 'Service image revision does not match requested commit'
}
$resolvedImage = $Image
if ($Image -notmatch '@sha256:') {
    $repository = $Image.Substring(0, $Image.LastIndexOf(':'))
    $resolvedImage = $imageDetails.RepoDigests | Where-Object { $_ -like "${repository}@sha256:*" } | Select-Object -First 1
    if (-not $resolvedImage) { throw "Cannot resolve an immutable digest for $Image" }
}
$env:SERVICE_IMAGE = $resolvedImage

New-Item -ItemType Directory -Force -Path $releaseDir | Out-Null
$oldInferenceId = & docker ps -q --filter 'label=com.docker.compose.project=ml-airflow-demo' --filter 'label=com.docker.compose.service=inference'
if ($LASTEXITCODE -ne 0 -or -not $oldInferenceId) { throw 'Current inference service is not running' }
$oldInference = (& docker inspect $oldInferenceId | ConvertFrom-Json)[0]
if ($LASTEXITCODE -ne 0) { throw 'Cannot inspect current inference' }
$oldTrafficId = & docker ps -q --filter 'label=com.docker.compose.project=ml-airflow-demo' --filter 'label=com.docker.compose.service=traffic'
if ($LASTEXITCODE -ne 0 -or -not $oldTrafficId) { throw 'Current traffic generator is not running' }
$oldTraffic = (& docker inspect $oldTrafficId | ConvertFrom-Json)[0]
if ($LASTEXITCODE -ne 0) { throw 'Cannot inspect current traffic generator' }

$configPaths = $oldInference.Config.Labels.'com.docker.compose.project.config_files' -split ','
$oldArgs = @('compose', '-p', 'ml-airflow-demo')
foreach ($path in $configPaths) { $oldArgs += @('-f', $path) }
$configJson = & docker @oldArgs config --format json
if ($LASTEXITCODE -ne 0) { throw 'Cannot capture previous deployment configuration' }
$previous = $configJson | ConvertFrom-Json
$previous.services.inference.image = $oldInference.Config.Image
$previous.services.traffic.image = $oldTraffic.Config.Image
$previous.services.inference.PSObject.Properties.Remove('build')
$previous.services.traffic.PSObject.Properties.Remove('build')
[IO.File]::WriteAllText($previousFile, ($previous | ConvertTo-Json -Depth 40))
if (Test-Path -LiteralPath $stateFile) { $previousState = Get-Content -LiteralPath $stateFile -Raw }

Copy-Item -LiteralPath (Join-Path $sourceRoot 'compose.yaml') -Destination $releaseDir -Force
Copy-Item -LiteralPath (Join-Path $sourceRoot 'compose.release.yaml') -Destination $releaseDir -Force
Copy-Item -LiteralPath (Join-Path $sourceRoot 'monitoring') -Destination $releaseDir -Recurse -Force
$composeArgs = @('compose', '-p', 'ml-airflow-demo', '-f', (Join-Path $releaseDir 'compose.yaml'), '-f', (Join-Path $releaseDir 'compose.release.yaml'))
$targetArgs = @('up', '-d', '--no-build', '--no-deps', '--wait', '--wait-timeout', '180', 'inference', 'traffic')
try {
    # --no-deps and explicit service names leave Airflow, MLflow and all stores untouched.
    MapEvent -Stage 'update'
    DockerCommand -DockerArgs ($composeArgs + $targetArgs)
    $platformAfter = @(PlatformContainers)
    if (Compare-Object $platformBefore $platformAfter) { throw 'A platform container changed during the service release' }
    MapEvent -Stage 'verify'
    DockerCommand -DockerArgs ($composeArgs + @('exec', '-T', 'inference', 'python', 'scripts/smoke_release.py', '--revision', $Revision))
    DockerCommand -DockerArgs ($composeArgs + @('exec', '-T', 'inference', 'python', 'scripts/verify_stores.py'))
    DockerCommand -DockerArgs ($composeArgs + @('exec', '-T', 'inference', 'python', 'scripts/verify_traffic.py'))
    DockerCommand -DockerArgs @('exec', $airflowId, 'python', '/opt/demo/scripts/verify_monitoring.py')
    $state = @{ image=$resolvedImage; revision=$Revision; platform_image=$platformImage;
        platform_revision=$platformRevision; release_dir=$releaseDir;
        deployed_at=(Get-Date).ToUniversalTime().ToString('o') }
    $stateJson = $state | ConvertTo-Json
    [IO.File]::WriteAllText((Join-Path $releaseDir 'release.json'), $stateJson)
    [IO.File]::WriteAllText($stateFile, $stateJson)
    MapEvent -Stage 'success'
    Write-Output "Service deployed: $Revision; platform retained: $platformRevision"
} catch {
    $failure = $_
    MapEvent -Stage 'rollback'
    Write-Warning 'Service deployment failed; restoring previous inference and traffic containers.'
    DockerCommand -DockerArgs (@('compose', '-p', 'ml-airflow-demo', '-f', $previousFile) + $targetArgs)
    if ($previousState) { [IO.File]::WriteAllText($stateFile, $previousState) }
    throw $failure
}
