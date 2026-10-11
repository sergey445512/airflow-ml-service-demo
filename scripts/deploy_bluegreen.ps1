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
function ContainerImage {
    param([string]$Service)
    $id = & docker ps -q --filter 'label=com.docker.compose.project=ml-airflow-demo' --filter "label=com.docker.compose.service=$Service"
    if ($LASTEXITCODE -ne 0) { throw "Cannot inspect $Service" }
    if (-not $id) { return $null }
    $details = (& docker inspect $id | ConvertFrom-Json)[0]
    if ($LASTEXITCODE -ne 0) { throw "Cannot inspect $Service image" }
    return $details.Config.Image
}
function MapEvent {
    param([string]$Stage)
    try {
        $body = @{ stage=$Stage; revision=$Revision } | ConvertTo-Json -Compress
        Invoke-RestMethod -Method Post -Uri 'http://127.0.0.1:8090/api/deploy-event' -ContentType 'application/json' -Body $body -TimeoutSec 2 | Out-Null
    } catch { }
}

$sourceRoot = Split-Path -Parent $PSScriptRoot
$stateFile = Join-Path $DeploymentRoot 'current.json'
$gatewayId = & docker ps -q --filter 'label=com.docker.compose.project=ml-airflow-demo' --filter 'label=com.docker.compose.service=inference'
if ($LASTEXITCODE -ne 0 -or -not $gatewayId) { throw 'Inference gateway is not running; bootstrap the new topology first' }
$gatewayConfig = (& docker exec $gatewayId cat /etc/nginx/conf.d/default.conf) -join "`n"
$route = [regex]::Match($gatewayConfig, 'proxy_pass\s+http://inference-(blue|green):8000;')
if ($LASTEXITCODE -ne 0 -or -not $route.Success) {
    throw 'Cannot determine active inference slot from gateway configuration'
}
$active = $route.Groups[1].Value
$candidate = if ($active -eq 'blue') { 'green' } else { 'blue' }
$activeImage = ContainerImage "inference-$active"
if (-not $activeImage) { throw "Active inference-$active is not running" }
$airflowImage = ContainerImage 'airflow'
$platformImage = ContainerImage 'mlflow'
if (-not $airflowImage -or -not $platformImage) { throw 'Airflow and MLflow must be running' }
$stableIds = @(& docker ps --format '{{.Names}}={{.ID}}' --filter 'label=com.docker.compose.project=ml-airflow-demo' |
    Where-Object { $_ -notmatch '^ml-airflow-demo-(inference-blue|inference-green)-' } | Sort-Object)

MapEvent 'pull'
if (-not $SkipPull) { DockerCommand @('pull', $Image) }
$details = (& docker image inspect $Image | ConvertFrom-Json)[0]
if ($LASTEXITCODE -ne 0 -or $details.Config.Labels.'org.opencontainers.image.revision' -ne $Revision) {
    throw 'Service image revision does not match requested commit'
}
$resolvedImage = $Image
if ($Image -notmatch '@sha256:') {
    $repository = $Image.Substring(0, $Image.LastIndexOf(':'))
    $resolvedImage = $details.RepoDigests | Where-Object { $_ -like "${repository}@sha256:*" } | Select-Object -First 1
    if (-not $resolvedImage) { throw "Cannot resolve immutable digest for $Image" }
}

$releaseDir = Join-Path $DeploymentRoot "releases\$Revision-$(Get-Date -Format 'yyyyMMddTHHmmssfff')"
New-Item -ItemType Directory -Force -Path $releaseDir | Out-Null
Copy-Item -LiteralPath (Join-Path $sourceRoot 'compose.yaml') -Destination $releaseDir
Copy-Item -LiteralPath (Join-Path $sourceRoot 'compose.release.yaml') -Destination $releaseDir
$composeArgs = @('compose', '-p', 'ml-airflow-demo', '-f', (Join-Path $releaseDir 'compose.yaml'), '-f', (Join-Path $releaseDir 'compose.release.yaml'))
$env:PLATFORM_IMAGE = $platformImage
$env:AIRFLOW_IMAGE = $airflowImage
$env:BLUE_IMAGE = if ($candidate -eq 'blue') { $resolvedImage } else { $activeImage }
$env:GREEN_IMAGE = if ($candidate -eq 'green') { $resolvedImage } else { $activeImage }
$previousConfig = Join-Path $releaseDir 'previous-gateway.conf'
$nextConfig = Join-Path $releaseDir 'next-gateway.conf'
DockerCommand @('cp', "${gatewayId}:/etc/nginx/conf.d/default.conf", $previousConfig)
@"
server {
    listen 8000;
    server_name _;
    location / {
        proxy_pass http://inference-${candidate}:8000;
        add_header X-Serving-Slot ${candidate} always;
        proxy_set_header Host `$host;
        proxy_connect_timeout 5s;
        proxy_read_timeout 180s;
    }
}
"@ | Set-Content -LiteralPath $nextConfig -Encoding ascii

$switched = $false
try {
    MapEvent 'update'
    DockerCommand ($composeArgs + @('up', '-d', '--no-build', '--no-deps', '--wait', '--wait-timeout', '180', "inference-$candidate"))
    # This checks the exact code, MLflow alias, S3 model load and a real prediction.
    DockerCommand ($composeArgs + @('exec', '-T', "inference-$candidate", 'python', 'scripts/smoke_release.py', '--revision', $Revision))
    $after = @(& docker ps --format '{{.Names}}={{.ID}}' --filter 'label=com.docker.compose.project=ml-airflow-demo' |
        Where-Object { $_ -notmatch '^ml-airflow-demo-(inference-blue|inference-green)-' } | Sort-Object)
    if (Compare-Object $stableIds $after) { throw 'A platform or gateway container changed during service release' }
    MapEvent 'verify'
    $switched = $true
    DockerCommand @('cp', $nextConfig, "${gatewayId}:/etc/nginx/conf.d/default.conf")
    DockerCommand @('exec', $gatewayId, 'nginx', '-t')
    DockerCommand @('exec', $gatewayId, 'nginx', '-s', 'reload')
    DockerCommand ($composeArgs + @('exec', '-T', "inference-$candidate", 'python', 'scripts/smoke_release.py', '--revision', $Revision, '--base-url', 'http://inference:8000', '--expected-slot', $candidate))
    DockerCommand ($composeArgs + @('exec', '-T', "inference-$candidate", 'python', 'scripts/verify_stores.py'))
    DockerCommand ($composeArgs + @('exec', '-T', "inference-$candidate", 'python', 'scripts/verify_traffic.py'))
    $state = @{ image=$resolvedImage; revision=$Revision; active_slot=$candidate;
        platform_image=$platformImage; airflow_image=$airflowImage; release_dir=$releaseDir;
        deployed_at=(Get-Date).ToUniversalTime().ToString('o') }
    $stateJson = $state | ConvertTo-Json
    [IO.File]::WriteAllText((Join-Path $releaseDir 'release.json'), $stateJson)
    [IO.File]::WriteAllText($stateFile, $stateJson)
    MapEvent 'success'
    Write-Output "Service deployed: $Revision; gateway now points to inference-$candidate"
} catch {
    $failure = $_
    MapEvent 'rollback'
    if ($switched) {
        DockerCommand @('cp', $previousConfig, "${gatewayId}:/etc/nginx/conf.d/default.conf")
        DockerCommand @('exec', $gatewayId, 'nginx', '-t')
        DockerCommand @('exec', $gatewayId, 'nginx', '-s', 'reload')
    }
    throw $failure
}
