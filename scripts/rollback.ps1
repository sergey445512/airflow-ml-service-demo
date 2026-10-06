param(
    [Parameter(Mandatory=$true, Position=0)][ValidatePattern('^[0-9a-f]{40}$')][string]$Revision,
    [string]$DeploymentRoot = 'D:\projects\_deploy\ml-airflow-demo',
    [switch]$Preview
)
$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$stateFile = Join-Path $DeploymentRoot 'current.json'
if (-not (Test-Path -LiteralPath $stateFile)) { throw "Current deployment state is missing: $stateFile" }
$current = Get-Content -LiteralPath $stateFile -Raw | ConvertFrom-Json
if ($current.revision -eq $Revision) { throw "Revision $Revision is already deployed" }
if ($current.image -notmatch '^(ghcr\.io/[^@:]+)(?:@sha256:[0-9a-f]{64}|:[^@:]+)$') {
    throw "Cannot determine the GHCR package from current image: $($current.image)"
}
$repository = $Matches[1]
$image = "${repository}:$Revision"
$source = 'GHCR commit tag'

# New releases record their exact digest. Older releases can still use their commit tag.
$releaseRoot = Join-Path $DeploymentRoot 'releases'
if (Test-Path -LiteralPath $releaseRoot) {
    $records = foreach ($file in Get-ChildItem -LiteralPath $releaseRoot -Recurse -Filter 'release.json' -File) {
        $record = Get-Content -LiteralPath $file.FullName -Raw | ConvertFrom-Json
        if ($record.revision -eq $Revision -and $record.image -match "^$([regex]::Escape($repository))(?:@|:)") {
            [pscustomobject]@{ Image = $record.image; DeployedAt = [datetime]$record.deployed_at }
        }
    }
    $record = $records | Sort-Object DeployedAt -Descending | Select-Object -First 1
    if ($record) {
        $image = $record.Image
        $source = 'saved release digest'
    }
}

Write-Output "Current revision: $($current.revision)"
Write-Output "Rollback target: $Revision"
Write-Output "Image: $image ($source)"
if ($Preview) { return }

& (Join-Path $PSScriptRoot 'deploy.ps1') -Image $image -Revision $Revision -DeploymentRoot $DeploymentRoot
if (-not $?) { throw 'Rollback deployment failed' }
$deployed = Get-Content -LiteralPath $stateFile -Raw | ConvertFrom-Json
if ($deployed.revision -ne $Revision) { throw 'Rollback did not record the requested revision' }
Write-Output "Rollback completed: $Revision"
