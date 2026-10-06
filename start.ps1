$ErrorActionPreference = 'Stop'
$demoWslPath = (wsl.exe wslpath -a $PSScriptRoot).Trim()
wsl.exe -e bash "$demoWslPath/start.sh"
