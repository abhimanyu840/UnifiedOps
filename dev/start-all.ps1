# =============================================================================
#   UnifiedOps v2 - dev one-shot launcher
# -----------------------------------------------------------------------------
#   Single command to bring up the full local dev stack:
#       1. 14 InfluxDB containers via direct `podman run`
#          (the same names + tokens + buckets as podman-compose.yml,
#          but no external docker-compose / podman-compose binary needed)
#       2. 11 syslog trap listeners
#       3. FastAPI backend (BACKEND_PORT, default 8001)
#       4. Trap-sender UI (TRAP_UI_PORT, default 7700)
#
#   Idempotent: re-running starts only what's not already up.
#       -Flush       wipe + recreate all containers/volumes from scratch
#       -SkipPodman  Influx is already running (e.g. native install)
#
#   Usage:
#       cd dev
#       .\start-all.ps1                 # normal start
#       .\start-all.ps1 -Flush          # wipe + recreate everything
#       .\start-all.ps1 -SkipPodman     # only python services
# =============================================================================
[CmdletBinding()]
param(
    [switch] $Flush,
    [switch] $SkipPodman
)

# Native commands like `podman` and `npm` write progress to stderr,
# which PowerShell 5.1 surfaces as a NativeCommandError when
# $ErrorActionPreference='Stop'. Use Continue at the script level and
# guard the critical sections with explicit $LASTEXITCODE checks below.
$ErrorActionPreference = 'Continue'

$DevDir = $PSScriptRoot
$Root   = Split-Path -Parent $DevDir
$Venv   = Join-Path $Root '.venv\Scripts\python.exe'

if (-not (Test-Path $Venv)) {
    Write-Host "FATAL: $Venv not found - create the dev venv first" -ForegroundColor Red
    exit 1
}

. (Join-Path $DevDir 'load-env.ps1')

# ----- container topology -------------------------------------------------
$Image = 'docker.io/influxdb:2.7'

$Containers = @(
    @{ Name='unifiedops-influx-hitachi-cdvl';     Port=$env:HITACHI_CDVL_PORT;    Bucket=$env:HITACHI_CDVL_BUCKET;    Token=$env:HITACHI_CDVL_TOKEN    },
    @{ Name='unifiedops-influx-hitachi-bcp';      Port=$env:HITACHI_BCP_PORT;     Bucket=$env:HITACHI_BCP_BUCKET;     Token=$env:HITACHI_BCP_TOKEN     },
    @{ Name='unifiedops-influx-hitachi-sify';     Port=$env:HITACHI_SIFY_PORT;    Bucket=$env:HITACHI_SIFY_BUCKET;    Token=$env:HITACHI_SIFY_TOKEN    },
    @{ Name='unifiedops-influx-brocade-cdvl';     Port=$env:BROCADE_CDVL_PORT;    Bucket=$env:BROCADE_CDVL_BUCKET;    Token=$env:BROCADE_CDVL_TOKEN    },
    @{ Name='unifiedops-influx-brocade-bcp';      Port=$env:BROCADE_BCP_PORT;     Bucket=$env:BROCADE_BCP_BUCKET;     Token=$env:BROCADE_BCP_TOKEN     },
    @{ Name='unifiedops-influx-netapp-cdvl';      Port=$env:NETAPP_CDVL_PORT;     Bucket=$env:NETAPP_CDVL_BUCKET;     Token=$env:NETAPP_CDVL_TOKEN     },
    @{ Name='unifiedops-influx-netapp-bcp';       Port=$env:NETAPP_BCP_PORT;      Bucket=$env:NETAPP_BCP_BUCKET;      Token=$env:NETAPP_BCP_TOKEN      },
    @{ Name='unifiedops-influx-netapp-sify';      Port=$env:NETAPP_SIFY_PORT;     Bucket=$env:NETAPP_SIFY_BUCKET;     Token=$env:NETAPP_SIFY_TOKEN     },
    @{ Name='unifiedops-influx-dell-cdvl';        Port=$env:DELL_CDVL_PORT;       Bucket=$env:DELL_CDVL_BUCKET;       Token=$env:DELL_CDVL_TOKEN       },
    @{ Name='unifiedops-influx-dell-bcp';         Port=$env:DELL_BCP_PORT;        Bucket=$env:DELL_BCP_BUCKET;        Token=$env:DELL_BCP_TOKEN        },
    @{ Name='unifiedops-influx-dell-sify';        Port=$env:DELL_SIFY_PORT;       Bucket=$env:DELL_SIFY_BUCKET;       Token=$env:DELL_SIFY_TOKEN       },
    @{ Name='unifiedops-influx-heartbeat-cdvl';   Port=$env:HEARTBEAT_CDVL_PORT;  Bucket=$env:HEARTBEAT_CDVL_BUCKET;  Token=$env:HEARTBEAT_CDVL_TOKEN  },
    @{ Name='unifiedops-influx-heartbeat-bcp';    Port=$env:HEARTBEAT_BCP_PORT;   Bucket=$env:HEARTBEAT_BCP_BUCKET;   Token=$env:HEARTBEAT_BCP_TOKEN   },
    @{ Name='unifiedops-influx-heartbeat-sify';   Port=$env:HEARTBEAT_SIFY_PORT;  Bucket=$env:HEARTBEAT_SIFY_BUCKET;  Token=$env:HEARTBEAT_SIFY_TOKEN  }
)

$Launchers = Get-ChildItem -Path $DevDir -Filter 'run-listener-*.ps1' | Select-Object -ExpandProperty Name
$ListenerPyFiles = Get-ChildItem -Path (Join-Path $Root 'listener') -Filter 'syslog_trap_listener_*.py' | Select-Object -ExpandProperty Name

function Stop-PythonByScript {
    param([string]$ScriptName)
    Get-CimInstance Win32_Process -Filter "Name='python.exe'" |
        Where-Object { $_.CommandLine -match [regex]::Escape($ScriptName) } |
        ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }
}

function Wait-Influx-Healthy {
    param([int]$Port, [string]$Label)
    $url = "http://127.0.0.1:$Port/health"
    for ($i = 0; $i -lt 30; $i++) {
        try {
            $r = Invoke-WebRequest -Uri $url -UseBasicParsing -TimeoutSec 2 -ErrorAction Stop
            if ($r.StatusCode -eq 200) { return $true }
        } catch {}
        Start-Sleep -Seconds 2
    }
    Write-Host "  WARN: $Label ($url) did not become healthy in 60s" -ForegroundColor Yellow
    return $false
}

function Ensure-Container {
    param($Cfg)

    $dataVol = "$($Cfg.Name)-data"
    $confVol = "$($Cfg.Name)-conf"

    $existing = & podman ps -a --format '{{.Names}}' 2>$null
    if ($existing -contains $Cfg.Name) {
        $status = & podman inspect --format '{{.State.Status}}' $Cfg.Name 2>$null
        if ($status -ne 'running') {
            & podman start $Cfg.Name *>&1 | Out-Null
        }
        return
    }

    $bucket = $Cfg.Bucket
    $token  = $Cfg.Token
    $args = @(
        'run','-d','--name',$Cfg.Name,'--restart','unless-stopped',
        '-p',"$($Cfg.Port):8086",
        '-e','DOCKER_INFLUXDB_INIT_MODE=setup',
        '-e',"DOCKER_INFLUXDB_INIT_USERNAME=$env:INFLUX_USERNAME",
        '-e',"DOCKER_INFLUXDB_INIT_PASSWORD=$env:INFLUX_PASSWORD",
        '-e',"DOCKER_INFLUXDB_INIT_ORG=$env:INFLUX_ORG",
        '-e',"DOCKER_INFLUXDB_INIT_BUCKET=$bucket",
        '-e',"DOCKER_INFLUXDB_INIT_RETENTION=$env:INFLUX_RETENTION",
        '-e',"DOCKER_INFLUXDB_INIT_ADMIN_TOKEN=$token",
        '-v',"${dataVol}:/var/lib/influxdb2:Z",
        '-v',"${confVol}:/etc/influxdb2:Z",
        '--health-cmd','curl -fsS http://localhost:8086/health',
        '--health-interval','10s','--health-timeout','5s',
        '--health-retries','6','--health-start-period','15s',
        $Image
    )
    & podman @args *>&1 | Out-Null
}

# =============================================================================
#   1. PODMAN
# =============================================================================
if (-not $SkipPodman) {
    Write-Host ''
    Write-Host '=== 1/4  podman: InfluxDB stack ===' -ForegroundColor Cyan

    if ($Flush) {
        Write-Host '  -Flush: removing existing containers + volumes' -ForegroundColor Yellow
        $existing = podman ps -a --format '{{.Names}}' 2>$null
        foreach ($c in $Containers) {
            if ($existing -contains $c.Name) {
                podman rm -f $c.Name 2>$null | Out-Null
            }
        }
        podman volume ls --format '{{.Name}}' 2>$null |
            Where-Object { $_ -like 'unifiedops-influx-*' } |
            ForEach-Object { podman volume rm $_ 2>$null | Out-Null }
    }

    Write-Host "  pulling/checking image $Image ..."
    $pullOut = & podman pull $Image *>&1
    Write-Host "    $($pullOut | Select-Object -Last 1)"

    Write-Host '  starting 14 containers ...'
    foreach ($c in $Containers) {
        Write-Host ('    {0,-44}  :{1}' -f $c.Name, $c.Port)
        Ensure-Container -Cfg $c
    }

    Write-Host '  waiting for /health on all 14 endpoints ...'
    $allHealthy = $true
    foreach ($c in $Containers) {
        $ok = Wait-Influx-Healthy -Port ([int]$c.Port) -Label $c.Name
        if (-not $ok) { $allHealthy = $false }
    }
    if ($allHealthy) {
        Write-Host '  all 14 InfluxDB instances healthy' -ForegroundColor Green
    } else {
        Write-Host '  WARN: some Influx instances unhealthy - listeners may fail to write' -ForegroundColor Yellow
    }
} else {
    Write-Host '=== 1/4  podman: SKIPPED (-SkipPodman) ===' -ForegroundColor Cyan
}

# =============================================================================
#   2. TOPOLOGY ECHO
# =============================================================================
Write-Host ''
Write-Host '=== 2/4  InfluxDB topology ===' -ForegroundColor Cyan
foreach ($c in $Containers) {
    Write-Host ('  {0,-44} -> http://127.0.0.1:{1,-5}  bucket={2}' -f $c.Name, $c.Port, $c.Bucket)
}

# =============================================================================
#   3. LISTENERS
# =============================================================================
Write-Host ''
Write-Host '=== 3/4  syslog listeners ===' -ForegroundColor Cyan
foreach ($pf in $ListenerPyFiles) { Stop-PythonByScript $pf }
Start-Sleep -Seconds 1

foreach ($lp in $Launchers) {
    $path = Join-Path $DevDir $lp
    if (-not (Test-Path $path)) {
        Write-Host "  WARN: $lp not found - skipped" -ForegroundColor Yellow
        continue
    }
    Write-Host "  start $lp"
    Start-Process -WindowStyle Hidden -FilePath 'powershell.exe' `
        -ArgumentList '-NoLogo','-NoProfile','-ExecutionPolicy','Bypass','-File',$path `
        -WorkingDirectory $DevDir
    Start-Sleep -Milliseconds 200
}

# =============================================================================
#   4. BACKEND + TRAP UI
# =============================================================================
Write-Host ''
Write-Host '=== 4/4  backend + trap-UI ===' -ForegroundColor Cyan
Stop-PythonByScript 'server\server.py'
Stop-PythonByScript 'dev\trap_sender_ui.py'
Start-Sleep -Seconds 1

$BackendScript = Join-Path $DevDir 'run-backend.ps1'
Start-Process -WindowStyle Hidden -FilePath 'powershell.exe' `
    -ArgumentList '-NoLogo','-NoProfile','-ExecutionPolicy','Bypass','-File',$BackendScript `
    -WorkingDirectory $Root
Write-Host "  start backend  -> http://${env:BACKEND_HOST}:${env:BACKEND_PORT}"

$TrapUiScript = Join-Path $DevDir 'run-trap-ui.ps1'
Start-Process -WindowStyle Hidden -FilePath 'powershell.exe' `
    -ArgumentList '-NoLogo','-NoProfile','-ExecutionPolicy','Bypass','-File',$TrapUiScript `
    -WorkingDirectory $DevDir
Write-Host "  start trap-UI  -> http://${env:TRAP_UI_HOST}:${env:TRAP_UI_PORT}"

Start-Sleep -Seconds 3

# =============================================================================
#   5. FRONTEND (VITE)
# =============================================================================
Write-Host ''
Write-Host '=== 5/5  frontend (Vite) ===' -ForegroundColor Cyan

# Kill existing Vite process
Get-CimInstance Win32_Process -Filter "Name='node.exe'" |
    Where-Object { $_.CommandLine -match 'vite' } |
    ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }

$FrontendDir = Join-Path $Root 'frontend'
Start-Process -WindowStyle Hidden -FilePath 'powershell.exe' `
    -ArgumentList '-NoLogo','-NoProfile','-ExecutionPolicy','Bypass','-Command',"cd `"$FrontendDir`"; npm run dev *> frontend.log" `
    -WorkingDirectory $FrontendDir

Write-Host "  start frontend -> http://localhost:3000"

Start-Sleep -Seconds 3

# =============================================================================
#   FINAL SUMMARY
# =============================================================================
Write-Host ''
Write-Host '=== running services ===' -ForegroundColor Green

$running = podman ps --format '{{.Names}}' 2>$null |
    Where-Object { $_ -match '^unifiedops-influx-' } |
    Sort-Object
Write-Host ("  InfluxDB:    {0,2}/14 containers up" -f ($running | Measure-Object).Count)

$pythonProcs = Get-CimInstance Win32_Process -Filter "Name='python.exe'"
$listenerCount = ($pythonProcs | Where-Object { $_.CommandLine -match 'syslog_trap_listener' } | Measure-Object).Count
$backendUp     = ($pythonProcs | Where-Object { $_.CommandLine -match 'server\\server\.py' } | Measure-Object).Count
$trapUiUp      = ($pythonProcs | Where-Object { $_.CommandLine -match 'trap_sender_ui\.py' } | Measure-Object).Count

$nodeProcs = Get-CimInstance Win32_Process -Filter "Name='node.exe'"
$frontendUp = ($nodeProcs | Where-Object { $_.CommandLine -match 'vite' } | Measure-Object).Count

Write-Host ("  Listeners:   {0,2}/$($ListenerPyFiles.Count) processes" -f $listenerCount)
Write-Host ("  Backend:     {0}/1" -f $backendUp)
Write-Host ("  Trap-UI:     {0}/1" -f $trapUiUp)
Write-Host ("  Frontend:    {0}/1" -f $frontendUp)

Write-Host ''
Write-Host "Open the frontend at http://localhost:3000/" -ForegroundColor Green
Write-Host "Open the dashboard at http://${env:BACKEND_HOST}:${env:BACKEND_PORT}/" -ForegroundColor Green
Write-Host "Open the trap-sender at http://${env:TRAP_UI_HOST}:${env:TRAP_UI_PORT}/" -ForegroundColor Green
