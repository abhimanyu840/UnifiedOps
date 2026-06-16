# =============================================================================
#   UnifiedOps v2 - start-services.ps1
# -----------------------------------------------------------------------------
#   Consolidated script to launch all local development services.
#   Replaces start-all.ps1, run-backend.ps1, run-trap-ui.ps1, and 
#   all run-listener-*.ps1 wrappers.
#
#   Usage:
#       cd dev
#       .\start-services.ps1                 # normal start
#       .\start-services.ps1 -Flush          # wipe + recreate containers
#       .\start-services.ps1 -SkipPodman     # only python/node services
# =============================================================================
[CmdletBinding()]
param(
    [switch] $Flush,
    [switch] $SkipPodman
)

$ErrorActionPreference = 'Continue'

$DevDir = $PSScriptRoot
$Root   = Split-Path -Parent $DevDir
$Venv   = Join-Path $Root '.venv\Scripts\python.exe'

if (-not (Test-Path $Venv)) {
    Write-Host "FATAL: $Venv not found - create the dev venv first" -ForegroundColor Red
    exit 1
}

. (Join-Path $DevDir 'load-env.ps1')

# ----- Container Topology -------------------------------------------------
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
    @{ Name='unifiedops-influx-heartbeat-sify';   Port=$env:HEARTBEAT_SIFY_PORT;  Bucket=$env:HEARTBEAT_SIFY_BUCKET;  Token=$env:HEARTBEAT_SIFY_TOKEN  },
    @{ Name='unifiedops-influx-health-report-cdvl'; Port=$env:BROCADE_CDVL_REPORT_PORT; Bucket=$env:BROCADE_CDVL_REPORT_BUCKET; Token=$env:BROCADE_CDVL_REPORT_TOKEN },
    @{ Name='unifiedops-influx-health-report-bcp';  Port=$env:BROCADE_BCP_REPORT_PORT;  Bucket=$env:BROCADE_BCP_REPORT_BUCKET;  Token=$env:BROCADE_BCP_REPORT_TOKEN  }
)

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

    Write-Host '  starting 16 containers ...'
    foreach ($c in $Containers) {
        Write-Host ('    {0,-44}  :{1}' -f $c.Name, $c.Port)
        Ensure-Container -Cfg $c
    }

    Write-Host '  waiting for /health on all 16 endpoints ...'
    $allHealthy = $true
    foreach ($c in $Containers) {
        $ok = Wait-Influx-Healthy -Port ([int]$c.Port) -Label $c.Name
        if (-not $ok) { $allHealthy = $false }
    }
    if ($allHealthy) {
        Write-Host '  all 16 InfluxDB instances healthy' -ForegroundColor Green
        Write-Host '  spinning off health-check-report buckets...' -ForegroundColor Cyan
        & podman exec unifiedops-influx-brocade-cdvl influx bucket create -n $env:BROCADE_CDVL_REPORT_BUCKET -o $env:INFLUX_ORG -t $env:BROCADE_CDVL_TOKEN 2>&1 | Out-Null
        & podman exec unifiedops-influx-brocade-bcp influx bucket create -n $env:BROCADE_BCP_REPORT_BUCKET -o $env:INFLUX_ORG -t $env:BROCADE_BCP_TOKEN 2>&1 | Out-Null
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
Get-ChildItem -Path (Join-Path $Root 'listener') -Filter 'syslog_trap_listener_*.py' | Select-Object -ExpandProperty Name | ForEach-Object { Stop-PythonByScript $_ }
Start-Sleep -Seconds 1

$Listeners = @(
    @{ Name='bcp'; Script='syslog_trap_listener_bcp.py'; Port='5515'; Url="http://127.0.0.1:$($env:HITACHI_BCP_PORT)"; Bucket=$env:HITACHI_BCP_BUCKET; Token=$env:HITACHI_BCP_TOKEN; HbUrl="http://127.0.0.1:$($env:HEARTBEAT_BCP_PORT)"; HbBucket=$env:HEARTBEAT_BCP_BUCKET; HbToken=$env:HEARTBEAT_BCP_TOKEN },
    @{ Name='brocade-cdvl-sify'; Script='syslog_trap_listener_cdvl_n_sify.py'; Port='5214'; Url="http://127.0.0.1:$($env:BROCADE_CDVL_PORT)"; Bucket=$env:BROCADE_CDVL_BUCKET; Token=$env:BROCADE_CDVL_TOKEN; HbUrl="http://127.0.0.1:$($env:HEARTBEAT_CDVL_PORT)"; HbBucket=$env:HEARTBEAT_CDVL_BUCKET; HbToken=$env:HEARTBEAT_CDVL_TOKEN; ReportUrl="http://127.0.0.1:$($env:BROCADE_CDVL_REPORT_PORT)"; ReportBucket=$env:BROCADE_CDVL_REPORT_BUCKET; ReportToken=$env:BROCADE_CDVL_REPORT_TOKEN },
    @{ Name='brocade-bcp-uat'; Script='syslog_trap_listener_bcp_n_uat.py'; Port='5215'; Url="http://127.0.0.1:$($env:BROCADE_BCP_PORT)"; Bucket=$env:BROCADE_BCP_BUCKET; Token=$env:BROCADE_BCP_TOKEN; HbUrl="http://127.0.0.1:$($env:HEARTBEAT_BCP_PORT)"; HbBucket=$env:HEARTBEAT_BCP_BUCKET; HbToken=$env:HEARTBEAT_BCP_TOKEN; ReportUrl="http://127.0.0.1:$($env:BROCADE_BCP_REPORT_PORT)"; ReportBucket=$env:BROCADE_BCP_REPORT_BUCKET; ReportToken=$env:BROCADE_BCP_REPORT_TOKEN },
    @{ Name='cdvl'; Script='syslog_trap_listener_cdvl.py'; Port='5514'; Url="http://127.0.0.1:$($env:HITACHI_CDVL_PORT)"; Bucket=$env:HITACHI_CDVL_BUCKET; Token=$env:HITACHI_CDVL_TOKEN; HbUrl="http://127.0.0.1:$($env:HEARTBEAT_CDVL_PORT)"; HbBucket=$env:HEARTBEAT_CDVL_BUCKET; HbToken=$env:HEARTBEAT_CDVL_TOKEN },
    @{ Name='dell-bcp'; Script='syslog_trap_listener_dell_bcp.py'; Port='5415'; Url="http://127.0.0.1:$($env:DELL_BCP_PORT)"; Bucket=$env:DELL_BCP_BUCKET; Token=$env:DELL_BCP_TOKEN; HbUrl="http://127.0.0.1:$($env:HEARTBEAT_BCP_PORT)"; HbBucket=$env:HEARTBEAT_BCP_BUCKET; HbToken=$env:HEARTBEAT_BCP_TOKEN },
    @{ Name='dell-cdvl'; Script='syslog_trap_listener_dell_cdvl.py'; Port='5414'; Url="http://127.0.0.1:$($env:DELL_CDVL_PORT)"; Bucket=$env:DELL_CDVL_BUCKET; Token=$env:DELL_CDVL_TOKEN; HbUrl="http://127.0.0.1:$($env:HEARTBEAT_CDVL_PORT)"; HbBucket=$env:HEARTBEAT_CDVL_BUCKET; HbToken=$env:HEARTBEAT_CDVL_TOKEN },
    @{ Name='dell-sify'; Script='syslog_trap_listener_dell_sify.py'; Port='5416'; Url="http://127.0.0.1:$($env:DELL_SIFY_PORT)"; Bucket=$env:DELL_SIFY_BUCKET; Token=$env:DELL_SIFY_TOKEN; HbUrl="http://127.0.0.1:$($env:HEARTBEAT_SIFY_PORT)"; HbBucket=$env:HEARTBEAT_SIFY_BUCKET; HbToken=$env:HEARTBEAT_SIFY_TOKEN },
    @{ Name='netapp-bcp'; Script='syslog_trap_listener_netapp_bcp.py'; Port='5315'; Url="http://127.0.0.1:$($env:NETAPP_BCP_PORT)"; Bucket=$env:NETAPP_BCP_BUCKET; Token=$env:NETAPP_BCP_TOKEN; HbUrl="http://127.0.0.1:$($env:HEARTBEAT_BCP_PORT)"; HbBucket=$env:HEARTBEAT_BCP_BUCKET; HbToken=$env:HEARTBEAT_BCP_TOKEN },
    @{ Name='netapp-cdvl'; Script='syslog_trap_listener_netapp_cdvl.py'; Port='5314'; Url="http://127.0.0.1:$($env:NETAPP_CDVL_PORT)"; Bucket=$env:NETAPP_CDVL_BUCKET; Token=$env:NETAPP_CDVL_TOKEN; HbUrl="http://127.0.0.1:$($env:HEARTBEAT_CDVL_PORT)"; HbBucket=$env:HEARTBEAT_CDVL_BUCKET; HbToken=$env:HEARTBEAT_CDVL_TOKEN },
    @{ Name='netapp-sify'; Script='syslog_trap_listener_netapp_sify.py'; Port='5316'; Url="http://127.0.0.1:$($env:NETAPP_SIFY_PORT)"; Bucket=$env:NETAPP_SIFY_BUCKET; Token=$env:NETAPP_SIFY_TOKEN; HbUrl="http://127.0.0.1:$($env:HEARTBEAT_SIFY_PORT)"; HbBucket=$env:HEARTBEAT_SIFY_BUCKET; HbToken=$env:HEARTBEAT_SIFY_TOKEN },
    @{ Name='sify'; Script='syslog_trap_listener_sify.py'; Port='5516'; Url="http://127.0.0.1:$($env:HITACHI_SIFY_PORT)"; Bucket=$env:HITACHI_SIFY_BUCKET; Token=$env:HITACHI_SIFY_TOKEN; HbUrl="http://127.0.0.1:$($env:HEARTBEAT_SIFY_PORT)"; HbBucket=$env:HEARTBEAT_SIFY_BUCKET; HbToken=$env:HEARTBEAT_SIFY_TOKEN }
)

$startup = New-CimInstance -ClassName Win32_ProcessStartup -ClientOnly -Property @{ ShowWindow = 0 }

foreach ($l in $Listeners) {
    Write-Host "  start listener: $($l.Name)"
    $cmd = "`$env:HITRACK_INFLUX_URL='$($l.Url)'; `$env:HITRACK_INFLUX_TOKEN='$($l.Token)'; `$env:HITRACK_INFLUX_ORG='HDFC'; `$env:HITRACK_INFLUX_BUCKET='$($l.Bucket)'; `$env:HITRACK_LISTEN_HOST='127.0.0.1'; `$env:HITRACK_LISTEN_PORT='$($l.Port)'; `$env:HITRACK_TEST_MODE='1'; `$env:HITRACK_HEARTBEAT_URL='$($l.HbUrl)'; `$env:HITRACK_HEARTBEAT_TOKEN='$($l.HbToken)'; `$env:HITRACK_HEARTBEAT_ORG='HDFC'; `$env:HITRACK_HEARTBEAT_BUCKET='$($l.HbBucket)'; `$env:HITRACK_HEARTBEAT_INTERVAL='10'; "
    if ($l.ReportUrl) {
        $cmd += "`$env:HITRACK_INFLUX_REPORT_URL='$($l.ReportUrl)'; `$env:HITRACK_INFLUX_REPORT_TOKEN='$($l.ReportToken)'; `$env:HITRACK_INFLUX_REPORT_BUCKET='$($l.ReportBucket)'; "
    }
    $cmd += "& '$Venv' '$Root\listener\$($l.Script)'"
    Start-Process -NoNewWindow -FilePath 'powershell.exe' -ArgumentList '-WindowStyle','Hidden','-NoProfile','-ExecutionPolicy','Bypass','-Command',$cmd -WorkingDirectory $Root
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

Write-Host "  start backend  -> http://${env:BACKEND_HOST}:${env:BACKEND_PORT}"
$backendCmd = "`$env:HITRACK_UI_HOST='$env:BACKEND_HOST'; `$env:HITRACK_UI_PORT='$env:BACKEND_PORT'; `$env:HITRACK_INFLUX_CDVL_URL='http://127.0.0.1:$env:HITACHI_CDVL_PORT'; `$env:HITRACK_INFLUX_CDVL_TOKEN='$env:HITACHI_CDVL_TOKEN'; `$env:HITRACK_INFLUX_CDVL_ORG='$env:INFLUX_ORG'; `$env:HITRACK_INFLUX_CDVL_BUCKET='$env:HITACHI_CDVL_BUCKET'; `$env:HITRACK_INFLUX_BCP_URL='http://127.0.0.1:$env:HITACHI_BCP_PORT'; `$env:HITRACK_INFLUX_BCP_TOKEN='$env:HITACHI_BCP_TOKEN'; `$env:HITRACK_INFLUX_BCP_ORG='$env:INFLUX_ORG'; `$env:HITRACK_INFLUX_BCP_BUCKET='$env:HITACHI_BCP_BUCKET'; `$env:HITRACK_INFLUX_SIFY_URL='http://127.0.0.1:$env:HITACHI_SIFY_PORT'; `$env:HITRACK_INFLUX_SIFY_TOKEN='$env:HITACHI_SIFY_TOKEN'; `$env:HITRACK_INFLUX_SIFY_ORG='$env:INFLUX_ORG'; `$env:HITRACK_INFLUX_SIFY_BUCKET='$env:HITACHI_SIFY_BUCKET'; `$env:HITRACK_INFLUX_BROCADE_CDVL_URL='http://127.0.0.1:$env:BROCADE_CDVL_PORT'; `$env:HITRACK_INFLUX_BROCADE_CDVL_TOKEN='$env:BROCADE_CDVL_TOKEN'; `$env:HITRACK_INFLUX_BROCADE_CDVL_ORG='$env:INFLUX_ORG'; `$env:HITRACK_INFLUX_BROCADE_CDVL_BUCKET='$env:BROCADE_CDVL_BUCKET'; `$env:HITRACK_INFLUX_BROCADE_CDVL_REPORT_URL='http://127.0.0.1:$env:BROCADE_CDVL_REPORT_PORT'; `$env:HITRACK_INFLUX_BROCADE_CDVL_REPORT_TOKEN='$env:BROCADE_CDVL_REPORT_TOKEN'; `$env:HITRACK_INFLUX_BROCADE_BCP_REPORT_URL='http://127.0.0.1:$env:BROCADE_BCP_REPORT_PORT'; `$env:HITRACK_INFLUX_BROCADE_BCP_REPORT_TOKEN='$env:BROCADE_BCP_REPORT_TOKEN'; `$env:HITRACK_INFLUX_BROCADE_BCP_URL='http://127.0.0.1:$env:BROCADE_BCP_PORT'; `$env:HITRACK_INFLUX_BROCADE_BCP_TOKEN='$env:BROCADE_BCP_TOKEN'; `$env:HITRACK_INFLUX_BROCADE_BCP_ORG='$env:INFLUX_ORG'; `$env:HITRACK_INFLUX_BROCADE_BCP_BUCKET='$env:BROCADE_BCP_BUCKET'; `$env:HITRACK_INFLUX_NETAPP_CDVL_URL='http://127.0.0.1:$env:NETAPP_CDVL_PORT'; `$env:HITRACK_INFLUX_NETAPP_CDVL_TOKEN='$env:NETAPP_CDVL_TOKEN'; `$env:HITRACK_INFLUX_NETAPP_CDVL_ORG='$env:INFLUX_ORG'; `$env:HITRACK_INFLUX_NETAPP_CDVL_BUCKET='$env:NETAPP_CDVL_BUCKET'; `$env:HITRACK_INFLUX_NETAPP_BCP_URL='http://127.0.0.1:$env:NETAPP_BCP_PORT'; `$env:HITRACK_INFLUX_NETAPP_BCP_TOKEN='$env:NETAPP_BCP_TOKEN'; `$env:HITRACK_INFLUX_NETAPP_BCP_ORG='$env:INFLUX_ORG'; `$env:HITRACK_INFLUX_NETAPP_BCP_BUCKET='$env:NETAPP_BCP_BUCKET'; `$env:HITRACK_INFLUX_NETAPP_SIFY_URL='http://127.0.0.1:$env:NETAPP_SIFY_PORT'; `$env:HITRACK_INFLUX_NETAPP_SIFY_TOKEN='$env:NETAPP_SIFY_TOKEN'; `$env:HITRACK_INFLUX_NETAPP_SIFY_ORG='$env:INFLUX_ORG'; `$env:HITRACK_INFLUX_NETAPP_SIFY_BUCKET='$env:NETAPP_SIFY_BUCKET'; `$env:HITRACK_INFLUX_DELL_CDVL_URL='http://127.0.0.1:$env:DELL_CDVL_PORT'; `$env:HITRACK_INFLUX_DELL_CDVL_TOKEN='$env:DELL_CDVL_TOKEN'; `$env:HITRACK_INFLUX_DELL_CDVL_ORG='$env:INFLUX_ORG'; `$env:HITRACK_INFLUX_DELL_CDVL_BUCKET='$env:DELL_CDVL_BUCKET'; `$env:HITRACK_INFLUX_DELL_BCP_URL='http://127.0.0.1:$env:DELL_BCP_PORT'; `$env:HITRACK_INFLUX_DELL_BCP_TOKEN='$env:DELL_BCP_TOKEN'; `$env:HITRACK_INFLUX_DELL_BCP_ORG='$env:INFLUX_ORG'; `$env:HITRACK_INFLUX_DELL_BCP_BUCKET='$env:DELL_BCP_BUCKET'; `$env:HITRACK_INFLUX_DELL_SIFY_URL='http://127.0.0.1:$env:DELL_SIFY_PORT'; `$env:HITRACK_INFLUX_DELL_SIFY_TOKEN='$env:DELL_SIFY_TOKEN'; `$env:HITRACK_INFLUX_DELL_SIFY_ORG='$env:INFLUX_ORG'; `$env:HITRACK_INFLUX_DELL_SIFY_BUCKET='$env:DELL_SIFY_BUCKET'; `$env:HITRACK_HEARTBEAT_CDVL_URL='http://127.0.0.1:$env:HEARTBEAT_CDVL_PORT'; `$env:HITRACK_HEARTBEAT_CDVL_TOKEN='$env:HEARTBEAT_CDVL_TOKEN'; `$env:HITRACK_HEARTBEAT_CDVL_ORG='$env:INFLUX_ORG'; `$env:HITRACK_HEARTBEAT_CDVL_BUCKET='$env:HEARTBEAT_CDVL_BUCKET'; `$env:HITRACK_HEARTBEAT_BCP_URL='http://127.0.0.1:$env:HEARTBEAT_BCP_PORT'; `$env:HITRACK_HEARTBEAT_BCP_TOKEN='$env:HEARTBEAT_BCP_TOKEN'; `$env:HITRACK_HEARTBEAT_BCP_ORG='$env:INFLUX_ORG'; `$env:HITRACK_HEARTBEAT_BCP_BUCKET='$env:HEARTBEAT_BCP_BUCKET'; `$env:HITRACK_HEARTBEAT_SIFY_URL='http://127.0.0.1:$env:HEARTBEAT_SIFY_PORT'; `$env:HITRACK_HEARTBEAT_SIFY_TOKEN='$env:HEARTBEAT_SIFY_TOKEN'; `$env:HITRACK_HEARTBEAT_SIFY_ORG='$env:INFLUX_ORG'; `$env:HITRACK_HEARTBEAT_SIFY_BUCKET='$env:HEARTBEAT_SIFY_BUCKET'; `$env:HITRACK_VERIFY_TLS='$env:HITRACK_VERIFY_TLS'; `$env:HITRACK_PIPELINE_POLL_SECS='$env:PIPELINE_POLL_SECS'; `$env:HITRACK_LISTENER_POLL_SECS='$env:LISTENER_POLL_SECS'; `$env:HITRACK_LISTENER_DOWN_THRESHOLD_S='$env:LISTENER_DOWN_THRESHOLD_S'; & '$Venv' '$Root\server\server.py' *> '$Root\server.log'"
Start-Process -NoNewWindow -FilePath 'powershell.exe' -ArgumentList '-WindowStyle','Hidden','-NoProfile','-ExecutionPolicy','Bypass','-Command',$backendCmd -WorkingDirectory $Root

Write-Host "  start trap-UI  -> http://${env:TRAP_UI_HOST}:${env:TRAP_UI_PORT}"
$trapUiCmd = "`$env:TRAP_UI_HOST='$env:TRAP_UI_HOST'; `$env:TRAP_UI_PORT='$env:TRAP_UI_PORT'; & '$Venv' '$Root\dev\trap_sender_ui.py'"
Start-Process -NoNewWindow -FilePath 'powershell.exe' -ArgumentList '-WindowStyle','Hidden','-NoProfile','-ExecutionPolicy','Bypass','-Command',$trapUiCmd -WorkingDirectory $DevDir

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
$viteCmd = "cd `"$FrontendDir`"; npm run dev *> frontend.log"
Start-Process -NoNewWindow -FilePath 'powershell.exe' -ArgumentList '-WindowStyle','Hidden','-NoProfile','-ExecutionPolicy','Bypass','-Command',$viteCmd -WorkingDirectory $FrontendDir

Write-Host "  start frontend -> http://localhost:3000"

Start-Sleep -Seconds 3

# =============================================================================
#   FINAL SUMMARY
# =============================================================================
Write-Host ''
Write-Host '=== running services ===' -ForegroundColor Green

$running = podman ps --format '{{.Names}}' 2>$null | Where-Object { $_ -match '^unifiedops-influx-' } | Sort-Object
Write-Host ("  InfluxDB:    {0,2}/16 containers up" -f ($running | Measure-Object).Count)

$pythonProcs = Get-CimInstance Win32_Process -Filter "Name='python.exe'"
$listenerCount = ($pythonProcs | Where-Object { $_.CommandLine -match 'syslog_trap_listener' } | Measure-Object).Count
$backendUp     = ($pythonProcs | Where-Object { $_.CommandLine -match 'server\\server\.py' } | Measure-Object).Count
$trapUiUp      = ($pythonProcs | Where-Object { $_.CommandLine -match 'trap_sender_ui\.py' } | Measure-Object).Count

$nodeProcs = Get-CimInstance Win32_Process -Filter "Name='node.exe'"
$frontendUp = ($nodeProcs | Where-Object { $_.CommandLine -match 'vite' } | Measure-Object).Count

Write-Host ("  Listeners:   {0,2}/11 processes" -f $listenerCount)
Write-Host ("  Backend:     {0}/1" -f $backendUp)
Write-Host ("  Trap-UI:     {0}/1" -f $trapUiUp)
Write-Host ("  Frontend:    {0}/1" -f $frontendUp)

Write-Host ''
Write-Host "Open the frontend at http://localhost:3000/" -ForegroundColor Green
Write-Host "Open the dashboard at http://${env:BACKEND_HOST}:${env:BACKEND_PORT}/" -ForegroundColor Green
Write-Host "Open the trap-sender at http://${env:TRAP_UI_HOST}:${env:TRAP_UI_PORT}/" -ForegroundColor Green
