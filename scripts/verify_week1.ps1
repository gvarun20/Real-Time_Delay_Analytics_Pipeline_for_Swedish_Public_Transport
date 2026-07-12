# Week 1 verification - run before starting Week 2
$ErrorActionPreference = "Continue"
Set-Location $PSScriptRoot\..

$pass = 0
$fail = 0

function Check($name, $condition) {
    if ($condition) {
        Write-Host "[PASS] $name" -ForegroundColor Green
        $script:pass++
    } else {
        Write-Host "[FAIL] $name" -ForegroundColor Red
        $script:fail++
    }
}

Write-Host "`n=== Week 1 Verification ===`n" -ForegroundColor Cyan

function Key-IsSet([string]$varName) {
    if (-not (Test-Path .env)) { return $false }
    $line = Get-Content .env | Where-Object { $_ -match "^$([regex]::Escape($varName))=" } | Select-Object -First 1
    if (-not $line) { return $false }
    $val = ($line -split "=", 2)[1].Trim().Trim('"').Trim("'")
    $placeholders = @("", "your_api_key_here", "your_static_key_here", "your_realtime_key_here")
    return ($val.Length -ge 8) -and ($placeholders -notcontains $val)
}

# .env
Check ".env file exists" (Test-Path .env)
if (Test-Path .env) {
    Check "TRAFIKLAB_STATIC_API_KEY set" (Key-IsSet "TRAFIKLAB_STATIC_API_KEY")
    Check "TRAFIKLAB_REALTIME_API_KEY set" (Key-IsSet "TRAFIKLAB_REALTIME_API_KEY")
    $envRaw = Get-Content .env -Raw
    Check "REALTIME_FEED=gtfs_regional" ($envRaw -match 'REALTIME_FEED=gtfs_regional')
    Check "STATIC_FEED=gtfs_sweden_3" ($envRaw -match 'STATIC_FEED=gtfs_sweden_3')
}

# Docker
$dockerPs = docker compose ps --format "{{.Name}} {{.Status}}" 2>$null
Check "Docker stack running" ($dockerPs -match "transit-delay-pipeline-airflow-scheduler")
Check "Airflow webserver healthy" ($dockerPs -match "airflow-webserver.*healthy|Up")
Check "Postgres analytics running" ($dockerPs -match "postgres-analytics")

# Raw data
Check "Static gtfs.zip exists" (Test-Path "data\raw\static\*\gtfs.zip")
Check "Static metadata.json exists" (Test-Path "data\raw\static\*\metadata.json")
Check "Realtime tripupdates.pb exists" (Test-Path "data\raw\realtime\*\*\tripupdates.pb")

# API test (if docker up)
if ($dockerPs -match "airflow-scheduler") {
    docker compose exec -T airflow-scheduler python /opt/airflow/project/scripts/test_api_key.py --operator sl 2>&1 | Out-Null
    Check "API keys valid (test_api_key.py)" ($LASTEXITCODE -eq 0)
}

# DAGs
if ($dockerPs -match "airflow-scheduler") {
    $dags = docker compose exec -T airflow-scheduler airflow dags list 2>&1
    Check "DAG gtfs_static_ingest loaded" ($dags -match "gtfs_static_ingest")
    Check "DAG gtfs_realtime_ingest loaded" ($dags -match "gtfs_realtime_ingest")
}

# Local pytest (optional)
if (Get-Command py -ErrorAction SilentlyContinue) {
    py -m pytest tests/ -q 2>&1 | Out-Null
    if ($LASTEXITCODE -eq 0) { Check "pytest passes" $true } else { Check "pytest passes" $false }
} elseif (Get-Command python -ErrorAction SilentlyContinue) {
    python -m pytest tests/ -q 2>&1 | Out-Null
    if ($LASTEXITCODE -eq 0) { Check "pytest passes" $true } else { Check "pytest passes" $false }
} else {
    Write-Host "[SKIP] pytest (Python not on PATH)" -ForegroundColor Yellow
}

Write-Host "`n=== Results: $pass passed, $fail failed ===`n" -ForegroundColor Cyan

if ($fail -eq 0) {
    Write-Host "Week 1 gate: READY for Week 2 (PySpark transform)" -ForegroundColor Green
    exit 0
} else {
    Write-Host "Week 1 gate: NOT complete - see docs/WEEK1_CHECKLIST.md" -ForegroundColor Yellow
    exit 1
}
