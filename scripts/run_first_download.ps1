# Run after you set both API keys in .env
# Usage: .\scripts\run_first_download.ps1

$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot\..

$envContent = Get-Content .env -Raw
$missing = @()

if ($envContent -notmatch 'TRAFIKLAB_STATIC_API_KEY=(?!your_static_key_here)(?!your_api_key_here)(\S+)') {
    $missing += "TRAFIKLAB_STATIC_API_KEY  (from 'GTFS Sweden 3 Static data')"
}
if ($envContent -notmatch 'TRAFIKLAB_REALTIME_API_KEY=(?!your_realtime_key_here)(?!your_api_key_here)(\S+)') {
    $missing += "TRAFIKLAB_REALTIME_API_KEY (from 'GTFS Regional Realtime')"
}

if ($missing.Count -gt 0) {
    Write-Host "ERROR: Missing API keys in .env" -ForegroundColor Red
    foreach ($m in $missing) { Write-Host "  → $m" }
    Write-Host ""
    Write-Host "Open .env in VS Code, paste both keys, save (Ctrl+S), then re-run."
    exit 1
}

Write-Host "Reloading Docker with updated .env..."
docker compose up -d --force-recreate airflow-scheduler airflow-webserver | Out-Null
Start-Sleep -Seconds 3

Write-Host "Downloading static GTFS + realtime snapshot for SL..."
docker compose exec airflow-scheduler python /opt/airflow/project/scripts/download_sample_gtfs.py --operator sl

if ($LASTEXITCODE -eq 0) {
    Write-Host ""
    Write-Host "Success! Files saved under data/raw/" -ForegroundColor Green
    Get-ChildItem -Recurse data\raw | Where-Object { -not $_.PSIsContainer } | Select-Object FullName, Length
} else {
    Write-Host "Download failed. Run: docker compose exec airflow-scheduler python /opt/airflow/project/scripts/test_api_key.py" -ForegroundColor Red
}
