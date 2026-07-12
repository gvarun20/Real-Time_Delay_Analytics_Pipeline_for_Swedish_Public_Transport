# Bootstrap script for Windows PowerShell
$ErrorActionPreference = "Stop"

Set-Location $PSScriptRoot\..

if (-not (Test-Path ".env")) {
    Copy-Item ".env.example" ".env"
    Write-Host "Created .env from .env.example — add your TRAFIKLAB_API_KEY before fetching data."
}

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    Write-Error "Docker is not installed or not on PATH."
}

Write-Host "Starting Docker stack (project: transit-delay-pipeline)..."
docker compose up airflow-init
docker compose up -d

Write-Host ""
Write-Host "Stack is up."
Write-Host "  Airflow UI:  http://localhost:8081  (admin / admin)"
Write-Host "  Postgres DW: localhost:5433           (transit / transit)"
Write-Host ""
Write-Host "Next steps:"
Write-Host "  1. Edit .env and set TRAFIKLAB_API_KEY"
Write-Host "  2. .\scripts\run_first_download.ps1"
Write-Host "  3. Open Airflow and trigger gtfs_static_ingest / gtfs_realtime_ingest"
