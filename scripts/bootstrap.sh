#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

if [ ! -f .env ]; then
  cp .env.example .env
  echo "Created .env — add your TRAFIKLAB_API_KEY before fetching data."
fi

docker compose up airflow-init
docker compose up -d

echo ""
echo "Airflow UI:  http://localhost:8080  (admin / admin)"
echo "Postgres DW: localhost:5432           (transit / transit)"
