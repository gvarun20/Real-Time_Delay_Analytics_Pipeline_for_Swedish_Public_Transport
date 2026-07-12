# Stop the old Docker stack (if you used the previous folder-based name)
docker compose -p summer_3rd_project down

# Start the renamed stack
docker compose up airflow-init
docker compose up -d

# Verify new container names
docker compose ps
