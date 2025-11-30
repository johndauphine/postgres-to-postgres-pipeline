#!/bin/bash
# Connect PostgreSQL containers to the Airflow network
#
# Run this after:
#   1. docker-compose up -d
#   2. astro dev start

set -e

# Find the Airflow network (created by astro dev start)
NETWORK=$(docker network ls --format '{{.Name}}' | grep -E "postgres-to-postgres.*airflow" | head -1)

if [ -z "$NETWORK" ]; then
    echo "Error: Airflow network not found."
    echo "Make sure you've run 'astro dev start' first."
    exit 1
fi

echo "Found Airflow network: $NETWORK"

# Connect postgres-source if not already connected
if docker network inspect "$NETWORK" --format '{{range .Containers}}{{.Name}} {{end}}' | grep -q "postgres-source"; then
    echo "postgres-source already connected to $NETWORK"
else
    echo "Connecting postgres-source to $NETWORK..."
    docker network connect "$NETWORK" postgres-source
    echo "Done."
fi

# Connect postgres-target if not already connected
if docker network inspect "$NETWORK" --format '{{range .Containers}}{{.Name}} {{end}}' | grep -q "postgres-target"; then
    echo "postgres-target already connected to $NETWORK"
else
    echo "Connecting postgres-target to $NETWORK..."
    docker network connect "$NETWORK" postgres-target
    echo "Done."
fi

echo ""
echo "Database containers connected to Airflow network."
echo "You can now run the migration DAG."
