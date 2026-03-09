#!/bin/bash
set -e

echo "=============================================="
echo "DataSync Ingestion - Running Solution"
echo "=============================================="

# Load .env file if it exists (for API_KEY)
if [ -f .env ]; then
    echo "Loading .env file..."
    set -a
    . ./.env
    set +a
fi

# Validate API_KEY is set
if [ -z "$API_KEY" ] && [ -z "$TARGET_API_KEY" ]; then
    echo "ERROR: API_KEY environment variable is required."
    echo "Set it via: export API_KEY=your_key_here"
    echo "Or create a .env file with: API_KEY=your_key_here"
    exit 1
fi

# Create output directory
mkdir -p output

# Start the ingestion services
echo "Starting services..."
docker compose up -d --build

echo ""
echo "Waiting for services to initialize..."
sleep 10

# Monitor progress
echo ""
echo "Monitoring ingestion progress..."
echo "=============================================="

while true; do
    COUNT=$(docker exec assignment-postgres psql -U postgres -d ingestion -t -c "SELECT COUNT(*) FROM ingested_events;" 2>/dev/null | tr -d ' ' || echo "0")

    # Check if ingestion completed successfully
    if docker logs assignment-ingestion 2>&1 | grep -q "ingestion complete" 2>/dev/null; then
        echo ""
        echo "=============================================="
        echo "INGESTION COMPLETE!"
        echo "Total events: $COUNT"
        echo "Event IDs exported to: output/event_ids.txt"
        echo "=============================================="
        exit 0
    fi

    # Check if the ingestion container has stopped (crashed or exited)
    if ! docker ps --format '{{.Names}}' | grep -q "assignment-ingestion"; then
        EXIT_CODE=$(docker inspect assignment-ingestion --format='{{.State.ExitCode}}' 2>/dev/null || echo "unknown")
        if [ "$EXIT_CODE" = "0" ]; then
            echo ""
            echo "=============================================="
            echo "INGESTION COMPLETE!"
            echo "Total events: $COUNT"
            echo "=============================================="
            exit 0
        else
            echo ""
            echo "ERROR: Ingestion container stopped with exit code $EXIT_CODE"
            echo "Check logs with: docker logs assignment-ingestion"
            exit 1
        fi
    fi

    echo "[$(date '+%H:%M:%S')] Events ingested: $COUNT"
    sleep 5
done
