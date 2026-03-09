#!/bin/bash
# API Discovery Script
# Run AFTER setting API_KEY: API_KEY=your_key bash scripts/discover.sh

BASE_URL="http://datasync-dev-alb-101078500.us-east-1.elb.amazonaws.com"

if [ -z "$API_KEY" ]; then
  echo "ERROR: API_KEY is not set."
  echo "Usage: API_KEY=your_key bash scripts/discover.sh"
  exit 1
fi

echo "=== Root Endpoint ==="
curl -s -D- -H "X-API-Key: $API_KEY" "$BASE_URL/api/v1" | head -40
echo -e "\n"

echo "=== Events (limit=1, verbose headers) ==="
curl -s -D- -H "X-API-Key: $API_KEY" "$BASE_URL/api/v1/events?limit=1" | head -80
echo -e "\n"

echo "=== Events max page size test (limit=10000) ==="
curl -s -D- -H "X-API-Key: $API_KEY" "$BASE_URL/api/v1/events?limit=10000" | head -80
echo -e "\n"

echo "=== Metrics ==="
curl -s -D- -H "X-API-Key: $API_KEY" "$BASE_URL/api/v1/metrics" | head -40
echo -e "\n"

echo "=== Sessions ==="
curl -s -D- -H "X-API-Key: $API_KEY" "$BASE_URL/api/v1/sessions?limit=1" | head -40
echo -e "\n"

echo "=== Probe: /api/v1/events/bulk ==="
curl -s -D- -H "X-API-Key: $API_KEY" "$BASE_URL/api/v1/events/bulk" | head -40
echo -e "\n"

echo "=== Probe: /api/v1/events/export ==="
curl -s -D- -H "X-API-Key: $API_KEY" "$BASE_URL/api/v1/events/export" | head -40
echo -e "\n"

echo "=== Probe: /api/v1/events/stream ==="
curl -s -D- -H "X-API-Key: $API_KEY" "$BASE_URL/api/v1/events/stream" | head -40
echo -e "\n"

echo "=== Probe: /api/v2 ==="
curl -s -D- -H "X-API-Key: $API_KEY" "$BASE_URL/api/v2" | head -40
echo -e "\n"

echo "=== Probe: /api/v2/events ==="
curl -s -D- -H "X-API-Key: $API_KEY" "$BASE_URL/api/v2/events?limit=1" | head -40
echo -e "\n"

echo "Discovery complete."
