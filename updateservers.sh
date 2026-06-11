#!/bin/bash
# updateservers.sh
# Reads encoder server IPs from the Server table and updates the ingest whitelist.
# Run this whenever you add/remove encoder servers.
#
# Usage: ./updateservers.sh
# Requires: psql, jq (or fallback to sed parsing)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
IPS_FILE="$SCRIPT_DIR/allowed_ips.json"

# Database connection — same as FireStreamEncoder
DB_URL="${DATABASE_URL:-postgresql://firestream:SiMoX1234%40%40@178.105.14.120:5432/openvibe}"

# Query all encoder server IPs
echo "[updateservers] querying Server table for IPs..."
IPS=$(psql "$DB_URL" -t -A -c "SELECT ip FROM \"Server\" WHERE ip IS NOT NULL AND ip != ''")

if [ -z "$IPS" ]; then
  echo "[updateservers] WARNING: no IPs found in Server table"
  echo "[]" > "$IPS_FILE"
else
  # Build JSON array
  JSON_ARRAY="["
  FIRST=true
  while IFS= read -r ip; do
    ip=$(echo "$ip" | xargs)  # trim whitespace
    if [ -n "$ip" ]; then
      if [ "$FIRST" = true ]; then
        FIRST=false
      else
        JSON_ARRAY+=","
      fi
      JSON_ARRAY+="\"$ip\""
    fi
  done <<< "$IPS"
  JSON_ARRAY+="]"

  echo "$JSON_ARRAY" > "$IPS_FILE"
  echo "[updateservers] wrote $(echo "$JSON_ARRAY" | grep -o '"' | wc -l | awk '{print int($1/2)}') IPs to $IPS_FILE"
fi

# Send SIGHUP to the ingest process to reload without restart
PID=$(pgrep -f "node.*ingest.js" 2>/dev/null || true)
if [ -n "$PID" ]; then
  kill -HUP "$PID" 2>/dev/null && echo "[updateservers] sent SIGHUP to ingest (pid $PID)" || echo "[updateservers] could not signal ingest process"
else
  echo "[updateservers] ingest process not running — IPs written, will load on next start"
fi
