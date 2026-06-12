#!/bin/bash
# updateservers.sh — sync allowed IPs from DB and reload ingest
set -euo pipefail
cd "$(dirname "$0")"
node scripts/update-allowed-ips.js
