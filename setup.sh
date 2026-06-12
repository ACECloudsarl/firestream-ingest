#!/bin/bash
# FireStream Ingest — one-command server setup
# Run as root:  bash setup.sh

set -euo pipefail

trap 'echo ""; echo "ERROR: Setup failed at line $LINENO. Check output above." >&2' ERR

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

echo "=== FireStream Ingest Setup ==="

# ── 1. System dependencies ──────────────────────────────────────────
echo "[1/8] Installing system packages..."
apt-get update
apt-get install -y nginx certbot curl

# ── 2. NVM + Node.js ───────────────────────────────────────────────
echo "[2/8] Installing NVM and Node.js..."
export NVM_DIR="$HOME/.nvm"
if [ ! -d "$NVM_DIR" ]; then
    curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.40.4/install.sh | bash
fi
[ -s "$NVM_DIR/nvm.sh" ] && \. "$NVM_DIR/nvm.sh"
[ -s "$NVM_DIR/bash_completion" ] && \. "$NVM_DIR/bash_completion"
nvm install node
echo "     Node $(node --version), npm $(npm --version)"

# ── 3. Environment file ────────────────────────────────────────────
echo "[3/8] Setting up .env..."
if [ ! -f .env ]; then
    if [ -f .env.example ]; then
        cp .env.example .env
        echo "     Created .env from .env.example — edit it with your settings."
    else
        echo "     WARNING: No .env.example found. Please create .env manually."
    fi
else
    echo "     .env already exists — skipping."
fi

# ── 4. Create server user and storage directories ──────────────────
echo "[4/8] Creating storage directories..."
INGEST_DIR=$(grep -oP 'INGEST_DIR=\K.*' .env 2>/dev/null || echo "/home/server/encodings")
MISC_DIR="/home/server/misc"

if ! id "server" &>/dev/null; then
    useradd -m -s /bin/bash server
    echo "     Created 'server' user."
fi

mkdir -p "$INGEST_DIR" "$MISC_DIR"
chown -R server:server "$INGEST_DIR" "$MISC_DIR" 2>/dev/null || true
echo "     $INGEST_DIR"
echo "     $MISC_DIR"

# ── 5. Install dependencies and PM2 ────────────────────────────────
echo "[5/8] Installing npm packages and PM2..."
npm install
if ! npm list -g pm2 &>/dev/null; then
    npm install -g pm2
else
    echo "     pm2 already installed — skipping."
fi

# ── 6. SSL certificate and nginx config ────────────────────────────
echo "[6/8] Setting up SSL and nginx..."
echo ""
read -rp "Enter your ingest domain (e.g. ingest-de-0.firestream.to) or 'none' to skip: " INGEST_DOMAIN
if [ -z "$INGEST_DOMAIN" ] || [[ "${INGEST_DOMAIN,,}" == "none" ]]; then
    echo "     No domain entered — skipping SSL and nginx setup."
    echo "     Run this later:"
    echo "       certbot certonly --webroot -w /var/www/html -d YOUR_DOMAIN"
else
    # Start nginx temporarily for certbot webroot challenge
    systemctl start nginx 2>/dev/null || true

    # Get SSL certificate (skip if already exists)
    CERT_PATH="/etc/letsencrypt/live/$INGEST_DOMAIN/fullchain.pem"
    if [ -f "$CERT_PATH" ]; then
        echo "     SSL certificate already exists for $INGEST_DOMAIN — skipping certbot."
    else
        echo "     Requesting SSL certificate for $INGEST_DOMAIN..."
        certbot certonly --webroot -w /var/www/html -d "$INGEST_DOMAIN" --non-interactive --agree-tos --register-unsafely-without-email
    fi

    # Deploy nginx config from project template
    rm -f /etc/nginx/sites-enabled/ingest-firestream
    rm -f /etc/nginx/sites-available/ingest-firestream
    rm -f /etc/nginx/sites-enabled/default
    cp ingest-firestream /etc/nginx/sites-available/ingest-firestream
    sed -i "s/__DOMAIN__/$INGEST_DOMAIN/g" /etc/nginx/sites-available/ingest-firestream
    ln -sf /etc/nginx/sites-available/ingest-firestream /etc/nginx/sites-enabled/ingest-firestream

    # Copy main nginx.conf if different
    cp nginx.conf /etc/nginx/nginx.conf

    # Test and reload
    if nginx -t 2>/dev/null; then
        systemctl reload nginx
        echo "     Nginx configured for $INGEST_DOMAIN"
    else
        echo "     ERROR: nginx config test failed. Check: nginx -t"
    fi
fi

# ── 7. Start ingest with PM2 ───────────────────────────────────────
echo "[7/8] Starting ingest with PM2..."
mkdir -p /var/log/firestream-dav
pm2 start ecosystem.config.cjs
pm2 save
pm2 startup
echo "     Ingest server is running."

# ── 8. Load allowed IPs from DB ────────────────────────────────────
echo "[8/8] Loading encoder IPs from database..."
node scripts/update-allowed-ips.js

echo ""
echo "=== Setup complete ==="
echo ""
if [ -n "$INGEST_DOMAIN" ] && [[ "${INGEST_DOMAIN,,}" != "none" ]]; then
    echo "Ingest server is live at https://$INGEST_DOMAIN"
else
    echo "Ingest server is running on port 2999 (no domain configured)."
fi
echo "Edit .env to adjust config (INGEST_DIR, PORT, DATABASE_URL)"
echo ""
