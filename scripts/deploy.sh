#!/bin/bash
# ============================================================
# Safe Deploy Script for Polymarket Weather Bot
# 
# NEVER deletes the database. Always backs up before deploying.
# Usage: ./scripts/deploy.sh
# ============================================================

set -e

DROPLET="root@161.35.129.129"
REMOTE_DIR="/opt/polymarket-weather"
SSH_KEY="$HOME/.ssh/droplet_key"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${GREEN}=== Polymarket Weather Bot — Safe Deploy ===${NC}"
echo ""

# Step 1: Backup the database on the droplet BEFORE touching anything
echo -e "${YELLOW}Step 1: Backing up database on droplet...${NC}"
ssh -i "$SSH_KEY" "$DROPLET" "
    cd $REMOTE_DIR
    if [ -f data/trades.db ]; then
        mkdir -p data/backups
        cp data/trades.db data/backups/trades_${TIMESTAMP}.db
        TRADE_COUNT=\$(sqlite3 data/trades.db 'SELECT COUNT(*) FROM trades;' 2>/dev/null || echo '0')
        echo \"Backed up trades.db (\$TRADE_COUNT trades) -> data/backups/trades_${TIMESTAMP}.db\"
        
        # Keep only last 10 backups
        cd data/backups
        ls -t trades_*.db | tail -n +11 | xargs -r rm --
        echo \"Backup retention: keeping last 10 backups\"
    else
        echo \"No database found (first deploy)\"
    fi
"
echo ""

# Step 2: Sync code files ONLY (exclude data, logs, .env, .venv)
echo -e "${YELLOW}Step 2: Syncing code to droplet...${NC}"

# Get the project root (where this script lives)
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

# Create a tarball of code only (no data, no venv, no env)
cd "$PROJECT_DIR"
tar -czf /tmp/pw-code-deploy.tar.gz \
    --exclude='.venv' \
    --exclude='__pycache__' \
    --exclude='*.pyc' \
    --exclude='data' \
    --exclude='logs' \
    --exclude='.env' \
    --exclude='.git' \
    --exclude='*.db' \
    --exclude='*.db-journal' \
    .

scp -i "$SSH_KEY" /tmp/pw-code-deploy.tar.gz "$DROPLET:/tmp/"

# Extract code WITHOUT overwriting data/, logs/, or .env
ssh -i "$SSH_KEY" "$DROPLET" "
    cd $REMOTE_DIR
    
    # Extract code files only, preserving data and config
    tar -xzf /tmp/pw-code-deploy.tar.gz \
        --exclude='data' \
        --exclude='logs' \
        --exclude='.env'
    
    rm /tmp/pw-code-deploy.tar.gz
    
    # Ensure directories exist
    mkdir -p data/reports data/backups logs
    
    echo 'Code synced successfully'
"
rm /tmp/pw-code-deploy.tar.gz
echo ""

# Step 3: Run any database migrations (add new columns without dropping data)
echo -e "${YELLOW}Step 3: Running database migrations...${NC}"
ssh -i "$SSH_KEY" "$DROPLET" "
    cd $REMOTE_DIR
    source .venv/bin/activate
    python3 -c \"
from core.database import get_engine, Base
from sqlalchemy import inspect

engine = get_engine()
inspector = inspect(engine)

# Check existing columns
existing_cols = {col['name'] for col in inspector.get_columns('trades')}
print(f'Existing trade columns: {len(existing_cols)}')

# Add any new columns that don't exist yet (non-destructive migration)
import sqlalchemy as sa
new_columns = {
    'actual_temp': 'FLOAT',
    'fee_usd': 'FLOAT DEFAULT 0.0',
    'resolution_price': 'FLOAT',
}

with engine.connect() as conn:
    for col_name, col_type in new_columns.items():
        if col_name not in existing_cols:
            conn.execute(sa.text(f'ALTER TABLE trades ADD COLUMN {col_name} {col_type}'))
            conn.commit()
            print(f'  Added new column: {col_name}')
        else:
            print(f'  Column exists: {col_name}')

print('Migration complete — no data lost')
\"
"
echo ""

# Step 4: Verify
echo -e "${YELLOW}Step 4: Verifying deployment...${NC}"
ssh -i "$SSH_KEY" "$DROPLET" "
    cd $REMOTE_DIR
    source .venv/bin/activate
    python3 -c \"
from core.database import session_scope, Trade
with session_scope() as s:
    total = s.query(Trade).count()
    resolved = s.query(Trade).filter_by(resolved=True).count()
    pending = s.query(Trade).filter_by(resolved=False).count()
    print(f'Database intact: {total} trades ({resolved} resolved, {pending} pending)')
\"
"
echo ""

echo -e "${GREEN}=== Deploy complete! Database preserved. ===${NC}"
echo ""
echo "To install new pip dependencies, run:"
echo "  ssh -i $SSH_KEY $DROPLET 'cd $REMOTE_DIR && source .venv/bin/activate && pip install -r requirements.txt'"
