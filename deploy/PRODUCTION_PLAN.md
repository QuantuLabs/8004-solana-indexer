# 8004-solana-indexer Production Deployment Plan

## Infrastructure Overview

**Target**: Self-hosted production deployment on Contabo VPS
**Budget**: €10-25/month (VPS + RPC)
**Capacity**: 1-2M agents, 50-100M feedbacks

```
┌─────────────────────────────────────────────────────────────────┐
│                        Cloudflare                                │
│              (DNS, DDoS, Cache, Rate Limiting)                  │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                     Contabo VPS (€10/mo)                        │
│            12 vCPU │ 48GB RAM │ 250GB NVMe                      │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────┐    ┌───────────┐    ┌─────────────┐               │
│  │  Caddy  │───▶│  Indexer  │───▶│ pg_bouncer  │               │
│  │ :80/443 │    │   :3000   │    │   :6432     │               │
│  └─────────┘    └───────────┘    └──────┬──────┘               │
│                       │                  │                      │
│                       │                  ▼                      │
│                       │         ┌─────────────┐                 │
│                       │         │ PostgreSQL  │                 │
│                       │         │    :5432    │                 │
│                       │         └─────────────┘                 │
│                       │                                         │
│                       ▼                                         │
│              ┌─────────────────┐                                │
│              │  Dedicated RPC  │                                │
│              │ (Helius/Triton) │                                │
│              └─────────────────┘                                │
│                                                                 │
│  ┌─────────┐    ┌───────────────┐                              │
│  │ Netdata │    │ Cron Backups  │──▶ Cloudflare R2             │
│  │ :19999  │    │   (daily)     │                              │
│  └─────────┘    └───────────────┘                              │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## Known Limitations & Tradeoffs

| Limitation | Impact | Rationale |
|------------|--------|-----------|
| Single VPS (no HA) | Downtime on failure | €10/mo budget constraint |
| No auto-failover | Manual recovery | Acceptable for MVP/beta |
| CORS wildcard | Open API access | Intentional: public read-only API |
| Single region | EU/US latency | Cost vs. global distribution |

**SLA Target**: 99.5% (allows ~3.6h downtime/month)

---

## Server Specifications

| Component | Spec |
|-----------|------|
| **Provider** | Contabo Cloud VPS |
| **CPU** | 12 vCPU (AMD EPYC) |
| **RAM** | 48 GB DDR4 |
| **Storage** | 250 GB NVMe |
| **Network** | 600 Mbit/s |
| **OS** | Ubuntu 24.04 LTS |
| **Location** | EU (Germany) or US |

---

## Solana RPC Provider

> **Critical**: Public RPC (`api.mainnet-beta.solana.com`) is rate-limited and unsuitable for production indexing.

### Recommended Providers

| Provider | Free Tier | Paid | Notes |
|----------|-----------|------|-------|
| **Helius** | 100K credits/mo | $49/mo | Best DX, geyser support |
| **Triton** | 50M req/mo | $99/mo | High reliability |
| **QuickNode** | 10M req/mo | $49/mo | Multi-chain |
| **Shyft** | 100K req/day | $29/mo | Budget option |

### Configuration

```bash
# .env.production
# Option 1: Helius (recommended)
SOLANA_RPC_URL=https://mainnet.helius-rpc.com/?api-key=${HELIUS_API_KEY}
SOLANA_WS_URL=wss://mainnet.helius-rpc.com/?api-key=${HELIUS_API_KEY}

# Option 2: Triton
SOLANA_RPC_URL=https://quantulabs-mainnet.rpcpool.com/${TRITON_API_KEY}
SOLANA_WS_URL=wss://quantulabs-mainnet.rpcpool.com/${TRITON_API_KEY}

# Option 3: Self-hosted (advanced, requires dedicated server)
SOLANA_RPC_URL=http://localhost:8899
SOLANA_WS_URL=ws://localhost:8900
```

### Fallback Strategy

```typescript
// config.ts - RPC failover
export const RPC_ENDPOINTS = [
  process.env.SOLANA_RPC_URL_PRIMARY,   // Helius
  process.env.SOLANA_RPC_URL_SECONDARY, // Triton
  'https://api.mainnet-beta.solana.com' // Public fallback (emergency)
];
```

---

## Component Stack

### 1. PostgreSQL 16

**Purpose**: Primary datastore for indexed blockchain data

**Configuration** (`/etc/postgresql/16/main/postgresql.conf`):
```ini
# Memory (optimized for 48GB RAM)
shared_buffers = 12GB
effective_cache_size = 36GB
work_mem = 256MB
maintenance_work_mem = 2GB

# WAL & Archiving (for point-in-time recovery)
wal_level = replica
archive_mode = on
archive_command = 'test ! -f /var/lib/postgresql/wal_archive/%f && cp %p /var/lib/postgresql/wal_archive/%f'
wal_buffers = 64MB
checkpoint_completion_target = 0.9
max_wal_size = 4GB
min_wal_size = 1GB

# Connections (pg_bouncer handles pooling)
max_connections = 100

# Performance
random_page_cost = 1.1  # NVMe
effective_io_concurrency = 200
default_statistics_target = 200

# Parallel queries
max_parallel_workers_per_gather = 4
max_parallel_workers = 8

# Logging
log_destination = 'csvlog'
logging_collector = on
log_directory = '/var/log/postgresql'
log_filename = 'postgresql-%Y-%m-%d.log'
log_statement = 'ddl'
log_min_duration_statement = 1000  # Log slow queries > 1s
```

**pg_hba.conf**:
```
local   all   all                 peer
host    all   all   127.0.0.1/32  scram-sha-256
```

**WAL Archive Directory**:
```bash
mkdir -p /var/lib/postgresql/wal_archive
chown postgres:postgres /var/lib/postgresql/wal_archive
chmod 700 /var/lib/postgresql/wal_archive
```

### 2. pg_bouncer

**Purpose**: Connection pooling (prevents connection exhaustion)

**Configuration** (`/etc/pgbouncer/pgbouncer.ini`):
```ini
[databases]
indexer_8004 = host=127.0.0.1 port=5432 dbname=indexer_8004

[pgbouncer]
listen_addr = 127.0.0.1
listen_port = 6432
auth_type = scram-sha-256
auth_file = /etc/pgbouncer/userlist.txt

# Pool settings
pool_mode = transaction
max_client_conn = 1000
default_pool_size = 50
min_pool_size = 10
reserve_pool_size = 10

# Timeouts
server_idle_timeout = 600
client_idle_timeout = 0
query_timeout = 30

# Logging
log_connections = 0
log_disconnections = 0
log_pooler_errors = 1
stats_period = 60
```

### 3. Caddy (Reverse Proxy)

**Purpose**: HTTPS termination, automatic SSL, rate limiting

**Caddyfile** (`/etc/caddy/Caddyfile`):
```caddyfile
{
    email admin@8004.io
    acme_ca https://acme-v02.api.letsencrypt.org/directory
}

api.8004.io {
    # Reverse proxy to indexer
    reverse_proxy localhost:3000 {
        header_up X-Real-IP {remote_host}
        header_up X-Forwarded-Proto {scheme}

        # Health checks
        health_uri /health
        health_interval 30s
        health_timeout 5s
    }

    # Compression
    encode gzip zstd

    # Security headers
    header {
        X-Content-Type-Options nosniff
        X-Frame-Options DENY
        Referrer-Policy strict-origin-when-cross-origin
        -Server
    }

    # CORS (public read-only API - wildcard intentional)
    header Access-Control-Allow-Origin "*"
    header Access-Control-Allow-Methods "GET, OPTIONS"
    header Access-Control-Allow-Headers "Content-Type"

    # Cache static responses
    @cacheable {
        path /rest/v1/global_stats*
        path /rest/v1/leaderboard*
        path /rest/v1/collection_stats*
    }
    header @cacheable Cache-Control "public, max-age=60"

    # Logging
    log {
        output file /var/log/caddy/access.log {
            roll_size 100mb
            roll_keep 5
        }
        format json
    }
}

# Metrics endpoint (internal only, not exposed)
:9180 {
    metrics /metrics
}
```

### 4. 8004-solana-indexer

**Secrets Management**:
```bash
# Generate secrets (run once, store securely)
DB_PASSWORD=$(openssl rand -base64 32)
echo "DB_PASSWORD=${DB_PASSWORD}" >> /root/.secrets/indexer.env

# Load secrets at runtime (not stored in .env files)
source /root/.secrets/indexer.env
```

**Environment** (`/etc/indexer-8004.env`):
```bash
# Database (password injected at runtime)
DATABASE_URL=POSTGRES_DSN_REDACTED

# Solana RPC (dedicated provider)
SOLANA_RPC_URL=https://mainnet.helius-rpc.com/?api-key=${HELIUS_API_KEY}
SOLANA_WS_URL=wss://mainnet.helius-rpc.com/?api-key=${HELIUS_API_KEY}

# Programs
AGENT_REGISTRY_PROGRAM_ID=8oo48pya1SZD23ZhzoNMhxR2UGb8BRa41Su4qP9EuaWm
ATOM_ENGINE_PROGRAM_ID=AToM1iKaniUCuWfHd5WQy5aLgJYWMiKq78NtNJmtzSXJ

# API
PORT=3000
HOST=127.0.0.1
NODE_ENV=production

# Indexer settings
POLL_INTERVAL_MS=1000
BATCH_SIZE=100
VERIFY_INTERVAL_MS=60000

# Logging
LOG_LEVEL=info
LOG_FORMAT=json
```

**Systemd Service** (`/etc/systemd/system/indexer-8004.service`):
```ini
[Unit]
Description=8004 Solana Indexer
After=network.target postgresql.service pgbouncer.service
Requires=postgresql.service pgbouncer.service

[Service]
Type=simple
User=indexer
Group=indexer
WorkingDirectory=/opt/8004-solana-indexer
ExecStart=/usr/bin/node dist/index.js
Restart=on-failure
RestartSec=5
StartLimitIntervalSec=300
StartLimitBurst=5

# Environment (secrets loaded from secure file)
EnvironmentFile=/etc/indexer-8004.env
EnvironmentFile=/root/.secrets/indexer.env

# Security
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/opt/8004-solana-indexer/logs

# Resource limits
MemoryMax=4G
CPUQuota=400%

# Logging
StandardOutput=journal
StandardError=journal
SyslogIdentifier=indexer-8004

[Install]
WantedBy=multi-user.target
```

### 5. Backup System

**Backup Script** (`/opt/scripts/backup-postgres.sh`):
```bash
#!/bin/bash
set -euo pipefail

BACKUP_DIR="/var/backups/postgresql"
WAL_ARCHIVE="/var/lib/postgresql/wal_archive"
DATE=$(date +%Y%m%d_%H%M%S)
RETENTION_DAYS=7

# Create backup
echo "[$(date)] Starting backup..."
pg_dump -U postgres -Fc indexer_8004 > "${BACKUP_DIR}/indexer_8004_${DATE}.dump"

# Compress with zstd
zstd -19 --rm "${BACKUP_DIR}/indexer_8004_${DATE}.dump"

# Upload to Cloudflare R2
rclone copy "${BACKUP_DIR}/indexer_8004_${DATE}.dump.zst" r2:8004-backups/daily/

# Archive WAL files to R2
if [ -d "$WAL_ARCHIVE" ] && [ "$(ls -A $WAL_ARCHIVE)" ]; then
    rclone sync "${WAL_ARCHIVE}/" r2:8004-backups/wal/
    # Clean up archived WAL files older than 24h
    find "${WAL_ARCHIVE}" -type f -mtime +1 -delete
fi

# Cleanup old local backups
find "${BACKUP_DIR}" -name "*.dump.zst" -mtime +${RETENTION_DAYS} -delete

# Cleanup old R2 backups (keep 30 days)
rclone delete r2:8004-backups/daily/ --min-age 30d

echo "[$(date)] Backup completed: indexer_8004_${DATE}.dump.zst"
```

**Backup Test Script** (`/opt/scripts/test-backup.sh`):
```bash
#!/bin/bash
set -euo pipefail

# Monthly backup integrity test
BACKUP_DIR="/var/backups/postgresql"
TEST_DB="indexer_8004_test"
LATEST=$(ls -t ${BACKUP_DIR}/*.dump.zst | head -1)

echo "[$(date)] Testing backup: ${LATEST}"

# Decompress
zstd -d -k "${LATEST}" -o /tmp/test_restore.dump

# Create test database
sudo -u postgres psql -c "DROP DATABASE IF EXISTS ${TEST_DB};"
sudo -u postgres psql -c "CREATE DATABASE ${TEST_DB};"

# Restore
sudo -u postgres pg_restore -d ${TEST_DB} /tmp/test_restore.dump

# Verify row counts
AGENTS=$(sudo -u postgres psql -t -d ${TEST_DB} -c "SELECT COUNT(*) FROM agents;")
FEEDBACKS=$(sudo -u postgres psql -t -d ${TEST_DB} -c "SELECT COUNT(*) FROM feedbacks;")

echo "[$(date)] Restore OK: ${AGENTS} agents, ${FEEDBACKS} feedbacks"

# Cleanup
sudo -u postgres psql -c "DROP DATABASE ${TEST_DB};"
rm /tmp/test_restore.dump

echo "[$(date)] Backup test PASSED"
```

**Cron** (`/etc/cron.d/postgres-backup`):
```cron
# Daily backup at 3 AM
0 3 * * * root /opt/scripts/backup-postgres.sh >> /var/log/backup.log 2>&1

# Monthly backup test (1st of month, 4 AM)
0 4 1 * * root /opt/scripts/test-backup.sh >> /var/log/backup-test.log 2>&1
```

**rclone config** (`/root/.config/rclone/rclone.conf`):
```ini
[r2]
type = s3
provider = Cloudflare
env_auth = true
endpoint = https://${CF_ACCOUNT_ID}.r2.cloudflarestorage.com
acl = private
```

```bash
# Secrets in environment (not config file)
echo 'export AWS_ACCESS_KEY_ID="<R2_ACCESS_KEY>"' >> /root/.secrets/rclone.env
echo 'export AWS_SECRET_ACCESS_KEY="<R2_SECRET_KEY>"' >> /root/.secrets/rclone.env
echo 'export CF_ACCOUNT_ID="<ACCOUNT_ID>"' >> /root/.secrets/rclone.env
chmod 600 /root/.secrets/rclone.env
```

### 6. Monitoring

**Netdata Installation**:
```bash
bash <(curl -Ss https://get.netdata.cloud/kickstart.sh) --stable-channel
```

**Custom Alerts** (`/etc/netdata/health.d/indexer.conf`):
```yaml
# PostgreSQL connections
alarm: postgres_connections_used
on: postgres.connections
lookup: average -1m percentage of max
units: %
every: 30s
warn: $this > 70
crit: $this > 90
info: PostgreSQL connection pool usage

# Disk space
alarm: disk_space_usage
on: disk.space
lookup: average -1m percentage of avail
units: %
every: 1m
warn: $this > 80
crit: $this > 90
info: Disk space usage

# Indexer process
alarm: indexer_process_down
on: apps.cpu
lookup: sum -1m of indexer
units: %
every: 30s
crit: $this == 0
info: Indexer process not running
```

**Application Metrics** (add to indexer):
```typescript
// src/metrics.ts
import { Registry, Counter, Histogram, Gauge } from 'prom-client';

export const registry = new Registry();

export const metrics = {
  eventsProcessed: new Counter({
    name: 'indexer_events_processed_total',
    help: 'Total events processed by type',
    labelNames: ['type'],
    registers: [registry],
  }),

  eventLatency: new Histogram({
    name: 'indexer_event_latency_seconds',
    help: 'Event processing latency',
    buckets: [0.01, 0.05, 0.1, 0.5, 1, 5],
    registers: [registry],
  }),

  lastProcessedSlot: new Gauge({
    name: 'indexer_last_processed_slot',
    help: 'Last processed Solana slot',
    registers: [registry],
  }),

  dbQueryDuration: new Histogram({
    name: 'indexer_db_query_duration_seconds',
    help: 'Database query duration',
    labelNames: ['operation'],
    buckets: [0.001, 0.01, 0.1, 0.5, 1],
    registers: [registry],
  }),
};

// Endpoint: GET /metrics
app.get('/metrics', async (req, res) => {
  res.set('Content-Type', registry.contentType);
  res.end(await registry.metrics());
});
```

**Health Endpoint** (enhanced):
```typescript
// GET /health
app.get('/health', async (req, res) => {
  const checks = {
    database: false,
    rpc: false,
    indexer: false,
  };

  try {
    // DB check
    await prisma.$queryRaw`SELECT 1`;
    checks.database = true;

    // RPC check
    const slot = await connection.getSlot();
    checks.rpc = slot > 0;

    // Indexer lag check (< 100 slots behind)
    const cursor = await prisma.indexerState.findUnique({ where: { id: 'main' } });
    checks.indexer = cursor && (slot - Number(cursor.lastSlot)) < 100;

    const healthy = Object.values(checks).every(Boolean);
    res.status(healthy ? 200 : 503).json({ status: healthy ? 'ok' : 'degraded', checks });
  } catch (error) {
    res.status(503).json({ status: 'error', error: error.message });
  }
});
```

---

## Cloudflare Configuration

### DNS Records

| Type | Name | Content | Proxy |
|------|------|---------|-------|
| A | api | `<VPS_IP>` | Proxied |
| A | @ | `<VPS_IP>` | Proxied |
| CNAME | www | 8004.io | Proxied |

### Page Rules

1. **API Caching**:
   - URL: `api.8004.io/rest/v1/global_stats*`
   - Cache Level: Cache Everything
   - Edge Cache TTL: 1 minute

2. **Bypass for mutations**:
   - URL: `api.8004.io/rest/v1/rpc/*`
   - Cache Level: Bypass

### Firewall Rules

```
# Rate limit API (primary - Cloudflare edge)
(http.host eq "api.8004.io" and http.request.uri.path contains "/rest/")
Action: Rate Limit (100 req/min per IP)

# Block script injection (SQL keywords are used by PostgREST query syntax)
(http.request.uri.query contains "<script" or
 http.request.uri.query contains "javascript:" or
 http.request.uri.query contains "onerror=")
Action: Block

# Allow only GET/OPTIONS (read-only API)
(http.host eq "api.8004.io" and
 not http.request.method in {"GET" "OPTIONS"})
Action: Block
```

### Cloudflare R2 (Backups)

- **Bucket**: `8004-backups`
- **Location**: Auto (nearest)
- **Free tier**: 10GB storage, 1M Class A ops, 10M Class B ops

---

## Deployment Steps

### Phase 1: Server Setup

```bash
# 1. Initial setup
apt update && apt upgrade -y
apt install -y curl git htop ufw fail2ban zstd

# 2. Create users
useradd -m -s /bin/bash indexer

# 3. Firewall
ufw default deny incoming
ufw default allow outgoing
ufw allow ssh
ufw allow 80/tcp
ufw allow 443/tcp
ufw enable

# 4. Fail2ban
systemctl enable fail2ban
systemctl start fail2ban

# 5. Secrets directory
mkdir -p /root/.secrets
chmod 700 /root/.secrets
```

### Phase 2: PostgreSQL

```bash
# 1. Install PostgreSQL 16
install -d /usr/share/keyrings
wget --quiet -O /usr/share/keyrings/pgdg.asc https://www.postgresql.org/media/keys/ACCC4CF8.asc
sh -c 'echo "deb [signed-by=/usr/share/keyrings/pgdg.asc] http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list'
apt update
apt install -y postgresql-16 postgresql-contrib-16

# 2. Generate secure password
DB_PASSWORD=$(openssl rand -base64 32)
echo "DB_PASSWORD=${DB_PASSWORD}" > /root/.secrets/indexer.env
chmod 600 /root/.secrets/indexer.env

# 3. Create WAL archive directory
mkdir -p /var/lib/postgresql/wal_archive
chown postgres:postgres /var/lib/postgresql/wal_archive

# 4. Apply PostgreSQL config (from above)

# 5. Create database and user
sudo -u postgres psql << EOF
CREATE USER indexer WITH PASSWORD '${DB_PASSWORD}';
CREATE DATABASE indexer_8004 OWNER indexer;
GRANT ALL PRIVILEGES ON DATABASE indexer_8004 TO indexer;
\c indexer_8004
GRANT ALL ON SCHEMA public TO indexer;
EOF

# 6. Restart
systemctl restart postgresql
```

### Phase 3: pg_bouncer

```bash
# 1. Install
apt install -y pgbouncer

# 2. Configure (as shown above)

# 3. User list (password from secrets)
source /root/.secrets/indexer.env
echo "\"indexer\" \"${DB_PASSWORD}\"" > /etc/pgbouncer/userlist.txt
chmod 600 /etc/pgbouncer/userlist.txt
chown postgres:postgres /etc/pgbouncer/userlist.txt

# 4. Start
systemctl enable pgbouncer
systemctl start pgbouncer
```

### Phase 4: Caddy

```bash
# 1. Install
apt install -y debian-keyring debian-archive-keyring apt-transport-https
curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/gpg.key' | gpg --dearmor -o /usr/share/keyrings/caddy-stable-archive-keyring.gpg
curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/debian.deb.txt' | tee /etc/apt/sources.list.d/caddy-stable.list
apt update
apt install -y caddy

# 2. Configure (Caddyfile as shown above)

# 3. Enable
systemctl enable caddy
systemctl start caddy
```

### Phase 5: Indexer

```bash
# 1. Install Node.js 20
curl -fsSL https://deb.nodesource.com/setup_20.x | bash -
apt install -y nodejs

# 2. Clone and build
cd /opt
git clone https://github.com/QuantuLabs/8004-solana-indexer.git
cd 8004-solana-indexer
npm ci
npm run build

# 3. Run Prisma migrations
npx prisma migrate deploy

# 4. Configure environment
cp /etc/indexer-8004.env.example /etc/indexer-8004.env
# Edit with production values (RPC keys, etc.)

# 5. Set permissions
chown -R indexer:indexer /opt/8004-solana-indexer

# 6. Install service
cp deploy/indexer-8004.service /etc/systemd/system/
systemctl daemon-reload
systemctl enable indexer-8004
systemctl start indexer-8004
```

### Phase 6: Backups

```bash
# 1. Install rclone
curl https://rclone.org/install.sh | bash

# 2. Configure R2 secrets
cat >> /root/.secrets/rclone.env << EOF
export AWS_ACCESS_KEY_ID="<R2_ACCESS_KEY>"
export AWS_SECRET_ACCESS_KEY="<R2_SECRET_KEY>"
export CF_ACCOUNT_ID="<ACCOUNT_ID>"
EOF
chmod 600 /root/.secrets/rclone.env

# 3. Configure rclone
mkdir -p /root/.config/rclone
# Add rclone.conf from above

# 4. Setup backup scripts
mkdir -p /opt/scripts /var/backups/postgresql
cp deploy/backup-postgres.sh /opt/scripts/
cp deploy/test-backup.sh /opt/scripts/
chmod +x /opt/scripts/*.sh

# 5. Wrapper script to load secrets
cat > /opt/scripts/run-backup.sh << 'EOF'
#!/bin/bash
source /root/.secrets/rclone.env
/opt/scripts/backup-postgres.sh
EOF
chmod +x /opt/scripts/run-backup.sh

# 6. Test backup
/opt/scripts/run-backup.sh

# 7. Install cron
cp deploy/postgres-backup.cron /etc/cron.d/postgres-backup
```

### Phase 7: Monitoring

```bash
# 1. Install Netdata
bash <(curl -Ss https://get.netdata.cloud/kickstart.sh) --stable-channel

# 2. Configure alerts
cp deploy/indexer.conf /etc/netdata/health.d/

# 3. Restart
systemctl restart netdata
```

---

## Rollback Procedures

### Application Rollback

```bash
# 1. List available versions
cd /opt/8004-solana-indexer
git log --oneline -10

# 2. Stop service
systemctl stop indexer-8004

# 3. Rollback to previous version
git checkout <commit-hash>
npm ci
npm run build

# 4. Restart
systemctl start indexer-8004

# 5. Verify
journalctl -u indexer-8004 -f
```

### Database Rollback (Point-in-Time)

```bash
# 1. Stop indexer
systemctl stop indexer-8004

# 2. Download backup and WAL from R2
rclone copy r2:8004-backups/daily/indexer_8004_YYYYMMDD.dump.zst /tmp/
rclone copy r2:8004-backups/wal/ /var/lib/postgresql/wal_restore/

# 3. Decompress
zstd -d /tmp/indexer_8004_YYYYMMDD.dump.zst

# 4. Restore base backup
sudo -u postgres pg_restore -d indexer_8004 --clean /tmp/indexer_8004_YYYYMMDD.dump

# 5. Apply WAL to target time (edit recovery.conf)
cat > /var/lib/postgresql/16/main/recovery.signal << EOF
restore_command = 'cp /var/lib/postgresql/wal_restore/%f %p'
recovery_target_time = '2026-02-03 12:00:00 UTC'
EOF

# 6. Restart PostgreSQL (will replay WAL)
systemctl restart postgresql

# 7. Restart indexer
systemctl start indexer-8004
```

---

## Disaster Recovery

### Scenario: Complete VPS Loss

**RTO**: ~2 hours | **RPO**: ~24 hours (daily backup)

```bash
# 1. Provision new VPS at Contabo

# 2. Run deployment phases 1-7 (automated with Ansible/script)

# 3. Restore from R2 backup
rclone copy r2:8004-backups/daily/ /var/backups/postgresql/ --include "*.dump.zst"
LATEST=$(ls -t /var/backups/postgresql/*.dump.zst | head -1)
zstd -d "${LATEST}"
sudo -u postgres pg_restore -d indexer_8004 ${LATEST%.zst}

# 4. Update Cloudflare DNS to new IP

# 5. Verify indexer catches up from last cursor
journalctl -u indexer-8004 -f
```

### Scenario: Database Corruption

```bash
# 1. Stop indexer
systemctl stop indexer-8004

# 2. Drop and recreate database
sudo -u postgres psql -c "DROP DATABASE indexer_8004;"
sudo -u postgres psql -c "CREATE DATABASE indexer_8004 OWNER indexer;"

# 3. Restore from backup
# (same as above)

# 4. Run Prisma migrations if needed
cd /opt/8004-solana-indexer
npx prisma migrate deploy

# 5. Restart
systemctl start indexer-8004
```

---

## Operational Procedures

### Health Checks

```bash
# Check all services
systemctl status postgresql pgbouncer caddy indexer-8004 netdata

# Check indexer logs
journalctl -u indexer-8004 -f

# Check database connections
sudo -u postgres psql -c "SELECT count(*) FROM pg_stat_activity;"

# Check pg_bouncer stats
PGPASSWORD="${DB_PASSWORD}" psql -h 127.0.0.1 -p 6432 -U indexer pgbouncer -c "SHOW STATS;"

# Check disk usage
df -h /var/lib/postgresql

# Check indexer lag
curl -s localhost:3000/health | jq
```

### Scaling Triggers

| Metric | Threshold | Action |
|--------|-----------|--------|
| Storage > 200GB | 80% | Add NVMe volume |
| RAM usage > 40GB | 83% | Upgrade VPS tier |
| QPS > 5000 | sustained | Add Redis cache layer |
| Agents > 2M | - | Evaluate sharding strategy |

---

## Security Checklist

- [ ] SSH key-only auth (disable password)
- [ ] UFW firewall enabled
- [ ] Fail2ban configured
- [ ] PostgreSQL only on localhost (127.0.0.1)
- [ ] pg_bouncer only on localhost
- [ ] Caddy HTTPS only
- [ ] Cloudflare proxy enabled
- [ ] Secrets in `/root/.secrets/` (chmod 600)
- [ ] No secrets in git or config files
- [ ] Regular security updates (unattended-upgrades)

---

## Cost Summary

| Service | Monthly Cost |
|---------|--------------|
| Contabo VPS | €10 |
| Helius RPC (starter) | $0-49 |
| Cloudflare (Free) | €0 |
| Cloudflare R2 (Free tier) | €0 |
| Domain (.io) | ~€3 (annual/12) |
| **Total** | **€13-60/mo** |

---

## Capacity Summary

| Metric | Limit |
|--------|-------|
| Agents | 1.5-2M |
| Feedbacks | 50-80M |
| Metadata | 15-20M |
| Validations | 3-5M |
| QPS | 3000-6000 |
| Concurrent connections | 1000 |

---

## Contacts & Resources

- **Contabo Support**: support@contabo.com
- **Cloudflare Status**: cloudflarestatus.com
- **Helius Support**: support@helius.dev
- **PostgreSQL Docs**: postgresql.org/docs/16
- **Caddy Docs**: caddyserver.com/docs
