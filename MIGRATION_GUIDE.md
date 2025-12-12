# Database Migration Guide

This guide will help you migrate your local `milestone2` PostgreSQL database to the containerized environment or to Supabase.

## Table of Contents
1. [Option 1: Migrate to Container PostgreSQL](#option-1-migrate-to-container-postgresql)
2. [Option 2: Migrate to Supabase](#option-2-migrate-to-supabase)
3. [Verification](#verification)
4. [Troubleshooting](#troubleshooting)

---

## Option 1: Migrate to Container PostgreSQL

### Prerequisites
- Docker Desktop running
- PostgreSQL client tools installed (pg_dump, psql)
- Access to your local `milestone2` database

### Quick Start

#### For Windows (PowerShell):
```powershell
# 1. Start the database container
docker-compose -f docker-compose.prod.yml up -d db

# 2. Run migration script
.\migrate_database.ps1
```

#### For Linux/Mac (Bash):
```bash
# 1. Start the database container
docker-compose -f docker-compose.prod.yml up -d db

# 2. Make script executable
chmod +x migrate_database.sh

# 3. Run migration script
./migrate_database.sh
```

### What the Script Does

1. **Backs up your local database** to `./db_backup/milestone2_dump_TIMESTAMP.sql`
2. **Checks prerequisites** (Docker, target container, pg_dump)
3. **Prepares target database** (creates database, enables PostGIS)
4. **Restores data** to container
5. **Verifies migration** (compares table counts)

### Manual Migration (Alternative)

If you prefer to do it manually:

```bash
# 1. Start database container
docker-compose -f docker-compose.prod.yml up -d db

# 2. Backup local database
pg_dump -h localhost -p 5432 -U postgres -d milestone2 --no-owner --no-acl -F p -f milestone2_backup.sql

# 3. Copy backup to container
docker cp milestone2_backup.sql milestone2-db:/tmp/

# 4. Restore in container
docker exec milestone2-db psql -U postgres -d milestone2 -f /tmp/milestone2_backup.sql

# 5. Verify
docker exec -it milestone2-db psql -U postgres -d milestone2 -c "\dt"
```

---

## Option 2: Migrate to Supabase

Supabase uses PostgreSQL, so migration is straightforward.

### Prerequisites
- Supabase account ([sign up free](https://supabase.com))
- Supabase project created
- PostgreSQL client tools installed

### Step 1: Get Supabase Connection Details

1. Go to your Supabase project
2. Navigate to **Settings** → **Database**
3. Note down:
   - Host (e.g., `db.xxxxx.supabase.co`)
   - Database name (usually `postgres`)
   - Port (`5432` for direct connection, `6543` for connection pooling)
   - User (usually `postgres`)
   - Password (your project password)

### Step 2: Backup Local Database

```bash
# Windows (PowerShell)
$env:PGPASSWORD="your_local_password"
pg_dump -h localhost -p 5432 -U postgres -d milestone2 --no-owner --no-acl -F p -f milestone2_backup.sql

# Linux/Mac (Bash)
export PGPASSWORD="your_local_password"
pg_dump -h localhost -p 5432 -U postgres -d milestone2 --no-owner --no-acl -F p -f milestone2_backup.sql
```

### Step 3: Restore to Supabase

```bash
# Windows (PowerShell)
$env:PGPASSWORD="your_supabase_password"
psql -h db.xxxxx.supabase.co -p 5432 -U postgres -d postgres -f milestone2_backup.sql

# Linux/Mac (Bash)
export PGPASSWORD="your_supabase_password"
psql -h db.xxxxx.supabase.co -p 5432 -U postgres -d postgres -f milestone2_backup.sql
```

### Step 4: Enable PostGIS Extension

```sql
-- Connect to Supabase via psql or SQL Editor
CREATE EXTENSION IF NOT EXISTS postgis;
```

### Step 5: Update Application Configuration

Edit your `.env` file:

```bash
DATABASE_TYPE=env
DB_HOST=db.xxxxx.supabase.co
DB_PORT=5432  # or 6543 for connection pooling
DB_NAME=postgres
DB_USER=postgres
DB_PASSWORD=your_supabase_password
DB_SCHEMA=public  # or your schema name
```

### Step 6: Update Connection String

For Supabase, use the connection pooler for better performance:

```bash
# In your .env
DB_HOST=db.xxxxx.supabase.co
DB_PORT=6543  # Transaction mode pooler
```

### Step 7: Test Connection

```bash
# Start your application
docker-compose -f docker-compose.prod.yml up -d streamlit

# Check Streamlit UI at http://localhost:8501
# The home page should show "Database connection is active"
```

---

## Verification

After migration, verify your data:

### 1. Check Tables

```bash
# Container PostgreSQL
docker exec -it milestone2-db psql -U postgres -d milestone2 -c "\dt"

# Supabase
psql -h db.xxxxx.supabase.co -U postgres -d postgres -c "\dt"
```

### 2. Check Row Counts

```sql
-- Check census data
SELECT COUNT(*) FROM test.census_data;

-- Check urban data
SELECT COUNT(*) FROM test.urban_ccd_directory_exp;

-- Check location data
SELECT COUNT(*) FROM test.location_data;
```

### 3. Check Schemas

```sql
-- List all schemas
SELECT schema_name FROM information_schema.schemata
WHERE schema_name NOT IN ('pg_catalog', 'information_schema');
```

### 4. Test Application

1. Start services: `docker-compose -f docker-compose.prod.yml up`
2. Open Streamlit: http://localhost:8501
3. Go to **Database Explorer** page
4. Browse your tables and run queries

---

## Troubleshooting

### Issue: pg_dump not found

**Solution**: Install PostgreSQL client tools

- **Windows**: Download from [postgresql.org/download/windows](https://www.postgresql.org/download/windows/)
- **Mac**: `brew install postgresql`
- **Linux**: `sudo apt-get install postgresql-client`

### Issue: Permission denied on restore

**Solution**: Use `--no-owner --no-acl` flags

```bash
pg_dump -h localhost -U postgres -d milestone2 --no-owner --no-acl -F p -f backup.sql
```

### Issue: Container not running

**Solution**: Start the database container

```bash
docker-compose -f docker-compose.prod.yml up -d db
```

### Issue: Table already exists errors

**Solution**: Drop and recreate database

```bash
# Container
docker exec milestone2-db psql -U postgres -c "DROP DATABASE IF EXISTS milestone2;"
docker exec milestone2-db psql -U postgres -c "CREATE DATABASE milestone2;"

# Supabase - drop specific schema
psql -h db.xxxxx.supabase.co -U postgres -d postgres -c "DROP SCHEMA test CASCADE;"
```

### Issue: PostGIS extension not available

**Solution**: Enable PostGIS in Supabase

1. Go to **Database** → **Extensions**
2. Search for "postgis"
3. Enable the extension

Or via SQL:
```sql
CREATE EXTENSION IF NOT EXISTS postgis;
```

### Issue: Connection timeout to Supabase

**Solution**: Check firewall and use connection pooling

```bash
# Try connection pooler port (6543)
DB_PORT=6543

# Or use direct connection (5432) with SSL
DB_PORT=5432
# Add to connection string: ?sslmode=require
```

### Issue: Data not showing in Streamlit

**Solution**: Check schema configuration

1. Verify schema name in `.env`:
```bash
DB_SCHEMA=test  # or your schema name
```

2. Check if schema exists:
```sql
SELECT schema_name FROM information_schema.schemata;
```

3. Restart Streamlit:
```bash
docker-compose -f docker-compose.prod.yml restart streamlit
```

---

## Post-Migration Checklist

- [ ] Backup file created successfully
- [ ] All tables migrated (verify count)
- [ ] Data integrity verified (spot-check some rows)
- [ ] PostGIS extension enabled
- [ ] Schemas created correctly
- [ ] Application connects successfully
- [ ] ETL pipelines can write to database
- [ ] dbt models run successfully
- [ ] Streamlit pages load correctly

---

## Migration Comparison

| Feature | Container PostgreSQL | Supabase |
|---------|---------------------|----------|
| **Cost** | Free (local resources) | Free tier: 500MB, paid beyond |
| **Setup** | Simple (one command) | Account setup required |
| **Performance** | Local (fastest) | Network latency |
| **Backups** | Manual (Docker volumes) | Automatic daily backups |
| **Scalability** | Limited to your machine | Elastic scaling |
| **Maintenance** | You manage | Managed service |
| **Access** | Local only | Internet accessible |
| **Best for** | Development, testing | Production, collaboration |

---

## Free Database Hosting Alternatives

If you want to try other free hosting options:

### 1. **Supabase** (Recommended)
- Free tier: 500MB database, 2GB bandwidth
- PostgreSQL with PostGIS
- Sign up: https://supabase.com

### 2. **ElephantSQL**
- Free tier: 20MB database
- PostgreSQL only
- Good for testing
- Sign up: https://www.elephantsql.com

### 3. **Neon**
- Free tier: 3GB database
- Serverless PostgreSQL
- Modern architecture
- Sign up: https://neon.tech

### 4. **Railway**
- Free tier: $5 credit/month
- PostgreSQL with extensions
- Easy deployment
- Sign up: https://railway.app

### Note on Free Tiers
- Supabase is best for this project (PostGIS support + generous limits)
- Container PostgreSQL is free and unlimited (uses your machine)
- Choose based on your needs (development vs. production)

---

## Backup Strategy

After successful migration, establish a backup strategy:

### Container PostgreSQL
```bash
# Automated daily backup script
docker exec milestone2-db pg_dump -U postgres milestone2 > backup_$(date +%Y%m%d).sql
```

### Supabase
- Automatic daily backups included
- Manual backup: Project Settings → Database → Backups

---

## Next Steps

1. ✅ Complete migration
2. ✅ Verify all data
3. Run ETL pipeline to test write operations
4. Set up regular backups
5. Monitor database size
6. Consider scaling options if needed

---

## Need Help?

- Check logs: `docker-compose -f docker-compose.prod.yml logs`
- Test connection: Use Streamlit's Database Explorer page
- Manual verification: `docker exec -it milestone2-db psql -U postgres -d milestone2`

**Support Resources:**
- Supabase Docs: https://supabase.com/docs
- PostgreSQL Docs: https://www.postgresql.org/docs/
- Project README: [README.md](README.md)
- Deployment Guide: [DEPLOYMENT.md](DEPLOYMENT.md)
