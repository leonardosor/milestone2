# Quick Migration Steps

## Fast Track: Local DB → Container PostgreSQL

### Prerequisites Check
```bash
# 1. Docker running?
docker ps

# 2. PostgreSQL client installed?
pg_dump --version

# 3. Can connect to local database?
psql -h localhost -U postgres -d milestone2 -c "SELECT 1"
```

### 3-Step Migration

#### Step 1: Test Connections
```bash
# Install dependencies if needed
pip install psycopg2-binary python-dotenv

# Test both databases
python test_connection.py
```

Expected output:
```
✅ Local Database: Pass
✅ Container Database: Pass
```

#### Step 2: Start Container Database
```bash
docker-compose -f docker-compose.prod.yml up -d db

# Wait for it to be ready (10-15 seconds)
docker logs milestone2-db
```

#### Step 3: Run Migration
```powershell
# Windows
.\migrate_database.ps1

# Linux/Mac
chmod +x migrate_database.sh
./migrate_database.sh
```

### What Happens During Migration?

1. ✅ Backs up local database to `./db_backup/`
2. ✅ Creates database in container
3. ✅ Enables PostGIS extension
4. ✅ Restores all data
5. ✅ Verifies table counts match

### Verification

```bash
# Check container database
docker exec -it milestone2-db psql -U postgres -d milestone2

# In psql:
\dt                          # List tables
SELECT COUNT(*) FROM test.census_data;  # Check data
\q                           # Quit
```

### Test Application

```bash
# Start all services
docker-compose -f docker-compose.prod.yml up

# Open browser
# http://localhost:8501
```

Go to **Database Explorer** page and verify your data is visible.

---

## Alternative: Migrate to Supabase

### Step 1: Create Supabase Project
1. Go to [supabase.com](https://supabase.com)
2. Sign up / Log in
3. Create new project
4. Note your connection details

### Step 2: Get Connection Info
From Supabase dashboard:
- **Settings** → **Database**
- Copy connection string or note:
  - Host: `db.xxxxx.supabase.co`
  - Database: `postgres`
  - User: `postgres`
  - Password: (your project password)

### Step 3: Backup Local Database
```bash
# Set your local password
export PGPASSWORD="your_local_password"  # Linux/Mac
$env:PGPASSWORD="your_local_password"    # Windows PowerShell

# Create backup
pg_dump -h localhost -U postgres -d milestone2 --no-owner --no-acl -F p -f supabase_migration.sql
```

### Step 4: Restore to Supabase
```bash
# Set Supabase password
export PGPASSWORD="your_supabase_password"  # Linux/Mac
$env:PGPASSWORD="your_supabase_password"    # Windows PowerShell

# Restore
psql -h db.xxxxx.supabase.co -U postgres -d postgres -f supabase_migration.sql
```

### Step 5: Enable PostGIS
In Supabase dashboard:
1. Go to **Database** → **Extensions**
2. Search "postgis"
3. Click "Enable"

Or via SQL:
```sql
CREATE EXTENSION IF NOT EXISTS postgis;
```

### Step 6: Update Application Config
Edit `.env`:
```bash
DATABASE_TYPE=env
DB_HOST=db.xxxxx.supabase.co
DB_PORT=6543  # Use connection pooler for better performance
DB_NAME=postgres
DB_USER=postgres
DB_PASSWORD=your_supabase_password
DB_SCHEMA=public
```

### Step 7: Test
```bash
# Test connection
python test_connection.py

# Start app
docker-compose -f docker-compose.prod.yml up

# Verify at http://localhost:8501
```

---

## Troubleshooting

### Issue: "pg_dump: command not found"
**Fix**: Install PostgreSQL client
- Windows: https://www.postgresql.org/download/windows/
- Mac: `brew install postgresql`
- Linux: `sudo apt-get install postgresql-client`

### Issue: "Container milestone2-db not running"
**Fix**:
```bash
docker-compose -f docker-compose.prod.yml up -d db
# Wait 15 seconds
docker ps | grep milestone2-db
```

### Issue: "Connection refused"
**Fix**: Check your local PostgreSQL is running
```bash
# Windows
Get-Service postgresql*

# Linux/Mac
sudo systemctl status postgresql
```

### Issue: Migration script fails partway
**Fix**:
1. Check backup file exists: `ls ./db_backup/`
2. Drop target database and retry:
```bash
docker exec milestone2-db psql -U postgres -c "DROP DATABASE IF EXISTS milestone2;"
# Run migration script again
```

### Issue: Supabase connection timeout
**Fix**:
1. Check IP allowlist in Supabase: Settings → Database → Connection Info
2. Try port 5432 instead of 6543
3. Ensure SSL mode: Add `?sslmode=require` to connection string

---

## Quick Reference

### Local Credentials (Default)
```bash
Host: localhost
Port: 5432
Database: milestone2
User: postgres
Password: postgres  # (or your password)
```

### Container Credentials (Default)
```bash
Host: db (or localhost from host machine)
Port: 5432
Database: milestone2
User: postgres
Password: postgres  # (from .env)
```

### Supabase Credentials
```bash
Host: db.xxxxx.supabase.co  # From Supabase dashboard
Port: 5432 (direct) or 6543 (pooler)
Database: postgres
User: postgres
Password: (your project password)
```

---

## After Migration Checklist

- [ ] Backup file created successfully
- [ ] All tables present in target
- [ ] Row counts match between source and target
- [ ] PostGIS extension enabled
- [ ] Streamlit connects successfully
- [ ] Can query data in Database Explorer
- [ ] ETL pipelines can write to database
- [ ] Keep backup file safe (don't delete yet!)

---

## Next Steps

1. ✅ Migration complete
2. Test ETL pipeline: Run a small census ETL (2020-2021)
3. Run dbt transformations
4. Set up regular backups
5. Update any hardcoded connection strings in code

---

## Need More Details?

See [MIGRATION_GUIDE.md](MIGRATION_GUIDE.md) for:
- Detailed explanations
- Manual migration steps
- Multiple hosting options
- Advanced troubleshooting
- Backup strategies

---

## Rollback Plan

If something goes wrong:

1. **Stop containers**:
```bash
docker-compose -f docker-compose.prod.yml down
```

2. **Your local database is unchanged** - the migration only reads from it

3. **Restore from backup** (if needed):
```bash
# Container
docker exec -i milestone2-db psql -U postgres -d milestone2 < ./db_backup/milestone2_dump_TIMESTAMP.sql

# Supabase
psql -h db.xxxxx.supabase.co -U postgres -d postgres < ./db_backup/milestone2_dump_TIMESTAMP.sql
```

---

## Success Indicators

After migration, you should see:
- ✅ Streamlit UI loads at http://localhost:8501
- ✅ "Database connection is active" on home page
- ✅ Tables visible in Database Explorer
- ✅ Can run queries successfully
- ✅ ETL pipelines can connect and write data

**Congratulations! Your database is migrated! 🎉**
