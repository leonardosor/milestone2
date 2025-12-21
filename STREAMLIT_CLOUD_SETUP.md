# Streamlit Cloud Setup Guide

## Issue: Connection to Supabase Failing

The error "Cannot assign requested address" on Streamlit Cloud typically indicates:
1. **IPv6 connectivity issue** - Streamlit Cloud may not support IPv6 connections to Supabase
2. **Connection pooler mode** - Port 6543 (transaction mode) may not be accessible
3. **Alternative solution needed** - Use Supabase's session mode (port 5432) or connection pooler

### Understanding the Error
The error happens because Streamlit Cloud's infrastructure tries to connect via IPv6 (`2600:1f13:838:...`),
but this might not be properly configured or supported for Supabase's transaction pooler (port 6543).

## Step-by-Step Fix

### Option A: Use Session Mode (Port 5432) - RECOMMENDED FOR STREAMLIT CLOUD

Supabase offers two connection modes. Port 6543 (transaction mode) may have IPv6 issues on Streamlit Cloud.
Try using Port 5432 (session mode) instead:

```toml
[database]
DB_HOST = "db.dplozyowioyjedbhykes.supabase.co"
DB_PORT = "5432"
DB_NAME = "postgres"
DB_USER = "postgres"
DB_PASSWORD = "your-actual-supabase-password"
DB_SCHEMA = "test"
```

**Why this works:**
- Port 5432 uses session mode which has better IPv4 support
- More compatible with Streamlit Cloud's infrastructure
- Establishes persistent connections

### Option B: Use Supavisor (Supabase Connection Pooler)

If Option A doesn't work, try using Supabase's newer pooler endpoint:

1. Go to your Supabase Dashboard
2. Project Settings → Database
3. Look for "Supavisor" or "Connection Pooler" section
4. Use the pooler connection string (usually port 6543 with different host)

### Option C: Use PgBouncer Mode

Some Supabase projects support PgBouncer:

```toml
[database]
DB_HOST = "db.dplozyowioyjedbhykes.supabase.co"
DB_PORT = "6543"
DB_NAME = "postgres"
DB_USER = "postgres"
DB_PASSWORD = "your-actual-supabase-password"
DB_SCHEMA = "test"
```

With connection parameter: `?pgbouncer=true`

---

## Original Instructions (For Reference)

### 1. Update Streamlit Cloud Secrets

Go to your Streamlit Cloud dashboard:
1. Navigate to https://share.streamlit.io/
2. Find your app
3. Click on "⚙️ Settings"
4. Click on "Secrets" in the left sidebar
5. Replace the entire contents with:

```toml
[database]
DB_HOST = "db.dplozyowioyjedbhykes.supabase.co"
DB_PORT = "6543"
DB_NAME = "postgres"
DB_USER = "postgres"
DB_PASSWORD = "your-actual-supabase-password"
DB_SCHEMA = "test"
```

**Important Notes:**
- Use port **6543** (Supabase's transaction mode port), not 5432
- Username: **postgres** (plain, without project reference)
- Password: Your actual Supabase password

### 2. Verify Supabase Settings

Check your Supabase database settings:

1. Go to https://supabase.com/dashboard/project/dplozyowioyjedbhykes
2. Click on "Project Settings" (gear icon)
3. Click on "Database"
4. Verify the connection details under "Connection string"

### 3. Alternative: Use Connection Pooling (Recommended)

Supabase offers two connection modes:

**Transaction Mode (Port 6543)** - Recommended for serverless:
- Better for Streamlit Cloud
- Handles many short-lived connections
- No connection pooling needed

**Session Mode (Port 5432)** - For persistent connections:
- Better for long-running apps
- Can hit connection limits faster

For Streamlit Cloud, use **Transaction Mode (port 6543)**.

### 4. Test Connection Locally First

Before deploying, test the connection locally:

```powershell
cd "d:\docs\MADS\696-Milestone 2\app"
streamlit run streamlit_app.py
```

If it works locally with the updated `secrets.toml`, it should work on Streamlit Cloud too.

### 5. Check Supabase IP Restrictions

If you still have issues:
1. Go to your Supabase project settings
2. Check "Network Restrictions"
3. Ensure Streamlit Cloud's IP ranges are not blocked
4. Consider temporarily disabling IP restrictions to test

### 6. Common Troubleshooting

**Error: "Cannot assign requested address"**
- Usually means wrong port or host
- Double-check port is 6543
- Verify host doesn't have typos

**Error: "password authentication failed"**
- Check username format: `postgres.PROJECT_REF`
- Verify password in Supabase dashboard
- Password may have been reset

**Error: "connection timed out"**
- Check Supabase project is not paused
- Verify network restrictions
- Ensure SSL is enabled (connection string includes `?sslmode=require`)

### 7. Update Connection String in Code (Already Done)

The `db_connector.py` already handles Supabase connections correctly:

```python
# Handle Supabase connections
if "supabase.co" in host:
    conn_string = f"postgresql://{username}:{encoded_password}@{host}:{port}/{database}?sslmode=require"
```

### 8. After Making Changes

1. Save secrets in Streamlit Cloud
2. Click "Reboot app" to restart with new secrets
3. Monitor the logs for any connection errors
4. Test the database connection from your app

### 9. Getting Your Correct Credentials

To find your exact Supabase credentials:

1. Go to Supabase Dashboard: https://supabase.com/dashboard
2. Select your project: `dplozyowioyjedbhykes`
3. Go to Project Settings → Database
4. Look for "Connection string" section
5. Select "URI" format
6. You'll see something like:
   ```
   postgresql://postgres:[YOUR-PASSWORD]@db.dplozyowioyjedbhykes.supabase.co:6543/postgres
   ```
7. Extract:
   - Host: `db.dplozyowioyjedbhykes.supabase.co`
   - Port: `6543`
   - User: `postgres` (plain username, no project reference)
   - Database: `postgres`

## Quick Fix Summary

Update your Streamlit Cloud secrets to use:
- **Port: 6543** (not 5432)
- **Username: postgres** (plain username, not postgres.PROJECT_REF)
- **SSL Mode: Enabled** (automatically handled in code)

Then reboot your Streamlit Cloud app.
