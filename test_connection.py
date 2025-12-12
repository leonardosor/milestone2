#!/usr/bin/env python3
"""
Test Database Connections
Verifies connectivity to source and target databases
"""

import os
import sys
from pathlib import Path

# Add config to path
sys.path.append(str(Path(__file__).parent / "etl" / "config"))

try:
    import psycopg2
    from dotenv import load_dotenv
    from config_loader import ConfigLoader
except ImportError as e:
    print(f"Error: Required package not installed: {e}")
    print("Install with: pip install psycopg2-binary python-dotenv")
    sys.exit(1)

# Load environment
load_dotenv()

def test_local_database():
    """Test connection to local database"""
    print("\n" + "="*60)
    print("Testing LOCAL Database Connection")
    print("="*60)

    # Try to connect to local milestone2 database
    try:
        host = os.getenv("SOURCE_HOST", "localhost")
        port = os.getenv("SOURCE_PORT", "5432")
        database = os.getenv("SOURCE_DB", "milestone2")
        user = os.getenv("SOURCE_USER", "postgres")
        password = os.getenv("SOURCE_PASSWORD", "postgres")

        print(f"Connecting to: {user}@{host}:{port}/{database}")

        conn = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )

        # Test query
        cur = conn.cursor()
        cur.execute("SELECT version();")
        version = cur.fetchone()[0]

        # Get database size
        cur.execute(f"SELECT pg_size_pretty(pg_database_size('{database}'));")
        size = cur.fetchone()[0]

        # Count tables
        cur.execute("""
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema NOT IN ('pg_catalog', 'information_schema');
        """)
        table_count = cur.fetchone()[0]

        # List schemas
        cur.execute("""
            SELECT schema_name
            FROM information_schema.schemata
            WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast');
        """)
        schemas = [row[0] for row in cur.fetchall()]

        print("\n✅ Connection successful!")
        print(f"   PostgreSQL Version: {version.split(',')[0]}")
        print(f"   Database Size: {size}")
        print(f"   Total Tables: {table_count}")
        print(f"   Schemas: {', '.join(schemas)}")

        # Check for key tables
        print("\n📊 Checking for key tables:")
        for schema in schemas:
            cur.execute(f"""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = '{schema}'
                ORDER BY table_name;
            """)
            tables = [row[0] for row in cur.fetchall()]
            if tables:
                print(f"   {schema}: {len(tables)} table(s)")
                for table in tables[:5]:  # Show first 5
                    print(f"      - {table}")
                if len(tables) > 5:
                    print(f"      ... and {len(tables)-5} more")

        cur.close()
        conn.close()
        return True

    except psycopg2.OperationalError as e:
        print(f"\n❌ Connection failed: {e}")
        print("\nTroubleshooting:")
        print("  1. Check if PostgreSQL is running")
        print("  2. Verify credentials in .env or script defaults")
        print("  3. Check firewall settings")
        return False
    except Exception as e:
        print(f"\n❌ Error: {e}")
        return False

def test_container_database():
    """Test connection to container database"""
    print("\n" + "="*60)
    print("Testing CONTAINER Database Connection")
    print("="*60)

    try:
        # Load config using ConfigLoader
        config_loader = ConfigLoader()

        print(f"Configuration Type: {config_loader.config.get('database_type', 'not set')}")
        print(f"Schema: {config_loader.config.get('schema', 'not set')}")

        # Get connection params
        conn_params = config_loader.get_psycopg2_connection_params()

        print(f"Connecting to: {conn_params['user']}@{conn_params['host']}:{conn_params['port']}/{conn_params['dbname']}")

        conn = psycopg2.connect(**conn_params)

        # Test query
        cur = conn.cursor()
        cur.execute("SELECT version();")
        version = cur.fetchone()[0]

        # Get database size
        cur.execute(f"SELECT pg_size_pretty(pg_database_size('{conn_params['dbname']}'));")
        size = cur.fetchone()[0]

        # Count tables
        cur.execute("""
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema NOT IN ('pg_catalog', 'information_schema');
        """)
        table_count = cur.fetchone()[0]

        # Check PostGIS
        cur.execute("""
            SELECT EXISTS(
                SELECT 1 FROM pg_extension WHERE extname = 'postgis'
            );
        """)
        has_postgis = cur.fetchone()[0]

        print("\n✅ Connection successful!")
        print(f"   PostgreSQL Version: {version.split(',')[0]}")
        print(f"   Database Size: {size}")
        print(f"   Total Tables: {table_count}")
        print(f"   PostGIS Enabled: {'Yes' if has_postgis else 'No'}")

        cur.close()
        conn.close()
        return True

    except psycopg2.OperationalError as e:
        print(f"\n❌ Connection failed: {e}")
        print("\nTroubleshooting:")
        print("  1. Start the container: docker-compose -f docker-compose.prod.yml up -d db")
        print("  2. Check container status: docker ps | grep milestone2-db")
        print("  3. Verify .env configuration")
        return False
    except FileNotFoundError:
        print("\n❌ Configuration file not found")
        print("Make sure you're running this from the project root directory")
        return False
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_supabase_connection():
    """Test connection to Supabase (if configured)"""
    print("\n" + "="*60)
    print("Testing SUPABASE Connection (Optional)")
    print("="*60)

    # Check if Supabase is configured
    db_type = os.getenv("DATABASE_TYPE", "local")
    db_host = os.getenv("DB_HOST", "localhost")

    if db_type != "env" or "supabase.co" not in db_host:
        print("⏭️  Skipping - Supabase not configured")
        print("   To test Supabase, set DATABASE_TYPE=env and DB_HOST in .env")
        return None

    try:
        host = os.getenv("DB_HOST")
        port = int(os.getenv("DB_PORT", "5432"))
        database = os.getenv("DB_NAME", "postgres")
        user = os.getenv("DB_USER", "postgres")
        password = os.getenv("DB_PASSWORD")

        print(f"Connecting to: {user}@{host}:{port}/{database}")

        conn = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password,
            sslmode='require'
        )

        # Test query
        cur = conn.cursor()
        cur.execute("SELECT version();")
        version = cur.fetchone()[0]

        # Check PostGIS
        cur.execute("""
            SELECT EXISTS(
                SELECT 1 FROM pg_extension WHERE extname = 'postgis'
            );
        """)
        has_postgis = cur.fetchone()[0]

        print("\n✅ Supabase connection successful!")
        print(f"   PostgreSQL Version: {version.split(',')[0]}")
        print(f"   PostGIS Enabled: {'Yes' if has_postgis else 'No'}")

        if not has_postgis:
            print("\n   ⚠️  PostGIS not enabled. Enable it in Supabase:")
            print("      Dashboard → Database → Extensions → Enable 'postgis'")

        cur.close()
        conn.close()
        return True

    except Exception as e:
        print(f"\n❌ Connection failed: {e}")
        print("\nTroubleshooting:")
        print("  1. Check Supabase project is active")
        print("  2. Verify credentials in .env")
        print("  3. Check IP allowlist in Supabase settings")
        return False

def main():
    print("\n" + "="*60)
    print("  Database Connection Test Suite")
    print("="*60)

    results = {}

    # Test local database
    results['local'] = test_local_database()

    # Test container database
    results['container'] = test_container_database()

    # Test Supabase (if configured)
    results['supabase'] = test_supabase_connection()

    # Summary
    print("\n" + "="*60)
    print("  Test Summary")
    print("="*60)

    print(f"\n{'Test':<20} {'Status':<15} {'Ready for Migration?'}")
    print("-" * 60)

    local_status = "✅ Pass" if results['local'] else "❌ Fail"
    local_ready = "Yes" if results['local'] else "No - Fix connection first"
    print(f"{'Local Database':<20} {local_status:<15} {local_ready}")

    container_status = "✅ Pass" if results['container'] else "❌ Fail"
    container_ready = "Yes" if results['container'] else "No - Start container first"
    print(f"{'Container Database':<20} {container_status:<15} {container_ready}")

    if results['supabase'] is not None:
        supabase_status = "✅ Pass" if results['supabase'] else "❌ Fail"
        supabase_ready = "Yes" if results['supabase'] else "No - Check credentials"
        print(f"{'Supabase':<20} {supabase_status:<15} {supabase_ready}")

    print("\n" + "="*60)

    # Migration recommendations
    if results['local'] and results['container']:
        print("\n✅ Ready to migrate!")
        print("\nRun migration script:")
        print("  Windows: .\\migrate_database.ps1")
        print("  Linux/Mac: ./migrate_database.sh")
    elif results['local'] and results.get('supabase'):
        print("\n✅ Ready to migrate to Supabase!")
        print("\nFollow the Supabase migration steps in MIGRATION_GUIDE.md")
    elif results['local']:
        print("\n⚠️  Local database ready, but target not available")
        print("Start container: docker-compose -f docker-compose.prod.yml up -d db")
    else:
        print("\n❌ Cannot proceed with migration")
        print("Fix connection issues before migrating")

    print("="*60 + "\n")

if __name__ == "__main__":
    main()
