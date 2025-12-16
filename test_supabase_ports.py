#!/usr/bin/env python3
"""
Supabase Connection Test - Port 5432 vs 6543
Tests both session mode (5432) and transaction mode (6543)
"""

import os
import psycopg2
from urllib.parse import quote_plus
from sqlalchemy import create_engine, text
import time

# Load password from environment variable
SUPABASE_PASSWORD = os.getenv("SUPABASE_PW")
if not SUPABASE_PASSWORD:
    print("ERROR: SUPABASE_PW environment variable not set!")
    print("Please set it with: $env:SUPABASE_PW='your-password' (PowerShell)")
    exit(1)

# Test configurations for both ports
CONFIGS = [
    {
        "name": "Session Mode (Port 5432)",
        "host": "db.dplozyowioyjedbhykes.supabase.co",
        "port": 5432,
        "database": "postgres",
        "user": "postgres",
        "password": SUPABASE_PASSWORD,
        "description": "Direct session mode - better for persistent connections",
    },
    {
        "name": "Transaction Mode (Port 6543)",
        "host": "db.dplozyowioyjedbhykes.supabase.co",
        "port": 6543,
        "database": "postgres",
        "user": "postgres",
        "password": SUPABASE_PASSWORD,
        "description": "Connection pooler mode - better for serverless",
    },
]


def test_connection(config):
    """Test connection with detailed error reporting"""
    results = {
        "psycopg2": {"success": False, "time": 0, "error": ""},
        "sqlalchemy": {"success": False, "time": 0, "error": ""},
    }
    
    # Test 1: psycopg2
    try:
        start = time.time()
        conn = psycopg2.connect(
            host=config["host"],
            port=config["port"],
            database=config["database"],
            user=config["user"],
            password=config["password"],
            sslmode="require",
            connect_timeout=15,
        )
        cur = conn.cursor()
        cur.execute("SELECT version();")
        version = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM information_schema.tables;")
        table_count = cur.fetchone()[0]
        elapsed = time.time() - start
        
        results["psycopg2"]["success"] = True
        results["psycopg2"]["time"] = elapsed
        results["psycopg2"]["version"] = version.split(',')[0]
        results["psycopg2"]["tables"] = table_count
        
        cur.close()
        conn.close()
    except Exception as e:
        results["psycopg2"]["error"] = str(e)
    
    # Test 2: SQLAlchemy
    try:
        start = time.time()
        encoded_password = quote_plus(config["password"])
        conn_string = (
            f"postgresql://{config['user']}:{encoded_password}@"
            f"{config['host']}:{config['port']}/{config['database']}"
            f"?sslmode=require&connect_timeout=15"
        )
        
        engine = create_engine(
            conn_string,
            pool_pre_ping=True,
            pool_size=1,
            max_overflow=2,
            pool_timeout=30,
            connect_args={
                "sslmode": "require",
                "connect_timeout": 15,
            }
        )
        
        with engine.connect() as conn:
            result = conn.execute(text("SELECT version();"))
            version = result.fetchone()[0]
            result = conn.execute(text("SELECT COUNT(*) FROM information_schema.tables;"))
            table_count = result.fetchone()[0]
        
        elapsed = time.time() - start
        
        results["sqlalchemy"]["success"] = True
        results["sqlalchemy"]["time"] = elapsed
        results["sqlalchemy"]["version"] = version.split(',')[0]
        results["sqlalchemy"]["tables"] = table_count
        
        engine.dispose()
    except Exception as e:
        results["sqlalchemy"]["error"] = str(e)
    
    return results


def main():
    print("=" * 100)
    print("SUPABASE CONNECTION TEST - PORT COMPARISON")
    print("=" * 100)
    print("\nTesting which port works best for Streamlit Cloud deployment...")
    print()
    
    working_configs = []
    
    for config in CONFIGS:
        print(f"\n{'='*100}")
        print(f"Testing: {config['name']}")
        print(f"{'='*100}")
        print(f"Host: {config['host']}")
        print(f"Port: {config['port']}")
        print(f"Description: {config['description']}")
        print()
        
        results = test_connection(config)
        
        # Display psycopg2 results
        print("📊 psycopg2 Test:")
        print("-" * 100)
        if results["psycopg2"]["success"]:
            print(f"✅ SUCCESS")
            print(f"   Version: {results['psycopg2']['version']}")
            print(f"   Tables: {results['psycopg2']['tables']}")
            print(f"   Connection Time: {results['psycopg2']['time']:.3f}s")
        else:
            print(f"❌ FAILED")
            print(f"   Error: {results['psycopg2']['error']}")
        
        print()
        
        # Display SQLAlchemy results
        print("📊 SQLAlchemy Test:")
        print("-" * 100)
        if results["sqlalchemy"]["success"]:
            print(f"✅ SUCCESS")
            print(f"   Version: {results['sqlalchemy']['version']}")
            print(f"   Tables: {results['sqlalchemy']['tables']}")
            print(f"   Connection Time: {results['sqlalchemy']['time']:.3f}s")
        else:
            print(f"❌ FAILED")
            print(f"   Error: {results['sqlalchemy']['error']}")
        
        # Check if both passed
        if results["psycopg2"]["success"] and results["sqlalchemy"]["success"]:
            working_configs.append({
                "config": config,
                "avg_time": (results["psycopg2"]["time"] + results["sqlalchemy"]["time"]) / 2
            })
            print()
            print("🎉 BOTH TESTS PASSED!")
    
    # Summary
    print("\n" + "=" * 100)
    print("SUMMARY & RECOMMENDATIONS")
    print("=" * 100)
    
    if working_configs:
        # Sort by connection time
        working_configs.sort(key=lambda x: x["avg_time"])
        
        print(f"\n✅ {len(working_configs)} configuration(s) working:")
        print()
        
        for i, wc in enumerate(working_configs, 1):
            config = wc["config"]
            print(f"{i}. {config['name']} (Port {config['port']})")
            print(f"   Average connection time: {wc['avg_time']:.3f}s")
            print(f"   {config['description']}")
            print()
        
        # Recommendation
        best = working_configs[0]["config"]
        print("🏆 RECOMMENDED FOR STREAMLIT CLOUD:")
        print("=" * 100)
        print(f"\nUse {best['name']} (Port {best['port']})")
        print("\nStreamlit Cloud Secrets Configuration:")
        print()
        print("[database]")
        print(f'DB_HOST = "{best["host"]}"')
        print(f'DB_PORT = "{best["port"]}"')
        print(f'DB_NAME = "{best["database"]}"')
        print(f'DB_USER = "{best["user"]}"')
        print(f'DB_PASSWORD = "{best["password"]}"')
        print()
        
    else:
        print("\n❌ No working configurations found!")
        print("\nPossible issues:")
        print("1. Check if your Supabase project is active and not paused")
        print("2. Verify the password is correct")
        print("3. Check Supabase dashboard for any IP restrictions")
        print("4. Ensure your project supports the connection modes")
    
    print("=" * 100)


if __name__ == "__main__":
    main()
