#!/usr/bin/env python3
"""
Direct Supabase Connection Test
Tests connection with various configurations to find what works
"""

import os
from urllib.parse import quote_plus

import psycopg2
from sqlalchemy import create_engine, text

# Load password from environment variable
SUPABASE_PASSWORD = os.getenv("SUPABASE_PW")
if not SUPABASE_PASSWORD:
    print("ERROR: SUPABASE_PW environment variable not set!")
    print("Please set it with: $env:SUPABASE_PW='your-password' (PowerShell)")
    exit(1)

# Test configurations
CONFIGS = [
    {
        "name": "Config 1: Port 6543 with postgres.PROJECT_REF username",
        "host": "db.dplozyowioyjedbhykes.supabase.co",
        "port": 6543,
        "database": "postgres",
        "user": "postgres.dplozyowioyjedbhykes",
        "password": SUPABASE_PASSWORD,
    },
    {
        "name": "Config 2: Port 6543 with plain postgres username",
        "host": "db.dplozyowioyjedbhykes.supabase.co",
        "port": 6543,
        "database": "postgres",
        "user": "postgres",
        "password": SUPABASE_PASSWORD,
    },
    {
        "name": "Config 3: Port 5432 with postgres.PROJECT_REF username",
        "host": "db.dplozyowioyjedbhykes.supabase.co",
        "port": 5432,
        "database": "postgres",
        "user": "postgres.dplozyowioyjedbhykes",
        "password": SUPABASE_PASSWORD,
    },
    {
        "name": "Config 4: Port 5432 with plain postgres username",
        "host": "db.dplozyowioyjedbhykes.supabase.co",
        "port": 5432,
        "database": "postgres",
        "user": "postgres",
        "password": SUPABASE_PASSWORD,
    },
]


def test_psycopg2(config):
    """Test with psycopg2 directly"""
    try:
        conn = psycopg2.connect(
            host=config["host"],
            port=config["port"],
            database=config["database"],
            user=config["user"],
            password=config["password"],
            sslmode="require",
            connect_timeout=10,
        )
        cur = conn.cursor()
        cur.execute("SELECT version();")
        version = cur.fetchone()[0]
        cur.close()
        conn.close()
        return True, f"Success! Version: {version.split(',')[0]}"
    except Exception as e:
        return False, str(e)


def test_sqlalchemy(config):
    """Test with SQLAlchemy"""
    try:
        encoded_password = quote_plus(config["password"])
        conn_string = (
            f"postgresql://{config['user']}:{encoded_password}@"
            f"{config['host']}:{config['port']}/{config['database']}"
            f"?sslmode=require&connect_timeout=10"
        )

        engine = create_engine(
            conn_string,
            pool_pre_ping=True,
            connect_args={
                "sslmode": "require",
                "connect_timeout": 10,
            },
        )

        with engine.connect() as conn:
            result = conn.execute(text("SELECT version();"))
            version = result.fetchone()[0]

        return True, f"Success! Version: {version.split(',')[0]}"
    except Exception as e:
        return False, str(e)


def main():
    print("=" * 80)
    print("SUPABASE CONNECTION TESTING")
    print("=" * 80)
    print()

    for i, config in enumerate(CONFIGS, 1):
        print(f"\n{config['name']}")
        print("-" * 80)
        print(f"Host: {config['host']}")
        print(f"Port: {config['port']}")
        print(f"User: {config['user']}")
        print(f"Database: {config['database']}")
        print()

        # Test with psycopg2
        print("Testing with psycopg2...")
        success, message = test_psycopg2(config)
        if success:
            print(f"✅ {message}")
        else:
            print(f"❌ Failed: {message}")

        print()

        # Test with SQLAlchemy
        print("Testing with SQLAlchemy...")
        success, message = test_sqlalchemy(config)
        if success:
            print(f"✅ {message}")
            print()
            print("🎉 THIS CONFIGURATION WORKS! Use these settings:")
            print(f"   DB_HOST = \"{config['host']}\"")
            print(f"   DB_PORT = \"{config['port']}\"")
            print(f"   DB_USER = \"{config['user']}\"")
            print(f"   DB_NAME = \"{config['database']}\"")
            print()
            break
        else:
            print(f"❌ Failed: {message}")

    print("\n" + "=" * 80)
    print("TEST COMPLETE")
    print("=" * 80)


if __name__ == "__main__":
    main()
