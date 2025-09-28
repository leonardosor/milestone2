#!/usr/bin/env python3
"""
Database Schema Explorer
========================

This script allows you to explore tables in a PostgreSQL database schema.
You can:
- Connect to your local database using environment variables
- List all schemas in the database
- Explore tables within a specific schema
- View table structure and sample data
- Execute custom SQL queries

Usage:
    python database_explorer.py
"""

import os
import warnings

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

warnings.filterwarnings("ignore")

# Load environment variables
load_dotenv()


def create_database_connection():
    """Create database connection using environment variables or config"""

    # Try to get connection details from environment variables first
    db_host = os.getenv("DB_HOST", "localhost")
    db_port = os.getenv("DB_PORT", "5432")
    db_name = os.getenv("DB_NAME", "milestone2")
    db_user = os.getenv("DB_USER", "postgres")
    db_password = os.getenv("DB_PASSWORD", "123")

    # If environment variables are not set, try to load from config.json
    if db_password == "123" and os.path.exists("config.json"):
        import json

        try:
            with open("config.json", "r") as f:
                config = json.load(f)
                local_db = config.get("local_database", {})
                db_host = local_db.get("host", db_host)
                db_port = local_db.get("port", db_port)
                db_name = local_db.get("database", db_name)
                db_user = local_db.get("username", db_user)
                db_password = local_db.get("password", db_password)
        except Exception as e:
            print(f"Warning: Could not load config.json: {e}")

    # Create connection string
    connection_string = (
        f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    )

    try:
        engine = create_engine(connection_string)
        # Test connection
        with engine.connect() as conn:
            result = conn.execute(text("SELECT version()"))
            version = result.fetchone()[0]
            print(f"✅ Connected to PostgreSQL successfully!")
            print(f"Database: {db_name} on {db_host}:{db_port}")
            print(f"PostgreSQL Version: {version.split(',')[0]}")
        return engine
    except Exception as e:
        print(f"❌ Failed to connect to database: {e}")
        print(
            f"Connection string: postgresql://{db_user}:***@{db_host}:{db_port}/{db_name}"
        )
        return None


def list_schemas(engine):
    """List all schemas in the database"""
    if not engine:
        print("No database connection available")
        return []

    try:
        with engine.connect() as conn:
            result = conn.execute(
                text(
                    """
                SELECT schema_name
                FROM information_schema.schemata
                WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
                ORDER BY schema_name
            """
                )
            )
            schemas = [row[0] for row in result.fetchall()]

            print(f"📚 Found {len(schemas)} user schemas:")
            for i, schema in enumerate(schemas, 1):
                print(f"  {i}. {schema}")

            return schemas
    except Exception as e:
        print(f"Error listing schemas: {e}")
        return []


def list_tables_in_schema(engine, schema_name):
    """List all tables in a specific schema"""
    if not engine:
        print("No database connection available")
        return []

    try:
        with engine.connect() as conn:
            result = conn.execute(
                text(
                    """
                                SELECT table_name,
                                             (
                                                     SELECT COUNT(*)
                                                     FROM information_schema.columns
                                                     WHERE table_schema = :schema
                                                         AND table_name = t.table_name
                                             ) as column_count
                FROM information_schema.tables t
                WHERE table_schema = :schema
                AND table_type = 'BASE TABLE'
                ORDER BY table_name
            """
                ),
                {"schema": schema_name},
            )

            tables = result.fetchall()

            if tables:
                print(f"📋 Tables in schema '{schema_name}':")
                for i, (table_name, column_count) in enumerate(tables, 1):
                    print(f"  {i}. {table_name} ({column_count} columns)")
            else:
                print(f"No tables found in schema '{schema_name}'")

            return [table[0] for table in tables]
    except Exception as e:
        print(f"Error listing tables: {e}")
        return []


def describe_table(engine, schema_name, table_name):
    """Show detailed information about a specific table"""
    if not engine:
        print("No database connection available")
        return

    try:
        with engine.connect() as conn:
            # Get column information
            columns_result = conn.execute(
                text(
                    """
                SELECT column_name, data_type, is_nullable, column_default, character_maximum_length
                FROM information_schema.columns
                WHERE table_schema = :schema AND table_name = :table
                ORDER BY ordinal_position
            """
                ),
                {"schema": schema_name, "table": table_name},
            )

            columns = columns_result.fetchall()

            # Get row count
            count_result = conn.execute(
                text(f"SELECT COUNT(*) FROM {schema_name}.{table_name}")
            )
            row_count = count_result.fetchone()[0]

            print(f"🔍 Table: {schema_name}.{table_name}")
            print(f"📊 Row count: {row_count:,}")
            print(f"📋 Columns ({len(columns)}):")
            print("-" * 80)

            for col in columns:
                col_name, data_type, nullable, default_val, max_length = col
                nullable_str = "NULL" if nullable == "YES" else "NOT NULL"
                length_str = f"({max_length})" if max_length else ""
                default_str = f" DEFAULT {default_val}" if default_val else ""

                print(
                    f"  {col_name:<25} {data_type}{length_str:<20} {nullable_str:<10}{default_str}"
                )

            return columns
    except Exception as e:
        print(f"Error describing table: {e}")
        return None


def show_table_sample(engine, schema_name, table_name, limit=10):
    """Show sample data from a table"""
    if not engine:
        print("No database connection available")
        return None

    try:
        query = f"SELECT * FROM {schema_name}.{table_name} LIMIT {limit}"
        df = pd.read_sql(query, engine)

        print(
            f"📊 Sample data from {schema_name}.{table_name} (showing {len(df)} rows):"
        )
        print("-" * 80)

        if not df.empty:
            print(df.to_string(index=False))
            print(f"\n📈 Data types:")
            print(df.dtypes)
        else:
            print("No data found in table")

        return df
    except Exception as e:
        print(f"Error showing table sample: {e}")
        return None


def execute_custom_query(engine, query, params=None):
    """Execute a custom SQL query"""
    if not engine:
        print("No database connection available")
        return None

    try:
        df = pd.read_sql(query, engine, params=params)

        if not df.empty:
            print(f"✅ Query executed successfully! Returned {len(df)} rows")
            print(df.to_string(index=False))
        else:
            print("Query executed successfully but returned no results")

        return df
    except Exception as e:
        print(f"❌ Query execution failed: {e}")
        return None


def get_database_stats(engine):
    """Get comprehensive database statistics"""
    if not engine:
        print("No database connection available")
        return

    try:
        with engine.connect() as conn:
            # Get database size
            size_query = """
            SELECT
                pg_size_pretty(pg_database_size(current_database())) as database_size,
                current_database() as database_name
            """
            size_result = conn.execute(text(size_query))
            db_info = size_result.fetchone()

            # Get schema and table counts
            stats_query = """
            SELECT
                COUNT(DISTINCT table_schema) as schema_count,
                COUNT(*) as table_count,
                SUM(CASE WHEN table_type = 'BASE TABLE' THEN 1 ELSE 0 END) as base_table_count,
                SUM(CASE WHEN table_type = 'VIEW' THEN 1 ELSE 0 END) as view_count
            FROM information_schema.tables
            WHERE table_schema NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
            """
            stats_result = conn.execute(text(stats_query))
            stats = stats_result.fetchone()

            print("📊 Database Statistics")
            print("=" * 50)
            print(f"Database: {db_info[1]}")
            print(f"Size: {db_info[0]}")
            print(f"Schemas: {stats[0]}")
            print(f"Total Tables/Views: {stats[1]}")
            print(f"Base Tables: {stats[2]}")
            print(f"Views: {stats[3]}")

    except Exception as e:
        print(f"Error getting database stats: {e}")


def get_dataframe_for_notebook(engine, schema_name, table_name, limit=None):
    """Load a table as DataFrame and return it for notebook use"""
    if not engine:
        print("No database connection available")
        return None

    try:
        # Build query with optional limit
        if limit:
            query = f"SELECT * FROM {schema_name}.{table_name} LIMIT {limit}"
        else:
            query = f"SELECT * FROM {schema_name}.{table_name}"

        print(f"🔄 Loading table {schema_name}.{table_name} as DataFrame...")
        df = pd.read_sql(query, engine)

        print(f"✅ Successfully loaded {schema_name}.{table_name}")
        print(f"�� DataFrame shape: {df.shape[0]} rows × {df.shape[1]} columns")

        return df

    except Exception as e:
        print(f"❌ Error loading table as DataFrame: {e}")
        return None


def display_connection_info():
    """Display current connection information"""
    print("🔗 Current Database Connection Info:")
    print(f"Host: {os.getenv('DB_HOST', 'localhost (default)')}")
    print(f"Port: {os.getenv('DB_PORT', '5432 (default)')}")
    print(f"Database: {os.getenv('DB_NAME', 'milestone2 (default)')}")
    print(f"User: {os.getenv('DB_USER', 'postgres (default)')}")
    print(
        f"Password: {'*' * len(os.getenv('DB_PASSWORD', '')) if os.getenv('DB_PASSWORD') else 'Not set'}"
    )
    print("\n💡 Tip: Create a .env file to override these defaults")


def interactive_menu(engine):
    """Interactive menu for database exploration"""
    # Global variable to store the current DataFrame
    current_df = None
    current_schema = None
    current_table = None

    while True:
        print("\n" + "=" * 60)
        print("🗄️  Database Schema Explorer Menu")
        print("=" * 60)
        print("1. List all schemas")
        print("2. Explore tables in a schema")
        print("3. Describe a specific table")
        print("4. Show sample data from a table")
        print("5. Execute custom SQL query")
        print("6. Show database statistics")
        print("7. Display connection info")
        print("8. Load table as DataFrame")
        print("9. Analyze current DataFrame")
        print("10. Export DataFrame")
        print("11. Filter DataFrame")
        print("12. Exit")
        print("-" * 60)

        # Show current DataFrame info if available
        if current_df is not None:
            print(
                f"📊 Current DataFrame: {current_schema}.{current_table} ({current_df.shape[0]} rows × {current_df.shape[1]} columns)"
            )
            print(f"💾 Memory: {current_df.memory_usage(deep=True).sum() / 1024:.1f} KB")
        print("-" * 60)

        choice = input("Enter your choice (1-12): ").strip()

        if choice == "1":
            schemas = list_schemas(engine)

        elif choice == "2":
            schemas = list_schemas(engine)
            if schemas:
                print(f"\nAvailable schemas: {schemas}")
                schema_name = input("Enter schema name to explore: ").strip()
                if schema_name in schemas:
                    list_tables_in_schema(engine, schema_name)
                else:
                    print(f"Schema '{schema_name}' not found!")

        elif choice == "3":
            schemas = list_schemas(engine)
            if schemas:
                print(f"\nAvailable schemas: {schemas}")
                schema_name = input("Enter schema name: ").strip()
                if schema_name in schemas:
                    tables = list_tables_in_schema(engine, schema_name)
                    if tables:
                        print(f"\nAvailable tables: {tables}")
                        table_name = input("Enter table name: ").strip()
                        if table_name in tables:
                            describe_table(engine, schema_name, table_name)
                        else:
                            print(f"Table '{table_name}' not found!")
                else:
                    print(f"Schema '{schema_name}' not found!")

        elif choice == "4":
            schemas = list_schemas(engine)
            if schemas:
                print(f"\nAvailable schemas: {schemas}")
                schema_name = input("Enter schema name: ").strip()
                if schema_name in schemas:
                    tables = list_tables_in_schema(engine, schema_name)
                    if tables:
                        print(f"\nAvailable tables: {tables}")
                        table_name = input("Enter table name: ").strip()
                        if table_name in tables:
                            limit = input(
                                "Enter number of rows to show (default 10): "
                            ).strip()
                            limit = int(limit) if limit.isdigit() else 10
                            show_table_sample(engine, schema_name, table_name, limit)
                        else:
                            print(f"Table '{table_name}' not found!")
                else:
                    print(f"Schema '{schema_name}' not found!")

        elif choice == "5":
            print("\nEnter your SQL query (type 'exit' to return to menu):")
            while True:
                query = input("SQL> ").strip()
                if query.lower() == "exit":
                    break
                if query:
                    execute_custom_query(engine, query)

        elif choice == "6":
            get_database_stats(engine)

        elif choice == "7":
            display_connection_info()

        elif choice == "8":
            # Load table as DataFrame
            schemas = list_schemas(engine)
            if schemas:
                print(f"\nAvailable schemas: {schemas}")
                schema_name = input("Enter schema name: ").strip()
                if schema_name in schemas:
                    tables = list_tables_in_schema(engine, schema_name)
                    if tables:
                        print(f"\nAvailable tables: {tables}")
                        table_name = input("Enter table name: ").strip()
                        if table_name in tables:
                            limit_input = input(
                                "Enter row limit (press Enter for all rows): "
                            ).strip()
                            limit = int(limit_input) if limit_input.isdigit() else None

                            # Load the DataFrame
                            df = load_table_as_dataframe(
                                engine, schema_name, table_name, limit
                            )
                            if df is not None:
                                current_df = df
                                current_schema = schema_name
                                current_table = table_name
                                print(
                                    f"\n✅ DataFrame loaded successfully! Use options 9-10 to work with it."
                                )
                            else:
                                print(f"\n❌ Failed to load DataFrame")
                        else:
                            print(f"Table '{table_name}' not found!")
                else:
                    print(f"Schema '{schema_name}' not found!")

        elif choice == "9":
            # Analyze current DataFrame
            if current_df is not None:
                analyze_dataframe(current_df, current_schema, current_table)
            else:
                print("❌ No DataFrame loaded. Please load a table first (option 8).")

        elif choice == "10":
            # Export DataFrame
            if current_df is not None:
                export_dataframe(current_df, current_schema, current_table)
            else:
                print("❌ No DataFrame loaded. Please load a table first (option 8).")

        elif choice == "11":
            # Filter DataFrame
            if current_df is not None:
                filtered_df = filter_dataframe(
                    current_df, current_schema, current_table
                )
                if filtered_df is not None:
                    print(
                        f"\n✅ Filtered DataFrame created with {len(filtered_df)} rows"
                    )
                    print("💡 You can now use option 9 to analyze the filtered data")
                    # Optionally replace current DataFrame with filtered one
                    replace = (
                        input(
                            "Replace current DataFrame with filtered version? (y/n): "
                        )
                        .strip()
                        .lower()
                    )
                    if replace == "y":
                        current_df = filtered_df
                        print("✅ Current DataFrame replaced with filtered version")
            else:
                print("❌ No DataFrame loaded. Please load a table first (option 8).")

        elif choice == "12":
            print("👋 Goodbye!")
            break

        else:
            print("❌ Invalid choice. Please enter a number between 1-12.")


def load_table_as_dataframe(engine, schema_name, table_name, limit=None):
    """Load a table as a pandas DataFrame"""
    if not engine:
        print("No database connection available")
        return None

    try:
        # Build query with optional limit
        if limit:
            query = f"SELECT * FROM {schema_name}.{table_name} LIMIT {limit}"
        else:
            query = f"SELECT * FROM {schema_name}.{table_name}"

        print(f"🔄 Loading table {schema_name}.{table_name} as DataFrame...")
        df = pd.read_sql(query, engine)

        print(f"✅ Successfully loaded {schema_name}.{table_name}")
        print(f"📊 DataFrame shape: {df.shape[0]} rows × {df.shape[1]} columns")
        print(f"📈 Data types:")
        print(df.dtypes)

        # Show basic statistics for numeric columns
        numeric_cols = df.select_dtypes(include=["number"]).columns
        if len(numeric_cols) > 0:
            print(f"\n📊 Basic statistics for numeric columns:")
            print(df[numeric_cols].describe())

        # Show memory usage
        memory_usage = df.memory_usage(deep=True).sum()
        print(f"\n💾 Memory usage: {memory_usage / 1024:.2f} KB")

        return df

    except Exception as e:
        print(f"❌ Error loading table as DataFrame: {e}")
        return None


def save_dataframe_to_csv(df, schema_name, table_name):
    """Save DataFrame to CSV file"""
    if df is None or df.empty:
        print("❌ No data to save")
        return False

    try:
        filename = f"{schema_name}_{table_name}_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv"
        df.to_csv(filename, index=False)
        print(f"💾 DataFrame saved to: {filename}")
        return True
    except Exception as e:
        print(f"❌ Error saving DataFrame: {e}")
        return False


def export_dataframe(df, schema_name, table_name):
    """Export DataFrame to various formats"""
    if df is None or df.empty:
        print("❌ No data to export")
        return False

    print(f"\n📤 Export Options for {schema_name}.{table_name}:")
    print("1. CSV file")
    print("2. Excel file (.xlsx)")
    print("3. JSON file")
    print("4. Parquet file")
    print("5. Cancel")

    choice = input("Enter your choice (1-5): ").strip()

    try:
        timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
        base_filename = f"{schema_name}_{table_name}_{timestamp}"

        if choice == "1":
            filename = f"{base_filename}.csv"
            df.to_csv(filename, index=False)
            print(f"💾 Exported to CSV: {filename}")
            return True

        elif choice == "2":
            filename = f"{base_filename}.xlsx"
            df.to_excel(filename, index=False, engine="openpyxl")
            print(f"💾 Exported to Excel: {filename}")
            return True

        elif choice == "3":
            filename = f"{base_filename}.json"
            df.to_json(filename, orient="records", indent=2)
            print(f"💾 Exported to JSON: {filename}")
            return True

        elif choice == "4":
            filename = f"{base_filename}.parquet"
            df.to_parquet(filename, index=False)
            print(f"💾 Exported to Parquet: {filename}")
            return True

        elif choice == "5":
            print("Export cancelled")
            return False

        else:
            print("❌ Invalid choice")
            return False

    except Exception as e:
        print(f"❌ Error exporting DataFrame: {e}")
        return False


def filter_dataframe(df, schema_name, table_name):
    """Filter DataFrame based on user input"""
    if df is None or df.empty:
        print("❌ No DataFrame to filter")
        return None

    print(f"\n🔍 Filter DataFrame: {schema_name}.{table_name}")
    print("=" * 50)

    # Show available columns
    print("Available columns:")
    for i, col in enumerate(df.columns, 1):
        print(f"  {i}. {col} ({df[col].dtype})")

    # Get column to filter on
    col_choice = input("\nEnter column number to filter on: ").strip()
    try:
        col_idx = int(col_choice) - 1
        if 0 <= col_idx < len(df.columns):
            column = df.columns[col_idx]
        else:
            print("❌ Invalid column number")
            return None
    except ValueError:
        print("❌ Please enter a valid number")
        return None

    # Get filter value
    filter_value = input(f"Enter value to filter '{column}' on: ").strip()

    try:
        # Apply filter
        if df[column].dtype in ["int64", "float64"]:
            # Numeric comparison
            try:
                filter_value = float(filter_value)
                filtered_df = df[df[column] == filter_value]
            except ValueError:
                print("❌ Invalid numeric value")
                return None
        else:
            # String comparison
            filtered_df = df[
                df[column].astype(str).str.contains(filter_value, case=False, na=False)
            ]

        print(
            f"✅ Filter applied: {len(filtered_df)} rows match '{column}' = '{filter_value}'"
        )
        print(f"📊 Original: {len(df)} rows, Filtered: {len(filtered_df)} rows")

        return filtered_df

    except Exception as e:
        print(f"❌ Error applying filter: {e}")
        return None


def analyze_dataframe(df, schema_name, table_name):
    """Provide comprehensive analysis of the DataFrame"""
    if df is None or df.empty:
        print("❌ No data to analyze")
        return

    print(f"\n🔍 DataFrame Analysis: {schema_name}.{table_name}")
    print("=" * 60)

    # Basic info
    print(f"📊 Shape: {df.shape[0]} rows × {df.shape[1]} columns")
    print(f"💾 Memory usage: {df.memory_usage(deep=True).sum() / 1024:.2f} KB")

    # Column types
    print(f"\n📋 Column Types:")
    for col, dtype in df.dtypes.items():
        print(f"  {col:<25} {dtype}")

    # Missing values
    missing_values = df.isnull().sum()
    if missing_values.sum() > 0:
        print(f"\n⚠️  Missing Values:")
        for col, missing in missing_values.items():
            if missing > 0:
                percentage = (missing / len(df)) * 100
                print(f"  {col:<25} {missing:,} ({percentage:.1f}%)")
    else:
        print(f"\n✅ No missing values found")

    # Unique values for categorical columns
    categorical_cols = df.select_dtypes(include=["object", "category"]).columns
    if len(categorical_cols) > 0:
        print(f"\n🏷️  Categorical Columns (first 5 unique values):")
        for col in categorical_cols[
            :5
        ]:  # Limit to first 5 to avoid overwhelming output
            unique_vals = df[col].nunique()
            print(f"  {col:<25} {unique_vals:,} unique values")
            if unique_vals <= 10:
                print(f"    Values: {sorted(df[col].dropna().unique())}")

    # Numeric columns statistics
    numeric_cols = df.select_dtypes(include=["number"]).columns
    if len(numeric_cols) > 0:
        print(f"\n📈 Numeric Columns Statistics:")
        print(df[numeric_cols].describe())

    # Sample data
    print(f"\n📊 Sample Data (first 5 rows):")
    print(df.head().to_string(index=False))


def main():
    """Main function"""
    print("🗄️  Database Schema Explorer")
    print("=" * 50)

    # Create database connection
    engine = create_database_connection()

    if not engine:
        print("\n❌ Failed to establish database connection.")
        print("Please check your database credentials and try again.")
        print("\nYou can create a .env file with the following content:")
        print("DB_HOST=localhost")
        print("DB_PORT=5432")
        print("DB_NAME=milestone2")
        print("DB_USER=postgres")
        print("DB_PASSWORD=your_password")
        return

    # Display connection info
    display_connection_info()

    # Start interactive menu
    interactive_menu(engine)


if __name__ == "__main__":
    main()
