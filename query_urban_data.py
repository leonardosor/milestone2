#!/usr/bin/env python3
"""
Simple script to query the urban_institute_data table
"""

import json
import pandas as pd
from sqlalchemy import create_engine, text

def load_config(config_file="config.json"):
    """Load configuration from JSON file"""
    try:
        with open(config_file, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Configuration file {config_file} not found")
        return None

def connect_to_database(config):
    """Connect to database based on configuration"""
    try:
        # Use local database by default
        db_config = config.get("local_database", {})
        
        # Create connection string
        connection_string = (
            f"postgresql://{db_config['username']}:{db_config['password']}"
            f"@{db_config['host']}:{db_config['port']}/{db_config['database']}"
        )
        
        # Create engine
        engine = create_engine(connection_string)
        
        # Test connection
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        
        print(f"Connected to database: {db_config['database']} on {db_config['host']}")
        return engine
        
    except Exception as e:
        print(f"Failed to connect to database: {e}")
        return None

def execute_query(engine, query):
    """Execute SQL query and return results"""
    try:
        with engine.connect() as conn:
            result = conn.execute(text(query))
            
            # Convert to DataFrame
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
            
            return df
            
    except Exception as e:
        print(f"Failed to execute query: {e}")
        return None

def main():
    """Main function"""
    # Load configuration
    config = load_config()
    if not config:
        return
    
    # Connect to database
    engine = connect_to_database(config)
    if not engine:
        return
    
    # Your SQL query
    query = """
    SELECT *
    FROM public.urban_institute_data
    WHERE public.urban_institute_data.students_SAT_ACT IS NOT NULL
    """
    
    print("Executing query...")
    print(query)
    print("\n" + "="*80 + "\n")
    
    # Execute query
    results = execute_query(engine, query)
    
    if results is not None:
        if not results.empty:
            print(f"Query returned {len(results)} rows")
            print(f"Columns: {list(results.columns)}")
            print("\nFirst few rows:")
            print(results.head())
            
            # Save results to CSV
            csv_filename = "urban_sat_act_results.csv"
            results.to_csv(csv_filename, index=False)
            print(f"\nResults saved to {csv_filename}")
        else:
            print("Query returned no results")
    else:
        print("Query execution failed")
    
    # Close engine
    engine.dispose()

if __name__ == "__main__":
    main() 