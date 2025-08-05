#!/usr/bin/env python3
"""
Database Configuration Switcher
==============================

This script helps you easily switch between local PostgreSQL and AWS RDS configurations.
It updates the config.json file to use either local or AWS database settings.

Usage:
    python switch_database.py local    # Switch to local database
    python switch_database.py aws      # Switch to AWS RDS
    python switch_database.py status   # Show current configuration
"""

import json
import sys
import os
from typing import Dict, Any

def load_config() -> Dict[str, Any]:
    """Load the current configuration from config.json"""
    try:
        with open('config.json', 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print("Error: config.json not found. Please ensure the file exists.")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in config.json: {e}")
        sys.exit(1)

def save_config(config: Dict[str, Any]) -> None:
    """Save the configuration to config.json"""
    try:
        with open('config.json', 'w') as f:
            json.dump(config, f, indent=4)
        print("Configuration updated successfully!")
    except Exception as e:
        print(f"Error saving configuration: {e}")
        sys.exit(1)

def switch_to_local(config: Dict[str, Any]) -> Dict[str, Any]:
    """Switch configuration to use local database"""
    config['database_type'] = 'local'
    print("Switched to local database configuration")
    print("Make sure your local PostgreSQL server is running and accessible")
    
    local_db = config.get('local_database', {})
    print("Local database settings:")
    print(f"  - Host: {local_db.get('host')}")
    print(f"  - Port: {local_db.get('port', 5432)}")
    print(f"  - Database: {local_db.get('database')}")
    print(f"  - Username: {local_db.get('username')}")
    print(f"  - Password: {'*' * len(local_db.get('password', '')) if local_db.get('password') else '(configure in local_database section)'}")
    return config

def switch_to_aws(config: Dict[str, Any]) -> Dict[str, Any]:
    """Switch configuration to use AWS RDS"""
    config['database_type'] = 'aws'
    print("Switched to AWS RDS configuration")
    print("Make sure your AWS RDS instance is running and accessible")
    
    use_secrets = config.get('use_aws_secrets', False)
    aws_config = config.get('aws', {})
    aws_db = config.get('database', {})
    
    print("AWS settings:")
    print(f"  - Use AWS Secrets Manager: {use_secrets}")
    if use_secrets:
        print(f"  - Secret Name: {aws_config.get('secret_name', 'Not configured')}")
        print(f"  - Region: {aws_config.get('region', 'Not configured')}")
    else:
        print(f"  - Host: {aws_db.get('host', 'Not configured')}")
        print(f"  - Port: {aws_db.get('port', 5432)}")
        print(f"  - Database: {aws_db.get('database', 'multi_api_data')}")
        print(f"  - Username: {aws_db.get('username', 'Not configured')}")
        print(f"  - Password: {'*' * len(aws_db.get('password', '')) if aws_db.get('password') else 'Not set'}")
    return config

def show_status(config: Dict[str, Any]) -> None:
    """Show current database configuration status"""
    database_type = config.get('database_type', 'aws')
    print(f"Current database type: {database_type}")
    
    if database_type == 'local':
        local_db = config.get('local_database', {})
        print("Local database settings:")
        print(f"  - Host: {local_db.get('host', 'localhost')}")
        print(f"  - Port: {local_db.get('port', 5432)}")
        print(f"  - Database: {local_db.get('database', 'multi_api_data')}")
        print(f"  - Username: {local_db.get('username', 'postgres')}")
        print(f"  - Password: {'*' * len(local_db.get('password', '')) if local_db.get('password') else 'Not set'}")
    else:
        aws_db = config.get('database', {})
        use_secrets = config.get('use_aws_secrets', False)
        print("AWS database settings:")
        print(f"  - Use AWS Secrets Manager: {use_secrets}")
        if not use_secrets:
            print(f"  - Host: {aws_db.get('host', 'Not configured')}")
            print(f"  - Port: {aws_db.get('port', 5432)}")
            print(f"  - Database: {aws_db.get('database', 'multi_api_data')}")
            print(f"  - Username: {aws_db.get('username', 'Not configured')}")
            print(f"  - Password: {'*' * len(aws_db.get('password', '')) if aws_db.get('password') else 'Not set'}")
        else:
            print(f"  - Secret Name: {config.get('aws', {}).get('secret_name', 'Not configured')}")
            print(f"  - Region: {config.get('aws', {}).get('region', 'Not configured')}")

def validate_local_setup(config: Dict[str, Any]) -> bool:
    """Validate local database configuration"""
    local_db = config.get('local_database', {})
    required_fields = ['host', 'port', 'database', 'username', 'password']
    
    missing_fields = [field for field in required_fields if not local_db.get(field)]
    
    if missing_fields:
        print("Warning: Local database configuration is incomplete:")
        for field in missing_fields:
            print(f"  - {field}: Not configured")
        print("\nPlease update the local_database section in config.json")
        return False
    
    return True

def validate_aws_setup(config: Dict[str, Any]) -> bool:
    """Validate AWS database configuration"""
    use_secrets = config.get('use_aws_secrets', False)
    
    if use_secrets:
        aws_config = config.get('aws', {})
        if not aws_config.get('secret_name') or not aws_config.get('region'):
            print("Warning: AWS Secrets Manager configuration is incomplete:")
            print("  - secret_name: Not configured")
            print("  - region: Not configured")
            return False
    else:
        aws_db = config.get('database', {})
        required_fields = ['host', 'port', 'database', 'username', 'password']
        missing_fields = [field for field in required_fields if not aws_db.get(field)]
        
        if missing_fields:
            print("Warning: AWS database configuration is incomplete:")
            for field in missing_fields:
                print(f"  - {field}: Not configured")
            print("\nPlease update the database section in config.json")
            return False
    
    return True

def main():
    """Main function to handle database switching"""
    if len(sys.argv) != 2:
        print("Usage:")
        print("  python switch_database.py local    # Switch to local database")
        print("  python switch_database.py aws      # Switch to AWS RDS")
        print("  python switch_database.py status   # Show current configuration")
        sys.exit(1)
    
    command = sys.argv[1].lower()
    
    # Load current configuration
    config = load_config()
    
    if command == 'local':
        config = switch_to_local(config)
        if not validate_local_setup(config):
            print("\nPlease configure the local database settings before running the ETL.")
        save_config(config)
        
    elif command == 'aws':
        config = switch_to_aws(config)
        if not validate_aws_setup(config):
            print("\nPlease configure the AWS database settings before running the ETL.")
        save_config(config)
        
    elif command == 'status':
        show_status(config)
        
    else:
        print(f"Unknown command: {command}")
        print("Valid commands: local, aws, status")
        sys.exit(1)

if __name__ == "__main__":
    main() 