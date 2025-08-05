#!/usr/bin/env python3
"""
Test Database Switching Functionality
====================================

This script tests the database switching functionality to ensure it works correctly
with both local PostgreSQL and AWS RDS configurations.
"""

import json
import subprocess
import sys
import os
from typing import Dict, Any

def load_config() -> Dict[str, Any]:
    """Load the current configuration from config.json"""
    try:
        with open('config.json', 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print("Error: config.json not found")
        return {}
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in config.json: {e}")
        return {}

def test_switch_to_local():
    """Test switching to local database configuration"""
    print("Testing switch to local database...")
    
    try:
        # Run the switch command
        result = subprocess.run(
            ['python', 'switch_database.py', 'local'],
            capture_output=True,
            text=True,
            check=True
        )
        
        print("✓ Switch to local database command executed successfully")
        print(f"Output: {result.stdout}")
        
        # Verify the configuration was updated
        config = load_config()
        if config.get('database_type') == 'local':
            print("✓ Configuration updated to local database")
        else:
            print("✗ Configuration not updated correctly")
            return False
            
        return True
        
    except subprocess.CalledProcessError as e:
        print(f"✗ Switch to local database failed: {e}")
        print(f"Error output: {e.stderr}")
        return False
    except Exception as e:
        print(f"✗ Unexpected error: {e}")
        return False

def test_switch_to_aws():
    """Test switching to AWS database configuration"""
    print("Testing switch to AWS database...")
    
    try:
        # Run the switch command
        result = subprocess.run(
            ['python', 'switch_database.py', 'aws'],
            capture_output=True,
            text=True,
            check=True
        )
        
        print("✓ Switch to AWS database command executed successfully")
        print(f"Output: {result.stdout}")
        
        # Verify the configuration was updated
        config = load_config()
        if config.get('database_type') == 'aws':
            print("✓ Configuration updated to AWS database")
        else:
            print("✗ Configuration not updated correctly")
            return False
            
        return True
        
    except subprocess.CalledProcessError as e:
        print(f"✗ Switch to AWS database failed: {e}")
        print(f"Error output: {e.stderr}")
        return False
    except Exception as e:
        print(f"✗ Unexpected error: {e}")
        return False

def test_status_command():
    """Test the status command"""
    print("Testing status command...")
    
    try:
        # Run the status command
        result = subprocess.run(
            ['python', 'switch_database.py', 'status'],
            capture_output=True,
            text=True,
            check=True
        )
        
        print("✓ Status command executed successfully")
        print(f"Output: {result.stdout}")
        return True
        
    except subprocess.CalledProcessError as e:
        print(f"✗ Status command failed: {e}")
        print(f"Error output: {e.stderr}")
        return False
    except Exception as e:
        print(f"✗ Unexpected error: {e}")
        return False

def test_invalid_command():
    """Test handling of invalid command"""
    print("Testing invalid command handling...")
    
    try:
        # Run an invalid command
        result = subprocess.run(
            ['python', 'switch_database.py', 'invalid'],
            capture_output=True,
            text=True,
            check=False  # We expect this to fail
        )
        
        if result.returncode != 0:
            print("✓ Invalid command handled correctly (returned error)")
            return True
        else:
            print("✗ Invalid command should have failed")
            return False
        
    except Exception as e:
        print(f"✗ Unexpected error: {e}")
        return False

def test_config_validation():
    """Test configuration validation"""
    print("Testing configuration validation...")
    
    try:
        # Test with incomplete local configuration
        config = load_config()
        if 'local_database' in config:
            # Temporarily remove password to test validation
            original_password = config['local_database'].get('password')
            config['local_database']['password'] = ''
            
            with open('config.json', 'w') as f:
                json.dump(config, f, indent=4)
            
            # Run local switch to test validation
            result = subprocess.run(
                ['python', 'switch_database.py', 'local'],
                capture_output=True,
                text=True,
                check=False
            )
            
            # Restore original password
            config['local_database']['password'] = original_password
            with open('config.json', 'w') as f:
                json.dump(config, f, indent=4)
            
            if "Warning: Local database configuration is incomplete" in result.stdout:
                print("✓ Configuration validation working correctly")
                return True
            else:
                print("✗ Configuration validation not working")
                return False
        else:
            print("⚠ Skipping validation test - local_database section not found")
            return True
            
    except Exception as e:
        print(f"✗ Validation test failed: {e}")
        return False

def main():
    """Run all tests"""
    print("Database Switching Test Suite")
    print("=" * 40)
    
    tests = [
        ("Switch to Local Database", test_switch_to_local),
        ("Switch to AWS Database", test_switch_to_aws),
        ("Status Command", test_status_command),
        ("Invalid Command Handling", test_invalid_command),
        ("Configuration Validation", test_config_validation)
    ]
    
    passed = 0
    total = len(tests)
    
    for test_name, test_func in tests:
        print(f"\n--- {test_name} ---")
        if test_func():
            passed += 1
            print(f"✓ {test_name} PASSED")
        else:
            print(f"✗ {test_name} FAILED")
    
    print(f"\n{'='*40}")
    print(f"Test Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed! Database switching functionality is working correctly.")
        return 0
    else:
        print("❌ Some tests failed. Please check the configuration and try again.")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 