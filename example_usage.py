#!/usr/bin/env python3
"""
Example Usage of Multi-API ETL System
=====================================

This script demonstrates how to use the multi-API ETL system with different
configurations and endpoints for both Census and Urban Institute APIs.
Supports both local PostgreSQL and AWS RDS databases.
"""

import asyncio
import logging
from multi_api_etl import MultiAPIDataETL

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def example_census_only():
    """Example: Fetch only Census data"""
    logger.info("Running Census-only example")
    
    try:
        # Initialize ETL process
        etl = MultiAPIDataETL('config.json')
        
        # Run ETL process with only Census data
        await etl.run_etl_async(
            census_years=(2015, 2019),
            urban_endpoints=None  # No Urban Institute data
        )
        
        logger.info("Census-only example completed successfully!")
        
    except Exception as e:
        logger.error(f"Census-only example failed: {e}")
        raise

async def example_local_database():
    """Example: Using local PostgreSQL database"""
    logger.info("Running example with local database")
    
    try:
        # Switch to local database configuration
        import subprocess
        subprocess.run(['python', 'switch_database.py', 'local'], check=True)
        
        # Initialize ETL process
        etl = MultiAPIDataETL('config.json')
        
        # Run ETL process with local database
        await etl.run_etl_async(
            census_years=(2019, 2023),
            urban_endpoints=None
        )
        
        logger.info("Local database example completed successfully!")
        
    except Exception as e:
        logger.error(f"Local database example failed: {e}")
        raise

async def example_aws_database():
    """Example: Using AWS RDS database"""
    logger.info("Running example with AWS RDS database")
    
    try:
        # Switch to AWS database configuration
        import subprocess
        subprocess.run(['python', 'switch_database.py', 'aws'], check=True)
        
        # Initialize ETL process
        etl = MultiAPIDataETL('config.json')
        
        # Run ETL process with AWS database
        await etl.run_etl_async(
            census_years=(2015, 2019),
            urban_endpoints=None
        )
        
        logger.info("AWS database example completed successfully!")
        
    except Exception as e:
        logger.error(f"AWS database example failed: {e}")
        raise

async def example_urban_only():
    """Example: Fetch only Urban Institute data"""
    logger.info("Running Urban Institute-only example")
    
    try:
        # Initialize ETL process
        etl = MultiAPIDataETL('config.json')
        
        # Define Urban Institute endpoints
        urban_endpoints = [
            {
                'endpoint': '/v1/education/schools',
                'parameters': {'limit': 1000},
                'year': 2023
            },
            {
                'endpoint': '/v1/education/districts',
                'parameters': {'limit': 1000},
                'year': 2023
            }
        ]
        
        # Run ETL process with only Urban Institute data
        await etl.run_etl_async(
            census_years=None,  # No Census data
            urban_endpoints=urban_endpoints
        )
        
        logger.info("Urban Institute-only example completed successfully!")
        
    except Exception as e:
        logger.error(f"Urban Institute-only example failed: {e}")
        raise

async def example_multi_api():
    """Example: Fetch both Census and Urban Institute data"""
    logger.info("Running multi-API example")
    
    try:
        # Initialize ETL process
        etl = MultiAPIDataETL('config.json')
        
        # Define Urban Institute endpoints
        urban_endpoints = [
            {
                'endpoint': '/v1/education/schools',
                'parameters': {'limit': 500},
                'year': 2023
            },
            {
                'endpoint': '/v1/education/districts',
                'parameters': {'limit': 500},
                'year': 2023
            },
            {
                'endpoint': '/v1/housing',
                'parameters': {'limit': 500},
                'year': 2023
            }
        ]
        
        # Run ETL process with both data sources
        await etl.run_etl_async(
            census_years=(2015, 2019),
            urban_endpoints=urban_endpoints
        )
        
        logger.info("Multi-API example completed successfully!")
        
    except Exception as e:
        logger.error(f"Multi-API example failed: {e}")
        raise

async def example_custom_urban_endpoints():
    """Example: Custom Urban Institute endpoints with filters"""
    logger.info("Running custom Urban Institute endpoints example")
    
    try:
        # Initialize ETL process
        etl = MultiAPIDataETL('config.json')
        
        # Define custom Urban Institute endpoints with filters
        urban_endpoints = [
            {
                'endpoint': '/v1/education/schools',
                'parameters': {
                    'state': 'CA',
                    'limit': 1000,
                    'type': 'public'
                },
                'year': 2023
            },
            {
                'endpoint': '/v1/education/districts',
                'parameters': {
                    'state': 'CA',
                    'limit': 500,
                    'type': 'unified'
                },
                'year': 2023
            },
            {
                'endpoint': '/v1/housing',
                'parameters': {
                    'city': 'Los Angeles',
                    'limit': 1000,
                    'type': 'rental'
                },
                'year': 2023
            },
            {
                'endpoint': '/v1/health',
                'parameters': {
                    'county': 'Los Angeles',
                    'limit': 500,
                    'type': 'hospitals'
                },
                'year': 2023
            }
        ]
        
        # Run ETL process with custom endpoints
        await etl.run_etl_async(
            census_years=(2015, 2019),
            urban_endpoints=urban_endpoints
        )
        
        logger.info("Custom Urban Institute endpoints example completed successfully!")
        
    except Exception as e:
        logger.error(f"Custom Urban Institute endpoints example failed: {e}")
        raise

async def example_error_handling():
    """Example: Demonstrating error handling"""
    logger.info("Running error handling example")
    
    try:
        # Initialize ETL process
        etl = MultiAPIDataETL('config.json')
        
        # Define endpoints that might cause errors (for testing)
        urban_endpoints = [
            {
                'endpoint': '/v1/education/schools',
                'parameters': {'limit': 1000},
                'year': 2023
            },
            {
                'endpoint': '/v1/nonexistent/endpoint',  # This will fail
                'parameters': {'limit': 1000},
                'year': 2023
            }
        ]
        
        # Run ETL process - the system should handle the error gracefully
        await etl.run_etl_async(
            census_years=(2015, 2019),
            urban_endpoints=urban_endpoints
        )
        
        logger.info("Error handling example completed successfully!")
        
    except Exception as e:
        logger.error(f"Error handling example failed: {e}")
        # This is expected behavior - the system should handle errors gracefully

async def main():
    """Main function to run examples"""
    logger.info("Starting Multi-API ETL Examples")
    
    # Choose which example to run
    examples = {
        '1': ('Census Only', example_census_only),
        '2': ('Urban Institute Only', example_urban_only),
        '3': ('Multi-API', example_multi_api),
        '4': ('Custom Urban Endpoints', example_custom_urban_endpoints),
        '5': ('Error Handling', example_error_handling),
        '6': ('Local Database', example_local_database),
        '7': ('AWS Database', example_aws_database),
        '8': ('All Examples', None)  # Will run all examples
    }
    
    print("Multi-API ETL Examples")
    print("=" * 30)
    for key, (name, _) in examples.items():
        if key != '8':
            print(f"{key}. {name}")
    print("8. Run All Examples")
    print()
    
    choice = input("Enter your choice (1-8): ").strip()
    
    if choice == '8':
        # Run all examples
        logger.info("Running all examples")
        
        for key, (name, func) in examples.items():
            if key != '8' and func:
                logger.info(f"Running example: {name}")
                try:
                    await func()
                    logger.info(f"Example '{name}' completed successfully")
                except Exception as e:
                    logger.error(f"Example '{name}' failed: {e}")
                
                # Add delay between examples
                await asyncio.sleep(2)
        
        logger.info("All examples completed")
        
    elif choice in examples and examples[choice][1]:
        # Run single example
        name, func = examples[choice]
        logger.info(f"Running example: {name}")
        await func()
        logger.info(f"Example '{name}' completed successfully")
        
    else:
        logger.error("Invalid choice")

if __name__ == "__main__":
    asyncio.run(main()) 