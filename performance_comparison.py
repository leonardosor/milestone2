#!/usr/bin/env python3
"""
Performance Comparison: Sync vs Async Census ETL
===============================================

This script compares the performance of the synchronous and asynchronous versions
of the Census ETL process.
"""

import time
import asyncio
import logging
from datetime import datetime
from census_to_postgresql import CensusDataETL
from census_to_postgresql_async import AsyncCensusDataETL

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class PerformanceComparison:
    """Compare performance between sync and async ETL processes"""
    
    def __init__(self, config_file='config.json'):
        self.config_file = config_file
        self.results = {}
    
    def run_sync_test(self, begin_year=2015, end_year=2017):
        """Run synchronous ETL test"""
        logger.info("Starting synchronous ETL test...")
        start_time = time.time()
        
        try:
            etl = CensusDataETL(self.config_file)
            
            # Only test the API data pull part for fair comparison
            census_data = etl.api_iterator(begin_year, end_year)
            
            end_time = time.time()
            duration = end_time - start_time
            
            self.results['sync'] = {
                'duration': duration,
                'records': len(census_data) if not census_data.empty else 0,
                'years_processed': end_year - begin_year
            }
            
            logger.info(f"Synchronous test completed in {duration:.2f} seconds")
            logger.info(f"Records processed: {self.results['sync']['records']}")
            
        except Exception as e:
            logger.error(f"Synchronous test failed: {e}")
            self.results['sync'] = {'error': str(e)}
    
    async def run_async_test(self, begin_year=2015, end_year=2017):
        """Run asynchronous ETL test"""
        logger.info("Starting asynchronous ETL test...")
        start_time = time.time()
        
        try:
            etl = AsyncCensusDataETL(self.config_file)
            
            # Only test the API data pull part for fair comparison
            census_data = await etl.api_iterator_async(begin_year, end_year)
            
            end_time = time.time()
            duration = end_time - start_time
            
            self.results['async'] = {
                'duration': duration,
                'records': len(census_data) if not census_data.empty else 0,
                'years_processed': end_year - begin_year
            }
            
            logger.info(f"Asynchronous test completed in {duration:.2f} seconds")
            logger.info(f"Records processed: {self.results['async']['records']}")
            
        except Exception as e:
            logger.error(f"Asynchronous test failed: {e}")
            self.results['async'] = {'error': str(e)}
    
    def calculate_improvements(self):
        """Calculate performance improvements"""
        if 'sync' not in self.results or 'async' not in self.results:
            logger.error("Both sync and async tests must be completed")
            return
        
        if 'error' in self.results['sync'] or 'error' in self.results['async']:
            logger.error("One or both tests failed")
            return
        
        sync_duration = self.results['sync']['duration']
        async_duration = self.results['async']['duration']
        
        # Calculate improvements
        time_saved = sync_duration - async_duration
        speedup_factor = sync_duration / async_duration
        percentage_improvement = ((sync_duration - async_duration) / sync_duration) * 100
        
        self.results['improvements'] = {
            'time_saved_seconds': time_saved,
            'speedup_factor': speedup_factor,
            'percentage_improvement': percentage_improvement
        }
        
        logger.info("=" * 50)
        logger.info("PERFORMANCE COMPARISON RESULTS")
        logger.info("=" * 50)
        logger.info(f"Synchronous Duration: {sync_duration:.2f} seconds")
        logger.info(f"Asynchronous Duration: {async_duration:.2f} seconds")
        logger.info(f"Time Saved: {time_saved:.2f} seconds")
        logger.info(f"Speedup Factor: {speedup_factor:.2f}x")
        logger.info(f"Performance Improvement: {percentage_improvement:.1f}%")
        logger.info("=" * 50)
    
    def generate_report(self):
        """Generate a detailed performance report"""
        if 'improvements' not in self.results:
            logger.error("Improvements not calculated. Run tests first.")
            return
        
        report = f"""
CENSUS ETL PERFORMANCE COMPARISON REPORT
========================================
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

SYNCHRONOUS ETL RESULTS:
- Duration: {self.results['sync']['duration']:.2f} seconds
- Records Processed: {self.results['sync']['records']:,}
- Years Processed: {self.results['sync']['years_processed']}

ASYNCHRONOUS ETL RESULTS:
- Duration: {self.results['async']['duration']:.2f} seconds
- Records Processed: {self.results['async']['records']:,}
- Years Processed: {self.results['async']['years_processed']}

PERFORMANCE IMPROVEMENTS:
- Time Saved: {self.results['improvements']['time_saved_seconds']:.2f} seconds
- Speedup Factor: {self.results['improvements']['speedup_factor']:.2f}x
- Performance Improvement: {self.results['improvements']['percentage_improvement']:.1f}%

CONFIGURATION:
- Max Concurrent Requests: {self.config_file.get('async', {}).get('max_concurrent_requests', 'N/A')}
- Year Batch Size: {self.config_file.get('async', {}).get('year_batch_size', 'N/A')}
- Batch Delay: {self.config_file.get('async', {}).get('batch_delay', 'N/A')} seconds

RECOMMENDATIONS:
1. Use async version for better performance
2. Adjust concurrent requests based on API limits
3. Monitor rate limiting and adjust batch delays
4. Consider database connection pooling for large datasets
"""
        
        # Save report to file
        with open('performance_report.txt', 'w') as f:
            f.write(report)
        
        logger.info("Performance report saved to 'performance_report.txt'")
        print(report)

async def main():
    """Main function to run performance comparison"""
    try:
        # Initialize comparison
        comparison = PerformanceComparison()
        
        # Run tests
        logger.info("Starting performance comparison...")
        
        # Run sync test
        comparison.run_sync_test(begin_year=2015, end_year=2017)
        
        # Run async test
        await comparison.run_async_test(begin_year=2015, end_year=2017)
        
        # Calculate improvements
        comparison.calculate_improvements()
        
        # Generate report
        comparison.generate_report()
        
        logger.info("Performance comparison completed successfully!")
        
    except Exception as e:
        logger.error(f"Performance comparison failed: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main()) 