import time
from typing import Dict
from .extract import CryptoExtractor
from .transform import CryptoTransformer
from .load import CryptoLoader
from .logger import setup_logger

logger = setup_logger(__name__)

def run_historical_etl_pipeline(days_back: int = 7) -> Dict:
    """Run ETL pipeline for historical data range"""
    
    start_time = time.time()
    
    try:
        logger.info("="*50)
        logger.info(f"STARTING HISTORICAL CRYPTO ETL PIPELINE ({days_back} days)")
        logger.info("="*50)
        
        # Initialize components
        extractor = CryptoExtractor()
        transformer = CryptoTransformer()
        loader = CryptoLoader()
        
        # Health checks
        logger.info("Running health checks...")
        if not extractor.health_check():
            raise Exception("CoinGecko API health check failed")
        
        if not loader.health_check():
            raise Exception("Database health check failed")
        
        logger.info("✓ All health checks passed")
        
        # Step 1: Extract Historical Range
        logger.info(f"Step 1: Extracting {days_back} days of data")
        raw_data = extractor.extract_historical_range(days_back=days_back, limit=50)
        
        # Step 2: Transform  
        logger.info("Step 2: Transforming historical data")
        clean_data = transformer.transform(raw_data)
        quality_report = transformer.get_data_quality_report(clean_data)
        
        # Step 3: Load
        logger.info("Step 3: Loading historical data")
        loader.create_tables()
        records_loaded = loader.load_data(clean_data)
        
        # Get final stats
        db_stats = loader.get_latest_stats()
        
        # Calculate duration
        duration = time.time() - start_time
        
        # Success metrics
        result = {
            'success': True,
            'days_processed': days_back,
            'records_processed': records_loaded,
            'duration_seconds': round(duration, 2),
            'data_quality': quality_report,
            'database_stats': db_stats
        }
        
        logger.info("="*50)
        logger.info("HISTORICAL PIPELINE COMPLETED SUCCESSFULLY")
        logger.info(f"Days processed: {days_back}")
        logger.info(f"Records processed: {records_loaded}")
        logger.info(f"Duration: {duration:.2f} seconds")
        logger.info("="*50)
        
        return result
        
    except Exception as e:
        duration = time.time() - start_time
        
        logger.error("="*50)
        logger.error("HISTORICAL PIPELINE FAILED")
        logger.error(f"Error: {str(e)}")
        logger.error(f"Duration: {duration:.2f} seconds")
        logger.error("="*50)
        
        return {
            'success': False,
            'error': str(e),
            'duration_seconds': round(duration, 2)
        }

if __name__ == "__main__":
    result = run_historical_etl_pipeline(days_back=14)  # 2 weeks
    exit(0 if result['success'] else 1)