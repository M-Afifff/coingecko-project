import time
from typing import Dict
from .extract import CryptoExtractor
from .transform import CryptoTransformer
from .load import CryptoLoader
from .logger import setup_logger

logger = setup_logger(__name__)

def run_etl_pipeline() -> Dict:
    """Run ETL pipeline with logging/monitoring"""
    
    start_time = time.time()
    
    try:
        logger.info("="*50)
        logger.info("STARTING CRYPTO ETL PIPELINE")
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
        
        logger.info("All health checks passed")
        
        # Step 1: Extract
        logger.info("Step 1: Extracting data")
        raw_data = extractor.extract_top_coins(limit=50)
        
        # Step 2: Transform  
        logger.info("Step 2: Transforming data")
        clean_data = transformer.transform(raw_data)
        quality_report = transformer.get_data_quality_report(clean_data)
        
        # Step 3: Load
        logger.info("Step 3: Loading data")
        loader.create_tables()
        records_loaded = loader.load_data(clean_data)
        
        # Get stats
        db_stats = loader.get_latest_stats()
        
        # Calculate duration
        duration = time.time() - start_time
        
        # Success metrics
        result = {
            'success': True,
            'records_processed': records_loaded,
            'duration_seconds': round(duration, 2),
            'data_quality': quality_report,
            'database_stats': db_stats
        }
        
        logger.info("="*50)
        logger.info("PIPELINE COMPLETED")
        logger.info(f"Records processed: {records_loaded}")
        logger.info(f"Duration: {duration:.2f} seconds")
        logger.info("="*50)
        
        return result
        
    except Exception as e:
        duration = time.time() - start_time
        
        logger.error("="*50)
        logger.error("PIPELINE FAILED")
        logger.error(f"Error: {str(e)}")
        logger.error(f"Duration: {duration:.2f} seconds")
        logger.error("="*50)
        
        return {
            'success': False,
            'error': str(e),
            'duration_seconds': round(duration, 2)
        }

if __name__ == "__main__":
    result = run_etl_pipeline()
    exit(0 if result['success'] else 1)