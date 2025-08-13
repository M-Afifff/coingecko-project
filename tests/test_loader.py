from etl.extract import CryptoExtractor
from etl.transform import CryptoTransformer
from etl.load import CryptoLoader
from etl.logger import setup_logger

logger = setup_logger("test_loader")

try:
    logger.info("Testing data loader...")
    
    # Get and transform data
    extractor = CryptoExtractor()
    transformer = CryptoTransformer()
    loader = CryptoLoader()
    
    raw_data = extractor.extract_top_coins(limit=5)
    clean_data = transformer.transform(raw_data)
    logger.info(f"Prepared {len(clean_data)} records for loading")
    
    # Create tables
    loader.create_tables()
    logger.info("✓ Database tables ready")
    
    # Load data
    records_loaded = loader.load_data(clean_data)
    logger.info(f"✓ Loaded {records_loaded} records to database")
    
    # Get stats
    stats = loader.get_latest_stats()
    logger.info(f"✓ Database stats: {stats}")
    
except Exception as e:
    logger.error(f"✗ Loader test failed: {e}")
    exit(1)
