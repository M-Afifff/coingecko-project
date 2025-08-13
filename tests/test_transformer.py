from etl.extract import CryptoExtractor
from etl.transform import CryptoTransformer
from etl.logger import setup_logger

logger = setup_logger("test_transformer")

try:
    logger.info("Testing data transformer...")
    
    # Get raw data
    extractor = CryptoExtractor()
    raw_data = extractor.extract_top_coins(limit=10)
    logger.info(f"Got {len(raw_data)} raw records")
    
    # Transform data
    transformer = CryptoTransformer()
    clean_data = transformer.transform(raw_data)
    logger.info(f"✓ Transformed to {len(clean_data)} clean records")
    
    # Check data quality
    quality_report = transformer.get_data_quality_report(clean_data)
    logger.info(f"✓ Data quality report: {quality_report}")
    
    # Show sample transformed data
    sample_cols = ['crypto_id', 'symbol', 'current_price', 'price_category', 'market_cap_billions']
    logger.info(f"Sample transformed data:")
    logger.info(f"{clean_data[sample_cols].head(3).to_dict('records')}")
    
except Exception as e:
    logger.error(f"✗ Transformer test failed: {e}")
    exit(1)
