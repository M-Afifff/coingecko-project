from etl.extract import CryptoExtractor
from etl.logger import setup_logger

logger = setup_logger("test_extractor")

try:
    logger.info("Testing CoinGecko API extractor...")
    
    # Test health check
    extractor = CryptoExtractor()
    if extractor.health_check():
        logger.info("✓ API health check passed")
    else:
        logger.error("✗ API health check failed")
        exit(1)
    
    # Test extraction
    data = extractor.extract_top_coins(limit=10)
    logger.info(f"✓ Extracted {len(data)} cryptocurrencies")
    logger.info(f"Sample data: {data[['id', 'symbol', 'current_price']].head(3).to_dict('records')}")
    
except Exception as e:
    logger.error(f"✗ Extractor test failed: {e}")
    exit(1)
# Updated: Mon Sep 15 13:54:14 +08 2025
