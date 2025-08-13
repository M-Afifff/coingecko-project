from etl.config import DATABASE_URL
from etl.logger import setup_logger
from sqlalchemy import create_engine, text

logger = setup_logger("test")

try:
    logger.info("Testing database connection...")
    engine = create_engine(DATABASE_URL)
    
    with engine.connect() as conn:
        result = conn.execute(text("SELECT 1"))
        logger.info("✓ Database connection successful!")
        
except Exception as e:
    logger.error(f"✗ Database connection failed: {e}")
    exit(1)
