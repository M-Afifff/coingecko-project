import logging
import os
from datetime import datetime

def setup_logger(name="crypto_etl"):
    """Logging/Monitoring for Crypto ETL Pipeline"""
    
    # Create logs directory
    os.makedirs('logs', exist_ok=True)
    
    # Create logger
    logger = logging.getLogger(name)
    
    if logger.handlers:
        return logger
    
    logger.setLevel(logging.INFO)
    
    # Formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # File handler
    file_handler = logging.FileHandler(
        f'logs/pipeline_{datetime.now().strftime("%Y%m%d")}.log'
    )
    file_handler.setFormatter(formatter)
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger