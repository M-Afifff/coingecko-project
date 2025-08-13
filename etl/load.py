import pandas as pd
from sqlalchemy import create_engine, text
from typing import Dict
from .config import DATABASE_URL
from .logger import setup_logger

logger = setup_logger(__name__)

class CryptoLoader:
    """Load cryptocurrency data to PostgreSQL"""
    
    def __init__(self):
        try:
            self.engine = create_engine(DATABASE_URL)
            logger.info("Database connection successful")
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise
    
    def create_tables(self) -> None:
        """Create crypto_prices table if not exists"""
        
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS crypto_prices (
            id SERIAL PRIMARY KEY,
            crypto_id VARCHAR(50) NOT NULL,
            symbol VARCHAR(10) NOT NULL,
            name VARCHAR(100) NOT NULL,
            current_price DECIMAL(20,8) NOT NULL,
            market_cap BIGINT NOT NULL,
            rank INTEGER,
            volume_24h BIGINT,
            price_change_24h DECIMAL(10,4),
            circulating_supply BIGINT,
            last_updated TIMESTAMP,
            price_category VARCHAR(10),
            market_cap_billions DECIMAL(10,2),
            extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            extracted_date DATE NOT NULL,
            
            CONSTRAINT positive_price CHECK (current_price > 0),
            CONSTRAINT positive_market_cap CHECK (market_cap > 0)
        );
        
        CREATE INDEX IF NOT EXISTS idx_crypto_id ON crypto_prices(crypto_id);
        CREATE INDEX IF NOT EXISTS idx_extracted_date ON crypto_prices(extracted_date);
        CREATE INDEX IF NOT EXISTS idx_rank ON crypto_prices(rank);
        """
        
        try:
            with self.engine.connect() as conn:
                conn.execute(text(create_table_sql))
                conn.commit()
            logger.info("Database tables created")
        except Exception as e:
            logger.error(f"Failed to create tables: {e}")
            raise
    
    def load_data(self, df: pd.DataFrame) -> int:
        """Load data to PostgreSQL"""
        
        if df.empty:
            logger.warning("No data to load")
            return 0
        
        try:
            record_count = len(df)
            logger.info(f"Loading {record_count} records to database")
            
            # Load data
            df.to_sql(
                'crypto_prices',
                self.engine,
                if_exists='append',
                index=False,
                method='multi'
            )
            
            logger.info(f"Successfully loaded {record_count} records")
            return record_count
            
        except Exception as e:
            logger.error(f"Failed to load data: {e}")
            raise
    
    def get_latest_stats(self) -> Dict:
        """Data Statistics"""
        
        stats_query = """
        SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT crypto_id) as unique_cryptos,
            MAX(extracted_date) as latest_date,
            AVG(current_price) as avg_price,
            SUM(market_cap_billions) as total_market_cap_billions
        FROM crypto_prices
        WHERE extracted_date = (SELECT MAX(extracted_date) FROM crypto_prices)
        """
        
        try:
            result = pd.read_sql(stats_query, self.engine)
            return result.iloc[0].to_dict()
        except Exception as e:
            logger.warning(f"No stats: {e}")
            return {}
    
    def health_check(self) -> bool:
        """Check database connection"""
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except:
            return False