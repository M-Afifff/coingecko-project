import requests
import pandas as pd
from typing import Optional
from .config import COINGECKO_BASE_URL, COINGECKO_API_KEY
from .logger import setup_logger

logger = setup_logger(__name__)

class CryptoExtractor:
    """Extract cryptocurrency data from CoinGecko API"""
    
    def __init__(self):
        self.base_url = COINGECKO_BASE_URL
        self.headers = {}
        
        if COINGECKO_API_KEY:
            self.headers['x-cg-demo-api-key'] = COINGECKO_API_KEY
    
    def extract_top_coins(self, limit: int = 50) -> pd.DataFrame:
                
        logger.info(f"Extracting top {limit} cryptocurrencies")
        
        try:
            url = f"{self.base_url}/coins/markets"
            params = {
                "vs_currency": "usd",
                "order": "market_cap_desc",
                "per_page": limit,
                "page": 1,
                "sparkline": False
            }
            
            response = requests.get(url, headers=self.headers, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            df = pd.DataFrame(data)
            
            logger.info(f"Successfully extracted {len(df)} records")
            return df
            
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {e}")
            raise
        except Exception as e:
            logger.error(f"Extraction failed: {e}")
            raise
    
    def health_check(self) -> bool:
        """Check if CoinGecko API is accessible"""
        try:
            response = requests.get(f"{self.base_url}/ping", timeout=10)
            return response.status_code == 200
        except:
            return False