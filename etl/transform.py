import pandas as pd
from typing import Dict
from .logger import setup_logger

logger = setup_logger(__name__)

class CryptoTransformer:
    """Crypto data transformation and data cleaning"""
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
                
        logger.info(f"Data transformation of {len(df)} records")
        
        try:
            # Columns selection and rename
            df_clean = self._select_columns(df)
            
            # Clean data
            df_clean = self._clean_data(df_clean)
            
            # Add calculated fields
            df_clean = self._add_calculated_fields(df_clean)
            
            logger.info(f"Transformation completed: {len(df_clean)} records ready")
            return df_clean
            
        except Exception as e:
            logger.error(f"Transformation failed: {e}")
            raise
    
    def _select_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Columns selection and rename for database"""
        
        columns_map = {
            'id': 'crypto_id',
            'symbol': 'symbol', 
            'name': 'name',
            'current_price': 'current_price',
            'market_cap': 'market_cap',
            'market_cap_rank': 'rank',
            'total_volume': 'volume_24h',
            'price_change_percentage_24h': 'price_change_24h',
            'circulating_supply': 'circulating_supply',
            'last_updated': 'last_updated'
        }
        
        df_selected = df[list(columns_map.keys())].copy()
        df_selected = df_selected.rename(columns=columns_map)
        
        return df_selected
    
    def _clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Data cleaning"""
        
        initial_count = len(df)
        
        # Remove records with missing critical data
        df_clean = df.dropna(subset=['current_price', 'market_cap'])
        
        # Remove negative prices (data quality issue)
        df_clean = df_clean[df_clean['current_price'] > 0]
        df_clean = df_clean[df_clean['market_cap'] > 0]
        
        cleaned_count = len(df_clean)
        removed_count = initial_count - cleaned_count
        
        if removed_count > 0:
            logger.warning(f"Removed {removed_count} records during cleaning")
        
        return df_clean
    
    def _add_calculated_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add calculated fields"""
        
        # Price category
        df['price_category'] = df['current_price'].apply(self._categorize_price)
        
        # Market cap in billions
        df['market_cap_billions'] = df['market_cap'] / 1e9
        
        # Processing timestamp
        df['extracted_at'] = pd.Timestamp.now()
        df['extracted_date'] = pd.Timestamp.now().date()
        
        return df
    
    def _categorize_price(self, price: float) -> str:
        """Categorize price into bins"""
        if price < 1:
            return "Low"
        elif price < 100:
            return "Medium"
        else:
            return "High"
    
    def get_data_quality_report(self, df: pd.DataFrame) -> Dict:
        """Data quality report"""
        return {
            'total_records': len(df),
            'null_prices': df['current_price'].isna().sum(),
            'null_market_caps': df['market_cap'].isna().sum(),
            'price_categories': df['price_category'].value_counts().to_dict(),
            'date_range': {
                'min': str(df['last_updated'].min()),
                'max': str(df['last_updated'].max())
            }
        }# Updated: Mon Sep 15 13:54:14 +08 2025
