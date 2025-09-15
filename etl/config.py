import os
from dotenv import load_dotenv

load_dotenv()

# Database
DATABASE_URL = os.getenv('DATABASE_URL', 
    'postgresql://username:password@localhost:5432/database')

# API
COINGECKO_API_KEY = os.getenv('COINGECKO_API_KEY')
COINGECKO_BASE_URL = "https://api.coingecko.com/api/v3"

# Logging
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
# Updated: Mon Sep 15 13:54:14 +08 2025
