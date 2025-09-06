import os
import logging
import tempfile
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration constants
class Config:
    # MongoDB Configuration
    MONGO_URI = os.environ.get('Mongo_URI')
    DATABASE_NAME = 'chipsDB'
    ORDERS_COLLECTION = 'orders'
    TRADES_COLLECTION = 'trades'
    
    # DEX order event queries
    PLACE_ORDER_QUERY = "n_e98a056e3e14203e6ec18fada427334b21b667d8.ce2.PLACE_ORDER"
    MATCH_ORDER_QUERY = "n_e98a056e3e14203e6ec18fada427334b21b667d8.ce2.MATCH_ORDER"
    CANCEL_ORDER_QUERY = "n_e98a056e3e14203e6ec18fada427334b21b667d8.ce2.CANCEL_ORDER"
    
    # Processing configuration
    BATCH_SIZE = 100
    LOCK_FILE_PATH = os.path.join(tempfile.gettempdir(), "dex_orders_script.lock")
    
    # Validation
    @classmethod
    def validate(cls):
        """Validate required configuration"""
        if not cls.MONGO_URI:
            raise ValueError("Mongo_URI environment variable not set")
        return True