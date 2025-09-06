from pymongo import MongoClient
from pymongo.server_api import ServerApi
import logging
from .config import Config

logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self):
        self.mongo_client = None
        self.mongo_db = None
        self.orders_collection = None
        self.trades_collection = None

    def connect_to_mongodb(self):
        """Establish MongoDB connection"""
        try:
            Config.validate()
            
            self.mongo_client = MongoClient(Config.MONGO_URI, server_api=ServerApi('1'))
            self.mongo_db = self.mongo_client[Config.DATABASE_NAME]
            self.orders_collection = self.mongo_db[Config.ORDERS_COLLECTION]
            self.trades_collection = self.mongo_db[Config.TRADES_COLLECTION]
            
            # Verify connection
            self.mongo_client.admin.command('ping')
            logger.info("Successfully connected to MongoDB!")
            
        except Exception as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            raise

    def get_last_processed_height(self):
        """Get the last processed order height"""
        try:
            last_height_doc = self.orders_collection.find_one({"_id": "last_order_height"})
            return last_height_doc['value'] if last_height_doc else 0
        except Exception as e:
            logger.error(f"Error getting last processed height: {e}")
            return 0

    def update_last_processed_height(self, height):
        """Update the last processed height"""
        try:
            self.orders_collection.update_one(
                {"_id": "last_order_height"},
                {"$set": {"value": height}},
                upsert=True
            )
            logger.info(f"Updated last processed height to: {height}")
        except Exception as e:
            logger.error(f"Error updating last processed height: {e}")

    def fetch_events(self, last_order_height):
        """Fetch all order events from database"""
        events = {
            'place_orders': [],
            'match_orders': [],
            'cancel_orders': []
        }
        
        try:
            # Fetch PLACE_ORDER events
            events['place_orders'] = list(
                self.mongo_db[Config.PLACE_ORDER_QUERY].find(
                    {"height": {"$gt": last_order_height}}
                ).sort("height", 1)
            )
            
            # Fetch MATCH_ORDER events
            events['match_orders'] = list(
                self.mongo_db[Config.MATCH_ORDER_QUERY].find(
                    {"height": {"$gt": last_order_height}}
                ).sort("height", 1)
            )
            
            # Fetch CANCEL_ORDER events
            events['cancel_orders'] = list(
                self.mongo_db[Config.CANCEL_ORDER_QUERY].find(
                    {"height": {"$gt": last_order_height}}
                ).sort("height", 1)
            )
            
            logger.info(f"Found {len(events['place_orders'])} PLACE_ORDER, "
                       f"{len(events['match_orders'])} MATCH_ORDER, and "
                       f"{len(events['cancel_orders'])} CANCEL_ORDER events")
            
        except Exception as e:
            logger.error(f"Error fetching order events: {e}")
            
        return events

    def create_indexes(self):
        """Create necessary indexes for efficient querying"""
        try:
            # Orders collection indexes
            self.orders_collection.create_index("orderId", unique=True)
            self.orders_collection.create_index("account")
            self.orders_collection.create_index("pair")
            self.orders_collection.create_index([("pair", 1), ("price", 1), ("orderType", 1)])
            self.orders_collection.create_index([("pair", 1), ("status", 1), ("orderType", 1), ("price", 1)])
            self.orders_collection.create_index([("price", 1), ("orderType", 1)])
            self.orders_collection.create_index([("status", 1), ("orderType", 1), ("price", 1)])
            self.orders_collection.create_index("status")
            self.orders_collection.create_index("height")
            self.orders_collection.create_index("txId")
            self.orders_collection.create_index("orderTime")
            self.orders_collection.create_index("lastFillTime")
            self.orders_collection.create_index("fillPercentage")
            self.orders_collection.create_index("totalDollarValue")
            self.orders_collection.create_index([("account", 1), ("status", 1)])
            self.orders_collection.create_index([("account", 1), ("orderType", 1)])
            self.orders_collection.create_index([("account", 1), ("pair", 1)])
            self.orders_collection.create_index([("pair", 1), ("account", 1), ("status", 1)])
            self.orders_collection.create_index("cancelledAt")  # New index for cancelled orders
            self.orders_collection.create_index([("status", 1), ("cancelledAt", -1)])  # Compound index for cancelled orders
            
            # Trades collection indexes
            self.trades_collection.create_index("orderId")
            self.trades_collection.create_index("account")
            self.trades_collection.create_index("pair")
            self.trades_collection.create_index("tradeTime")
            self.trades_collection.create_index("height")
            self.trades_collection.create_index("txId")
            self.trades_collection.create_index("dollarValue")
            self.trades_collection.create_index("fee")  # New index for fees
            self.trades_collection.create_index([("pair", 1), ("tradeTime", -1)])
            self.trades_collection.create_index([("account", 1), ("tradeTime", -1)])
            self.trades_collection.create_index([("orderId", 1), ("tradeTime", -1)])
            self.trades_collection.create_index([("pair", 1), ("price", 1), ("orderType", 1)])
            self.trades_collection.create_index([("price", 1), ("orderType", 1)])
            self.trades_collection.create_index([("maker", 1), ("pair", 1), ("tradeTime", -1)])
            self.trades_collection.create_index([("taker", 1), ("pair", 1), ("tradeTime", -1)])
            self.trades_collection.create_index([("pair", 1), ("dollarValue", -1)])
            self.trades_collection.create_index([("pair", 1), ("fee", -1)])  # Compound index for fee analysis
            
            logger.info("Database indexes created successfully")
            
        except Exception as e:
            logger.error(f"Error creating indexes: {e}")

    def get_order_statistics(self):
        """Get order statistics for monitoring"""
        try:
            total_orders = self.orders_collection.count_documents({})
            active_orders = self.orders_collection.count_documents({"status": "active"})
            filled_orders = self.orders_collection.count_documents({"status": "filled"})
            partially_filled_orders = self.orders_collection.count_documents({"status": "partially_filled"})
            cancelled_orders = self.orders_collection.count_documents({"status": "cancelled"})
            total_trades = self.trades_collection.count_documents({})
            
            # Get pair statistics
            pair_pipeline = [
                {"$group": {"_id": "$pair", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}}
            ]
            pair_stats = list(self.orders_collection.aggregate(pair_pipeline))
            
            # Get total dollar volume statistics
            dollar_volume_pipeline = [
                {"$group": {
                    "_id": None, 
                    "totalDollarVolume": {"$sum": "$dollarValue"},
                    "avgDollarValue": {"$avg": "$dollarValue"},
                    "totalFees": {"$sum": "$fee"},
                    "avgFee": {"$avg": "$fee"}
                }}
            ]
            dollar_volume_stats = list(self.trades_collection.aggregate(dollar_volume_pipeline))
            
            stats = {
                "total_orders": total_orders,
                "active_orders": active_orders,
                "filled_orders": filled_orders,
                "partially_filled_orders": partially_filled_orders,
                "cancelled_orders": cancelled_orders,
                "total_trades": total_trades,
                "orders_by_pair": pair_stats,
                "dollar_volume_stats": dollar_volume_stats[0] if dollar_volume_stats else {}
            }
            
            logger.info(f"Order Statistics: {stats}")
            return stats
            
        except Exception as e:
            logger.error(f"Error getting order statistics: {e}")
            return {}

    def close_connection(self):
        """Close database connection"""
        if self.mongo_client:
            self.mongo_client.close()