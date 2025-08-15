from pymongo import MongoClient
from pymongo.server_api import ServerApi
from bson import Decimal128  # Added import for Decimal128
import os
import sys
import json
from dotenv import load_dotenv
import tempfile
from collections import defaultdict
import logging
from decimal import Decimal
from datetime import datetime

load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

lock_file_path = os.path.join(tempfile.gettempdir(), "dex_orders_script.lock")

class DexOrderProcessor:
    def __init__(self):
        self.mongo_client = None
        self.mongo_db = None
        self.orders_collection = None
        self.trades_collection = None
        
        # DEX order event queries
        self.place_order_query = "n_e98a056e3e14203e6ec18fada427334b21b667d8.ce1.PLACE_ORDER"
        self.match_order_query = "n_e98a056e3e14203e6ec18fada427334b21b667d8.ce1.MATCH_ORDER"
        
        # Batch operations storage
        self.order_updates = []
        self.trade_inserts = []
        self.batch_size = 100

    def safe_decimal_to_float(self, value):
        """Safely convert various decimal types to float"""
        if isinstance(value, Decimal128):
            return float(value.to_decimal())
        elif isinstance(value, Decimal):
            return float(value)
        elif isinstance(value, (int, float)):
            return float(value)
        elif isinstance(value, str):
            return float(value)
        else:
            # Try to convert directly as fallback
            return float(value)

    def normalize_pair(self, pair):
        """Normalize pair format from cKDA/KDA to cKDA-KDA"""
        if isinstance(pair, str):
            return pair.replace('/', '-')
        return pair

    def connect_to_mongodb(self):
        """Establish MongoDB connection"""
        try:
            mongo_uri = os.environ.get('Mongo_URI')
            if not mongo_uri:
                raise ValueError("Mongo_URI environment variable not set")
            
            self.mongo_client = MongoClient(mongo_uri, server_api=ServerApi('1'))
            self.mongo_db = self.mongo_client['chipsDB']  
            self.orders_collection = self.mongo_db['orders']
            self.trades_collection = self.mongo_db['trades']
            
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

    def validate_place_order_params(self, params):
        """Validate PLACE_ORDER event parameters"""
        if not isinstance(params, list):
            return False, f"Expected params to be a list, got {type(params)}"
        
        if len(params) < 7:
            return False, f"Expected at least 7 parameters for PLACE_ORDER [account, id, amount, price, is_ask, order_time, pair], got {len(params)}"
        
        try:
            # Validate parameter types
            account = str(params[0])    # account string
            order_id = str(params[1])   # string
            amount = self.safe_decimal_to_float(params[2])   # decimal -> float
            price = self.safe_decimal_to_float(params[3])    # decimal -> float
            is_ask = bool(params[4])    # boolean
            order_time = params[5]      # time object
            pair = str(params[6])       # pair string
            
            if not account:
                return False, "Account cannot be empty"
            if not order_id:
                return False, "Order ID cannot be empty"
            if amount <= 0:
                return False, "Amount must be positive"
            if price <= 0:
                return False, "Price must be positive"
            if not pair:
                return False, "Pair cannot be empty"
                
        except (ValueError, TypeError) as e:
            return False, f"Invalid parameter types: {e}"
        
        return True, ""

    def validate_match_order_params(self, params):
        """Validate MATCH_ORDER event parameters - Updated to handle dollar-value"""
        if not isinstance(params, list):
            return False, f"Expected params to be a list, got {type(params)}"
        
        if len(params) < 9:
            return False, f"Expected at least 9 parameters for MATCH_ORDER [maker, taker, id, amount, price, is_ask, order_time, pair, dollar_value], got {len(params)}"
        
        try:
            # Validate parameter types
            maker = str(params[0])      # maker account string
            taker = str(params[1])      # taker account string
            order_id = str(params[2])   # string
            amount = self.safe_decimal_to_float(params[3])   # decimal -> float
            price = self.safe_decimal_to_float(params[4])    # decimal -> float
            is_ask = bool(params[5])    # boolean
            order_time = params[6]      # time object
            pair = str(params[7])       # pair string
            dollar_value = self.safe_decimal_to_float(params[8])  # decimal -> float
            
            if not maker:
                return False, "Maker account cannot be empty"
            if not taker:
                return False, "Taker account cannot be empty"
            if not order_id:
                return False, "Order ID cannot be empty"
            if amount <= 0:
                return False, "Amount must be positive"
            if price <= 0:
                return False, "Price must be positive"
            if not pair:
                return False, "Pair cannot be empty"
            if dollar_value <= 0:
                return False, "Dollar value must be positive"
                
        except (ValueError, TypeError) as e:
            return False, f"Invalid parameter types: {e}"
        
        return True, ""

    def extract_timestamp(self, order_time):
        """Extract timestamp from order_time object"""
        timestamp = None
        if isinstance(order_time, dict) and 'timep' in order_time:
            timestamp = order_time['timep']
        elif isinstance(order_time, str):
            timestamp = order_time
        else:
            timestamp = str(order_time)
        return timestamp

    def process_place_order_event(self, event_params, height, block_time=None, tx_id=None):
        """Process PLACE_ORDER event"""
        try:
            account = str(event_params[0])
            order_id = str(event_params[1])
            amount = self.safe_decimal_to_float(event_params[2])
            price = self.safe_decimal_to_float(event_params[3])
            is_ask = bool(event_params[4])
            order_time = event_params[5]
            pair = self.normalize_pair(str(event_params[6]))
            
            timestamp = self.extract_timestamp(order_time)
            
            order_data = {
                "orderId": order_id,
                "account": account,
                "pair": pair,
                "originalAmount": amount,
                "amount": amount,  # Remaining amount
                "filledAmount": 0.0,
                "price": price,
                "isAsk": is_ask,
                "orderType": "ask" if is_ask else "bid",
                "orderTime": timestamp,
                "height": height,
                "blockTime": block_time,
                "txId": tx_id,
                "status": "active",  # active, filled, partially_filled, cancelled
                "fillPercentage": 0.0,
                "averageFillPrice": 0.0,
                "totalFillValue": 0.0,
                "totalDollarValue": 0.0,  # New field for tracking dollar value
                "numberOfFills": 0,
                "lastFillTime": None,
                "lastFillHeight": None,
                "createdAt": block_time or timestamp,
                "lastUpdated": height,
                "cancelledAt": None,
                "cancelReason": None
            }
            
            # Use orderId as the unique identifier to prevent duplicates
            self.order_updates.append({
                'filter': {"orderId": order_id},
                'update': {"$setOnInsert": order_data},
                'upsert': True
            })
            
            logger.debug(f"Queued PLACE_ORDER update for order {order_id} - {pair} {order_data['orderType']} {amount} @ {price} by {account}")
            
        except Exception as e:
            logger.error(f"Error processing PLACE_ORDER event: {e}")

    def process_match_order_event(self, event_params, height, block_time=None, tx_id=None):
        """Process MATCH_ORDER event - Updated to handle dollar-value"""
        try:
            maker = str(event_params[0])
            taker = str(event_params[1])
            order_id = str(event_params[2])
            matched_amount = self.safe_decimal_to_float(event_params[3])
            matched_price = self.safe_decimal_to_float(event_params[4])
            is_ask = bool(event_params[5])
            order_time = event_params[6]
            pair = self.normalize_pair(str(event_params[7]))
            dollar_value = self.safe_decimal_to_float(event_params[8])  # New parameter
            
            timestamp = self.extract_timestamp(order_time)
            
            # First, get the current order to calculate new values
            existing_order = self.orders_collection.find_one({"orderId": order_id})
            
            if not existing_order:
                logger.error(f"Cannot process MATCH_ORDER for non-existent order: {order_id}")
                return
            
            # Calculate new values
            new_filled_amount = existing_order.get('filledAmount', 0.0) + matched_amount
            new_remaining_amount = existing_order.get('originalAmount', 0.0) - new_filled_amount
            new_number_of_fills = existing_order.get('numberOfFills', 0) + 1
            
            # Calculate average fill price
            current_total_value = existing_order.get('totalFillValue', 0.0)
            new_total_value = current_total_value + (matched_amount * matched_price)
            new_average_fill_price = new_total_value / new_filled_amount if new_filled_amount > 0 else 0.0
            
            # Calculate total dollar value
            current_total_dollar_value = existing_order.get('totalDollarValue', 0.0)
            new_total_dollar_value = current_total_dollar_value + dollar_value
            
            # Calculate fill percentage
            original_amount = existing_order.get('originalAmount', 0.0)
            fill_percentage = (new_filled_amount / original_amount * 100) if original_amount > 0 else 0.0
            
            # Determine new status
            if new_remaining_amount <= 0.001:  # Consider floating point precision
                new_status = "filled"
                new_remaining_amount = 0.0
            elif new_filled_amount > 0:
                new_status = "partially_filled"
            else:
                new_status = existing_order.get('status', 'active')
            
            # Update order with match information
            order_update = {
                "amount": new_remaining_amount,
                "filledAmount": new_filled_amount,
                "fillPercentage": fill_percentage,
                "averageFillPrice": new_average_fill_price,
                "totalFillValue": new_total_value,
                "totalDollarValue": new_total_dollar_value,  # New field
                "numberOfFills": new_number_of_fills,
                "lastFillTime": timestamp,
                "lastFillHeight": height,
                "status": new_status,
                "lastUpdated": height
            }
            
            self.order_updates.append({
                'filter': {"orderId": order_id},
                'update': {"$set": order_update},
                'upsert': False
            })
            
            # Create trade record
            trade_data = {
                "orderId": order_id,
                "pair": pair,
                "maker": maker,
                "taker": taker,
                "makerAccount": maker,  # For backward compatibility
                "takerAccount": taker,  # For backward compatibility
                "amount": matched_amount,
                "price": matched_price,
                "dollarValue": dollar_value,  # New field for dollar value
                "isAsk": is_ask,
                "orderType": "ask" if is_ask else "bid",
                "tradeTime": timestamp,
                "height": height,
                "blockTime": block_time,
                "txId": tx_id,
                "tradeValue": matched_amount * matched_price,
                "createdAt": block_time or timestamp
            }
            
            self.trade_inserts.append(trade_data)
            
            logger.debug(f"Queued MATCH_ORDER update for order {order_id} - {pair} matched {matched_amount} @ {matched_price} (${dollar_value}) (maker: {maker}, taker: {taker}), status: {new_status}")
            
        except Exception as e:
            logger.error(f"Error processing MATCH_ORDER event: {e}")

    def execute_batch_updates(self):
        """Execute all queued batch updates"""
        try:
            # Process order updates
            if self.order_updates:
                for update in self.order_updates:
                    if update['upsert']:
                        self.orders_collection.update_one(
                            update['filter'],
                            update['update'],
                            upsert=update['upsert']
                        )
                    else:
                        result = self.orders_collection.update_one(
                            update['filter'],
                            update['update']
                        )
                        if result.matched_count == 0:
                            logger.warning(f"No order found for update: {update['filter']}")
                
                logger.info(f"Executed {len(self.order_updates)} order updates")
                self.order_updates.clear()
            
            # Process trade inserts
            if self.trade_inserts:
                self.trades_collection.insert_many(self.trade_inserts)
                logger.info(f"Inserted {len(self.trade_inserts)} trade records")
                self.trade_inserts.clear()

        except Exception as e:
            logger.error(f"Error executing batch updates: {e}")
            raise

    def fetch_and_organize_events(self, last_order_height):
        """Fetch all order events and organize them by height"""
        events_by_height = defaultdict(list)
        
        logger.info("Fetching order events...")
        
        try:
            # Fetch PLACE_ORDER events
            place_events = list(
                self.mongo_db[self.place_order_query].find(
                    {"height": {"$gt": last_order_height}}
                ).sort("height", 1)
            )
            
            # Fetch MATCH_ORDER events
            match_events = list(
                self.mongo_db[self.match_order_query].find(
                    {"height": {"$gt": last_order_height}}
                ).sort("height", 1)
            )
            
            logger.info(f"Found {len(place_events)} PLACE_ORDER events and {len(match_events)} MATCH_ORDER events")
            
            # Organize PLACE_ORDER events by height
            for event in place_events:
                height = event.get('height')
                if height is not None:
                    event['event_type'] = 'PLACE_ORDER'
                    events_by_height[height].append(event)
            
            # Organize MATCH_ORDER events by height
            for event in match_events:
                height = event.get('height')
                if height is not None:
                    event['event_type'] = 'MATCH_ORDER'
                    events_by_height[height].append(event)
                    
        except Exception as e:
            logger.error(f"Error fetching order events: {e}")
            return defaultdict(list)
        
        return events_by_height

    def process_events_by_height(self, events_by_height):
        """Process all order events for each height"""
        for height in sorted(events_by_height.keys()):
            logger.info(f"Processing order events at height {height}")
            
            events = events_by_height[height]
            
            # Sort events by type to ensure PLACE_ORDER events are processed before MATCH_ORDER events
            # This ensures orders exist before they can be matched
            events.sort(key=lambda x: 0 if x.get('event_type') == 'PLACE_ORDER' else 1)
            
            for event in events:
                event_type = event.get('event_type')
                event_params = event.get('params', [])
                block_time = event.get('blockTime')
                tx_id = event.get('txId')
                
                if event_type == 'PLACE_ORDER':
                    # Validate and process PLACE_ORDER event
                    is_valid, error_msg = self.validate_place_order_params(event_params)
                    if not is_valid:
                        logger.error(f"Invalid PLACE_ORDER event at height {height}: {error_msg}")
                        continue
                    
                    self.process_place_order_event(event_params, height, block_time, tx_id)
                
                elif event_type == 'MATCH_ORDER':
                    # Validate and process MATCH_ORDER event
                    is_valid, error_msg = self.validate_match_order_params(event_params)
                    if not is_valid:
                        logger.error(f"Invalid MATCH_ORDER event at height {height}: {error_msg}")
                        continue
                    
                    self.process_match_order_event(event_params, height, block_time, tx_id)
            
            # Execute batch updates for this height
            self.execute_batch_updates()
            
            # Update last processed height after successfully processing all events at this height
            self.update_last_processed_height(height)

    def create_indexes(self):
        """Create necessary indexes for efficient querying"""
        try:
            # Orders collection indexes
            self.orders_collection.create_index("orderId", unique=True)
            self.orders_collection.create_index("account")
            self.orders_collection.create_index("pair")  # New index for pair
            self.orders_collection.create_index([("pair", 1), ("price", 1), ("orderType", 1)])  # Updated compound index
            self.orders_collection.create_index([("pair", 1), ("status", 1), ("orderType", 1), ("price", 1)])  # Updated compound index
            self.orders_collection.create_index([("price", 1), ("orderType", 1)])
            self.orders_collection.create_index([("status", 1), ("orderType", 1), ("price", 1)])
            self.orders_collection.create_index("status")
            self.orders_collection.create_index("height")
            self.orders_collection.create_index("txId")
            self.orders_collection.create_index("orderTime")
            self.orders_collection.create_index("lastFillTime")
            self.orders_collection.create_index("fillPercentage")
            self.orders_collection.create_index("totalDollarValue")  # New index for dollar value
            self.orders_collection.create_index([("account", 1), ("status", 1)])
            self.orders_collection.create_index([("account", 1), ("orderType", 1)])
            self.orders_collection.create_index([("account", 1), ("pair", 1)])  # New compound index
            self.orders_collection.create_index([("pair", 1), ("account", 1), ("status", 1)])  # New compound index
            
            # Trades collection indexes
            self.trades_collection.create_index("orderId")
            self.trades_collection.create_index("account")
            self.trades_collection.create_index("pair")  # New index for pair
            self.trades_collection.create_index("tradeTime")
            self.trades_collection.create_index("height")
            self.trades_collection.create_index("txId")
            self.trades_collection.create_index("dollarValue")  # New index for dollar value
            self.trades_collection.create_index([("pair", 1), ("tradeTime", -1)])  # New compound index
            self.trades_collection.create_index([("account", 1), ("tradeTime", -1)])
            self.trades_collection.create_index([("orderId", 1), ("tradeTime", -1)])
            self.trades_collection.create_index([("pair", 1), ("price", 1), ("orderType", 1)])  # Updated compound index
            self.trades_collection.create_index([("price", 1), ("orderType", 1)])
            self.trades_collection.create_index([("maker", 1), ("pair", 1), ("tradeTime", -1)])  # New compound index
            self.trades_collection.create_index([("taker", 1), ("pair", 1), ("tradeTime", -1)])  # New compound index
            self.trades_collection.create_index([("pair", 1), ("dollarValue", -1)])  # New compound index for dollar value queries
            
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
                    "avgDollarValue": {"$avg": "$dollarValue"}
                }}
            ]
            dollar_volume_stats = list(self.trades_collection.aggregate(dollar_volume_pipeline))
            
            stats = {
                "total_orders": total_orders,
                "active_orders": active_orders,
                "filled_orders": filled_orders,
                "partially_filled_orders": partially_filled_orders,
                "total_trades": total_trades,
                "orders_by_pair": pair_stats,
                "dollar_volume_stats": dollar_volume_stats[0] if dollar_volume_stats else {}
            }
            
            logger.info(f"Order Statistics: {stats}")
            return stats
            
        except Exception as e:
            logger.error(f"Error getting order statistics: {e}")
            return {}

def process_dex_orders():
    """Main processing function"""
    if os.path.exists(lock_file_path):
        logger.info(f"Lock file exists at {lock_file_path}")
        logger.info("Another instance of the script is running. Exiting.")
        sys.exit()

    # Create lock file
    with open(lock_file_path, 'w') as lock_file:
        lock_file.write("LOCKED")
        logger.info("Lock file created.")

    processor = DexOrderProcessor()
    
    try:
        # Connect to MongoDB
        processor.connect_to_mongodb()
        
        # Create indexes
        processor.create_indexes()
        
        # Get last processed height
        last_order_height = processor.get_last_processed_height()
        logger.info(f"Last processed height: {last_order_height}")
        
        # Fetch and organize all order events
        events_by_height = processor.fetch_and_organize_events(last_order_height)
        
        if not events_by_height:
            logger.info("No order events found to process.")
            processor.get_order_statistics()
            return
        
        # Process events by height in ascending order
        processor.process_events_by_height(events_by_height)
        
        # Get final statistics
        processor.get_order_statistics()
        
        logger.info("DEX order processing completed successfully!")
        
    except Exception as e:
        logger.error(f"An error occurred during processing: {e}")
        raise
    finally:
        # Clean up
        if processor.mongo_client:
            processor.mongo_client.close()
        
        if os.path.exists(lock_file_path):
            os.remove(lock_file_path)
            logger.info("Lock file removed.")

if __name__ == "__main__":
    process_dex_orders()