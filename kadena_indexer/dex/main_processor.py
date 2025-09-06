import sys
import os
import logging
from collections import defaultdict
from .config import Config
from .database import DatabaseManager
from .event_processor import EventProcessor
from .utils import (
    validate_place_order_params, validate_match_order_params, validate_cancel_order_params
)

logger = logging.getLogger(__name__)

class DexOrderProcessor:
    def __init__(self):
        self.db_manager = DatabaseManager()
        self.event_processor = EventProcessor(self.db_manager)

    def fetch_and_organize_events(self, last_order_height):
        """Fetch all order events and organize them by height"""
        events_by_height = defaultdict(list)
        
        logger.info("Fetching order events...")
        
        events = self.db_manager.fetch_events(last_order_height)
        
        # Organize PLACE_ORDER events by height
        for event in events['place_orders']:
            height = event.get('height')
            if height is not None:
                event['event_type'] = 'PLACE_ORDER'
                events_by_height[height].append(event)
        
        # Organize MATCH_ORDER events by height
        for event in events['match_orders']:
            height = event.get('height')
            if height is not None:
                event['event_type'] = 'MATCH_ORDER'
                events_by_height[height].append(event)
        
        # Organize CANCEL_ORDER events by height
        for event in events['cancel_orders']:
            height = event.get('height')
            if height is not None:
                event['event_type'] = 'CANCEL_ORDER'
                events_by_height[height].append(event)
                
        return events_by_height

    def process_events_by_height(self, events_by_height):
        """Process all order events for each height"""
        for height in sorted(events_by_height.keys()):
            logger.info(f"Processing order events at height {height}")
            
            events = events_by_height[height]
            
            # Sort events by type to ensure proper processing order:
            # 1. PLACE_ORDER events first (to ensure orders exist)
            # 2. MATCH_ORDER events second (to fill existing orders)
            # 3. CANCEL_ORDER events last (to cancel remaining orders)
            event_priority = {'PLACE_ORDER': 0, 'MATCH_ORDER': 1, 'CANCEL_ORDER': 2}
            events.sort(key=lambda x: event_priority.get(x.get('event_type'), 999))
            
            for event in events:
                event_type = event.get('event_type')
                event_params = event.get('params', [])
                block_time = event.get('blockTime')
                tx_id = event.get('txId')
                
                if event_type == 'PLACE_ORDER':
                    # Validate and process PLACE_ORDER event
                    is_valid, error_msg = validate_place_order_params(event_params)
                    if not is_valid:
                        logger.error(f"Invalid PLACE_ORDER event at height {height}: {error_msg}")
                        continue
                    
                    self.event_processor.process_place_order_event(event_params, height, block_time, tx_id)
                
                elif event_type == 'MATCH_ORDER':
                    # Validate and process MATCH_ORDER event
                    is_valid, error_msg = validate_match_order_params(event_params)
                    if not is_valid:
                        logger.error(f"Invalid MATCH_ORDER event at height {height}: {error_msg}")
                        continue
                    
                    self.event_processor.process_match_order_event(event_params, height, block_time, tx_id)
                
                elif event_type == 'CANCEL_ORDER':
                    # Validate and process CANCEL_ORDER event
                    is_valid, error_msg = validate_cancel_order_params(event_params)
                    if not is_valid:
                        logger.error(f"Invalid CANCEL_ORDER event at height {height}: {error_msg}")
                        continue
                    
                    self.event_processor.process_cancel_order_event(event_params, height, block_time, tx_id)
            
            # Execute batch updates for this height
            self.event_processor.execute_batch_updates()
            
            # Update last processed height after successfully processing all events at this height
            self.db_manager.update_last_processed_height(height)

    def run(self):
        """Main processing function"""
        try:
            # Connect to MongoDB
            self.db_manager.connect_to_mongodb()
            
            # Create indexes
            self.db_manager.create_indexes()
            
            # Get last processed height
            last_order_height = self.db_manager.get_last_processed_height()
            logger.info(f"Last processed height: {last_order_height}")
            
            # Fetch and organize all order events
            events_by_height = self.fetch_and_organize_events(last_order_height)
            
            if not events_by_height:
                logger.info("No order events found to process.")
                self.db_manager.get_order_statistics()
                return
            
            # Process events by height in ascending order
            self.process_events_by_height(events_by_height)
            
            # Get final statistics
            self.db_manager.get_order_statistics()
            
            logger.info("DEX order processing completed successfully!")
            
        except Exception as e:
            logger.error(f"An error occurred during processing: {e}")
            raise
        finally:
            # Clean up
            self.db_manager.close_connection()


def process_dex_orders():
    """Main entry point with lock file handling"""
    if os.path.exists(Config.LOCK_FILE_PATH):
        logger.info(f"Lock file exists at {Config.LOCK_FILE_PATH}")
        logger.info("Another instance of the script is running. Exiting.")
        sys.exit()

    # Create lock file
    with open(Config.LOCK_FILE_PATH, 'w') as lock_file:
        lock_file.write("LOCKED")
        logger.info("Lock file created.")

    processor = DexOrderProcessor()
    
    try:
        processor.run()
        
    except Exception as e:
        logger.error(f"An error occurred during processing: {e}")
        raise
    finally:
        if os.path.exists(Config.LOCK_FILE_PATH):
            os.remove(Config.LOCK_FILE_PATH)
            logger.info("Lock file removed.")


if __name__ == "__main__":
    process_dex_orders()