import logging
from datetime import datetime
from .utils import (
    safe_decimal_to_float, normalize_pair, extract_timestamp,
    validate_place_order_params, validate_match_order_params, validate_cancel_order_params
)

logger = logging.getLogger(__name__)

class EventProcessor:
    def __init__(self, database_manager):
        self.db = database_manager
        self.order_updates = []
        self.trade_inserts = []

    def process_place_order_event(self, event_params, height, block_time=None, tx_id=None):
        """Process PLACE_ORDER event - Updated to handle dollar-value"""
        try:
            account = str(event_params[0])
            order_id = str(event_params[1])
            amount = safe_decimal_to_float(event_params[2])
            price = safe_decimal_to_float(event_params[3])
            is_ask = bool(event_params[4])
            order_time = event_params[5]
            pair = normalize_pair(str(event_params[6]))
            dollar_value = safe_decimal_to_float(event_params[7])  # New parameter
            
            timestamp = extract_timestamp(order_time)
            
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
                "totalDollarValue": dollar_value,  # Store initial dollar value from event
                "totalFees": 0.0,  # New field for tracking total fees
                "numberOfFills": 0,
                "lastFillTime": None,
                "lastFillHeight": None,
                "createdAt": block_time or timestamp,
                "lastUpdated": height,
                "cancelledAt": None,
                "cancelReason": None,
                "tokenReturned": None,  # New field for cancelled orders
                "amountReturned": None  # New field for cancelled orders
            }
            
            # Use orderId as the unique identifier to prevent duplicates
            self.order_updates.append({
                'filter': {"orderId": order_id},
                'update': {"$setOnInsert": order_data},
                'upsert': True
            })
            
            logger.debug(f"Queued PLACE_ORDER update for order {order_id} - {pair} {order_data['orderType']} {amount} @ {price} (${dollar_value}) by {account}")
            
        except Exception as e:
            logger.error(f"Error processing PLACE_ORDER event: {e}")

    def process_match_order_event(self, event_params, height, block_time=None, tx_id=None):
        """Process MATCH_ORDER event - Updated to handle fee"""
        try:
            maker = str(event_params[0])
            taker = str(event_params[1])
            order_id = str(event_params[2])
            matched_amount = safe_decimal_to_float(event_params[3])
            matched_price = safe_decimal_to_float(event_params[4])
            is_ask = bool(event_params[5])
            order_time = event_params[6]
            pair = normalize_pair(str(event_params[7]))
            dollar_value = safe_decimal_to_float(event_params[8])
            fee = safe_decimal_to_float(event_params[9])  # New parameter
            
            timestamp = extract_timestamp(order_time)
            
            # First, get the current order to calculate new values
            existing_order = self.db.orders_collection.find_one({"orderId": order_id})
            
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
            
            # Calculate total dollar value and fees
            current_total_dollar_value = existing_order.get('totalDollarValue', 0.0)
            new_total_dollar_value = current_total_dollar_value + dollar_value
            current_total_fees = existing_order.get('totalFees', 0.0)
            new_total_fees = current_total_fees + fee
            
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
                "totalDollarValue": new_total_dollar_value,
                "totalFees": new_total_fees,  # New field
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
                "dollarValue": dollar_value,
                "fee": fee,  # New field for fee
                "isAsk": is_ask,
                "orderType": "ask" if is_ask else "bid",
                "tradeTime": timestamp,
                "height": height,
                "blockTime": block_time,
                "txId": tx_id,
                "tradeValue": matched_amount * matched_price,
                "netValue": (matched_amount * matched_price) - fee,  # New field for net value after fees
                "createdAt": block_time or timestamp
            }
            
            self.trade_inserts.append(trade_data)
            
            logger.debug(f"Queued MATCH_ORDER update for order {order_id} - {pair} matched {matched_amount} @ {matched_price} (${dollar_value}, fee: ${fee}) (maker: {maker}, taker: {taker}), status: {new_status}")
            
        except Exception as e:
            logger.error(f"Error processing MATCH_ORDER event: {e}")

    def process_cancel_order_event(self, event_params, height, block_time=None, tx_id=None):
        """Process CANCEL_ORDER event"""
        try:
            account = str(event_params[0])
            order_id = str(event_params[1])
            token_returned = str(event_params[2])
            amount_returned = safe_decimal_to_float(event_params[3])
            
            # Get the existing order to update
            existing_order = self.db.orders_collection.find_one({"orderId": order_id})
            
            if not existing_order:
                logger.error(f"Cannot process CANCEL_ORDER for non-existent order: {order_id}")
                return
            
            # Verify the account matches
            if existing_order.get('account') != account:
                logger.error(f"Account mismatch for CANCEL_ORDER: order {order_id} belongs to {existing_order.get('account')}, but cancel request from {account}")
                return
            
            # Update order with cancellation information
            cancel_time = block_time or datetime.utcnow()
            
            order_update = {
                "status": "cancelled",
                "cancelledAt": cancel_time,
                "cancelReason": "user_cancelled",  # Could be enhanced with more specific reasons
                "tokenReturned": token_returned,
                "amountReturned": amount_returned,
                "lastUpdated": height
            }
            
            self.order_updates.append({
                'filter': {"orderId": order_id, "account": account},  # Double-check account for security
                'update': {"$set": order_update},
                'upsert': False
            })
            
            logger.debug(f"Queued CANCEL_ORDER update for order {order_id} - cancelled by {account}, returned {amount_returned} {token_returned}")
            
        except Exception as e:
            logger.error(f"Error processing CANCEL_ORDER event: {e}")

    def execute_batch_updates(self):
        """Execute all queued batch updates"""
        try:
            # Process order updates
            if self.order_updates:
                for update in self.order_updates:
                    if update['upsert']:
                        self.db.orders_collection.update_one(
                            update['filter'],
                            update['update'],
                            upsert=update['upsert']
                        )
                    else:
                        result = self.db.orders_collection.update_one(
                            update['filter'],
                            update['update']
                        )
                        if result.matched_count == 0:
                            logger.warning(f"No order found for update: {update['filter']}")
                
                logger.info(f"Executed {len(self.order_updates)} order updates")
                self.order_updates.clear()
            
            # Process trade inserts
            if self.trade_inserts:
                self.db.trades_collection.insert_many(self.trade_inserts)
                logger.info(f"Inserted {len(self.trade_inserts)} trade records")
                self.trade_inserts.clear()

        except Exception as e:
            logger.error(f"Error executing batch updates: {e}")
            raise