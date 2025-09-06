from bson import Decimal128
from decimal import Decimal
import logging

logger = logging.getLogger(__name__)

def safe_decimal_to_float(value):
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

def normalize_pair(pair):
    """Normalize pair format from cKDA/KDA to cKDA-KDA"""
    if isinstance(pair, str):
        return pair.replace('/', '-')
    return pair

def extract_timestamp(order_time):
    """Extract timestamp from order_time object"""
    timestamp = None
    if isinstance(order_time, dict) and 'timep' in order_time:
        timestamp = order_time['timep']
    elif isinstance(order_time, str):
        timestamp = order_time
    else:
        timestamp = str(order_time)
    return timestamp

def validate_place_order_params(params):
    """Validate PLACE_ORDER event parameters"""
    if not isinstance(params, list):
        return False, f"Expected params to be a list, got {type(params)}"
    
    if len(params) < 8:  # Updated to include dollar-value
        return False, f"Expected at least 8 parameters for PLACE_ORDER [account, id, amount, price, is_ask, order_time, pair, dollar_value], got {len(params)}"
    
    try:
        # Validate parameter types
        account = str(params[0])    # account string
        order_id = str(params[1])   # string
        amount = safe_decimal_to_float(params[2])   # decimal -> float
        price = safe_decimal_to_float(params[3])    # decimal -> float
        is_ask = bool(params[4])    # boolean
        order_time = params[5]      # time object
        pair = str(params[6])       # pair string
        dollar_value = safe_decimal_to_float(params[7])  # decimal -> float
        
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
        if dollar_value <= 0:
            return False, "Dollar value must be positive"
            
    except (ValueError, TypeError) as e:
        return False, f"Invalid parameter types: {e}"
    
    return True, ""

def validate_match_order_params(params):
    """Validate MATCH_ORDER event parameters - Updated to handle fee"""
    if not isinstance(params, list):
        return False, f"Expected params to be a list, got {type(params)}"
    
    if len(params) < 10:  # Updated to include fee
        return False, f"Expected at least 10 parameters for MATCH_ORDER [maker, taker, id, amount, price, is_ask, order_time, pair, dollar_value, fee], got {len(params)}"
    
    try:
        # Validate parameter types
        maker = str(params[0])      # maker account string
        taker = str(params[1])      # taker account string
        order_id = str(params[2])   # string
        amount = safe_decimal_to_float(params[3])   # decimal -> float
        price = safe_decimal_to_float(params[4])    # decimal -> float
        is_ask = bool(params[5])    # boolean
        order_time = params[6]      # time object
        pair = str(params[7])       # pair string
        dollar_value = safe_decimal_to_float(params[8])  # decimal -> float
        fee = safe_decimal_to_float(params[9])  # decimal -> float
        
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
        if fee < 0:
            return False, "Fee cannot be negative"
            
    except (ValueError, TypeError) as e:
        return False, f"Invalid parameter types: {e}"
    
    return True, ""

def validate_cancel_order_params(params):
    """Validate CANCEL_ORDER event parameters"""
    if not isinstance(params, list):
        return False, f"Expected params to be a list, got {type(params)}"
    
    if len(params) < 4:
        return False, f"Expected at least 4 parameters for CANCEL_ORDER [account, id, token_returned, amount], got {len(params)}"
    
    try:
        # Validate parameter types
        account = str(params[0])    # account string
        order_id = str(params[1])   # string
        token_returned = str(params[2])  # token string
        amount = safe_decimal_to_float(params[3])   # decimal -> float
        
        if not account:
            return False, "Account cannot be empty"
        if not order_id:
            return False, "Order ID cannot be empty"
        if not token_returned:
            return False, "Token returned cannot be empty"
        if amount < 0:
            return False, "Amount cannot be negative"
            
    except (ValueError, TypeError) as e:
        return False, f"Invalid parameter types: {e}"
    
    return True, ""