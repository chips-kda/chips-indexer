import os
import sys

# Add the current directory to Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)

from kadena_indexer.dex.main_processor import process_dex_orders

def run_scripts():
    """Run all processing scripts in sequence"""
    print("Starting script execution...")
    
    try:
        print("1. Processing dex buy/sell orders...")
        process_dex_orders()
        print("✓ Dex orders processed successfully")
    except Exception as e:
        print(f"✗ Error processing dex orders: {e}")
    
    print("All scripts execution completed!")

if __name__ == "__main__":
    run_scripts()