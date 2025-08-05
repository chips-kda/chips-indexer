import sys
import os
import time
import tempfile
import psutil
from pathlib import Path

# Add the kadena_indexer directory to Python path
kadena_indexer_path = os.path.join(os.path.dirname(__file__), 'kadena_indexer')
sys.path.insert(0, kadena_indexer_path)

from kadena_indexer.scriptsMaster import run_scripts

# Use temp directory for lock file on Windows
LOCK_FILE = os.path.join(tempfile.gettempdir(), 'run_scripts.lock')

def is_process_running(pid):
    """Check if a process with given PID is still running"""
    try:
        return psutil.pid_exists(pid)
    except:
        return False

def acquire_lock():
    """Acquire lock with stale lock detection"""
    # Check if lock file exists and if the process is still running
    if os.path.exists(LOCK_FILE):
        try:
            with open(LOCK_FILE, 'r') as f:
                old_pid = int(f.read().strip())
            
            if is_process_running(old_pid):
                print("Another instance of the script is running. Exiting.")
                return None
            else:
                print("Removing stale lock file...")
                try:
                    os.remove(LOCK_FILE)
                except OSError:
                    pass
        except (ValueError, FileNotFoundError, OSError):
            # Invalid or missing PID, remove the lock file
            try:
                os.remove(LOCK_FILE)
            except (FileNotFoundError, OSError):
                pass
    
    # Try to create new lock file with current PID
    try:
        # Use exclusive creation mode to prevent race conditions
        with open(LOCK_FILE, 'x') as lockfile:
            lockfile.write(str(os.getpid()))
            lockfile.flush()
        return True
    except FileExistsError:
        # Another process created the file between our check and creation
        print("Another instance of the script is running. Exiting.")
        return None
    except OSError as e:
        print(f"Error creating lock file: {e}")
        return None

def release_lock():
    """Remove the lock file"""
    try:
        if os.path.exists(LOCK_FILE):
            os.remove(LOCK_FILE)
    except OSError:
        pass

def main():
    print("Starting script execution...")
    
    if not acquire_lock():
        return
    
    try:
        run_scripts()
    finally:
        # Clean up lock file
        release_lock()

if __name__ == "__main__":
    main()