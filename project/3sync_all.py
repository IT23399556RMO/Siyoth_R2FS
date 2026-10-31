import os
import time
import boto3
from dotenv import load_dotenv
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# 1. Load variables
load_dotenv()
R2_ACCOUNT_ID = os.getenv("R2_ACCOUNT_ID")
R2_ACCESS_KEY_ID = os.getenv("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY")
R2_BUCKET = os.getenv("R2_BUCKET")
WATCH_DIR = os.getenv("WATCH_DIR")
R2_PREFIX = os.getenv("R2_PREFIX")

# 2. Initialize client
s3_client = boto3.client(
    's3',
    endpoint_url=f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
    aws_access_key_id=R2_ACCESS_KEY_ID,
    aws_secret_access_key=R2_SECRET_ACCESS_KEY,
    region_name="auto"
)

def get_r2_key(file_path):
    """Helper function to generate the R2 key from a local file path."""
    relative_path = os.path.relpath(file_path, WATCH_DIR)
    cloud_path = relative_path.replace(os.sep, '/')
    return f"{R2_PREFIX}/{cloud_path}"

def wait_for_file(file_path, retries=10, delay=2):
    """
    Waits for a file to be completely written by the OS before uploading.
    Solves the 'File Lock' problem for large files.
    """
    for attempt in range(retries):
        try:
            # Try to open the file in append mode.
            # If the OS is still writing to it, this will raise a PermissionError.
            with open(file_path, 'a'):
                pass
            return True
        except PermissionError:
            print(f"File {os.path.basename(file_path)} is locked. Waiting {delay}s (Attempt {attempt+1}/{retries})...")
            time.sleep(delay)
        except Exception as e:
            print(f"Unexpected error checking file {file_path}: {e}")
            return False
    
    print(f"Error: Could not access {file_path} after {retries} attempts.")
    return False

# 3. Updated File Handler for Subdirectories, Modifications, Moves, and Deletions
class R2SyncHandler(FileSystemEventHandler):
    
    def on_created(self, event):
        if event.is_directory:
            return
        
        file_path = event.src_path
        r2_key = get_r2_key(file_path)
        
        print(f"Detected New File: {file_path} | Uploading...")
        
        if wait_for_file(file_path):
            try:
                s3_client.upload_file(file_path, R2_BUCKET, r2_key)
                print(f"Success: Uploaded to {R2_BUCKET}/{r2_key}")
            except Exception as e:
                print(f"Error uploading {file_path}: {e}")

    def on_modified(self, event):
        # Triggered when an existing file is edited and saved
        if event.is_directory:
            return
            
        file_path = event.src_path
        r2_key = get_r2_key(file_path)
        
        print(f"Detected Modification: {file_path} | Updating in cloud...")
        
        if wait_for_file(file_path):
            try:
                s3_client.upload_file(file_path, R2_BUCKET, r2_key)
                print(f"Success: Updated {R2_BUCKET}/{r2_key}")
            except Exception as e:
                print(f"Error updating {file_path}: {e}")

    def on_moved(self, event):
        # Triggered when a file is renamed or moved to another folder
        if event.is_directory:
            return
            
        old_path = event.src_path
        new_path = event.dest_path
        
        old_key = get_r2_key(old_path)
        new_key = get_r2_key(new_path)
        
        print(f"Detected Move/Rename: {old_path} -> {new_path}")
        
        try:
            # 1. Upload the file to the new location
            if wait_for_file(new_path):
                s3_client.upload_file(new_path, R2_BUCKET, new_key)
                print(f"Success: Uploaded to new path {R2_BUCKET}/{new_key}")
                
            # 2. Delete the old object from the bucket
            s3_client.delete_object(Bucket=R2_BUCKET, Key=old_key)
            print(f"Success: Deleted old path {R2_BUCKET}/{old_key}")
            
        except Exception as e:
            print(f"Error moving file from {old_key} to {new_key}: {e}")

    def on_deleted(self, event):
        # Triggered when a file is deleted locally (Mirror Strategy)
        if event.is_directory:
            return
            
        file_path = event.src_path
        r2_key = get_r2_key(file_path)
        
        print(f"Detected Deletion locally: {file_path} | Deleting from cloud...")
        
        try:
            s3_client.delete_object(Bucket=R2_BUCKET, Key=r2_key)
            print(f"Success: Deleted {R2_BUCKET}/{r2_key}")
        except Exception as e:
            print(f"Error deleting {r2_key}: {e}")

# 4. Start the Watchdog Observer
if __name__ == "__main__":
    if not os.path.exists(WATCH_DIR):
        os.makedirs(WATCH_DIR)
        
    event_handler = R2SyncHandler()
    observer = Observer()
    
    # recursive=True allows watching subdirectories
    observer.schedule(event_handler, WATCH_DIR, recursive=True)
    
    print(f"Service started. Watching directory and subdirectories in: {WATCH_DIR}")
    print("Press Ctrl+C to stop.")
    
    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
