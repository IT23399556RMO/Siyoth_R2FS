import os
import time
import boto3
from dotenv import load_dotenv
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# 1. Configuration Variables
load_dotenv()

R2_ACCOUNT_ID = os.getenv("R2_ACCOUNT_ID")
R2_ACCESS_KEY_ID = os.getenv("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY")
R2_BUCKET = os.getenv("R2_BUCKET")
WATCH_DIR = os.getenv("WATCH_DIR")  # The 'r' ensures Windows backslashes are read correctly
R2_PREFIX = os.getenv("R2_PREFIX")
TOKEN = os.getenv("TOKEN")  # (Keep this handy if your team's internal API requires it)

# 2. Initialize the Cloudflare R2 Client
s3_client = boto3.client(
    's3',
    endpoint_url=f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
    aws_access_key_id=R2_ACCESS_KEY_ID,
    aws_secret_access_key=R2_SECRET_ACCESS_KEY,
    region_name="auto"  # Required by boto3 syntax, but R2 ignores it
)


# 3. Define what happens when a new file is detected
class R2SyncHandler(FileSystemEventHandler):
    def on_created(self, event):
        # Ignore new folders, only process files
        if event.is_directory:
            return

        file_path = event.src_path
        file_name = os.path.basename(file_path)

        # Combine prefix and filename for R2 (e.g., "incoming/photo.jpg")
        r2_key = f"{R2_PREFIX}/{file_name}"

        print(f"New file detected: {file_name}. Uploading to R2...")

        try:
            # Brief delay ensures Windows has fully finished saving the file before reading it
            time.sleep(1)

            # Upload the file to Cloudflare R2
            s3_client.upload_file(file_path, R2_BUCKET, r2_key)
            print(f"Success: {file_name} successfully uploaded to {R2_BUCKET}/{r2_key}")
        except Exception as e:
            print(f"Error uploading {file_name}: {e}")


# 4. Start the Watchdog Observer
if __name__ == "__main__":
    # Automatically create the folder on your C: drive if it doesn't exist yet
    if not os.path.exists(WATCH_DIR):
        os.makedirs(WATCH_DIR)

    event_handler = R2SyncHandler()
    observer = Observer()

    # schedule the watcher
    observer.schedule(event_handler, WATCH_DIR, recursive=False)

    print(f"Service started. Watching directory: {WATCH_DIR}")
    print("Press Ctrl+C to stop.")

    observer.start()
    try:
        # Keep the script running in the background
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
