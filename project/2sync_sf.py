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


# 3. Updated File Handler for Subdirectories
class R2SyncHandler(FileSystemEventHandler):
    def on_created(self, event):
        if event.is_directory:
            return

        file_path = event.src_path

        # Calculate the file's path relative to C:\data\incoming
        # Example: returns "home\raveen\photo.png"
        relative_path = os.path.relpath(file_path, WATCH_DIR)

        # Convert Windows backslashes to Cloudflare/S3 forward slashes
        # Example: becomes "home/raveen/photo.png"
        cloud_path = relative_path.replace(os.sep, '/')

        # Combine with the prefix: "incoming/home/raveen/photo.png"
        r2_key = f"{R2_PREFIX}/{cloud_path}"

        print(f"Detected: {relative_path} | Uploading...")

        try:
            time.sleep(1)  # Wait for Windows to finish writing the file
            s3_client.upload_file(file_path, R2_BUCKET, r2_key)
            print(f"Success: Uploaded to {R2_BUCKET}/{r2_key}")
        except Exception as e:
            print(f"Error: {e}")


# 4. Start the Watchdog Observer
if __name__ == "__main__":
    if not os.path.exists(WATCH_DIR):
        os.makedirs(WATCH_DIR)

    event_handler = R2SyncHandler()
    observer = Observer()

    # CRITICAL CHANGE: recursive=True allows watching subdirectories
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