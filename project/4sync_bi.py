import os
import time
import threading
import boto3
from dotenv import load_dotenv
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# 1. Load configuration variables from the .env file[cite: 1]
load_dotenv()
R2_ACCOUNT_ID = os.getenv("R2_ACCOUNT_ID")
R2_ACCESS_KEY_ID = os.getenv("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY")
R2_BUCKET = os.getenv("R2_BUCKET")
WATCH_DIR = os.getenv("WATCH_DIR")
R2_PREFIX = os.getenv("R2_PREFIX")

# 2. Initialize the Cloudflare R2 client using boto3[cite: 1]
s3_client = boto3.client(
    's3',
    endpoint_url=f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
    aws_access_key_id=R2_ACCESS_KEY_ID,
    aws_secret_access_key=R2_SECRET_ACCESS_KEY,
    region_name="auto"
)

# Active download tracker to prevent infinite upload loops during polling
active_downloads = set()


def get_r2_key(file_path):
    """Helper function to generate the R2 cloud path from a local Windows path[cite: 1]."""
    relative_path = os.path.relpath(file_path, WATCH_DIR)
    cloud_path = relative_path.replace(os.sep, '/')
    return f"{R2_PREFIX}/{cloud_path}"


def wait_for_file(file_path, retries=10, delay=2):
    """
    Waits for a file to be completely written by the OS before uploading[cite: 1].
    Solves the 'File Lock' problem for large files being copied into the folder[cite: 1].
    """
    for attempt in range(retries):
        try:
            with open(file_path, 'a'):
                pass
            return True
        except PermissionError:
            print(f"File {os.path.basename(file_path)} is locked. Waiting {delay}s...")
            time.sleep(delay)
        except Exception as e:
            return False
    return False


# 3. The Polling Thread: Cloud to Local Sync (Reverse Sync)
def poll_cloud_changes():
    """Continuously checks the R2 bucket for new or updated files to download."""
    while True:
        time.sleep(30)  # Cyclical pause to prevent API rate-limiting
        print("Polling cloud for updates...")
        try:
            response = s3_client.list_objects_v2(Bucket=R2_BUCKET, Prefix=R2_PREFIX)
            if 'Contents' not in response:
                continue

            for obj in response['Contents']:
                cloud_key = obj['Key']
                # Skip the prefix folder itself if it appears as a blank object
                if cloud_key == f"{R2_PREFIX}/":
                    continue

                # Convert cloud path back to local Windows path format
                relative_key = cloud_key[len(R2_PREFIX) + 1:]
                local_path = os.path.join(WATCH_DIR, relative_key.replace('/', os.sep))

                cloud_size = obj['Size']
                cloud_time = obj['LastModified'].timestamp()

                download_needed = False
                if not os.path.exists(local_path):
                    download_needed = True
                else:
                    # Compare sizes or timestamps to fetch newer files
                    local_size = os.path.getsize(local_path)
                    local_time = os.path.getmtime(local_path)
                    if cloud_size != local_size or cloud_time > local_time:
                        download_needed = True

                if download_needed:
                    print(f"Downloading missing/newer file: {cloud_key}")
                    os.makedirs(os.path.dirname(local_path), exist_ok=True)

                    # Temporarily lock this exact path to pause Watchdog observation
                    active_downloads.add(local_path)

                    # Stream directly into the locked file path to prevent .tmp file creation loop
                    with open(local_path, 'wb') as f:
                        s3_client.download_fileobj(R2_BUCKET, cloud_key, f)

                    # Allow Windows 1 second to finish firing any residual 'modified' events
                    time.sleep(1)
                    active_downloads.remove(local_path)
                    print(f"Success: Downloaded {cloud_key} to {local_path}")

        except Exception as e:
            print(f"Cloud polling error: {e}")


# 4. Local to Cloud Sync: File Handler for Watchdog
class R2SyncHandler(FileSystemEventHandler):

    def on_created(self, event):
        """Triggered when a new file is dropped into the watched folder[cite: 1]."""
        if event.is_directory or event.src_path in active_downloads:
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
        """Triggered when an existing file is edited and saved[cite: 1]."""
        if event.is_directory or event.src_path in active_downloads:
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
        """Triggered when a file is renamed or moved to another folder[cite: 1]."""
        if event.is_directory or event.src_path in active_downloads or event.dest_path in active_downloads:
            return

        old_path = event.src_path
        new_path = event.dest_path

        old_key = get_r2_key(old_path)
        new_key = get_r2_key(new_path)

        print(f"Detected Move/Rename: {old_path} -> {new_path}")
        try:
            # 1. Upload the file to the new location[cite: 1]
            if wait_for_file(new_path):
                s3_client.upload_file(new_path, R2_BUCKET, new_key)

            # 2. Delete the old object from the bucket to prevent duplicates[cite: 1]
            s3_client.delete_object(Bucket=R2_BUCKET, Key=old_key)
            print(f"Success: Moved {old_key} to {new_key}")
        except Exception as e:
            print(f"Error moving file: {e}")

    def on_deleted(self, event):
        """Triggered when a file is deleted locally (Mirror Strategy)[cite: 1]."""
        if event.is_directory or event.src_path in active_downloads:
            return

        file_path = event.src_path
        r2_key = get_r2_key(file_path)

        print(f"Detected Deletion locally: {file_path} | Deleting from cloud...")
        try:
            s3_client.delete_object(Bucket=R2_BUCKET, Key=r2_key)
            print(f"Success: Deleted {R2_BUCKET}/{r2_key}")
        except Exception as e:
            print(f"Error deleting {r2_key}: {e}")


# 5. Start the Background Services
if __name__ == "__main__":
    if not os.path.exists(WATCH_DIR):
        os.makedirs(WATCH_DIR)

    # Start the cloud-to-local polling thread in the background
    cloud_thread = threading.Thread(target=poll_cloud_changes, daemon=True)
    cloud_thread.start()

    event_handler = R2SyncHandler()
    observer = Observer()

    # recursive=True allows watching all subdirectories (like home/gayan)[cite: 1]
    observer.schedule(event_handler, WATCH_DIR, recursive=True)

    print(f"Service started. Bi-directional sync active for: {WATCH_DIR}")
    print("Press Ctrl+C to stop.")

    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
