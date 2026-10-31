import os
import time
import threading
import boto3
from dotenv import load_dotenv
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# 1. Load Configuration
load_dotenv()
R2_ACCOUNT_ID = os.getenv("R2_ACCOUNT_ID")
R2_ACCESS_KEY_ID = os.getenv("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY")
R2_BUCKET = os.getenv("R2_BUCKET")
WATCH_DIR = os.getenv("WATCH_DIR")
R2_PREFIX = os.getenv("R2_PREFIX")

s3_client = boto3.client(
    's3',
    endpoint_url=f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
    aws_access_key_id=R2_ACCESS_KEY_ID,
    aws_secret_access_key=R2_SECRET_ACCESS_KEY,
    region_name="auto"
)

# Locks to prevent infinite loops and race conditions
active_downloads = set()
pending_uploads = set()


def get_r2_key(file_path):
    relative_path = os.path.relpath(file_path, WATCH_DIR)
    cloud_path = relative_path.replace(os.sep, '/')
    return f"{R2_PREFIX}/{cloud_path}"


def wait_for_file(file_path, retries=10, delay=2):
    for attempt in range(retries):
        try:
            with open(file_path, 'a'):
                pass
            return True
        except PermissionError:
            time.sleep(delay)
        except Exception:
            return False
    return False


# 2. Cloud to Local (The Polling Thread)
def poll_cloud_changes():
    while True:
        time.sleep(30)
        try:
            response = s3_client.list_objects_v2(Bucket=R2_BUCKET, Prefix=R2_PREFIX)
            cloud_keys = set()

            if 'Contents' in response:
                for obj in response['Contents']:
                    cloud_key = obj['Key']
                    if cloud_key == f"{R2_PREFIX}/":
                        continue

                    cloud_keys.add(cloud_key)
                    relative_key = cloud_key[len(R2_PREFIX) + 1:]
                    local_path = os.path.join(WATCH_DIR, relative_key.replace('/', os.sep))

                    cloud_size = obj['Size']
                    cloud_time = obj['LastModified'].timestamp()

                    # Check for Additions / Edits in R2
                    download_needed = False
                    if not os.path.exists(local_path):
                        download_needed = True
                    elif cloud_size != os.path.getsize(local_path) or cloud_time > os.path.getmtime(local_path):
                        download_needed = True

                    if download_needed:
                        print(f"Cloud update detected. Downloading: {cloud_key}")
                        os.makedirs(os.path.dirname(local_path), exist_ok=True)

                        active_downloads.add(local_path)
                        with open(local_path, 'wb') as f:
                            s3_client.download_fileobj(R2_BUCKET, cloud_key, f)
                        time.sleep(1)
                        active_downloads.remove(local_path)

            # Check for Deletions / Renames in R2 (Mirror Logic)
            for root, dirs, files in os.walk(WATCH_DIR):
                for file in files:
                    local_path = os.path.join(root, file)
                    r2_key = get_r2_key(local_path)

                    # If file is on PC but missing in R2, AND not actively uploading/downloading, delete it.
                    if r2_key not in cloud_keys and local_path not in pending_uploads and local_path not in active_downloads:
                        print(f"Cloud deletion/rename detected. Removing locally: {local_path}")
                        try:
                            os.remove(local_path)
                        except Exception as e:
                            print(f"Error removing local file: {e}")

        except Exception as e:
            print(f"Cloud polling error: {e}")


# 3. Local to Cloud (The Watchdog Observer)
class R2SyncHandler(FileSystemEventHandler):

    def on_created(self, event):
        if event.is_directory or event.src_path in active_downloads:
            return

        pending_uploads.add(event.src_path)
        r2_key = get_r2_key(event.src_path)
        print(f"Local Add: {event.src_path} | Uploading...")

        if wait_for_file(event.src_path):
            try:
                s3_client.upload_file(event.src_path, R2_BUCKET, r2_key)
            except Exception as e:
                print(f"Upload error: {e}")
        pending_uploads.discard(event.src_path)

    def on_modified(self, event):
        if event.is_directory or event.src_path in active_downloads:
            return

        pending_uploads.add(event.src_path)
        r2_key = get_r2_key(event.src_path)
        print(f"Local Edit: {event.src_path} | Updating...")

        if wait_for_file(event.src_path):
            try:
                s3_client.upload_file(event.src_path, R2_BUCKET, r2_key)
            except Exception as e:
                print(f"Update error: {e}")
        pending_uploads.discard(event.src_path)

    def on_moved(self, event):
        if event.is_directory or event.src_path in active_downloads or event.dest_path in active_downloads:
            return

        pending_uploads.add(event.dest_path)
        old_key = get_r2_key(event.src_path)
        new_key = get_r2_key(event.dest_path)
        print(f"Local Move/Rename: {event.src_path} -> {event.dest_path}")

        try:
            if wait_for_file(event.dest_path):
                s3_client.upload_file(event.dest_path, R2_BUCKET, new_key)
            s3_client.delete_object(Bucket=R2_BUCKET, Key=old_key)
        except Exception as e:
            print(f"Move error: {e}")
        pending_uploads.discard(event.dest_path)

    def on_deleted(self, event):
        if event.is_directory or event.src_path in active_downloads:
            return

        r2_key = get_r2_key(event.src_path)
        print(f"Local Delete: {event.src_path} | Deleting from cloud...")
        try:
            s3_client.delete_object(Bucket=R2_BUCKET, Key=r2_key)
        except Exception as e:
            print(f"Delete error: {e}")


# 4. Start Services
if __name__ == "__main__":
    if not os.path.exists(WATCH_DIR):
        os.makedirs(WATCH_DIR)

    cloud_thread = threading.Thread(target=poll_cloud_changes, daemon=True)
    cloud_thread.start()

    event_handler = R2SyncHandler()
    observer = Observer()
    observer.schedule(event_handler, WATCH_DIR, recursive=True)

    print(f"Service started. FULL Bi-directional mirror active for: {WATCH_DIR}")
    print("Press Ctrl+C to stop.")

    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
