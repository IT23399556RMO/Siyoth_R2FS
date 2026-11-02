# Cloudflare R2 Directory Synchronization

A robust, Python-based synchronization service that monitors a local Windows file system and instantly mirrors changes to a Cloudflare R2 cloud bucket. Designed as a highly performant alternative to legacy FUSE mounts, this toolset supports workflows ranging from simple one-way uploads to fully automated, bi-directional synchronization.

## Features

* **Real-Time Local Monitoring:** Utilizes the `watchdog` library to capture file creations, modifications, renames, and deletions instantly on the local disk.
* **Cloudflare R2 Integration:** Connects directly to R2 storage using the standard `boto3` AWS SDK.
* **Conflict Resolution:** Employs active file locking, pending upload sets, and retry loops to prevent infinite upload/download cycles and safely handle large, incomplete file copies.
* **True Mirror Logic:** Accurately maps local folder movements and deletions to the cloud (and vice versa) to prevent orphan files and data duplication.

## Prerequisites

Ensure you have Python installed on your system, then install the required dependencies:

```bash
pip install watchdog boto3 python-dotenv
```

## Configuration

Create a `.env` file in the root directory of the project to securely store your Cloudflare R2 credentials and environment variables:

```env
R2_ACCOUNT_ID=your_account_id_string
R2_ACCESS_KEY_ID=your_access_key_string
R2_SECRET_ACCESS_KEY=your_secret_key_string
R2_BUCKET=filesync
WATCH_DIR=C:\data\incoming
R2_PREFIX=incoming
TOKEN=your_token_string
```
*Note: The script will automatically create the `WATCH_DIR` on your local system if it does not already exist.*

## Available Scripts & Architecture

Execute the scripts via the terminal depending on your specific synchronization requirements (e.g., `python 5sync_bi_all.py`). The service runs continuously in the background until manually stopped (`Ctrl+C`).

### 1. One-Way Synchronization (Local ➔ R2)
* **`1sync.py`:** Detects new files dropped specifically into the root local folder and pushes them to R2.
* **`2sync_sf.py`:** Extends the initial sync with recursive capabilities to monitor and upload nested subdirectories.
* **`3sync_all.py`:** Comprehensive one-way mirror. All local file additions, edits, renames, location changes, and deletions are perfectly mirrored to the cloud. 

### 2. Bi-Directional Synchronization (Local ⮂ R2)
* **`4sync_bi.py`:** Introduces a secondary polling thread to download newly added R2 files down to the PC, while maintaining full local-to-R2 one-way mirroring for edits, renames, and deletions.
* **`5sync_bi_all.py`:** The fully automated two-way bi-directional mirror. Handles complete conflict resolution, tracks cloud deletions/renames to accurately remove or update local PC files, and utilizes file locking to sync additions, edits, moves, and deletions perfectly in both directions.

### 3. Auditing Tools
* **`check_bucket.py`:** An independent auditing tool that queries the Cloudflare R2 API directly to list live bucket contents, confirming successful cloud synchronization without relying on local file system logs.
