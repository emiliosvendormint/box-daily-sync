# box-daily-sync

Incremental Box → GCS sync. Runs daily via Cloud Run Job triggered by Google Cloud Scheduler.

Walks the Box folder tree to discover new files, uploads them to GCS, and registers them in `processed_documents` so the DMS3 processing pipeline picks them up.

## How it works

Each run executes 9 idempotent steps. Re-running after a crash picks up exactly where it left off.

| Step | What it does |
|------|-------------|
| 1 | Walk the Box folder tree; upsert new files/folders into `box_file_export` / `box_folder_export` staging tables (`ON CONFLICT DO NOTHING`) |
| 2 | Create GCS HNS folder objects for any folder not yet created, depth-first |
| 3 | Download each unprocessed file from Box, detect MIME type from the bytes, upload to GCS, update `box_file_export.content_type` and mark `processed = TRUE`. Files that return 404 from Box are deleted from `box_file_export`. |
| 4 | Insert `processed_documents` rows for newly uploaded files whose path matches an account's `gcs_root_path`. Sets `original_file_type` from the detected MIME. |
| 5 | `CALL app.backfill_folders()` — creates `document_folders` rows for new GCS folders |
| 6 | `CALL app.backfill_documents()` — assigns `folder_id` on `processed_documents` |
| 7 | Propagate `document_match_group_id` from `accounts` to new documents |
| 8 | Enqueue PDFs and images (`status = 'New'`, `enqueued_at IS NULL`) so `ProcessQueueService` dispatches them via Pub/Sub |
| 9 | Record `last_sync_completed_at` in `public.sync_state` |

Only files whose `box_folder_path` matches an account's `gcs_root_path` are inserted into `processed_documents`. All files are uploaded to GCS regardless.

## Requirements

```
Python 3.11+
```

Install dependencies:

```bash
pip install -r requirements.txt
```

## Environment variables

Copy `.env.example` to `.env` and fill in the values.

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `APP_DATABASE_URL` | ✅ | — | PostgreSQL connection string |
| `BOX_CLIENT_ID` | ✅ | — | Box OAuth client ID (CCG) |
| `BOX_CLIENT_SECRET` | ✅ | — | Box OAuth client secret |
| `BOX_SUBJECT_ID` | ✅ | — | Box enterprise ID |
| `GCS_BUCKET_NAME` | ✅ | — | GCS bucket name |
| `GOOGLE_CREDS` | — | ADC | Base64-encoded service account JSON |
| `BOX_API_DELAY` | — | `0.2` | Seconds between Box API calls |
| `BOX_MAX_RETRIES` | — | `4` | Max retries on transient errors |
| `SYNC_BATCH_SIZE` | — | `20` | Files per parallel upload batch |
| `SYNC_FOLDER_BATCH` | — | `100` | Folders per GCS creation batch |
| `SYNC_COOLDOWN` | — | `2.0` | Sleep between batches (seconds) |

## Usage

```bash
python box_daily_sync.py [--folder-id BOX_FOLDER_ID] [--path-prefix PREFIX]
```

| Argument | Default | Description |
|----------|---------|-------------|
| `--folder-id` | `311079808665` | Box root folder ID to walk |
| `--path-prefix` | `Clients` | Path prefix prepended to every stored GCS path |
| `--batch-size` | `20` | Files per upload batch (overrides `SYNC_BATCH_SIZE`) |
| `--folder-batch` | `100` | Folders per batch (overrides `SYNC_FOLDER_BATCH`) |
| `--cooldown` | `2.0` | Seconds between batches (overrides `SYNC_COOLDOWN`) |

## Deployment

This script is intended to run as a **Cloud Run Job** triggered by **Google Cloud Scheduler** on a daily schedule. Cloud Run Jobs support up to 24-hour execution time, which avoids the 1-hour HTTP timeout limit of Cloud Run Services.

### Staging tables

The script auto-creates the staging tables on first run if they don't exist:

- `public.box_file_export` — one row per Box file discovered
- `public.box_folder_export` — one row per Box folder discovered
- `public.sync_state` — stores `last_sync_completed_at` as the cursor for the next run
