#!/usr/bin/env python3
"""
Box Daily Sync — incremental Box→GCS sync for new files.

Run daily (e.g. via cron or Cloud Scheduler). Walks the Box folder tree to
discover files added since the last run, uploads them to GCS, creates any new
GCS folder objects, inserts processed_documents rows, and wires folder
assignments.

Steps on each run:
  1. Walk the Box folder tree; upsert new files/folders to staging tables
     (existing rows are untouched via ON CONFLICT DO NOTHING)
  2. Create GCS HNS folder objects for folders where processed=FALSE
  3. Upload files where processed=FALSE to GCS, detect MIME from the downloaded
     bytes, and mark them processed=TRUE (content_type updated in box_file_export)
  4. Insert processed_documents rows for files not already in the table,
     including original_file_type from box_file_export.content_type
  5. Call CALL app.backfill_folders() — creates document_folders rows
  6. Call CALL app.backfill_documents() — assigns folder_id on processed_documents
  7. Propagate document_match_group_id from accounts to new documents
  8. Enqueue PDFs and images (enqueued_at IS NULL, any Box-migrated file): set
     enqueued_at + priority so ProcessQueueService picks them up for Pub/Sub dispatch
  9. Record sync completion timestamp in public.box_sync_state

All steps are idempotent — re-running after a crash picks up exactly where it
left off.

Usage:
    python box_daily_sync.py [--folder-id 311079808665] [--path-prefix Clients]

    Defaults to folder-id=311079808665 and path-prefix=Clients. Override only
    when targeting a different Box root or path layout.

Env vars (same as the other Box/GCS scripts):
    APP_DATABASE_URL
    BOX_CLIENT_ID, BOX_CLIENT_SECRET, BOX_SUBJECT_ID
    GCS_BUCKET_NAME
    GOOGLE_CREDS         base64-encoded service account JSON (optional, falls back to ADC)
    BOX_API_DELAY        seconds between Box API calls (default: 0.2)
    BOX_MAX_RETRIES      max retries on transient errors (default: 4)
    SYNC_BATCH_SIZE      files per upload batch (default: 20)
    SYNC_FOLDER_BATCH    folders per GCS creation batch (default: 100)
    SYNC_COOLDOWN        sleep between batches in seconds (default: 2.0)
"""

import argparse
import asyncio
import base64
import json
import logging
import os
import random
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Generator, Optional

import asyncpg
import filetype
from boxsdk import CCGAuth
from boxsdk.client import Client as BoxClient
from boxsdk.exception import BoxAPIException
from dotenv import load_dotenv
from google.cloud import storage, storage_control_v2
from google.oauth2 import service_account
import google.api_core.exceptions as gcs_exceptions

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
)
logging.getLogger("boxsdk").setLevel(logging.WARNING)
logging.getLogger("asyncpg").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.ERROR)
logger = logging.getLogger("box_daily_sync")

DEFAULT_BATCH_SIZE   = 20
DEFAULT_FOLDER_BATCH = 100
DEFAULT_COOLDOWN     = 2.0
DEFAULT_API_DELAY    = 0.2
DEFAULT_MAX_RETRIES  = 4
BOX_FILE_FIELDS      = ["id", "name", "type", "size", "modified_at", "content_type"]
_BOX_RETRY_STATUSES  = frozenset({429, 500, 502, 503, 504})
_GCS_RETRYABLE       = (
    gcs_exceptions.ServiceUnavailable,
    gcs_exceptions.InternalServerError,
    gcs_exceptions.TooManyRequests,
    gcs_exceptions.DeadlineExceeded,
)

# ── SQL ───────────────────────────────────────────────────────────────────────

_ENSURE_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS public.box_sync_state (
    key        TEXT        PRIMARY KEY,
    value      TEXT        NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS public.box_file_export (
    id              SERIAL      PRIMARY KEY,
    document_id     TEXT        NOT NULL UNIQUE,
    filename        TEXT        NOT NULL,
    box_folder_path TEXT        NOT NULL,
    size_bytes      BIGINT,
    content_type    TEXT,
    modified_at     TIMESTAMPTZ,
    exported_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processed       BOOLEAN     NOT NULL DEFAULT FALSE
);
CREATE INDEX IF NOT EXISTS idx_box_file_export_document_id
    ON public.box_file_export (document_id);
CREATE INDEX IF NOT EXISTS idx_box_file_export_not_uploaded
    ON public.box_file_export (id) WHERE processed = FALSE;
CREATE INDEX IF NOT EXISTS idx_box_file_export_exported_at
    ON public.box_file_export (exported_at);

CREATE TABLE IF NOT EXISTS public.box_folder_export (
    id          SERIAL      PRIMARY KEY,
    folder_id   TEXT        NOT NULL UNIQUE,
    folder_path TEXT        NOT NULL,
    exported_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processed   BOOLEAN     NOT NULL DEFAULT FALSE
);
CREATE INDEX IF NOT EXISTS idx_box_folder_export_not_processed
    ON public.box_folder_export (id) WHERE processed = FALSE;
"""

_UPSERT_FILE_SQL = """
INSERT INTO public.box_file_export
    (document_id, filename, box_folder_path, size_bytes, content_type, modified_at, exported_at)
VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (document_id) DO NOTHING;
"""

_UPSERT_FOLDER_SQL = """
INSERT INTO public.box_folder_export (folder_id, folder_path, exported_at)
VALUES ($1, $2, $3)
ON CONFLICT (folder_id) DO NOTHING;
"""

_FETCH_UNPROCESSED_FILES_SQL = """
SELECT id, document_id, filename, box_folder_path, content_type
FROM public.box_file_export
WHERE processed = FALSE AND id > $2
ORDER BY id
LIMIT $1
"""

_MARK_FILE_UPLOADED_SQL = "UPDATE public.box_file_export SET processed = TRUE, content_type = $2 WHERE id = $1"

_DELETE_BOX_404_SQL = "DELETE FROM public.box_file_export WHERE id = $1"

_GET_MIN_FOLDER_DEPTH_SQL = """
SELECT MIN(char_length(folder_path) - char_length(replace(folder_path, '/', '')))
FROM public.box_folder_export
WHERE processed = FALSE
  AND id != ALL($1::int[])
"""

_FETCH_FOLDERS_AT_DEPTH_SQL = """
SELECT id, folder_path
FROM public.box_folder_export
WHERE processed = FALSE
  AND char_length(folder_path) - char_length(replace(folder_path, '/', '')) = $1
  AND id != ALL($2::int[])
ORDER BY id
LIMIT $3
"""

_MARK_FOLDER_PROCESSED_SQL = "UPDATE public.box_folder_export SET processed = TRUE WHERE id = $1"

# Uses last_sync_completed_at as a lower bound so that files from any previous
# incomplete run are also caught. NOT EXISTS ensures idempotency.
_INSERT_NEW_PROCESSED_DOCUMENTS_SQL = """
INSERT INTO app.processed_documents (
    id,
    document_id,
    customer_original_file_name,
    document_box_hyperlink,
    uploaded_date,
    file_size_bytes,
    source,
    account_id,
    original_file_type
)
SELECT
    gen_random_uuid(),
    bfe.document_id,
    bfe.filename,
    'https://app.box.com/file/' || bfe.document_id,
    bfe.modified_at,
    bfe.size_bytes,
    'GCS',
    a.id,
    bfe.content_type
FROM public.box_file_export bfe
JOIN app.accounts a
    ON starts_with(bfe.box_folder_path || '/', a.gcs_root_path)
WHERE bfe.exported_at >= $1
  AND bfe.processed = TRUE
  AND NOT EXISTS (
      SELECT 1 FROM app.processed_documents pd
      WHERE pd.document_id = bfe.document_id
  )
"""

_PROPAGATE_DMGI_SQL = """
UPDATE app.processed_documents pd
SET document_match_group_id = a.document_match_group_id
FROM app.accounts a
WHERE pd.account_id = a.id
  AND pd.document_match_group_id IS NULL
  AND pd.account_id IS NOT NULL
"""

# Enqueue any Box-migrated document that hasn't been enqueued yet and has a supported
# file type. No date window — upload errors in step 3 are non-fatal so last_sync_ts can
# advance past a file's exported_at before it's uploaded; relying on enqueued_at IS NULL
# is the correct idempotency guard. EXISTS against box_file_export scopes this to the
# Box→GCS pipeline only.
_ENQUEUE_SQL = """
UPDATE app.processed_documents pd
SET
    enqueued_at = NOW(),
    priority    = 2
WHERE pd.enqueued_at IS NULL
  AND pd.deleted_at IS NULL
  AND pd.account_id IS NOT NULL
  AND pd.status = 'New'
  AND (pd.original_file_type = 'application/pdf' OR pd.original_file_type LIKE 'image/%')
  AND EXISTS (
      SELECT 1 FROM public.box_file_export bfe
      WHERE bfe.document_id = pd.document_id
  )
"""

_GET_LAST_SYNC_SQL = "SELECT value FROM public.box_sync_state WHERE key = 'last_sync_completed_at'"

_UPSERT_SYNC_STATE_SQL = """
INSERT INTO public.box_sync_state (key, value, updated_at)
VALUES ($1, $2::text, NOW())
ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()
"""


# ── Box helpers ───────────────────────────────────────────────────────────────

@dataclass
class _FileRecord:
    document_id: str
    filename: str
    box_folder_path: str
    size_bytes: Optional[int]
    content_type: Optional[str]
    modified_at: Optional[datetime]


@dataclass
class _FolderRecord:
    folder_id: str
    folder_path: str


def _parse_dt(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None


class _BoxWalker:
    def __init__(self, client_id: str, client_secret: str, subject_id: str,
                 api_delay: float, max_retries: int) -> None:
        self._api_delay = api_delay
        self._max_retries = max_retries
        self._last_call: float = 0.0
        auth = CCGAuth(client_id=client_id, client_secret=client_secret,
                       enterprise_id=subject_id)
        self._client = BoxClient(auth)
        logger.info("Box client authenticated (CCG)")

    def _throttle(self) -> None:
        elapsed = time.monotonic() - self._last_call
        if elapsed < self._api_delay:
            time.sleep(self._api_delay - elapsed)
        self._last_call = time.monotonic()

    def _fetch_page(self, folder_id: str, offset: int, page_size: int) -> list:
        folder = self._client.folder(folder_id)
        for attempt in range(self._max_retries + 1):
            try:
                items = []
                for item in folder.get_items(limit=page_size, offset=offset,
                                             fields=BOX_FILE_FIELDS):
                    items.append(item)
                    if len(items) >= page_size:
                        break
                return items
            except BoxAPIException as exc:
                if attempt >= self._max_retries or exc.status not in _BOX_RETRY_STATUSES:
                    raise
                wait = (float((exc.headers or {}).get("Retry-After", 2 ** attempt))
                        if exc.status == 429 else 2 ** attempt + random.random())
                logger.warning("Box API %s on folder_id=%s offset=%d — retry %d/%d in %.1fs",
                               exc.status, folder_id, offset, attempt + 1, self._max_retries, wait)
                time.sleep(wait)
        return []

    def walk(self, folder_id: str, relative_path: str = "") -> Generator[tuple, None, None]:
        _PAGE_SIZE = 1000
        offset = 0
        while True:
            self._throttle()
            try:
                page = self._fetch_page(folder_id, offset, _PAGE_SIZE)
            except Exception as exc:
                logger.warning("Skipped folder_id=%s path=%r: %s", folder_id, relative_path, exc)
                break
            for item in page:
                if item.type == "file":
                    yield item, relative_path
                elif item.type == "folder":
                    child_path = f"{relative_path}/{item.name}" if relative_path else item.name
                    yield item, child_path
                    yield from self.walk(item.id, child_path)
            if len(page) < _PAGE_SIZE:
                break
            offset += _PAGE_SIZE


# ── GCS helpers ───────────────────────────────────────────────────────────────

def _build_gcs_credentials() -> Optional[service_account.Credentials]:
    raw = os.environ.get("GOOGLE_CREDS")
    if not raw:
        return None
    data = json.loads(base64.b64decode(raw).decode())
    return service_account.Credentials.from_service_account_info(
        data, scopes=["https://www.googleapis.com/auth/cloud-platform"])


def _build_gcs_client() -> storage.Client:
    creds = _build_gcs_credentials()
    return storage.Client(credentials=creds) if creds else storage.Client()


def _build_storage_control_client() -> storage_control_v2.StorageControlClient:
    creds = _build_gcs_credentials()
    return (storage_control_v2.StorageControlClient(credentials=creds)
            if creds else storage_control_v2.StorageControlClient())


class _Uploader:
    def __init__(self, box_client: BoxClient, bucket: storage.Bucket,
                 api_delay: float, max_retries: int) -> None:
        self._box = box_client
        self._bucket = bucket
        self._api_delay = api_delay
        self._max_retries = max_retries
        self._last_call: float = 0.0

    def _throttle(self) -> None:
        elapsed = time.monotonic() - self._last_call
        if elapsed < self._api_delay:
            time.sleep(self._api_delay - elapsed)
        self._last_call = time.monotonic()

    def _download(self, document_id: str, dest) -> None:
        for attempt in range(self._max_retries + 1):
            self._throttle()
            try:
                dest.seek(0)
                dest.truncate()
                self._box.file(document_id).download_to(dest)
                return
            except BoxAPIException as exc:
                if attempt >= self._max_retries or exc.status not in _BOX_RETRY_STATUSES:
                    raise
                wait = (float((exc.headers or {}).get("Retry-After", 2 ** attempt))
                        if exc.status == 429 else 2 ** attempt + random.random())
                logger.warning("Box %s on file_id=%s — retry %d/%d in %.1fs",
                               exc.status, document_id, attempt + 1, self._max_retries, wait)
                time.sleep(wait)

    def _upload(self, key: str, src, content_type: Optional[str]) -> None:
        ct = content_type or "application/octet-stream"
        for attempt in range(self._max_retries + 1):
            try:
                src.seek(0)
                self._bucket.blob(key).upload_from_file(src, content_type=ct)
                return
            except _GCS_RETRYABLE as exc:
                if attempt >= self._max_retries:
                    raise
                wait = 2 ** attempt + random.random()
                logger.warning("GCS transient error on %s — retry %d/%d in %.1fs: %s",
                               key, attempt + 1, self._max_retries, wait, exc)
                time.sleep(wait)

    def upload_one(self, row: asyncpg.Record) -> tuple[int, str]:
        key = f"{row['box_folder_path']}/{row['filename']}" if row["box_folder_path"] else row["filename"]
        with tempfile.TemporaryFile() as tmp:
            self._download(row["document_id"], tmp)
            tmp.seek(0)
            kind = filetype.guess(tmp.read(512))
            mime = kind.mime if kind else "application/octet-stream"
            self._upload(key, tmp, mime)
        logger.info("  [%d] %s → gs://%s/%s", row["id"], row["document_id"],
                    self._bucket.name, key)
        return row["id"], mime


def _create_gcs_folder(control: storage_control_v2.StorageControlClient,
                        bucket_name: str, row: asyncpg.Record,
                        max_retries: int = 3) -> None:
    folder_id = row["folder_path"].rstrip("/") + "/"
    for attempt in range(max_retries + 1):
        try:
            control.create_folder(request=storage_control_v2.CreateFolderRequest(
                parent=f"projects/_/buckets/{bucket_name}",
                folder_id=folder_id,
            ))
            logger.info("  [%d] gs://%s/%s", row["id"], bucket_name, folder_id)
            return
        except gcs_exceptions.AlreadyExists:
            return
        except _GCS_RETRYABLE as exc:
            if attempt >= max_retries:
                raise
            wait = 2 ** attempt
            logger.warning("GCS transient error on folder %s — retry %d/%d in %.1fs: %s",
                           folder_id, attempt + 1, max_retries, wait, exc)
            time.sleep(wait)


# ── sync steps ────────────────────────────────────────────────────────────────

async def _step1_export_metadata(
    conn: asyncpg.Connection,
    walker: _BoxWalker,
    folder_id: str,
    path_prefix: str,
    batch_size: int,
    sync_ts: datetime,
) -> tuple[int, int]:
    logger.info("Step 1 — Walking Box folder_id=%s (prefix=%r)", folder_id, path_prefix)
    file_batch: list[_FileRecord] = []
    folder_batch: list[_FolderRecord] = []
    total_files = total_folders = errors = 0

    for item, path in walker.walk(folder_id, path_prefix):
        try:
            if item.type == "folder":
                folder_batch.append(_FolderRecord(folder_id=str(item.id), folder_path=path))
                if len(folder_batch) >= batch_size:
                    await conn.executemany(_UPSERT_FOLDER_SQL,
                                           [(r.folder_id, r.folder_path, sync_ts) for r in folder_batch])
                    total_folders += len(folder_batch)
                    folder_batch.clear()
            else:
                file_batch.append(_FileRecord(
                    document_id=str(item.id),
                    filename=item.name,
                    box_folder_path=path,
                    size_bytes=getattr(item, "size", None),
                    content_type=getattr(item, "content_type", None),
                    modified_at=_parse_dt(getattr(item, "modified_at", None)),
                ))
                if len(file_batch) >= batch_size:
                    await conn.executemany(
                        _UPSERT_FILE_SQL,
                        [(r.document_id, r.filename, r.box_folder_path,
                          r.size_bytes, r.content_type, r.modified_at, sync_ts)
                         for r in file_batch],
                    )
                    total_files += len(file_batch)
                    logger.info("  flushed %d files (running total: %d)", len(file_batch), total_files)
                    file_batch.clear()
        except Exception as exc:
            errors += 1
            logger.warning("Skipping item: %s", exc)

    if file_batch:
        await conn.executemany(
            _UPSERT_FILE_SQL,
            [(r.document_id, r.filename, r.box_folder_path,
              r.size_bytes, r.content_type, r.modified_at, sync_ts)
             for r in file_batch],
        )
        total_files += len(file_batch)
    if folder_batch:
        await conn.executemany(_UPSERT_FOLDER_SQL,
                               [(r.folder_id, r.folder_path, sync_ts) for r in folder_batch])
        total_folders += len(folder_batch)

    logger.info("Step 1 done — files seen: %d | folders seen: %d | errors skipped: %d",
                total_files, total_folders, errors)
    return total_files, total_folders


async def _step3_upload_files(
    conn: asyncpg.Connection,
    uploader: _Uploader,
    batch_size: int,
    cooldown: float,
) -> tuple[int, int]:
    logger.info("Step 3 — Uploading new files to GCS")
    total_uploaded = total_errors = 0
    last_id = 0
    loop = asyncio.get_event_loop()

    pending = await conn.fetchval(
        "SELECT COUNT(*) FROM public.box_file_export WHERE processed = FALSE")
    logger.info("  Files pending upload: %d", pending)

    while True:
        batch = await conn.fetch(_FETCH_UNPROCESSED_FILES_SQL, batch_size, last_id)
        if not batch:
            break

        with ThreadPoolExecutor(max_workers=len(batch)) as executor:
            results = await asyncio.gather(
                *[loop.run_in_executor(executor, uploader.upload_one, row) for row in batch],
                return_exceptions=True,
            )

        last_id = max(row["id"] for row in batch)

        for row, result in zip(batch, results):
            if isinstance(result, BoxAPIException) and result.status == 404:
                await conn.execute(_DELETE_BOX_404_SQL, row["id"])
                total_errors += 1
                logger.warning("NOT FOUND [%d] %s — no longer exists in Box, removed from box_file_export",
                               row["id"], row["document_id"])
            elif isinstance(result, Exception):
                total_errors += 1
                logger.error("SKIP [%d] %s — %s: %s", row["id"], row["document_id"],
                             type(result).__name__, result)
            else:
                row_id, mime = result
                await conn.execute(_MARK_FILE_UPLOADED_SQL, row_id, mime)
                total_uploaded += 1

        logger.info("  uploaded: %d | errors: %d | cooling down %.1fs",
                    total_uploaded, total_errors, cooldown)
        await asyncio.sleep(cooldown)

    logger.info("Step 3 done — uploaded: %d | errors: %d", total_uploaded, total_errors)
    return total_uploaded, total_errors


async def _step2_create_gcs_folders(
    conn: asyncpg.Connection,
    control: storage_control_v2.StorageControlClient,
    bucket_name: str,
    batch_size: int,
    cooldown: float,
) -> tuple[int, int]:
    logger.info("Step 2 — Creating GCS folder objects (one depth level at a time)")
    total_created = total_errors = 0
    failed_ids: list[int] = []  # excluded this run; retried on next run (stay processed=FALSE)
    loop = asyncio.get_event_loop()

    pending = await conn.fetchval(
        "SELECT COUNT(*) FROM public.box_folder_export WHERE processed = FALSE")
    logger.info("  Folders pending creation: %d", pending)

    while True:
        current_depth = await conn.fetchval(_GET_MIN_FOLDER_DEPTH_SQL, failed_ids)
        if current_depth is None:
            break

        logger.info("  Depth level %d", current_depth)

        while True:
            batch = await conn.fetch(_FETCH_FOLDERS_AT_DEPTH_SQL, current_depth, failed_ids, batch_size)
            if not batch:
                break

            with ThreadPoolExecutor(max_workers=len(batch)) as executor:
                results = await asyncio.gather(
                    *[loop.run_in_executor(executor, _create_gcs_folder, control, bucket_name, row)
                      for row in batch],
                    return_exceptions=True,
                )

            for row, result in zip(batch, results):
                if isinstance(result, Exception):
                    total_errors += 1
                    failed_ids.append(row["id"])
                    logger.error("SKIP [%d] %s — %s: %s", row["id"], row["folder_path"],
                                 type(result).__name__, result)
                else:
                    await conn.execute(_MARK_FOLDER_PROCESSED_SQL, row["id"])
                    total_created += 1

            logger.info("  created: %d | errors: %d | cooling down %.1fs",
                        total_created, total_errors, cooldown)
            await asyncio.sleep(cooldown)

    logger.info("Step 2 done — created: %d | errors: %d", total_created, total_errors)
    return total_created, total_errors


async def _step4_insert_processed_documents(
    conn: asyncpg.Connection,
    since_ts: datetime,
) -> int:
    logger.info("Step 4 — Inserting processed_documents for new files (since %s)", since_ts.isoformat())
    result = await conn.execute(_INSERT_NEW_PROCESSED_DOCUMENTS_SQL, since_ts)
    count = int(result.split()[-1])
    logger.info("Step 4 done — inserted: %d rows", count)
    return count


async def _step5_backfill_folders(conn: asyncpg.Connection) -> None:
    logger.info("Step 5 — CALL app.backfill_folders()")
    await conn.execute("CALL app.backfill_folders()")
    logger.info("Step 5 done")


async def _step6_backfill_document_assignments(conn: asyncpg.Connection) -> None:
    logger.info("Step 6 — CALL app.backfill_documents()")
    await conn.execute("CALL app.backfill_documents()")
    logger.info("Step 6 done")


async def _step9_enqueue_documents(conn: asyncpg.Connection) -> int:
    logger.info("Step 9 — Enqueueing PDFs and images with enqueued_at IS NULL")
    count = int((await conn.execute(_ENQUEUE_SQL)).split()[-1])
    logger.info("Step 9 done — enqueued: %d", count)
    return count


async def _step7_propagate_document_match_group_id(conn: asyncpg.Connection) -> int:
    logger.info("Step 7 — Propagating document_match_group_id")
    result = await conn.execute(_PROPAGATE_DMGI_SQL)
    count = int(result.split()[-1])
    logger.info("Step 7 done — updated: %d rows", count)
    return count


# ── orchestrator ──────────────────────────────────────────────────────────────

async def _run(
    folder_id: str,
    path_prefix: str,
    batch_size: int,
    folder_batch: int,
    cooldown: float,
) -> None:
    db_url      = os.environ["APP_DATABASE_URL"].replace("postgresql+asyncpg://", "postgresql://")
    bucket_name = os.environ["GCS_BUCKET_NAME"]
    api_delay   = float(os.environ.get("BOX_API_DELAY", DEFAULT_API_DELAY))
    max_retries = int(os.environ.get("BOX_MAX_RETRIES", DEFAULT_MAX_RETRIES))

    gcs_client = _build_gcs_client()
    bucket     = gcs_client.bucket(bucket_name)
    control    = _build_storage_control_client()
    box_client = BoxClient(CCGAuth(
        client_id=os.environ["BOX_CLIENT_ID"],
        client_secret=os.environ["BOX_CLIENT_SECRET"],
        enterprise_id=os.environ["BOX_SUBJECT_ID"],
    ))

    walker   = _BoxWalker(os.environ["BOX_CLIENT_ID"], os.environ["BOX_CLIENT_SECRET"],
                          os.environ["BOX_SUBJECT_ID"], api_delay, max_retries)
    uploader = _Uploader(box_client, bucket, api_delay, max_retries)

    conn: asyncpg.Connection = await asyncpg.connect(db_url)
    try:
        await conn.execute(_ENSURE_TABLES_SQL)

        last_sync_row = await conn.fetchrow(_GET_LAST_SYNC_SQL)
        if last_sync_row:
            last_sync_ts = datetime.fromisoformat(last_sync_row["value"])
        else:
            # No completed sync on record — seed from the latest exported_at in
            # box_file_export so we treat the initial migration as the baseline
            # and only pick up files added after it.
            last_sync_ts = await conn.fetchval(
                "SELECT MAX(exported_at) FROM public.box_file_export"
            ) or datetime(1970, 1, 1, tzinfo=timezone.utc)
            logger.info("No prior sync found — seeding last_sync_ts from box_file_export MAX(exported_at)")
        logger.info("Last successful sync: %s", last_sync_ts.isoformat())

        sync_ts = datetime.now(timezone.utc)
        logger.info("Sync started at: %s", sync_ts.isoformat())

        await _step1_export_metadata(conn, walker, folder_id, path_prefix, batch_size, sync_ts)
        await _step2_create_gcs_folders(conn, control, bucket_name, folder_batch, cooldown)
        await _step3_upload_files(conn, uploader, batch_size, cooldown)
        await _step4_insert_processed_documents(conn, last_sync_ts)
        await _step5_backfill_folders(conn)
        await _step6_backfill_document_assignments(conn)
        await _step7_propagate_document_match_group_id(conn)
        await _step9_enqueue_documents(conn)

        await conn.execute(_UPSERT_SYNC_STATE_SQL,
                           "last_sync_completed_at", sync_ts.isoformat())
        logger.info("Sync complete. Completion timestamp recorded: %s", sync_ts.isoformat())

    finally:
        await conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Daily incremental Box→GCS sync for new files."
    )
    parser.add_argument(
        "--folder-id", default="311079808665", metavar="BOX_FOLDER_ID",
        help="Box folder ID to walk (default: 311079808665)",
    )
    parser.add_argument(
        "--path-prefix", default="Clients", metavar="PREFIX",
        help='Path prefix prepended to every stored path (default: "Clients")',
    )
    parser.add_argument(
        "--batch-size", type=int,
        default=int(os.environ.get("SYNC_BATCH_SIZE", DEFAULT_BATCH_SIZE)),
        metavar="N", help=f"Files per parallel upload batch (default: {DEFAULT_BATCH_SIZE})",
    )
    parser.add_argument(
        "--folder-batch", type=int,
        default=int(os.environ.get("SYNC_FOLDER_BATCH", DEFAULT_FOLDER_BATCH)),
        metavar="N", help=f"Folders per GCS creation batch (default: {DEFAULT_FOLDER_BATCH})",
    )
    parser.add_argument(
        "--cooldown", type=float,
        default=float(os.environ.get("SYNC_COOLDOWN", DEFAULT_COOLDOWN)),
        metavar="SECS", help=f"Sleep between batches in seconds (default: {DEFAULT_COOLDOWN})",
    )
    args = parser.parse_args()
    asyncio.run(_run(args.folder_id, args.path_prefix, args.batch_size,
                     args.folder_batch, args.cooldown))


if __name__ == "__main__":
    main()
