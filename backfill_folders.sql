-- Phase 2, Task 02a — Document Folders Backfill
--
-- Populates app.document_folders from public.box_folder_export.
-- Covers all folders including empty ones. Run BEFORE
-- backfill_document_folder_assignments.sql.
--
-- IMPORTANT: Set synchronous_commit = off before calling to avoid WAL pressure.
--
-- From psql:
--   SET synchronous_commit = off;
--   CALL app.backfill_folders();                           -- all accounts
--   CALL app.backfill_folders('account-uuid-here'::uuid); -- single account
--
-- Prerequisites:
--   1. public.box_folder_export must be populated (run box_metadata_export.py first).
--   2. accounts.gcs_root_path must be set for each account.
--
-- To roll back an account:
--   DELETE FROM app.document_folders WHERE account_id = '<uuid>';

CREATE OR REPLACE PROCEDURE app.backfill_folders(
    p_account_id UUID DEFAULT NULL
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_batch_size  CONSTANT INT := 500;
    v_batch_ids   INT[];
    v_last_id     INT := 0;
    v_rows        INT;
    v_total       INT := 0;
BEGIN
    LOOP
        -- Advance the cursor over all box_folder_export rows regardless of
        -- account match, so unmatched rows never stall the loop.
        SELECT array_agg(id ORDER BY id), MAX(id)
        INTO v_batch_ids, v_last_id
        FROM (
            SELECT id
            FROM public.box_folder_export
            WHERE id > v_last_id
            ORDER BY id
            LIMIT v_batch_size
        ) sub;

        EXIT WHEN v_batch_ids IS NULL;

        -- Insert folders that belong to a known account, stripping the first two
        -- path segments ("Clients/AccountName/") to get the relative stored path.
        INSERT INTO app.document_folders (account_id, name, path)
        SELECT
            a.id AS account_id,
            reverse(split_part(reverse(relative_path), '/', 1)) AS name,
            relative_path AS path
        FROM public.box_folder_export bfe
        JOIN app.accounts a
            ON starts_with(bfe.folder_path, a.gcs_root_path)
        CROSS JOIN LATERAL (
            SELECT trim(both '/' from regexp_replace(bfe.folder_path, '^[^/]+/[^/]+/?', '')) AS relative_path
        ) rp
        WHERE bfe.id = ANY(v_batch_ids)
          AND (p_account_id IS NULL OR a.id = p_account_id)
          AND relative_path != ''
        ON CONFLICT (account_id, path) WHERE deleted_at IS NULL DO NOTHING;

        -- Wire parent_id for any folders that are still unlinked.
        UPDATE app.document_folders df
        SET parent_id = parent.id
        FROM app.document_folders parent
        WHERE df.account_id = parent.account_id
          AND parent.path = left(
                df.path,
                length(df.path) - length(reverse(split_part(reverse(df.path), '/', 1))) - 1
              )
          AND df.parent_id IS NULL
          AND df.path LIKE '%/%'
          AND df.deleted_at IS NULL
          AND parent.deleted_at IS NULL
          AND (p_account_id IS NULL OR df.account_id = p_account_id);

        GET DIAGNOSTICS v_rows = ROW_COUNT;
        v_total := v_total + v_rows;
        RAISE NOTICE 'Batch committed: % parent links wired (running total: %)', v_rows, v_total;

        COMMIT;
    END LOOP;

    RAISE NOTICE 'Folder backfill complete: % parent links wired', v_total;
END;
$$;
