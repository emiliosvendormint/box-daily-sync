-- Phase 2, Task 02b — Document Folder Assignment Backfill
--
-- Assigns folder_id on app.processed_documents by looking up the already-
-- populated app.document_folders. Run AFTER backfill_document_folders.sql.
--
-- IMPORTANT: Set synchronous_commit = off before calling to avoid WAL pressure.
--
-- From psql:
--   SET synchronous_commit = off;
--   CALL app.backfill_documents();                           -- all accounts
--   CALL app.backfill_documents('account-uuid-here'::uuid); -- single account
--
-- Prerequisites:
--   1. public.box_file_export must be populated (run box_metadata_export.py first).
--   2. processed_documents.account_id must be pre-populated for all Box documents.
--   3. app.backfill_document_folders must have been run first.
--
-- To roll back an account:
--   UPDATE app.processed_documents SET folder_id = NULL WHERE account_id = '<uuid>';

CREATE OR REPLACE PROCEDURE app.backfill_documents(
    p_account_id UUID DEFAULT NULL
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_batch_size  CONSTANT INT := 500;
    v_batch_ids   UUID[];
    v_rows        INT;
    v_total       INT := 0;
BEGIN
    -- Exhaustion loop: keep processing until no processed_documents rows remain
    -- with folder_id IS NULL and a non-root box_folder_path.
    -- Root files (relative path = '') correctly keep folder_id = NULL.
    LOOP
        SELECT array_agg(pd.id ORDER BY pd.id)
        INTO v_batch_ids
        FROM (
            SELECT pd.id
            FROM app.processed_documents pd
            JOIN public.box_file_export bfe ON bfe.document_id = pd.document_id
            WHERE pd.folder_id IS NULL
              AND pd.account_id IS NOT NULL
              AND (p_account_id IS NULL OR pd.account_id = p_account_id)
              AND pd.deleted_at IS NULL
              AND trim(both '/' from regexp_replace(bfe.box_folder_path, '^[^/]+/[^/]+/?', '')) != ''
            ORDER BY pd.id
            LIMIT v_batch_size
        ) pd;

        EXIT WHEN v_batch_ids IS NULL;

        -- Folders already exist from backfill_document_folders — just look them up.
        UPDATE app.processed_documents pd
        SET folder_id = df.id
        FROM public.box_file_export bfe
        JOIN app.document_folders df
            ON df.path = trim(both '/' from regexp_replace(bfe.box_folder_path, '^[^/]+/[^/]+/?', ''))
           AND df.deleted_at IS NULL
        WHERE pd.document_id = bfe.document_id
          AND df.account_id = pd.account_id
          AND pd.id = ANY(v_batch_ids);

        GET DIAGNOSTICS v_rows = ROW_COUNT;
        v_total := v_total + v_rows;
        RAISE NOTICE 'Batch committed: % rows updated (running total: %)', v_rows, v_total;

        COMMIT;
    END LOOP;

    RAISE NOTICE 'Document folder assignment complete: % processed_documents updated', v_total;
END;
$$;
