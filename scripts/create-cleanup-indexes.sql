-- Run outside a transaction. CONCURRENTLY avoids blocking production writes.
CREATE INDEX CONCURRENTLY IF NOT EXISTS "Video_cleanup_storage_status_id_idx"
ON "Video" ("storageId", status, id)
WHERE status IN ('active', 'offline', 'removed', 'hard_deleted');

CREATE INDEX CONCURRENTLY IF NOT EXISTS "Video_encodedPath_idx"
ON "Video" ("encodedPath")
WHERE "encodedPath" IS NOT NULL;
