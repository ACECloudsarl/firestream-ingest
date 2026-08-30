BEGIN;

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

ALTER TABLE "Video" DROP CONSTRAINT IF EXISTS "Video_coldStorageStatus_check";
ALTER TABLE "Video"
  ADD CONSTRAINT "Video_coldStorageStatus_check"
  CHECK ("coldStorageStatus" IN ('local', 'staged', 'archiving', 'cold', 'restoring', 'purging', 'failed'))
  NOT VALID;

CREATE TABLE IF NOT EXISTS cold_storage_staging (
  "videoId" text PRIMARY KEY REFERENCES "Video"(id) ON DELETE CASCADE,
  "storageId" text NOT NULL REFERENCES "Server"(id) ON UPDATE CASCADE ON DELETE CASCADE,
  "sourcePath" text NOT NULL,
  "stagePath" text NOT NULL UNIQUE,
  bytes bigint NOT NULL CHECK (bytes >= 0),
  files integer,
  checksum text,
  status text NOT NULL CHECK (status IN ('copying', 'ready', 'uploading', 'restoring', 'failed')),
  error text,
  "retryAt" timestamp without time zone,
  "createdAt" timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updatedAt" timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "readyAt" timestamp without time zone
);

ALTER TABLE "Video" VALIDATE CONSTRAINT "Video_coldStorageStatus_check";

COMMIT;

CREATE INDEX CONCURRENTLY IF NOT EXISTS cold_storage_staging_status_idx
  ON cold_storage_staging (status, "readyAt", "videoId");

