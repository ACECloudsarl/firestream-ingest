BEGIN;

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

-- Additive-only migration: existing Storage Box and Video values are untouched.
ALTER TABLE storage_boxes
  ADD COLUMN IF NOT EXISTS "ftpPort" integer NOT NULL DEFAULT 21,
  ADD COLUMN IF NOT EXISTS "ftpPassword" text;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = 'storage_boxes'::regclass
      AND conname = 'storage_boxes_ftp_port_check'
  ) THEN
    ALTER TABLE storage_boxes
      ADD CONSTRAINT storage_boxes_ftp_port_check
      CHECK ("ftpPort" BETWEEN 1 AND 65535)
      NOT VALID;
  END IF;
END
$$;

ALTER TABLE storage_boxes VALIDATE CONSTRAINT storage_boxes_ftp_port_check;

COMMIT;
