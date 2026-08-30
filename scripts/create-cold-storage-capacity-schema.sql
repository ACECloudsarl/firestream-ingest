BEGIN;

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

ALTER TABLE storage_boxes
  ADD COLUMN IF NOT EXISTS "minimumFreeBytes" bigint NOT NULL DEFAULT 100000000000,
  ADD COLUMN IF NOT EXISTS "acceptingUploads" boolean NOT NULL DEFAULT true;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = 'storage_boxes'::regclass
      AND conname = 'storage_boxes_minimum_free_check'
  ) THEN
    ALTER TABLE storage_boxes
      ADD CONSTRAINT storage_boxes_minimum_free_check
      CHECK ("minimumFreeBytes" >= 0)
      NOT VALID;
  END IF;
END
$$;

ALTER TABLE storage_boxes VALIDATE CONSTRAINT storage_boxes_minimum_free_check;

COMMIT;
