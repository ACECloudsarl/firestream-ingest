BEGIN;

-- Keep this migration transactional and fail quickly instead of blocking video
-- traffic if another process is holding a conflicting table lock.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

CREATE TABLE IF NOT EXISTS storage_boxes (
  id text PRIMARY KEY,
  provider text NOT NULL DEFAULT 'hetzner',
  plan text NOT NULL,
  hostname text NOT NULL UNIQUE,
  username text NOT NULL,
  "sshPort" integer NOT NULL DEFAULT 23,
  "sshKeyPath" text NOT NULL,
  "knownHostsPath" text NOT NULL DEFAULT '/root/.ssh/known_hosts',
  location text,
  "basePath" text NOT NULL DEFAULT 'firestream/cold-videos',
  "capacityBytes" bigint NOT NULL,
  "usedBytes" bigint NOT NULL DEFAULT 0,
  "maxConnections" integer NOT NULL DEFAULT 10,
  priority integer NOT NULL DEFAULT 100,
  "inactivityDays" integer NOT NULL DEFAULT 15,
  "maxUsagePercent" numeric(5,2) NOT NULL DEFAULT 99,
  "archiveConcurrency" integer NOT NULL DEFAULT 6,
  "restoreConcurrency" integer NOT NULL DEFAULT 2,
  "scanIntervalSeconds" integer NOT NULL DEFAULT 60,
  "isActive" boolean NOT NULL DEFAULT true,
  "createdAt" timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updatedAt" timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT storage_boxes_ssh_port_check CHECK ("sshPort" BETWEEN 1 AND 65535),
  CONSTRAINT storage_boxes_capacity_check CHECK ("capacityBytes" > 0),
  CONSTRAINT storage_boxes_used_bytes_check CHECK ("usedBytes" >= 0),
  CONSTRAINT storage_boxes_max_connections_check CHECK ("maxConnections" > 0),
  CONSTRAINT storage_boxes_priority_check CHECK (priority >= 0),
  CONSTRAINT storage_boxes_inactivity_days_check CHECK ("inactivityDays" > 0),
  CONSTRAINT storage_boxes_max_usage_check CHECK ("maxUsagePercent" > 0 AND "maxUsagePercent" < 100),
  CONSTRAINT storage_boxes_concurrency_check CHECK (
    "archiveConcurrency" > 0 AND "restoreConcurrency" > 0
    AND "archiveConcurrency" + "restoreConcurrency" <= "maxConnections"
  ),
  CONSTRAINT storage_boxes_scan_interval_check CHECK ("scanIntervalSeconds" > 0)
);

-- Keep upgrades idempotent for databases where storage_boxes was created by an
-- earlier version of this migration.
ALTER TABLE storage_boxes
  ADD COLUMN IF NOT EXISTS "sshKeyPath" text,
  ADD COLUMN IF NOT EXISTS "knownHostsPath" text NOT NULL DEFAULT '/root/.ssh/known_hosts',
  ADD COLUMN IF NOT EXISTS priority integer NOT NULL DEFAULT 100,
  ADD COLUMN IF NOT EXISTS "inactivityDays" integer NOT NULL DEFAULT 15,
  ADD COLUMN IF NOT EXISTS "maxUsagePercent" numeric(5,2) NOT NULL DEFAULT 99,
  ADD COLUMN IF NOT EXISTS "archiveConcurrency" integer NOT NULL DEFAULT 6,
  ADD COLUMN IF NOT EXISTS "restoreConcurrency" integer NOT NULL DEFAULT 2,
  ADD COLUMN IF NOT EXISTS "scanIntervalSeconds" integer NOT NULL DEFAULT 60;

-- PostgreSQL adds constant defaults without rewriting existing rows. The
-- remaining columns are nullable, so this does not modify existing video data.
ALTER TABLE "Video"
  ADD COLUMN IF NOT EXISTS "coldStorage" boolean NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS "coldStorageId" text,
  ADD COLUMN IF NOT EXISTS "coldStorageStatus" text NOT NULL DEFAULT 'local',
  ADD COLUMN IF NOT EXISTS "coldStoragePath" text,
  ADD COLUMN IF NOT EXISTS "coldStorageChecksum" text,
  ADD COLUMN IF NOT EXISTS "coldStorageBytes" bigint,
  ADD COLUMN IF NOT EXISTS "archivedAt" timestamp without time zone,
  ADD COLUMN IF NOT EXISTS "restoreRequestedAt" timestamp without time zone,
  ADD COLUMN IF NOT EXISTS "coldStorageUpdatedAt" timestamp without time zone,
  ADD COLUMN IF NOT EXISTS "coldStorageRetryAt" timestamp without time zone,
  ADD COLUMN IF NOT EXISTS "coldStorageError" text;

ALTER TABLE "Server"
  ADD COLUMN IF NOT EXISTS "coldStorageApiHost" text NOT NULL DEFAULT '0.0.0.0',
  ADD COLUMN IF NOT EXISTS "coldStorageApiPort" integer NOT NULL DEFAULT 3003;

ALTER TABLE "Video" DROP CONSTRAINT IF EXISTS "Video_coldStorageStatus_check";
ALTER TABLE "Video"
  ADD CONSTRAINT "Video_coldStorageStatus_check"
  CHECK ("coldStorageStatus" IN ('local', 'archiving', 'cold', 'restoring', 'purging', 'failed'))
  NOT VALID;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid = 'storage_boxes'::regclass AND conname = 'storage_boxes_priority_check') THEN
    ALTER TABLE storage_boxes ADD CONSTRAINT storage_boxes_priority_check CHECK (priority >= 0) NOT VALID;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid = 'storage_boxes'::regclass AND conname = 'storage_boxes_inactivity_days_check') THEN
    ALTER TABLE storage_boxes ADD CONSTRAINT storage_boxes_inactivity_days_check CHECK ("inactivityDays" > 0) NOT VALID;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid = 'storage_boxes'::regclass AND conname = 'storage_boxes_max_usage_check') THEN
    ALTER TABLE storage_boxes ADD CONSTRAINT storage_boxes_max_usage_check CHECK ("maxUsagePercent" > 0 AND "maxUsagePercent" < 100) NOT VALID;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid = 'storage_boxes'::regclass AND conname = 'storage_boxes_concurrency_check') THEN
    ALTER TABLE storage_boxes ADD CONSTRAINT storage_boxes_concurrency_check CHECK (
      "archiveConcurrency" > 0 AND "restoreConcurrency" > 0
      AND "archiveConcurrency" + "restoreConcurrency" <= "maxConnections"
    ) NOT VALID;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid = 'storage_boxes'::regclass AND conname = 'storage_boxes_scan_interval_check') THEN
    ALTER TABLE storage_boxes ADD CONSTRAINT storage_boxes_scan_interval_check CHECK ("scanIntervalSeconds" > 0) NOT VALID;
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = '"Server"'::regclass
      AND conname = 'Server_coldStorageApiPort_check'
  ) THEN
    ALTER TABLE "Server"
      ADD CONSTRAINT "Server_coldStorageApiPort_check"
      CHECK ("coldStorageApiPort" BETWEEN 1 AND 65535)
      NOT VALID;
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = '"Video"'::regclass
      AND conname = 'Video_coldStorageId_fkey'
  ) THEN
    ALTER TABLE "Video"
      ADD CONSTRAINT "Video_coldStorageId_fkey"
      FOREIGN KEY ("coldStorageId") REFERENCES storage_boxes(id)
      ON UPDATE CASCADE ON DELETE SET NULL
      NOT VALID;
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = '"Video"'::regclass
      AND conname = 'Video_coldStorageBytes_check'
  ) THEN
    ALTER TABLE "Video"
      ADD CONSTRAINT "Video_coldStorageBytes_check"
      CHECK ("coldStorageBytes" IS NULL OR "coldStorageBytes" >= 0)
      NOT VALID;
  END IF;
END
$$;

ALTER TABLE "Video" VALIDATE CONSTRAINT "Video_coldStorageId_fkey";
ALTER TABLE "Video" VALIDATE CONSTRAINT "Video_coldStorageStatus_check";
ALTER TABLE "Video" VALIDATE CONSTRAINT "Video_coldStorageBytes_check";
ALTER TABLE "Server" VALIDATE CONSTRAINT "Server_coldStorageApiPort_check";

INSERT INTO storage_boxes (
  id, provider, plan, hostname, username, "sshPort", "sshKeyPath",
  "knownHostsPath", "basePath", "capacityBytes",
  "maxConnections", priority, "inactivityDays", "maxUsagePercent",
  "archiveConcurrency", "restoreConcurrency", "scanIntervalSeconds", "isActive"
) VALUES (
  'hetzner-bx21-u654289',
  'hetzner',
  'BX21',
  'u654289.your-storagebox.de',
  'u654289',
  23,
  '/root/.ssh/firestream-storagebox-u654289',
  '/root/.ssh/known_hosts',
  'firestream/cold-videos',
  5000000000000,
  10,
  100,
  15,
  99,
  6,
  2,
  60,
  true
)
ON CONFLICT (id) DO UPDATE SET
  provider = EXCLUDED.provider,
  plan = EXCLUDED.plan,
  hostname = EXCLUDED.hostname,
  username = EXCLUDED.username,
  "sshPort" = EXCLUDED."sshPort",
  "sshKeyPath" = EXCLUDED."sshKeyPath",
  "knownHostsPath" = EXCLUDED."knownHostsPath",
  "basePath" = EXCLUDED."basePath",
  "maxConnections" = EXCLUDED."maxConnections",
  priority = EXCLUDED.priority,
  "inactivityDays" = EXCLUDED."inactivityDays",
  "maxUsagePercent" = EXCLUDED."maxUsagePercent",
  "archiveConcurrency" = EXCLUDED."archiveConcurrency",
  "restoreConcurrency" = EXCLUDED."restoreConcurrency",
  "scanIntervalSeconds" = EXCLUDED."scanIntervalSeconds",
  "isActive" = EXCLUDED."isActive",
  "updatedAt" = CURRENT_TIMESTAMP;

ALTER TABLE storage_boxes ALTER COLUMN "sshKeyPath" SET NOT NULL;
ALTER TABLE storage_boxes VALIDATE CONSTRAINT storage_boxes_priority_check;
ALTER TABLE storage_boxes VALIDATE CONSTRAINT storage_boxes_inactivity_days_check;
ALTER TABLE storage_boxes VALIDATE CONSTRAINT storage_boxes_max_usage_check;
ALTER TABLE storage_boxes VALIDATE CONSTRAINT storage_boxes_concurrency_check;
ALTER TABLE storage_boxes VALIDATE CONSTRAINT storage_boxes_scan_interval_check;

COMMIT;

-- Partial indexes stay small: the 69k+ local videos do not get index entries.
CREATE INDEX CONCURRENTLY IF NOT EXISTS "Video_coldStorageId_idx"
  ON "Video" ("coldStorageId")
  WHERE "coldStorageId" IS NOT NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS "Video_coldStorageStatus_idx"
  ON "Video" ("coldStorageStatus")
  WHERE "coldStorageStatus" <> 'local';
