# Homepage Cold-Storage Integration

The continuous cold archiver is active. Videos with no view in the previous 15
days can first be staged in the storage server's emergency local buffer and
then moved to a Hetzner Storage Box.
The homepage must request restoration before assigning a cold video URL to
Plyr.

## Database fields

Read these fields with the normal video metadata:

- `coldStorage`: `true` when local video files are unavailable.
- `coldStorageStatus`: `staged`, `cold`, `restoring`, `failed`, or `local`.
- `storageId`: identifies the production storage server that must restore it.

Resolve `storageId` against `Server`. The restore API listens on that storage
server's `coldStorageApiPort` (currently `3003`).

## Storage Box overview

The `storage_boxes` table is the dashboard source for each remote box. Safe
fields to expose internally are `id`, `provider`, `plan`, `hostname`,
`capacityBytes`, `usedBytes`, `minimumFreeBytes`, `acceptingUploads`,
`isActive`, and `updatedAt`.

- Remaining bytes: `capacityBytes - usedBytes`.
- `acceptingUploads=false` means the box reached its configured free-space
  reserve; restores and policy purges remain enabled.
- `isActive=false` means the box was administratively disabled entirely.

Do not expose `username`, `sshKeyPath`, `knownHostsPath`, or other connection
credentials through the homepage API.

## Security

Call the restore API from the homepage backend, not directly from browser
JavaScript. The backend server IP must be present as an active `Server` so it is
included in the storage server's allowlist.

If the target storage server has a non-empty `Server.apiKey`, include:

```http
Authorization: Bearer <storage Server.apiKey>
```

## API

### Request or join a restore

```http
POST http://<storage-server>:3003/restore/<videoId>
```

Responses:

- `200`: video is already local and ready.
- `202`: restore is queued or already in progress.
- `404`: video does not belong to that storage server.
- `409`: archive metadata is incomplete; do not initialize Plyr.
- `410`: video was permanently removed by the normal hosting policy.

### Poll progress

```http
GET http://<storage-server>:3003/restore/<videoId>
```

Example:

```json
{
  "videoId": "video-uuid",
  "state": "restoring",
  "ready": false,
  "percent": 42,
  "bytesTransferred": 120000000,
  "totalBytes": 285000000,
  "speedBytesPerSecond": 18400000,
  "speed": "18.4 MB/s",
  "error": null,
  "requestedAt": "2026-08-20T10:00:00.000Z",
  "updatedAt": "2026-08-20T10:00:04.000Z"
}
```

`speedBytesPerSecond` is machine-readable; `speed` is preformatted for the
waiting overlay. Progress is refreshed every five seconds; poll every five
seconds until `ready` is `true`. Multiple visitors requesting
the same video join the same restore operation.

The same API handles both locations. A `staged` video is restored directly
from the server's local emergency buffer; a `cold` video is downloaded from
its Storage Box. The homepage does not need separate logic for them.

## Homepage/Plyr flow

1. Load video metadata without initializing Plyr.
2. If `coldStorage` is false, initialize Plyr normally.
3. If `coldStorage` is true, display a blocking player overlay such as:
   `This video is being prepared. Please wait…`
4. Have the homepage backend call `POST /restore/<videoId>`.
5. Poll through a homepage backend endpoint every five seconds and display the
   returned percentage/bytes.
6. When `ready` becomes true, remove the overlay, assign the HLS/MP4 source,
   initialize or reload Plyr, and optionally autoplay if the visitor already
   interacted with the page.
7. On `failed`, keep the overlay visible and offer a Retry button that sends
   the same idempotent `POST` request again.

Do not give Plyr the HLS playlist or MP4 URL before `ready=true`; doing so can
cache a 404 or put the player into a permanent error state.

## Suggested homepage-backend routes

```text
POST /api/videos/<videoId>/restore
GET  /api/videos/<videoId>/restore-status
```

These routes should look up the video's `storageId`, proxy to the correct
storage server, and return the storage server JSON without exposing internal
credentials or topology to the browser.
