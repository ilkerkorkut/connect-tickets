# Schema notes

`repositories.normalized_full_name` is the lowercase identity used by the API to make concurrent duplicate creation fail at the database boundary. Repository notes are stored on the repository itself and are therefore independent of GitHub refresh fields.

`batch_jobs` has one row per `(batch_id, repo_id)`. Its status, retry-time, and lease indexes support atomic claims and retry scans; the foreign keys prevent orphan jobs while preserving watched repositories during batch history retention.

`job_attempts` records each attempt once per job and attempt number. `outbox_events.event_id` is unique, so duplicate writes or Kafka publication attempts are harmless; its two state-and-time indexes support the publisher's claim and lease-recovery scans.

Refresh attempt starts are persisted immediately before the GitHub call. A process crash after that durable write may conservatively consume an attempt, while the expired job lease leaves the Kafka record replayable. Invalid refresh payloads are retained in `invalid_refresh_messages` before their offset is committed for later DLQ handling.

`change_sequences` is initialized idempotently with exactly the `repository_changes` singleton. Change records deliberately have no repository foreign key, so a deletion tombstone remains readable after its repository has been removed.
