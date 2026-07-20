ALTER TABLE repositories ADD COLUMN deleted_at timestamptz;

CREATE INDEX repository_deleted_at ON repositories (deleted_at);
