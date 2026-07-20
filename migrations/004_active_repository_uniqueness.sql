ALTER TABLE repositories DROP CONSTRAINT repositories_normalized_full_name_key;

CREATE UNIQUE INDEX repository_normalized_full_name_active ON repositories (normalized_full_name) WHERE deleted_at IS NULL;
