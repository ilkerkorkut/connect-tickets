INSERT INTO change_sequences (name, last_value)
VALUES ('repository_changes', 0)
ON CONFLICT (name) DO NOTHING;
