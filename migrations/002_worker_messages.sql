CREATE TABLE invalid_refresh_messages (
    event_key varchar(128) PRIMARY KEY,
    error_code varchar(64) NOT NULL,
    payload bytea NOT NULL,
    recorded_at timestamptz NOT NULL
);

CREATE INDEX invalidrefreshmessage_recorded_at ON invalid_refresh_messages (recorded_at);
