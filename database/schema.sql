CREATE TABLE IF NOT EXISTS experiments (
    experiment_id TEXT PRIMARY KEY,
    researcher TEXT NOT NULL,
    sensors TEXT[] NOT NULL,
    lower_threshold DOUBLE PRECISION NOT NULL,
    upper_threshold DOUBLE PRECISION NOT NULL,
    stabilization_started BOOLEAN NOT NULL DEFAULT FALSE,
    started BOOLEAN NOT NULL DEFAULT FALSE,
    terminated BOOLEAN NOT NULL DEFAULT FALSE,
    stabilized_notified BOOLEAN NOT NULL DEFAULT FALSE,
    last_out_of_range BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS pending_readings (
    experiment_id TEXT NOT NULL REFERENCES experiments(experiment_id),
    measurement_id TEXT NOT NULL,
    sensor_id TEXT NOT NULL,
    temperature DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (experiment_id, measurement_id, sensor_id)
);

CREATE TABLE IF NOT EXISTS measurements (
    experiment_id TEXT NOT NULL REFERENCES experiments(experiment_id),
    measurement_id TEXT NOT NULL,
    measured_at DOUBLE PRECISION NOT NULL,
    temperature DOUBLE PRECISION NOT NULL,
    out_of_range BOOLEAN NOT NULL,
    during_experiment BOOLEAN NOT NULL,
    PRIMARY KEY (experiment_id, measurement_id)
);

CREATE INDEX IF NOT EXISTS measurements_history_idx
    ON measurements (experiment_id, measured_at)
    WHERE during_experiment;

CREATE INDEX IF NOT EXISTS measurements_out_of_range_idx
    ON measurements (experiment_id, measured_at)
    WHERE during_experiment AND out_of_range;

CREATE TABLE IF NOT EXISTS notification_outbox (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    experiment_id TEXT NOT NULL REFERENCES experiments(experiment_id),
    measurement_id TEXT NOT NULL,
    notification_type TEXT NOT NULL
        CHECK (notification_type IN ('Stabilized', 'OutOfRange')),
    researcher TEXT NOT NULL,
    cipher_data TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    sent_at TIMESTAMPTZ,
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_error TEXT,
    UNIQUE (experiment_id, measurement_id, notification_type)
);

CREATE INDEX IF NOT EXISTS notification_pending_idx
    ON notification_outbox (next_attempt_at, id)
    WHERE sent_at IS NULL;
