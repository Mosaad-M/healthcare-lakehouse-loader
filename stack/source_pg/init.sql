-- A clinic's operational database: the CDC source (synthetic data only).
CREATE TABLE patients (
    patient_id  bigint PRIMARY KEY,
    mrn         text NOT NULL,
    full_name   text NOT NULL,
    email       text,
    birth_year  integer,
    zip3        text,
    plan_code   text,
    balance     numeric(10, 2),
    updated_at  timestamptz NOT NULL DEFAULT now()
);
-- Deletes carry the whole old row, so a delete still resolves to its subject.
ALTER TABLE patients REPLICA IDENTITY FULL;
