-- Schema for the Hyperdrive example Worker.
--
-- The table is namespaced because a Hyperdrive config often points at a shared
-- database. Nothing here drops or alters pre-existing objects.
--
-- Run against the PostgreSQL database that the Hyperdrive config points at:
--   psql "$POSTGRES_URL" -f db_init.sql

CREATE TABLE IF NOT EXISTS sqlalchemy_hyperdrive_example (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    quantity INTEGER NOT NULL DEFAULT 0
);

INSERT INTO sqlalchemy_hyperdrive_example (name, quantity)
SELECT 'widget', 10
WHERE NOT EXISTS (
    SELECT 1 FROM sqlalchemy_hyperdrive_example WHERE name = 'widget'
);

INSERT INTO sqlalchemy_hyperdrive_example (name, quantity)
SELECT 'gadget', 5
WHERE NOT EXISTS (
    SELECT 1 FROM sqlalchemy_hyperdrive_example WHERE name = 'gadget'
);

INSERT INTO sqlalchemy_hyperdrive_example (name, quantity)
SELECT 'sprocket', 0
WHERE NOT EXISTS (
    SELECT 1 FROM sqlalchemy_hyperdrive_example WHERE name = 'sprocket'
);
