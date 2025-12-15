-- Initialize the D1 database with a users table
-- Run this with: uv run pywrangler d1 execute example-db --local --file db_init.sql

DROP TABLE IF EXISTS users;

CREATE TABLE users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    email TEXT UNIQUE NOT NULL,
    created_at TEXT DEFAULT (datetime('now'))
);

-- Insert sample data
INSERT INTO users (name, email) VALUES
    ('Alice Smith', 'alice@example.com'),
    ('Bob Johnson', 'bob@example.com'),
    ('Charlie Brown', 'charlie@example.com'),
    ('Diana Prince', 'diana@example.com'),
    ('Eve Wilson', 'eve@example.com');
