#!/bin/bash
set -e

docker compose up -d db
sleep 5

docker compose exec -T db psql -U validator -d project-sem-1 <<EOF
CREATE TABLE IF NOT EXISTS prices (
    id INTEGER,
    created_at DATE,
    name TEXT,
    category TEXT,
    price INTEGER
);
EOF
