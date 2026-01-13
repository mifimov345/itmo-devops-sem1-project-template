#!/bin/bash
set -e

docker compose up -d db
sleep 5

docker compose exec -T db psql -U validator -d project-sem-1 <<EOF
DROP TABLE IF EXISTS prices;
CREATE TABLE prices (
    id INTEGER,
    name TEXT,
    category TEXT,
    price INTEGER,
    create_date DATE
);
EOF
