#!/bin/bash
set -e

if PGPASSWORD=val1dat0r psql -h localhost -U validator -d project-sem-1 -c '\q' 2>/dev/null; then
  echo "Using existing PostgreSQL on localhost:5432"
else
  echo "Starting PostgreSQL via docker-compose"
  docker compose up -d db
  sleep 5
fi

PGPASSWORD=val1dat0r psql -h localhost -U validator -d project-sem-1 <<EOF
DROP TABLE IF EXISTS prices;
CREATE TABLE prices (
    id INTEGER,
    name TEXT,
    category TEXT,
    price INTEGER,
    create_date DATE
);
EOF
