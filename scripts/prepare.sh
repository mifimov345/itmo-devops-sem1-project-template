#!/bin/bash
set -e

PGPASSWORD=val1dat0r psql -h localhost -U validator -d project-sem-1 -c '\q'

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
