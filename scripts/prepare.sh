#!/bin/bash
set -e

PGPASSWORD=val1dat0r psql -h localhost -U validator -d project-sem-1 -c '\q'

PGPASSWORD=val1dat0r psql -h localhost -U validator -d project-sem-1 <<EOF
CREATE TABLE IF NOT EXISTS prices (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    category VARCHAR(255) NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    create_date TIMESTAMP NOT NULL
);
EOF
