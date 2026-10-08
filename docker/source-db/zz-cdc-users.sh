#!/bin/bash
# Sourced by the official image's entrypoint AFTER its own inventory.sql
# (files run alphabetically). Adds least-privilege users; root stays
# localhost-only as the image intends.
#   cdc_reader  : read-only inspection (contract generation, drift discovery)
#   app_writer  : the simulated upstream application (seed data + demo DDL)
# The Debezium capture user (debezium) ships with the image.
# The AI repair worker receives NONE of these credentials.
"${mysql[@]}" <<SQL
CREATE USER IF NOT EXISTS '${CDC_READER_USER}'@'%' IDENTIFIED BY '${CDC_READER_PASSWORD}';
GRANT SELECT ON \`${CDC_DATABASE}\`.* TO '${CDC_READER_USER}'@'%';
CREATE USER IF NOT EXISTS '${CDC_WRITER_USER}'@'%' IDENTIFIED BY '${CDC_WRITER_PASSWORD}';
GRANT SELECT, INSERT, UPDATE, ALTER ON \`${CDC_DATABASE}\`.* TO '${CDC_WRITER_USER}'@'%';
FLUSH PRIVILEGES;
SQL
echo "[cdc] least-privilege users created"
