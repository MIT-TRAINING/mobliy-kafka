#!/bin/bash
# Runs once, when the cdc-postgres container starts with an empty data volume.
# Builds the CRM database the lab captures changes from:
#   - table crm.subscribers with five subscribers
#   - REPLICA IDENTITY FULL, so updates and deletes carry the old row ("before")
#   - user cdc_reader: may log in, replicate and SELECT the table, nothing else
#   - publication cdc_subscribers: the only table Debezium may stream
#   - a pg_hba.conf line that lets cdc_reader open a replication connection from the Connect container
# CDC_DB_PASSWORD comes from $CDC_DIR/cdc-db.env (see "Before you start"); it is never written here.
set -euo pipefail

psql -v ON_ERROR_STOP=1 -v cdc_pw="$CDC_DB_PASSWORD" --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<'SQL'
CREATE SCHEMA crm;

CREATE TABLE crm.subscribers (
  msisdn     text        PRIMARY KEY,
  name       text        NOT NULL,
  plan       text        NOT NULL,
  status     text        NOT NULL DEFAULT 'ACTIVE',
  circle     text        NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now()
);

INSERT INTO crm.subscribers (msisdn, name, plan, circle) VALUES
  ('919845000001', 'Asha Rao',      'prepaid-299',  'KA'),
  ('919845000002', 'Vikram Shah',   'postpaid-599', 'MH'),
  ('919845000003', 'Meera Iyer',    'prepaid-199',  'TN'),
  ('919845000004', 'Rahul Verma',   'postpaid-999', 'DL'),
  ('919845000005', 'Fatima Khan',   'prepaid-299',  'KA');

ALTER TABLE crm.subscribers REPLICA IDENTITY FULL;

CREATE ROLE cdc_reader WITH LOGIN REPLICATION PASSWORD :'cdc_pw';
GRANT USAGE ON SCHEMA crm TO cdc_reader;
GRANT SELECT ON crm.subscribers TO cdc_reader;

CREATE PUBLICATION cdc_subscribers FOR TABLE crm.subscribers;
SQL

echo "host replication cdc_reader all scram-sha-256" >> "$PGDATA/pg_hba.conf"
