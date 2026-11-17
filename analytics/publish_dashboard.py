"""Publish the built dbt data mart to the Grafana demo's PostgreSQL database."""

import csv
import io
from pathlib import Path
import subprocess

import duckdb

ROOT = Path(__file__).resolve().parents[1]
SQL = """
BEGIN;
CREATE TABLE IF NOT EXISTS public.mart_species_month (
    observation_month date NOT NULL,
    species text NOT NULL,
    observation_count bigint NOT NULL,
    PRIMARY KEY (observation_month, species)
);
DO $role$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'grafana_reader') THEN
        CREATE ROLE grafana_reader LOGIN PASSWORD 'local-reader-only';
    END IF;
END
$role$;
GRANT SELECT ON public.mart_species_month TO grafana_reader;
TRUNCATE public.mart_species_month;
COPY public.mart_species_month FROM STDIN WITH (FORMAT csv);
"""


def publish():
    with duckdb.connect(str(ROOT / "output" / "analytics.duckdb"), read_only=True) as db:
        rows = db.execute("""
            SELECT observation_month, species, observation_count
            FROM mart_species_month
            ORDER BY observation_month, species
        """).fetchall()
    payload = io.StringIO()
    payload.write(SQL)
    csv.writer(payload, lineterminator="\n").writerows(rows)
    payload.write("\\.\nCOMMIT;\n")
    subprocess.run(
        ["docker", "compose", "exec", "-T", "postgres", "psql", "-v", "ON_ERROR_STOP=1",
         "-U", "amazonas", "-d", "amazonas"],
        cwd=ROOT, text=True, input=payload.getvalue(), check=True,
    )


if __name__ == "__main__":
    publish()
