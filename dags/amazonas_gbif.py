"""Orchestrate a reproducible Amazonas wildlife snapshot in SQLite."""

from contextlib import closing
from datetime import datetime, timezone
from html import escape
import json
import os
from pathlib import Path
from urllib.parse import urlsplit
import sqlite3


ROOT = Path(__file__).resolve().parent.parent
SOURCE = ROOT / "data" / "amazon_gbif.json"
FOLLOWUP = ROOT / "data" / "amazon_gbif_followup.json"
DATABASE = Path(os.environ.get("AMAZONAS_DATABASE", str(ROOT / "output" / "amazonas.sqlite")))


def load_snapshot():
    records = json.loads(SOURCE.read_text(encoding="utf-8"))
    DATABASE.parent.mkdir(parents=True, exist_ok=True)
    with closing(sqlite3.connect(DATABASE)) as db, db:
        db.execute("""
            CREATE TABLE IF NOT EXISTS staging_occurrences (
                gbif_id INTEGER PRIMARY KEY,
                species TEXT NOT NULL CHECK (trim(species) <> ''),
                observed_on TEXT,
                rights_holder TEXT,
                license_url TEXT NOT NULL CHECK (trim(license_url) <> '')
            )
        """)
        db.execute("DELETE FROM staging_occurrences")
        db.executemany("INSERT INTO staging_occurrences VALUES (?, ?, ?, ?, ?)", records)


def validate_snapshot():
    with closing(sqlite3.connect(DATABASE)) as db:
        counts = db.execute(
            "SELECT COUNT(*), COUNT(DISTINCT species) FROM staging_occurrences"
        ).fetchone()
    if counts != (765, 374):
        raise ValueError(f"Expected (765 occurrences, 374 species), found {counts}")


def publish_snapshot():
    with closing(sqlite3.connect(DATABASE)) as db, db:
        db.execute("BEGIN")
        db.execute("""
            CREATE TABLE IF NOT EXISTS occurrences AS
            SELECT gbif_id, species, observed_on, rights_holder, license_url,
                   'https://www.gbif.org/occurrence/' || gbif_id AS source_url
            FROM staging_occurrences
        """)
        db.execute("CREATE UNIQUE INDEX IF NOT EXISTS occurrences_gbif_id ON occurrences(gbif_id)")
        db.execute("CREATE INDEX IF NOT EXISTS occurrences_species ON occurrences(species)")
        db.execute("""
            CREATE VIEW IF NOT EXISTS species_summary AS
            SELECT species, COUNT(*) AS observations,
                   MIN(NULLIF(observed_on, '')) AS first_observed,
                   MAX(NULLIF(observed_on, '')) AS last_observed
            FROM occurrences
            GROUP BY species
        """)


def ingest_followup():
    records = json.loads(FOLLOWUP.read_text(encoding="utf-8"))
    counts = {"new": 0, "known": 0, "rejected": 0}
    with closing(sqlite3.connect(DATABASE)) as db, db:
        db.execute("BEGIN IMMEDIATE")
        db.execute("CREATE TABLE IF NOT EXISTS followup_added (gbif_id INTEGER PRIMARY KEY)")
        db.execute("""
            CREATE TABLE IF NOT EXISTS followup_rejects (
                row_number INTEGER PRIMARY KEY, raw_record TEXT NOT NULL, reason TEXT NOT NULL
            )
        """)
        for row_number, record in enumerate(records, start=1):
            problems = []
            fields = record if isinstance(record, list) and len(record) == 5 else [None] * 5
            if fields is not record:
                problems.append("expected five fields")
            gbif_id, species, observed_on, rights_holder, license_url = fields
            if type(gbif_id) is not int or gbif_id <= 0:
                problems.append("missing or invalid GBIF ID")
            if not isinstance(species, str) or not species.strip():
                problems.append("missing species")
            if not isinstance(license_url, str) or not license_url.strip():
                problems.append("missing license")
            elif urlsplit(license_url).scheme not in {"http", "https"}:
                problems.append("invalid license URL")
            if any(value is not None and not isinstance(value, str) for value in (observed_on, rights_holder)):
                problems.append("invalid date or rights holder")
            if problems:
                db.execute(
                    "INSERT OR REPLACE INTO followup_rejects VALUES (?, ?, ?)",
                    (row_number, json.dumps(record, ensure_ascii=False), ", ".join(problems)),
                )
                counts["rejected"] += 1
                continue
            inserted = db.execute(
                """INSERT INTO occurrences
                   (gbif_id, species, observed_on, rights_holder, license_url, source_url)
                   VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT(gbif_id) DO NOTHING""",
                (*record, f"https://www.gbif.org/occurrence/{gbif_id}"),
            ).rowcount
            if inserted:
                db.execute("INSERT INTO followup_added VALUES (?)", (gbif_id,))
                counts["new"] += 1
            else:
                counts["known"] += 1
    print(json.dumps(counts))
    return counts


def write_report(counts):
    with closing(sqlite3.connect(DATABASE)) as db:
        rows = db.execute("""
            SELECT species, gbif_id, source_url, COALESCE(rights_holder, ''), license_url
            FROM occurrences JOIN followup_added USING (gbif_id)
            ORDER BY species, gbif_id
        """).fetchall()
    species_links = {}
    for species, gbif_id, url, rights_holder, license_url in rows:
        link = f'<a href="{escape(url)}">{gbif_id}</a>'
        license_link = f'<a href="{escape(license_url)}">License</a>'
        species_links.setdefault(species, []).append(f"{link} {escape(rights_holder)} ({license_link})")
    entries = "\n".join(
        f"<li><strong>{escape(species)}</strong> ({len(links)}): {', '.join(links)}</li>"
        for species, links in species_links.items()
    )
    report = DATABASE.with_name("followup_report.html")
    temporary = report.with_suffix(".tmp")
    temporary.write_text(
        '<!doctype html><html lang="en"><meta charset="utf-8">'
        '<meta name="viewport" content="width=device-width, initial-scale=1">'
        '<title>Amazonas Wildlife Data Pipeline</title>'
        '<style>body{font:1rem/1.65 system-ui;max-width:48rem;margin:3rem auto;padding:0 1rem}'
        'h1{color:#214c38}li{margin:.4rem 0}</style>'
        '<main><h1>Amazonas Wildlife Data Pipeline</h1>'
        f'<p>Last import: {counts["new"]} new, {counts["known"]} already known, '
        f'{counts["rejected"]} rejected.</p>'
        f'<h2>Observations added by the second delivery</h2><ul>{entries}</ul></main></html>',
        encoding="utf-8",
    )
    temporary.replace(report)


if __name__ == "__main__":
    load_snapshot()
    validate_snapshot()
    publish_snapshot()
    write_report(ingest_followup())
    print(f"Published the database and report to {DATABASE.parent}")
else:
    from airflow.sdk import dag, task

    @dag(
        dag_id="amazonas_gbif",
        start_date=datetime(2026, 11, 16, tzinfo=timezone.utc),
        schedule=None,
        catchup=False,
        max_active_runs=1,
        tags=["gbif", "sqlite"],
    )
    def amazonas_gbif():
        staged = task(load_snapshot)()
        validated = task(validate_snapshot)()
        published = task(publish_snapshot)()
        followup = task(ingest_followup)()
        staged >> validated >> published >> followup
        task(write_report)(followup)

    amazonas_dag = amazonas_gbif()
