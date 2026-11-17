"""Bridge the existing SQLite import to the dbt analytics database."""

from contextlib import closing
from pathlib import Path
import sqlite3

import duckdb

OUTPUT = Path(__file__).resolve().parents[1] / "output"


def prepare():
    source = OUTPUT / "amazonas.sqlite"
    with closing(sqlite3.connect(source.as_uri() + "?mode=ro", uri=True)) as sqlite:
        rows = sqlite.execute("""
            select gbif_id, species, observed_on, source_url,
                   case when followup_added.gbif_id is null
                        then '2026-11-16 00:00:00' else '2026-11-17 00:00:00' end
            from occurrences left join followup_added using (gbif_id)
            order by gbif_id
        """).fetchall()

    with duckdb.connect(str(OUTPUT / "analytics.duckdb")) as db:
        db.begin()
        db.execute("""
            create or replace table observation_input (
                gbif_id bigint primary key, species varchar not null,
                observed_on date, source_url varchar, batch_timestamp timestamp
            )
        """)
        db.executemany("insert into observation_input values (?, ?, try_cast(? as date), ?, ?)", rows)
        db.execute("""
            create or replace table observation_versions as
            select gbif_id, observed_on, 1 as version_number, batch_timestamp as version_loaded_at, false as is_simulated
            from observation_input
            union all
            select gbif_id, observed_on + 1, 2, timestamp '2026-11-18 00:00:00', true
            from (
                select gbif_id, observed_on from observation_input
                where observed_on is not null order by gbif_id limit 1
            )
        """)
        db.commit()


if __name__ == "__main__":
    prepare()
