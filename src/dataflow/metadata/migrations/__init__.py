"""Minimal versioned SQL migration runner for DataFlow metadata."""

from __future__ import annotations

import argparse
import os
from importlib.resources import files

import psycopg
from psycopg import Connection

_MIGRATION_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS dataflow_schema_migrations (
    version TEXT PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
)
"""


def apply_migrations(connection: Connection) -> list[str]:
    """Apply pending packaged SQL migrations and return versions applied this call."""
    applied: list[str] = []
    with connection.transaction():
        connection.execute("SELECT pg_advisory_xact_lock(%s)", (0x44415441464C4F57,))
        connection.execute(_MIGRATION_TABLE_SQL)
        existing = {
            row[0]
            for row in connection.execute(
                "SELECT version FROM dataflow_schema_migrations"
            ).fetchall()
        }

        migration_root = files(__package__)
        for migration in sorted(
            (entry for entry in migration_root.iterdir() if entry.name.endswith(".sql")),
            key=lambda entry: entry.name,
        ):
            version = migration.name
            if version in existing:
                continue
            connection.execute(migration.read_text(encoding="utf-8"))
            connection.execute(
                "INSERT INTO dataflow_schema_migrations (version) VALUES (%s)",
                (version,),
            )
            applied.append(version)
    return applied


def migrate(dsn: str) -> list[str]:
    with psycopg.connect(dsn) as connection:
        return apply_migrations(connection)


def main() -> None:
    parser = argparse.ArgumentParser(description="Apply DataFlow PostgreSQL metadata migrations")
    parser.add_argument(
        "--dsn",
        default=os.environ.get("DATABASE_URL"),
        help="PostgreSQL DSN; defaults to DATABASE_URL",
    )
    args = parser.parse_args()
    if not args.dsn:
        parser.error("--dsn or DATABASE_URL is required")
    applied = migrate(args.dsn)
    for version in applied:
        print(version)


__all__ = ["apply_migrations", "migrate"]
