#!/usr/bin/env python3
"""
setup_db.py — Initialise the Stock Signal Engine database.

Reads config/database.json for the database path, creates the data/ and
data/backups/ directories if they do not exist, then creates all schema tables
and indexes. Safe to run multiple times — fully idempotent.

Usage:
    python scripts/setup_db.py
"""

import json
import logging
import os
import sys

# Ensure the project root is on sys.path so src.common.db can be imported
# regardless of the directory from which this script is invoked.
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _PROJECT_ROOT)

from src.common.db import create_all_tables, get_connection  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

_CONFIG_PATH = os.path.join(_PROJECT_ROOT, "config", "database.json")


def load_database_config(config_path: str) -> dict:
    """
    Load the database configuration from a JSON file.

    Parameters:
        config_path: Absolute path to the database.json config file.

    Returns:
        A dict containing database configuration keys including 'path' and 'backup_dir'.
    """
    with open(config_path, "r", encoding="utf-8") as config_file:
        return json.load(config_file)


def ensure_directories_exist(db_path: str, backup_dir: str) -> None:
    """
    Create the data directory (parent of db_path) and backup_dir if they do not exist.

    Both paths are resolved relative to the project root when they are not absolute.

    Parameters:
        db_path: Path to the database file; its parent directory will be created.
        backup_dir: Path to the backups directory to create.

    Returns:
        None
    """
    if not os.path.isabs(db_path):
        db_path = os.path.join(_PROJECT_ROOT, db_path)
    if not os.path.isabs(backup_dir):
        backup_dir = os.path.join(_PROJECT_ROOT, backup_dir)

    data_dir = os.path.dirname(db_path)
    if data_dir:
        os.makedirs(data_dir, exist_ok=True)
        logger.info(f"Data directory ready: {data_dir!r}")

    os.makedirs(backup_dir, exist_ok=True)
    logger.info(f"Backup directory ready: {backup_dir!r}")


def print_table_summary(connection) -> None:
    """
    Print a human-readable summary of all tables and indexes in the database.

    Parameters:
        connection: An open sqlite3.Connection to the initialised database.

    Returns:
        None
    """
    cursor = connection.execute(
        "SELECT type, name FROM sqlite_master "
        "WHERE type IN ('table', 'index') ORDER BY type, name"
    )
    rows = cursor.fetchall()
    tables = [row[1] for row in rows if row[0] == "table"]
    indexes = [row[1] for row in rows if row[0] == "index"]

    print("\nDatabase initialised successfully.")
    print(f"  Tables  ({len(tables)}):")
    for table_name in tables:
        print(f"    - {table_name}")
    print(f"  Indexes ({len(indexes)}):")
    for index_name in indexes:
        print(f"    - {index_name}")


def main() -> None:
    """
    Entry point: load config, create directories, open connection, create all tables.

    Returns:
        None
    """
    logger.info("Starting database setup")

    config = load_database_config(_CONFIG_PATH)
    if "path" not in config:
        raise ValueError(f"Config file {_CONFIG_PATH} is missing required key 'path'")
    db_path = config["path"]
    backup_dir = config.get("backup_dir", "data/backups")

    logger.info(f"Database path from config: {db_path!r}")

    ensure_directories_exist(db_path, backup_dir)

    # Resolve db_path to absolute before connecting
    if not os.path.isabs(db_path):
        db_path = os.path.join(_PROJECT_ROOT, db_path)

    conn = get_connection(db_path)
    try:
        create_all_tables(conn)
        print_table_summary(conn)
        logger.info("Database setup complete")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
