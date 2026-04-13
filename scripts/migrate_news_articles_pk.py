#!/usr/bin/env python3
"""
migrate_news_articles_pk.py — Migrate news_articles to composite PRIMARY KEY (id, ticker).

SQLite does not support ALTER TABLE to change a PRIMARY KEY, so this script
recreates the table with the new schema using the create-new/copy/drop/rename pattern.

Before running:
  - Optionally snapshot the DB: cp data/signals.db data/backups/signals_pre_migrate.db
  - Stop any running pipeline processes

After running:
  - Re-backfill news to populate per-ticker rows: python scripts/run_backfill.py --phase news --force
  - Re-run calculator to fix summaries:           python scripts/run_calculator.py --force
  - Verify pipeline is clean:                     python scripts/verify_pipeline.py

Usage:
    python scripts/migrate_news_articles_pk.py
"""

import json
import logging
import os
import sqlite3
import sys

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _PROJECT_ROOT)

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s [%(name)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

_DB_CONFIG_PATH = os.path.join(_PROJECT_ROOT, "config", "database.json")


def load_db_path() -> str:
    """
    Load the database file path from config/database.json.

    Returns:
        Absolute path to the SQLite database file.
    """
    with open(_DB_CONFIG_PATH) as config_file:
        db_config = json.load(config_file)
    relative_path = db_config["path"]
    return os.path.join(_PROJECT_ROOT, relative_path)


def migrate_news_articles_pk(db_path: str) -> None:
    """
    Recreate news_articles with PRIMARY KEY (id, ticker) instead of id TEXT PRIMARY KEY.

    Steps performed inside a single transaction:
      1. Create news_articles_new with the new composite PK.
      2. Copy all rows from news_articles into news_articles_new.
      3. Drop news_articles.
      4. Rename news_articles_new to news_articles.
      5. Recreate the (ticker, date) index.

    Existing rows are unique by id (old PK), so the INSERT SELECT is conflict-free.
    Row count is verified before and after to confirm no data loss.

    Args:
        db_path: Absolute path to the SQLite database file.

    Raises:
        RuntimeError: If the row count after migration does not match the count before.
        sqlite3.Error: If any SQL statement fails (transaction is rolled back).
    """
    logger.info(f"Opening database: {db_path}")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    try:
        # Enable WAL mode for consistency with the rest of the pipeline.
        conn.execute("PRAGMA journal_mode=WAL")

        # Snapshot row count before migration.
        row_before = conn.execute("SELECT COUNT(*) AS cnt FROM news_articles").fetchone()
        count_before: int = row_before["cnt"]
        logger.info(f"news_articles row count before migration: {count_before:,}")

        logger.info("Starting migration transaction...")
        with conn:
            # Step 1 — Create new table with composite PK.
            conn.execute("""
                CREATE TABLE news_articles_new (
                    id TEXT NOT NULL,
                    ticker TEXT NOT NULL,
                    date TEXT NOT NULL,
                    source TEXT,
                    headline TEXT,
                    summary TEXT,
                    url TEXT,
                    sentiment TEXT,
                    sentiment_reasoning TEXT,
                    published_utc TEXT,
                    fetched_at TEXT,
                    PRIMARY KEY (id, ticker)
                )
            """)
            logger.info("Created news_articles_new with PRIMARY KEY (id, ticker)")

            # Step 2 — Copy all existing rows.
            conn.execute("""
                INSERT INTO news_articles_new
                SELECT id, ticker, date, source, headline, summary, url,
                       sentiment, sentiment_reasoning, published_utc, fetched_at
                FROM news_articles
            """)
            logger.info("Copied rows from news_articles to news_articles_new")

            # Step 3 — Drop the old table.
            conn.execute("DROP TABLE news_articles")
            logger.info("Dropped old news_articles table")

            # Step 4 — Rename the new table.
            conn.execute("ALTER TABLE news_articles_new RENAME TO news_articles")
            logger.info("Renamed news_articles_new to news_articles")

            # Step 5 — Recreate the index.
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_news_ticker_date "
                "ON news_articles(ticker, date)"
            )
            logger.info("Recreated idx_news_ticker_date index")

        # Verify row count matches.
        row_after = conn.execute("SELECT COUNT(*) AS cnt FROM news_articles").fetchone()
        count_after: int = row_after["cnt"]
        logger.info(f"news_articles row count after migration: {count_after:,}")

        if count_after != count_before:
            raise RuntimeError(
                f"Row count mismatch after migration: before={count_before} after={count_after}"
            )

        logger.info("Migration complete — row counts match, PRIMARY KEY is now (id, ticker)")

    except sqlite3.Error as exc:
        logger.error(f"Migration failed, transaction rolled back: {exc}")
        raise
    finally:
        conn.close()


def main() -> None:
    """
    Entry point: load DB path from config and run the migration.
    """
    db_path = load_db_path()

    if not os.path.exists(db_path):
        logger.error(f"Database file not found: {db_path}")
        sys.exit(1)

    migrate_news_articles_pk(db_path)


if __name__ == "__main__":
    main()
