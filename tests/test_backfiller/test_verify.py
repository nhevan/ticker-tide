"""Tests for src/backfiller/verify.py.

TDD: tests written before implementation.
All external API calls and config loading are mocked.
"""

from __future__ import annotations

import sqlite3
from datetime import date, timedelta
from unittest.mock import MagicMock, patch

import pytest

from src.backfiller.verify import (
    CheckResult,
    VerificationReport,
    check_cross_table_consistency,
    check_data_freshness,
    check_date_gaps,
    check_date_range,
    check_null_coverage,
    check_optional_ticker_coverage,
    check_table_row_counts,
    check_ticker_coverage,
    check_value_sanity,
    detect_market_wide_closures,
    format_verification_report,
    run_full_verification,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _generate_trading_days(start: str, end: str) -> list[str]:
    """Return a list of ISO date strings for every Mon-Fri between start and end inclusive."""
    current = date.fromisoformat(start)
    end_date = date.fromisoformat(end)
    days: list[str] = []
    while current <= end_date:
        if current.weekday() < 5:
            days.append(current.isoformat())
        current += timedelta(days=1)
    return days


def _insert_ohlcv_rows(conn: sqlite3.Connection, ticker: str, dates: list[str], close: float = 150.0) -> None:
    """Insert OHLCV rows for the given ticker and list of date strings."""
    conn.executemany(
        "INSERT OR REPLACE INTO ohlcv_daily (ticker, date, open, high, low, close, volume) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        [(ticker, day, close * 0.99, close * 1.01, close * 0.98, close, 1_000_000) for day in dates],
    )
    conn.commit()


def _insert_fundamentals_row(conn: sqlite3.Connection, ticker: str, report_date: str, pe_ratio: float = None) -> None:
    """Insert one fundamentals row for ticker."""
    conn.execute(
        "INSERT OR REPLACE INTO fundamentals "
        "(ticker, report_date, period, revenue, eps, pe_ratio, fetched_at) "
        "VALUES (?, ?, 'Q1', 1000000.0, 2.50, ?, '2026-01-01')",
        (ticker, report_date, pe_ratio),
    )
    conn.commit()


def _insert_news_article(
    conn: sqlite3.Connection,
    article_id: str,
    ticker: str,
    article_date: str,
    sentiment: str = None,
) -> None:
    """Insert one news_articles row for ticker."""
    conn.execute(
        "INSERT OR REPLACE INTO news_articles "
        "(id, ticker, date, source, headline, sentiment, fetched_at) "
        "VALUES (?, ?, ?, 'polygon', 'Test headline', ?, '2026-01-01')",
        (article_id, ticker, article_date, sentiment),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# check_table_row_counts
# ---------------------------------------------------------------------------

def test_check_table_row_counts_all_populated(db_connection: sqlite3.Connection) -> None:
    """Insert rows into ohlcv_daily, fundamentals, and news_articles for 3 tickers.

    Verify check_table_row_counts returns a CheckResult whose data dict contains
    table names as keys and row counts as values, with non-zero counts for
    the populated tables.
    """
    for ticker in ("AAPL", "MSFT", "NVDA"):
        _insert_ohlcv_rows(db_connection, ticker, ["2026-01-02", "2026-01-03"])
        _insert_fundamentals_row(db_connection, ticker, "2026-01-01")
        _insert_news_article(db_connection, f"art-{ticker}-1", ticker, "2026-01-02", "positive")

    result = check_table_row_counts(db_connection)

    assert isinstance(result, CheckResult)
    assert isinstance(result.data, dict)
    assert result.data["ohlcv_daily"] == 6
    assert result.data["fundamentals"] == 3
    assert result.data["news_articles"] == 3


def test_check_table_row_counts_empty_table(db_connection: sqlite3.Connection) -> None:
    """Create tables but insert no data. Verify the count is 0 and the table is flagged.

    The check should return a fail status when ohlcv_daily is empty.
    """
    result = check_table_row_counts(db_connection)

    assert isinstance(result, CheckResult)
    assert result.data["ohlcv_daily"] == 0
    assert result.status == "fail"


# ---------------------------------------------------------------------------
# check_ticker_coverage
# ---------------------------------------------------------------------------

def test_check_ticker_coverage_all_present(db_connection: sqlite3.Connection) -> None:
    """Insert OHLCV data for 3 tickers; verify all 3 are returned as present and missing is empty."""
    for ticker in ("AAPL", "MSFT", "NVDA"):
        _insert_ohlcv_rows(db_connection, ticker, ["2026-01-02"])

    result = check_ticker_coverage(db_connection, "ohlcv_daily", ["AAPL", "MSFT", "NVDA"])

    assert isinstance(result, CheckResult)
    assert result.status == "pass"
    assert result.data["missing"] == []
    assert len(result.data["present"]) == 3


def test_check_ticker_coverage_some_missing(db_connection: sqlite3.Connection) -> None:
    """Insert data for AAPL and MSFT only; verify NVDA is in the missing list."""
    for ticker in ("AAPL", "MSFT"):
        _insert_ohlcv_rows(db_connection, ticker, ["2026-01-02"])

    result = check_ticker_coverage(db_connection, "ohlcv_daily", ["AAPL", "MSFT", "NVDA"])

    assert isinstance(result, CheckResult)
    assert result.status == "fail"
    assert "NVDA" in result.data["missing"]
    assert "AAPL" in result.data["present"]
    assert "MSFT" in result.data["present"]


def test_check_ticker_coverage_fundamentals(db_connection: sqlite3.Connection) -> None:
    """Insert fundamentals for AAPL and MSFT only.

    When expected_tickers includes SPY (an ETF that legitimately has no
    fundamentals), the check should return warn (not fail) for the fundamentals
    table, since missing fundamentals for ETFs is expected.
    """
    _insert_fundamentals_row(db_connection, "AAPL", "2026-01-01")
    _insert_fundamentals_row(db_connection, "MSFT", "2026-01-01")

    result = check_ticker_coverage(
        db_connection, "fundamentals", ["AAPL", "MSFT", "SPY"]
    )

    assert isinstance(result, CheckResult)
    # fundamentals table: missing is a warn, not a fail
    assert result.status == "warn"
    assert "SPY" in result.data["missing"]


# ---------------------------------------------------------------------------
# check_date_range
# ---------------------------------------------------------------------------

def test_check_date_range_correct(db_connection: sqlite3.Connection) -> None:
    """Insert OHLCV spanning 2021-03-17 to 2026-03-16. Verify min, max, and trading_days count."""
    trading_days = _generate_trading_days("2021-03-17", "2026-03-16")
    _insert_ohlcv_rows(db_connection, "AAPL", trading_days)

    result = check_date_range(db_connection, "AAPL")

    assert result["min_date"] == "2021-03-17"
    assert result["max_date"] == "2026-03-16"
    assert result["trading_days"] == len(trading_days)
    # Should be approximately 5 years of trading days
    assert 1200 <= result["trading_days"] <= 1400


def test_check_date_range_too_short(db_connection: sqlite3.Connection) -> None:
    """Insert only 100 days of data; verify the result flags a warning for insufficient history."""
    trading_days = _generate_trading_days("2025-10-01", "2026-03-16")[:100]
    _insert_ohlcv_rows(db_connection, "AAPL", trading_days)

    result = check_date_range(db_connection, "AAPL")

    assert result["trading_days"] == 100
    assert result["status"] == "warn"
    assert "100" in result["message"]


# ---------------------------------------------------------------------------
# check_date_gaps
# ---------------------------------------------------------------------------

def test_check_date_gaps_no_gaps(db_connection: sqlite3.Connection) -> None:
    """Insert consecutive Mon-Fri data for one week. Verify no gaps returned."""
    # 2026-03-09 (Mon) through 2026-03-13 (Fri)
    trading_days = _generate_trading_days("2026-03-09", "2026-03-13")
    _insert_ohlcv_rows(db_connection, "AAPL", trading_days)

    gaps = check_date_gaps(db_connection, "AAPL")

    assert gaps == []


def test_check_date_gaps_with_gaps(db_connection: sqlite3.Connection) -> None:
    """Insert one week but skip Wednesday and Thursday; verify those 2 dates are in gaps."""
    all_days = _generate_trading_days("2026-03-09", "2026-03-13")
    # Skip 2026-03-11 (Wed) and 2026-03-12 (Thu)
    days_with_gap = [d for d in all_days if d not in ("2026-03-11", "2026-03-12")]
    _insert_ohlcv_rows(db_connection, "AAPL", days_with_gap)

    gaps = check_date_gaps(db_connection, "AAPL")

    assert "2026-03-11" in gaps
    assert "2026-03-12" in gaps
    assert len(gaps) == 2


def test_check_date_gaps_ignores_weekends(db_connection: sqlite3.Connection) -> None:
    """Insert Mon-Fri data for 2 weeks with no Saturday/Sunday rows. Verify no gaps flagged."""
    trading_days = _generate_trading_days("2026-03-09", "2026-03-20")
    _insert_ohlcv_rows(db_connection, "AAPL", trading_days)

    gaps = check_date_gaps(db_connection, "AAPL")

    assert gaps == []
    # Confirm no weekends were inserted
    for day in trading_days:
        assert date.fromisoformat(day).weekday() < 5


def test_check_date_gaps_ignores_holidays(db_connection: sqlite3.Connection) -> None:
    """Insert data skipping 2026-12-25 (Friday). Pass it as a holiday; verify no gap flagged."""
    # 2026-12-21 (Mon) through 2026-12-28 (Mon), excluding 2026-12-25 (Fri)
    all_days = _generate_trading_days("2026-12-21", "2026-12-28")
    days_without_holiday = [d for d in all_days if d != "2026-12-25"]
    _insert_ohlcv_rows(db_connection, "AAPL", days_without_holiday)

    gaps = check_date_gaps(db_connection, "AAPL", holidays=["2026-12-25"])

    assert "2026-12-25" not in gaps
    assert gaps == []


# ---------------------------------------------------------------------------
# check_data_freshness
# ---------------------------------------------------------------------------

def test_check_data_freshness_recent(db_connection: sqlite3.Connection) -> None:
    """Insert OHLCV with max date = yesterday. Verify status='fresh'."""
    with patch("src.backfiller.verify.date") as mock_date:
        mock_date.today.return_value = date(2026, 3, 17)
        mock_date.fromisoformat = date.fromisoformat

        _insert_ohlcv_rows(db_connection, "AAPL", ["2026-03-16"])

        result = check_data_freshness(db_connection, "AAPL")

    assert result["status"] == "fresh"
    assert result["max_date"] == "2026-03-16"
    assert result["days_behind"] == 1


def test_check_data_freshness_stale(db_connection: sqlite3.Connection) -> None:
    """Insert OHLCV with max date = 30 days ago. Verify status='stale' and days_behind=30."""
    with patch("src.backfiller.verify.date") as mock_date:
        mock_date.today.return_value = date(2026, 3, 17)
        mock_date.fromisoformat = date.fromisoformat

        _insert_ohlcv_rows(db_connection, "AAPL", ["2026-02-15"])

        result = check_data_freshness(db_connection, "AAPL")

    assert result["status"] == "stale"
    assert result["days_behind"] == 30


# ---------------------------------------------------------------------------
# check_value_sanity
# ---------------------------------------------------------------------------

def test_check_value_sanity_valid(db_connection: sqlite3.Connection) -> None:
    """Insert OHLCV with normal prices (150-200 range). Verify no issues returned."""
    days = ["2026-03-10", "2026-03-11", "2026-03-12"]
    _insert_ohlcv_rows(db_connection, "AAPL", days, close=175.0)

    issues = check_value_sanity(db_connection, "AAPL")

    assert issues == []


def test_check_value_sanity_zero_close(db_connection: sqlite3.Connection) -> None:
    """Insert a row with close=0. Verify it is flagged as an issue."""
    db_connection.execute(
        "INSERT INTO ohlcv_daily (ticker, date, open, high, low, close, volume) "
        "VALUES ('AAPL', '2026-03-10', 150.0, 155.0, 148.0, 0.0, 1000000)"
    )
    db_connection.commit()

    issues = check_value_sanity(db_connection, "AAPL")

    assert len(issues) >= 1
    assert any("close" in issue.lower() or "zero" in issue.lower() or "0" in issue for issue in issues)


def test_check_value_sanity_negative_volume(db_connection: sqlite3.Connection) -> None:
    """Insert a row with volume=-100. Verify it is flagged."""
    db_connection.execute(
        "INSERT INTO ohlcv_daily (ticker, date, open, high, low, close, volume) "
        "VALUES ('AAPL', '2026-03-10', 150.0, 155.0, 148.0, 152.0, -100)"
    )
    db_connection.commit()

    issues = check_value_sanity(db_connection, "AAPL")

    assert len(issues) >= 1
    assert any("volume" in issue.lower() or "negative" in issue.lower() for issue in issues)


def test_check_value_sanity_extreme_price_change(db_connection: sqlite3.Connection) -> None:
    """Insert two consecutive days where price jumps 500%. Verify flagged as a warning."""
    db_connection.execute(
        "INSERT INTO ohlcv_daily (ticker, date, open, high, low, close, volume) "
        "VALUES ('AAPL', '2026-03-10', 100.0, 105.0, 98.0, 100.0, 1000000)"
    )
    db_connection.execute(
        "INSERT INTO ohlcv_daily (ticker, date, open, high, low, close, volume) "
        "VALUES ('AAPL', '2026-03-11', 600.0, 630.0, 590.0, 600.0, 1000000)"
    )
    db_connection.commit()

    issues = check_value_sanity(db_connection, "AAPL")

    assert len(issues) >= 1
    assert any(
        "500" in issue or "price change" in issue.lower() or "%" in issue
        for issue in issues
    )


# ---------------------------------------------------------------------------
# check_cross_table_consistency
# ---------------------------------------------------------------------------

def test_check_cross_table_consistency(db_connection: sqlite3.Connection) -> None:
    """Insert ticker with active=1 but no OHLCV data. Verify it is flagged as fail."""
    db_connection.execute(
        "INSERT INTO tickers (symbol, active) VALUES ('AAPL', 1)"
    )
    db_connection.commit()

    result = check_cross_table_consistency(db_connection)

    assert isinstance(result, CheckResult)
    assert result.status == "fail"
    assert result.details is not None
    assert any("AAPL" in detail for detail in result.details)


def test_check_cross_table_consistency_all_good(db_connection: sqlite3.Connection) -> None:
    """Insert ticker + OHLCV + fundamentals + news for the same ticker. Verify no issues."""
    db_connection.execute(
        "INSERT INTO tickers (symbol, active) VALUES ('AAPL', 1)"
    )
    _insert_ohlcv_rows(db_connection, "AAPL", ["2026-03-10"])
    _insert_fundamentals_row(db_connection, "AAPL", "2026-01-01")
    _insert_news_article(db_connection, "art-001", "AAPL", "2026-03-10", "positive")

    result = check_cross_table_consistency(db_connection)

    assert isinstance(result, CheckResult)
    assert result.status == "pass"


# ---------------------------------------------------------------------------
# check_null_coverage
# ---------------------------------------------------------------------------

def test_check_null_coverage(db_connection: sqlite3.Connection) -> None:
    """Insert 5 fundamentals rows for AAPL, 2 with pe_ratio=NULL. Verify null_pct=40.0."""
    for idx in range(5):
        pe = None if idx < 2 else 25.0
        db_connection.execute(
            "INSERT INTO fundamentals (ticker, report_date, period, pe_ratio, fetched_at) "
            "VALUES ('AAPL', ?, 'Q1', ?, '2026-01-01')",
            (f"2025-0{idx + 1}-01", pe),
        )
    db_connection.commit()

    result = check_null_coverage(db_connection, "fundamentals", "pe_ratio", "AAPL")

    assert result["total"] == 5
    assert result["nulls"] == 2
    assert result["null_pct"] == 40.0


# ---------------------------------------------------------------------------
# check_news_sentiment_coverage
# ---------------------------------------------------------------------------

def test_check_news_sentiment_coverage(db_connection: sqlite3.Connection) -> None:
    """Insert 10 news articles, 7 with sentiment, 3 without. Verify 70% coverage."""
    for idx in range(10):
        sentiment = "positive" if idx < 7 else None
        _insert_news_article(
            db_connection, f"art-{idx}", "AAPL", "2026-03-10", sentiment
        )

    from src.backfiller.verify import check_news_sentiment_coverage

    result = check_news_sentiment_coverage(db_connection, ["AAPL"])

    assert isinstance(result, CheckResult)
    assert result.data is not None
    assert result.data["overall_sentiment_pct"] == pytest.approx(70.0)


# ---------------------------------------------------------------------------
# run_full_verification
# ---------------------------------------------------------------------------

def test_run_full_verification_returns_report(db_connection: sqlite3.Connection, tmp_path) -> None:
    """Mock a populated DB with data for 3 tickers. Verify the report has required sections."""
    db_path = str(tmp_path / "signals.db")

    # Build a populated DB at tmp_path
    import shutil
    # We need a DB file on disk; replicate db_connection's data to a file
    on_disk = sqlite3.connect(db_path)
    on_disk.execute("PRAGMA journal_mode=WAL")
    on_disk.row_factory = sqlite3.Row
    # Copy schema from the in-memory connection by running the same setup
    from src.common.db import create_all_tables
    create_all_tables(on_disk)

    for ticker in ("AAPL", "MSFT", "NVDA"):
        on_disk.execute("INSERT INTO tickers (symbol, active) VALUES (?, 1)", (ticker,))
        trading_days = _generate_trading_days("2021-03-17", "2026-03-16")
        on_disk.executemany(
            "INSERT OR REPLACE INTO ohlcv_daily (ticker, date, open, high, low, close, volume) "
            "VALUES (?, ?, 150.0, 155.0, 148.0, 153.0, 1000000)",
            [(ticker, day) for day in trading_days],
        )
        on_disk.execute(
            "INSERT INTO fundamentals (ticker, report_date, period, pe_ratio, fetched_at) "
            "VALUES (?, '2026-01-01', 'Q1', 25.0, '2026-01-01')", (ticker,)
        )
        on_disk.execute(
            "INSERT INTO news_articles (id, ticker, date, source, headline, sentiment, fetched_at) "
            "VALUES (?, ?, '2026-03-10', 'polygon', 'Test', 'positive', '2026-01-01')",
            (f"art-{ticker}", ticker),
        )
    on_disk.commit()

    mock_tickers = [
        {"symbol": "AAPL", "active": True},
        {"symbol": "MSFT", "active": True},
        {"symbol": "NVDA", "active": True},
    ]

    with patch("src.backfiller.verify.get_active_tickers", return_value=mock_tickers):
        with patch("src.backfiller.verify.load_config", return_value={"path": db_path}):
            with patch("src.backfiller.verify.date") as mock_date:
                mock_date.today.return_value = date(2026, 3, 17)
                mock_date.fromisoformat = date.fromisoformat
                report = run_full_verification(db_path=db_path)

    on_disk.close()

    assert isinstance(report, VerificationReport)
    check_names = {check.name for check in report.checks}
    assert "table_row_counts" in check_names
    assert "ticker_coverage_ohlcv_daily" in check_names
    assert "date_range_all_tickers" in check_names
    assert "data_freshness" in check_names
    assert "value_sanity" in check_names
    assert "cross_table_consistency" in check_names


def test_run_full_verification_overall_pass(db_connection: sqlite3.Connection, tmp_path) -> None:
    """All checks pass. Verify overall_status='PASS'."""
    db_path = str(tmp_path / "signals.db")
    on_disk = sqlite3.connect(db_path)
    on_disk.execute("PRAGMA journal_mode=WAL")
    from src.common.db import create_all_tables
    create_all_tables(on_disk)

    for ticker in ("AAPL", "MSFT", "NVDA"):
        on_disk.execute("INSERT INTO tickers (symbol, active) VALUES (?, 1)", (ticker,))
        trading_days = _generate_trading_days("2021-03-17", "2026-03-16")
        on_disk.executemany(
            "INSERT OR REPLACE INTO ohlcv_daily (ticker, date, open, high, low, close, volume) "
            "VALUES (?, ?, 150.0, 155.0, 148.0, 153.0, 1000000)",
            [(ticker, day) for day in trading_days],
        )
    on_disk.commit()

    mock_tickers = [
        {"symbol": "AAPL", "active": True},
        {"symbol": "MSFT", "active": True},
        {"symbol": "NVDA", "active": True},
    ]

    with patch("src.backfiller.verify.get_active_tickers", return_value=mock_tickers):
        with patch("src.backfiller.verify.load_config", return_value={"path": db_path}):
            with patch("src.backfiller.verify.date") as mock_date:
                mock_date.today.return_value = date(2026, 3, 17)
                mock_date.fromisoformat = date.fromisoformat
                report = run_full_verification(db_path=db_path)

    on_disk.close()

    assert report.overall_status == "PASS"
    assert report.fail_count == 0


def test_run_full_verification_overall_warn(db_connection: sqlite3.Connection, tmp_path) -> None:
    """Some non-critical issues found. Verify overall_status='PASS' with warnings > 0."""
    db_path = str(tmp_path / "signals.db")
    on_disk = sqlite3.connect(db_path)
    on_disk.execute("PRAGMA journal_mode=WAL")
    from src.common.db import create_all_tables
    create_all_tables(on_disk)

    # Insert tickers and OHLCV but skip fundamentals (produces a warn)
    for ticker in ("AAPL", "MSFT", "NVDA"):
        on_disk.execute("INSERT INTO tickers (symbol, active) VALUES (?, 1)", (ticker,))
        trading_days = _generate_trading_days("2021-03-17", "2026-03-16")
        on_disk.executemany(
            "INSERT OR REPLACE INTO ohlcv_daily (ticker, date, open, high, low, close, volume) "
            "VALUES (?, ?, 150.0, 155.0, 148.0, 153.0, 1000000)",
            [(ticker, day) for day in trading_days],
        )
    on_disk.commit()

    mock_tickers = [
        {"symbol": "AAPL", "active": True},
        {"symbol": "MSFT", "active": True},
        {"symbol": "NVDA", "active": True},
    ]

    with patch("src.backfiller.verify.get_active_tickers", return_value=mock_tickers):
        with patch("src.backfiller.verify.load_config", return_value={"path": db_path}):
            with patch("src.backfiller.verify.date") as mock_date:
                mock_date.today.return_value = date(2026, 3, 17)
                mock_date.fromisoformat = date.fromisoformat
                report = run_full_verification(db_path=db_path)

    on_disk.close()

    # No fails, but some warns (e.g., missing fundamentals/news)
    assert report.overall_status == "PASS"
    assert report.warn_count > 0


def test_run_full_verification_overall_fail(db_connection: sqlite3.Connection, tmp_path) -> None:
    """Critical issue: a ticker has 0 OHLCV rows. Verify overall_status='FAIL'."""
    db_path = str(tmp_path / "signals.db")
    on_disk = sqlite3.connect(db_path)
    on_disk.execute("PRAGMA journal_mode=WAL")
    from src.common.db import create_all_tables
    create_all_tables(on_disk)

    # Insert ticker in tickers table but NO ohlcv rows
    on_disk.execute("INSERT INTO tickers (symbol, active) VALUES ('AAPL', 1)")
    on_disk.commit()

    mock_tickers = [{"symbol": "AAPL", "active": True}]

    with patch("src.backfiller.verify.get_active_tickers", return_value=mock_tickers):
        with patch("src.backfiller.verify.load_config", return_value={"path": db_path}):
            with patch("src.backfiller.verify.date") as mock_date:
                mock_date.today.return_value = date(2026, 3, 17)
                mock_date.fromisoformat = date.fromisoformat
                report = run_full_verification(db_path=db_path)

    on_disk.close()

    assert report.overall_status == "FAIL"
    assert report.fail_count >= 1


# ---------------------------------------------------------------------------
# format_verification_report
# ---------------------------------------------------------------------------

def test_format_verification_report(tmp_path) -> None:
    """Verify the formatted string contains section headers, emoji indicators, and is under 4096 chars."""
    checks = [
        CheckResult(name="table_row_counts", status="pass", message="All tables have data"),
        CheckResult(name="ticker_coverage_ohlcv_daily", status="warn", message="2 tickers missing",
                    details=["AAPL missing", "MSFT missing"]),
        CheckResult(name="cross_table_consistency", status="fail", message="1 ticker has no OHLCV",
                    details=["NVDA has no OHLCV rows"]),
    ]
    report = VerificationReport(
        checks=checks,
        overall_status="FAIL",
        pass_count=1,
        warn_count=1,
        fail_count=1,
        timestamp="2026-03-17T00:00:00",
    )

    formatted = format_verification_report(report)

    assert isinstance(formatted, str)
    assert len(formatted) <= 4096
    assert "✅" in formatted
    assert "⚠️" in formatted
    assert "❌" in formatted
    assert "FAIL" in formatted
    assert "2026-03-17" in formatted


def test_format_verification_report_truncates_long_lists() -> None:
    """Create a CheckResult with 50 detail items. Verify the report shows first 20 and a count of the rest."""
    details = [f"Warning item {idx}" for idx in range(50)]
    checks = [
        CheckResult(
            name="date_gaps",
            status="warn",
            message="50 gaps found",
            details=details,
        )
    ]
    report = VerificationReport(
        checks=checks,
        overall_status="PASS",
        pass_count=0,
        warn_count=1,
        fail_count=0,
        timestamp="2026-03-17T00:00:00",
    )

    formatted = format_verification_report(report)

    assert "Warning item 0" in formatted
    assert "Warning item 19" in formatted
    assert "Warning item 20" not in formatted
    assert "30 more" in formatted
    assert len(formatted) <= 4096


# ---------------------------------------------------------------------------
# detect_market_wide_closures
# ---------------------------------------------------------------------------

def test_detect_market_wide_closures_identifies_shared_gaps() -> None:
    """
    A date missing in all 5 tickers (100% >= 80% threshold) is identified
    as a market-wide closure.
    """
    gaps_by_ticker = {
        "AAPL": ["2026-01-19", "2026-02-16"],
        "MSFT": ["2026-01-19", "2026-02-16"],
        "NVDA": ["2026-01-19", "2026-02-16"],
        "TSLA": ["2026-01-19", "2026-02-16"],
        "AMZN": ["2026-01-19", "2026-02-16"],
    }
    closures = detect_market_wide_closures(gaps_by_ticker)

    assert "2026-01-19" in closures
    assert "2026-02-16" in closures


def test_detect_market_wide_closures_ignores_ticker_specific_gaps() -> None:
    """
    A date missing in only 1 out of 5 tickers (20% < 80% threshold) is NOT
    identified as a market-wide closure.
    """
    gaps_by_ticker = {
        "AAPL": ["2026-01-19"],   # only AAPL is missing this date
        "MSFT": [],
        "NVDA": [],
        "TSLA": [],
        "AMZN": [],
    }
    closures = detect_market_wide_closures(gaps_by_ticker)

    assert "2026-01-19" not in closures


def test_detect_market_wide_closures_uses_threshold() -> None:
    """
    With 5 tickers and min_ticker_fraction=0.80, a date missing in 4/5 (80%)
    is a closure; missing in 3/5 (60%) is not.
    """
    gaps_by_ticker = {
        "AAPL": ["2026-01-19", "2026-02-16"],
        "MSFT": ["2026-01-19"],
        "NVDA": ["2026-01-19"],
        "TSLA": ["2026-01-19"],
        "AMZN": [],
    }
    closures = detect_market_wide_closures(gaps_by_ticker, min_ticker_fraction=0.80)

    # 2026-01-19 missing in 4/5 = 80% → should be closure
    assert "2026-01-19" in closures
    # 2026-02-16 missing in 1/5 = 20% → not a closure
    assert "2026-02-16" not in closures


def test_detect_market_wide_closures_returns_empty_for_no_gaps() -> None:
    """All tickers have no gaps → empty closure set."""
    gaps_by_ticker = {"AAPL": [], "MSFT": [], "NVDA": []}
    closures = detect_market_wide_closures(gaps_by_ticker)

    assert closures == set()


def test_detect_market_wide_closures_returns_empty_for_empty_input() -> None:
    """Empty gaps_by_ticker dict → empty closure set."""
    assert detect_market_wide_closures({}) == set()


# ---------------------------------------------------------------------------
# check_date_gaps_all_tickers — market closure auto-detection
# ---------------------------------------------------------------------------

def test_date_gaps_all_tickers_excludes_market_wide_closures(
    db_connection: sqlite3.Connection,
) -> None:
    """
    All 5 tickers missing the same date (a holiday) — auto-detected as a
    market closure and excluded. Result should be 'pass'.
    """
    # 2026-01-19 is MLK Day (market closed) — insert a week of data around it
    # (Mon 2026-01-12 to Fri 2026-01-23, skipping 2026-01-19)
    all_days = _generate_trading_days("2026-01-12", "2026-01-23")
    days_without_holiday = [d for d in all_days if d != "2026-01-19"]

    for ticker in ("AAPL", "MSFT", "NVDA", "TSLA", "AMZN"):
        _insert_ohlcv_rows(db_connection, ticker, days_without_holiday)

    from src.backfiller.verify import check_date_gaps_all_tickers
    result = check_date_gaps_all_tickers(
        db_connection,
        ["AAPL", "MSFT", "NVDA", "TSLA", "AMZN"],
    )

    assert result.status == "pass", f"Expected pass, got warn: {result.message}"


def test_date_gaps_all_tickers_flags_ticker_specific_gaps(
    db_connection: sqlite3.Connection,
) -> None:
    """
    All 5 tickers span the same date range, but AAPL has 10 dates removed
    from the middle (a halt/gap). Other tickers have complete data.
    After market closure auto-detection, AAPL's specific gaps remain and
    the result should be 'warn' with AAPL listed.
    """
    all_days = _generate_trading_days("2026-01-05", "2026-03-13")

    # 4 tickers: full data
    for ticker in ("MSFT", "NVDA", "TSLA", "AMZN"):
        _insert_ohlcv_rows(db_connection, ticker, all_days)

    # AAPL: remove 10 dates from the middle (unique gap, not shared with others)
    gap_dates = set(all_days[20:30])  # 10 specific dates only AAPL is missing
    aapl_days = [d for d in all_days if d not in gap_dates]
    _insert_ohlcv_rows(db_connection, "AAPL", aapl_days)

    from src.backfiller.verify import check_date_gaps_all_tickers
    result = check_date_gaps_all_tickers(
        db_connection,
        ["AAPL", "MSFT", "NVDA", "TSLA", "AMZN"],
    )

    assert result.status == "warn"
    assert any("AAPL" in detail for detail in (result.details or []))
    # The 4 tickers with complete data should not be in issues
    assert not any("MSFT" in detail for detail in (result.details or []))


def test_date_gaps_all_tickers_reports_excluded_count(
    db_connection: sqlite3.Connection,
) -> None:
    """
    When market closures are auto-detected, the pass message mentions
    'Excluded N detected market holidays/closures'.
    """
    all_days = _generate_trading_days("2026-01-12", "2026-01-23")
    days_without_holiday = [d for d in all_days if d != "2026-01-19"]

    for ticker in ("AAPL", "MSFT", "NVDA"):
        _insert_ohlcv_rows(db_connection, ticker, days_without_holiday)

    from src.backfiller.verify import check_date_gaps_all_tickers
    result = check_date_gaps_all_tickers(
        db_connection, ["AAPL", "MSFT", "NVDA"]
    )

    assert "Excluded" in result.message
    assert "market holidays" in result.message


# ---------------------------------------------------------------------------
# check_optional_ticker_coverage
# ---------------------------------------------------------------------------

def test_dividends_coverage_pass_for_partial_coverage(
    db_connection: sqlite3.Connection,
) -> None:
    """
    3/5 tickers have dividend data (60% >= 20% threshold).
    Result should be 'pass' with an informational message.
    """
    for idx, ticker in enumerate(("AAPL", "MSFT", "JPM")):
        db_connection.execute(
            "INSERT OR REPLACE INTO dividends (id, ticker, ex_dividend_date, cash_amount, fetched_at) "
            "VALUES (?, ?, '2026-01-15', 0.25, '2026-01-01')",
            (f"div-{idx}", ticker),
        )
    db_connection.commit()

    result = check_optional_ticker_coverage(
        db_connection, "dividends",
        ["AAPL", "MSFT", "JPM", "AMZN", "NVDA"],
    )

    assert result.status == "pass"
    assert "normal" in result.message
    assert result.data["missing"] == ["AMZN", "NVDA"]


def test_dividends_coverage_warn_below_threshold(
    db_connection: sqlite3.Connection,
) -> None:
    """
    1/10 tickers have dividend data (10% < 20% threshold).
    Result should be 'warn' indicating a possible fetch failure.
    """
    db_connection.execute(
        "INSERT OR REPLACE INTO dividends (id, ticker, ex_dividend_date, cash_amount, fetched_at) "
        "VALUES ('div-1', 'AAPL', '2026-01-15', 0.25, '2026-01-01')"
    )
    db_connection.commit()

    tickers = ["AAPL", "MSFT", "NVDA", "TSLA", "AMZN",
               "GOOGL", "META", "JPM", "BAC", "XOM"]
    result = check_optional_ticker_coverage(db_connection, "dividends", tickers)

    assert result.status == "warn"
    assert "possible fetch failure" in result.message


def test_short_interest_optional_coverage_pass(
    db_connection: sqlite3.Connection,
) -> None:
    """
    4/5 tickers have short interest data (80% >= 20% threshold).
    Result should be 'pass'.
    """
    for ticker in ("AAPL", "MSFT", "NVDA", "TSLA"):
        db_connection.execute(
            "INSERT OR REPLACE INTO short_interest "
            "(ticker, settlement_date, short_interest, days_to_cover, fetched_at) "
            "VALUES (?, '2026-01-15', 1000000, 2.5, '2026-01-01')",
            (ticker,),
        )
    db_connection.commit()

    result = check_optional_ticker_coverage(
        db_connection, "short_interest",
        ["AAPL", "MSFT", "NVDA", "TSLA", "AMZN"],
    )

    assert result.status == "pass"
    assert "normal" in result.message


def test_optional_coverage_pass_when_all_present(
    db_connection: sqlite3.Connection,
) -> None:
    """All tickers have data → pass with informational message (0 missing — normal)."""
    for idx, ticker in enumerate(("AAPL", "MSFT")):
        db_connection.execute(
            "INSERT OR REPLACE INTO dividends (id, ticker, ex_dividend_date, cash_amount, fetched_at) "
            "VALUES (?, ?, '2026-01-15', 0.25, '2026-01-01')",
            (f"div-{idx}", ticker),
        )
    db_connection.commit()

    result = check_optional_ticker_coverage(
        db_connection, "dividends", ["AAPL", "MSFT"]
    )

    assert result.status == "pass"
    assert result.data["missing"] == []
