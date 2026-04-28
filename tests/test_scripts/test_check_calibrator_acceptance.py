"""
Tests for scripts/check_calibrator_acceptance.py.

Exercises the snapshot + check subcommands via direct invocation of
`main()` (not subprocess). All Telegram side-effects are monkeypatched —
tests must never hit the real Telegram API.

Exit-code contract (also documented in OPERATIONS.md):
  0 = PASS (no warning)
  0 = PASS WITH WARNING (warning flag in output, exit code unchanged)
  1 = FAIL
  2 = INSUFFICIENT_DATA (date mismatch / missing baseline date in DB / count too low)
"""

from __future__ import annotations

import json
import os
import sqlite3
import sys
from typing import Any

import pytest

from src.common.db import create_all_tables

# Allow importing the script directly without making it a package.
_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
_SCRIPTS_DIR = os.path.join(_REPO_ROOT, "scripts")
if _SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, _SCRIPTS_DIR)

import check_calibrator_acceptance as gate_cli  # noqa: E402


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def empty_db(tmp_path) -> str:
    """A fresh DB file with all tables created and no rows."""
    db_path = str(tmp_path / "gate.db")
    conn = sqlite3.connect(db_path)
    create_all_tables(conn)
    conn.commit()
    conn.close()
    return db_path


def _seed_calibrated(
    db_path: str,
    scoring_date: str,
    ticker_to_score: dict[str, float | None],
) -> None:
    """Seed scores_daily rows on a single date."""
    conn = sqlite3.connect(db_path)
    for ticker_symbol, score_value in ticker_to_score.items():
        conn.execute(
            "INSERT INTO scores_daily (ticker, date, signal, final_score, calibrated_score) "
            "VALUES (?, ?, 'BULLISH', 50.0, ?)",
            (ticker_symbol, scoring_date, score_value),
        )
    conn.commit()
    conn.close()


@pytest.fixture
def stub_telegram(monkeypatch) -> list[tuple[Any, Any, Any]]:
    """
    Replace gate_cli.send_telegram_message with a recorder. Returns the recorder
    list so tests can assert on calls.
    """
    calls: list[tuple[Any, Any, Any]] = []

    def _record(bot_token: Any, chat_id: Any, text: Any) -> int:
        calls.append((bot_token, chat_id, text))
        return 12345

    monkeypatch.setattr(gate_cli, "send_telegram_message", _record)
    # Ensure env vars are set so the wrapper attempts a "send" (which is now a no-op).
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "stub-token")
    monkeypatch.setenv("TELEGRAM_ADMIN_CHAT_ID", "stub-chat")
    return calls


def _run_cli(argv: list[str]) -> int:
    """Invoke gate_cli.main() with argv injected; capture exit code."""
    saved_argv = sys.argv[:]
    sys.argv = ["check_calibrator_acceptance.py", *argv]
    try:
        return gate_cli.main()
    finally:
        sys.argv = saved_argv


# ---------------------------------------------------------------------------
# Snapshot subcommand
# ---------------------------------------------------------------------------

def test_snapshot_writes_correct_json_with_per_ticker_dict(
    empty_db: str, tmp_path, stub_telegram
) -> None:
    """snapshot writes a JSON file with scoring_date, mean, std, count, tickers."""
    scores = {f"T{i}": float(i) for i in range(40)}
    _seed_calibrated(empty_db, "2026-04-25", scores)

    output_path = str(tmp_path / "baseline.json")
    rc = _run_cli([
        "snapshot",
        "--db-path", empty_db,
        "--scoring-date", "2026-04-25",
        "--output", output_path,
    ])

    assert rc == 0
    with open(output_path) as fh:
        snap = json.load(fh)
    assert snap["scoring_date"] == "2026-04-25"
    assert snap["count"] == 40
    assert "tickers" in snap
    assert snap["tickers"]["T0"] == 0.0
    assert snap["tickers"]["T39"] == 39.0


def test_snapshot_uses_explicit_scoring_date_when_provided(
    empty_db: str, tmp_path, stub_telegram
) -> None:
    """When --scoring-date is supplied it is used verbatim, even if newer dates exist."""
    _seed_calibrated(empty_db, "2026-04-23", {"AAPL": 1.0, "MSFT": 2.0})
    _seed_calibrated(empty_db, "2026-04-25", {"AAPL": 9.0, "MSFT": 9.0})

    output_path = str(tmp_path / "snap.json")
    rc = _run_cli([
        "snapshot",
        "--db-path", empty_db,
        "--scoring-date", "2026-04-23",
        "--output", output_path,
    ])

    assert rc == 0
    with open(output_path) as fh:
        snap = json.load(fh)
    assert snap["scoring_date"] == "2026-04-23"
    assert snap["tickers"] == {"AAPL": 1.0, "MSFT": 2.0}


def test_snapshot_auto_discovers_when_scoring_date_omitted(
    empty_db: str, tmp_path, stub_telegram
) -> None:
    """Omitting --scoring-date triggers auto-discovery of the latest non-NULL date."""
    _seed_calibrated(empty_db, "2026-04-23", {"AAPL": 1.0})
    _seed_calibrated(empty_db, "2026-04-25", {"AAPL": 7.0, "MSFT": 7.0})

    output_path = str(tmp_path / "snap.json")
    rc = _run_cli([
        "snapshot",
        "--db-path", empty_db,
        "--output", output_path,
    ])

    assert rc == 0
    with open(output_path) as fh:
        snap = json.load(fh)
    assert snap["scoring_date"] == "2026-04-25"


def test_snapshot_exits_2_when_no_calibrated_data_anywhere(
    empty_db: str, tmp_path, stub_telegram
) -> None:
    """Auto-discovery with no calibrated rows → INSUFFICIENT_DATA exit 2."""
    output_path = str(tmp_path / "snap.json")
    rc = _run_cli([
        "snapshot",
        "--db-path", empty_db,
        "--output", output_path,
    ])
    assert rc == 2


# ---------------------------------------------------------------------------
# Check subcommand
# ---------------------------------------------------------------------------

def _write_baseline(path: str, scoring_date: str, tickers: dict[str, float]) -> None:
    """Write a baseline JSON snapshot file for the check tests."""
    import statistics

    values = list(tickers.values())
    mean_value = sum(values) / len(values) if values else 0.0
    std_value = statistics.pstdev(values) if len(values) > 1 else 0.0
    snap = {
        "scoring_date": scoring_date,
        "mean": mean_value,
        "std": std_value,
        "count": len(values),
        "tickers": tickers,
    }
    with open(path, "w") as fh:
        json.dump(snap, fh)


def test_check_exits_0_on_pass(
    empty_db: str, tmp_path, stub_telegram
) -> None:
    """Identical baseline + current → PASS exit 0."""
    tickers = {f"T{i}": 4.0 for i in range(40)}
    _seed_calibrated(empty_db, "2026-04-25", tickers)

    baseline_path = str(tmp_path / "baseline.json")
    _write_baseline(baseline_path, "2026-04-25", tickers)

    rc = _run_cli([
        "check",
        "--db-path", empty_db,
        "--baseline", baseline_path,
    ])
    assert rc == 0


def test_check_exits_0_on_pass_with_warning(
    empty_db: str, tmp_path, stub_telegram
) -> None:
    """Δ in [70%, 100%) of threshold → exit 0, warning surfaced in Telegram."""
    baseline_tickers = {f"T{i}": 0.0 for i in range(40)}
    # Δ mean = 4.0 → 80% of max_mean_delta=5.0 → warning band
    current_tickers = {f"T{i}": 4.0 for i in range(40)}
    _seed_calibrated(empty_db, "2026-04-25", current_tickers)

    baseline_path = str(tmp_path / "baseline.json")
    _write_baseline(baseline_path, "2026-04-25", baseline_tickers)

    rc = _run_cli([
        "check",
        "--db-path", empty_db,
        "--baseline", baseline_path,
    ])
    assert rc == 0
    # Telegram message must indicate WARNING
    assert any("WARNING" in call[2] for call in stub_telegram)


def test_check_exits_1_on_fail(
    empty_db: str, tmp_path, stub_telegram
) -> None:
    """Δ mean > max_mean_delta → FAIL exit 1."""
    baseline_tickers = {f"T{i}": 0.0 for i in range(40)}
    current_tickers = {f"T{i}": 12.0 for i in range(40)}  # mean shifts by 12
    _seed_calibrated(empty_db, "2026-04-25", current_tickers)

    baseline_path = str(tmp_path / "baseline.json")
    _write_baseline(baseline_path, "2026-04-25", baseline_tickers)

    rc = _run_cli([
        "check",
        "--db-path", empty_db,
        "--baseline", baseline_path,
    ])
    assert rc == 1
    assert any("FAIL" in call[2] for call in stub_telegram)


def test_check_exits_2_when_baseline_date_has_no_current_rows(
    empty_db: str, tmp_path, stub_telegram
) -> None:
    """
    Baseline scoring_date has zero non-NULL calibrated rows in current DB →
    INSUFFICIENT_DATA exit 2. Must NOT silently fall back to a different date.
    """
    # Seed calibrated rows on a DIFFERENT date than the baseline anchor.
    _seed_calibrated(empty_db, "2026-04-26", {f"T{i}": 1.0 for i in range(40)})

    baseline_path = str(tmp_path / "baseline.json")
    _write_baseline(
        baseline_path, "2026-04-25", {f"T{i}": 1.0 for i in range(40)}
    )

    rc = _run_cli([
        "check",
        "--db-path", empty_db,
        "--baseline", baseline_path,
    ])
    assert rc == 2


def test_check_exits_2_when_current_count_below_min(
    empty_db: str, tmp_path, stub_telegram
) -> None:
    """Current count below min_sample_size → INSUFFICIENT_DATA exit 2."""
    _seed_calibrated(
        empty_db, "2026-04-25", {f"T{i}": 1.0 for i in range(5)}  # only 5 rows
    )

    baseline_path = str(tmp_path / "baseline.json")
    _write_baseline(
        baseline_path, "2026-04-25", {f"T{i}": 1.0 for i in range(40)}
    )

    rc = _run_cli([
        "check",
        "--db-path", empty_db,
        "--baseline", baseline_path,
    ])
    assert rc == 2


def test_check_exits_2_when_baseline_date_mismatches_in_data(
    empty_db: str, tmp_path, stub_telegram
) -> None:
    """
    The check subcommand always derives scoring_date FROM the baseline file
    (never queries a fresh "latest" date). When that date has no rows the gate
    must exit 2, not silently fall back.
    """
    # No data at all on the baseline anchor date.
    baseline_path = str(tmp_path / "baseline.json")
    _write_baseline(
        baseline_path, "2026-04-25", {f"T{i}": 1.0 for i in range(40)}
    )

    rc = _run_cli([
        "check",
        "--db-path", empty_db,
        "--baseline", baseline_path,
    ])
    assert rc == 2


# ---------------------------------------------------------------------------
# Telegram robustness
# ---------------------------------------------------------------------------

def test_telegram_failure_does_not_change_exit_code(
    empty_db: str, tmp_path, monkeypatch
) -> None:
    """A raising Telegram send must not crash the gate or change the exit code."""
    tickers = {f"T{i}": 4.0 for i in range(40)}
    _seed_calibrated(empty_db, "2026-04-25", tickers)

    baseline_path = str(tmp_path / "baseline.json")
    _write_baseline(baseline_path, "2026-04-25", tickers)

    def _raises(*_args: Any, **_kwargs: Any) -> int:
        raise RuntimeError("simulated telegram outage")

    monkeypatch.setattr(gate_cli, "send_telegram_message", _raises)
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "stub-token")
    monkeypatch.setenv("TELEGRAM_ADMIN_CHAT_ID", "stub-chat")

    rc = _run_cli([
        "check",
        "--db-path", empty_db,
        "--baseline", baseline_path,
    ])
    assert rc == 0  # still PASS, Telegram failure is non-fatal


def test_telegram_skipped_when_env_missing(
    empty_db: str, tmp_path, monkeypatch
) -> None:
    """Missing TELEGRAM_BOT_TOKEN means no send attempt; exit code unaffected."""
    tickers = {f"T{i}": 4.0 for i in range(40)}
    _seed_calibrated(empty_db, "2026-04-25", tickers)
    baseline_path = str(tmp_path / "baseline.json")
    _write_baseline(baseline_path, "2026-04-25", tickers)

    calls: list[Any] = []

    def _record(*args: Any, **_kwargs: Any) -> int:
        calls.append(args)
        return 1

    monkeypatch.setattr(gate_cli, "send_telegram_message", _record)
    monkeypatch.delenv("TELEGRAM_BOT_TOKEN", raising=False)
    monkeypatch.delenv("TELEGRAM_ADMIN_CHAT_ID", raising=False)
    monkeypatch.delenv("TELEGRAM_CHAT_ID", raising=False)

    rc = _run_cli([
        "check",
        "--db-path", empty_db,
        "--baseline", baseline_path,
    ])
    assert rc == 0
    assert calls == []  # send_telegram_message not invoked
