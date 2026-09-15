"""Tests for the persistent weekly-run marker.

The marker exists because an in-memory one is lost on restart, and systemd
runs the bot with Restart=on-failure -- so a crash inside the 2 AM Saturday
hour would restart and run the whole weekly cycle again, producing the
duplicate weekly report the loop is supposed to prevent.
"""

import asyncio
import sqlite3

import pytest

import Functions


@pytest.fixture
def temp_functions_db(tmp_path, monkeypatch):
    """Point Functions' module-level connection at a throwaway database so
    these tests never touch the real DatabaseLedBot.db."""
    db_path = tmp_path / "marker.db"
    conn = sqlite3.connect(str(db_path))
    monkeypatch.setattr(Functions, "conn", conn)
    monkeypatch.setattr(Functions, "c", conn.cursor())
    yield db_path
    conn.close()


RUN_KEY = "GP2026_09_12"


def test_unmarked_week_reads_as_not_started(temp_functions_db):
    assert asyncio.run(Functions.weekly_run_already_started(RUN_KEY)) is False


def test_marked_week_reads_as_started(temp_functions_db):
    asyncio.run(Functions.mark_weekly_run_started(RUN_KEY))
    assert asyncio.run(Functions.weekly_run_already_started(RUN_KEY)) is True


def test_marker_is_per_week(temp_functions_db):
    asyncio.run(Functions.mark_weekly_run_started(RUN_KEY))
    assert asyncio.run(Functions.weekly_run_already_started("GP2026_09_19")) is False


def test_marking_twice_is_harmless(temp_functions_db):
    asyncio.run(Functions.mark_weekly_run_started(RUN_KEY))
    asyncio.run(Functions.mark_weekly_run_started(RUN_KEY))
    assert asyncio.run(Functions.weekly_run_already_started(RUN_KEY)) is True


def test_marker_survives_a_process_restart(temp_functions_db, monkeypatch):
    """The finding this fixes: the bot runs the job at 02:00, is restarted at
    02:20 by systemd, and must not run the whole cycle a second time."""
    asyncio.run(Functions.mark_weekly_run_started(RUN_KEY))

    # A restarted process: brand new connection to the same file, exactly as
    # module import would create. An in-memory marker would be gone here.
    restarted = sqlite3.connect(str(temp_functions_db))
    monkeypatch.setattr(Functions, "conn", restarted)
    monkeypatch.setattr(Functions, "c", restarted.cursor())
    try:
        assert asyncio.run(Functions.weekly_run_already_started(RUN_KEY)) is True
    finally:
        restarted.close()


def test_loop_gate_blocks_the_restart_rerun(temp_functions_db):
    """End to end through the pure decision function: Saturday, 2 AM, already
    marked -> the loop must not fire again."""
    import logic
    from datetime import datetime

    saturday_0220 = datetime(2026, 9, 12, 2, 20)
    assert logic.should_run_weekly_gp(saturday_0220, False) is True

    asyncio.run(Functions.mark_weekly_run_started(RUN_KEY))
    already = asyncio.run(Functions.weekly_run_already_started(RUN_KEY))
    assert logic.should_run_weekly_gp(saturday_0220, already) is False
