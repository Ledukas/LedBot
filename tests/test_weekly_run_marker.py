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


RUN_KEY = "GP2026_09_12"


def test_unmarked_week_reads_as_not_started(temp_functions_db):
    assert Functions.weekly_run_times(RUN_KEY)[0] is None


def test_marked_week_reads_as_started(temp_functions_db):
    asyncio.run(Functions.mark_weekly_run_started(RUN_KEY))
    assert Functions.weekly_run_times(RUN_KEY)[0] is not None


def test_marker_is_per_week(temp_functions_db):
    asyncio.run(Functions.mark_weekly_run_started(RUN_KEY))
    assert Functions.weekly_run_times("GP2026_09_19")[0] is None


def test_marking_twice_is_harmless(temp_functions_db):
    asyncio.run(Functions.mark_weekly_run_started(RUN_KEY))
    asyncio.run(Functions.mark_weekly_run_started(RUN_KEY))
    assert Functions.weekly_run_times(RUN_KEY)[0] is not None


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
        assert Functions.weekly_run_times(RUN_KEY)[0] is not None
    finally:
        restarted.close()


def test_loop_gate_blocks_the_restart_rerun(temp_functions_db):
    """End to end through the pure decision function: an attempt started at
    02:20, the bot restarts at 02:25 -> the loop must not fire again until the
    retry interval has passed, because the start time is in the database."""
    import logic
    from datetime import datetime

    saturday_0220 = datetime(2026, 9, 12, 2, 20)
    assert logic.should_run_weekly_gp(saturday_0220, False, None) is True

    asyncio.run(Functions.mark_weekly_run_started(RUN_KEY, saturday_0220))
    started_at, _ = Functions.weekly_run_times(RUN_KEY)
    assert logic.should_run_weekly_gp(datetime(2026, 9, 12, 2, 25), False, started_at) is False
    assert logic.should_run_weekly_gp(datetime(2026, 9, 12, 2, 30), False, started_at) is True


def test_a_new_attempt_keeps_the_reminder_time(temp_functions_db):
    """An upsert, not INSERT OR REPLACE, which would wipe reminded_at."""
    from datetime import datetime
    Functions.mark_snapshot_reminded(RUN_KEY, datetime(2026, 9, 13, 0, 5))
    asyncio.run(Functions.mark_weekly_run_started(RUN_KEY, datetime(2026, 9, 13, 9, 0)))
    assert Functions.weekly_run_times(RUN_KEY) == (
        datetime(2026, 9, 13, 9, 0), datetime(2026, 9, 13, 0, 5),
    )


def test_the_reminder_time_survives_a_restart(temp_functions_db, monkeypatch):
    from datetime import datetime
    Functions.mark_snapshot_reminded(RUN_KEY, datetime(2026, 9, 13, 0, 5))
    restarted = sqlite3.connect(str(temp_functions_db))
    monkeypatch.setattr(Functions, "conn", restarted)
    monkeypatch.setattr(Functions, "c", restarted.cursor())
    try:
        assert Functions.weekly_run_times(RUN_KEY)[1] == datetime(2026, 9, 13, 0, 5)
    finally:
        restarted.close()


def test_a_reminder_only_row_is_not_an_attempt_or_an_interruption(temp_functions_db):
    """The bot was down all Saturday: the reminder creates the row."""
    from datetime import datetime
    Functions.mark_snapshot_reminded(RUN_KEY, datetime(2026, 9, 13, 0, 5))
    assert Functions.weekly_run_times(RUN_KEY)[0] is None
    assert Functions.weekly_run_times(RUN_KEY)[0] is None
    assert Functions.interrupted_weekly_run() is None


def test_an_old_table_gets_the_reminder_column(temp_functions_db):
    Functions.c.execute("CREATE TABLE weekly_runs (run_key TEXT PRIMARY KEY, started_at TEXT, ended_at TEXT)")
    Functions.c.execute("INSERT INTO weekly_runs VALUES ('GP2026_09_05', '2026-09-05T02:00:00', '2026-09-05T02:03:00')")
    assert Functions.weekly_run_times('GP2026_09_05')[1] is None
    columns = [row[1] for row in Functions.c.execute("PRAGMA table_info(weekly_runs)")]
    assert 'reminded_at' in columns
