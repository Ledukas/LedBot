import sqlite3

import pytest


@pytest.fixture
def tmp_sqlite_db(tmp_path):
    """A small, real sqlite database file with one table and a couple of rows."""
    db_path = tmp_path / "source.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE members (id INTEGER PRIMARY KEY, name TEXT)")
    conn.executemany(
        "INSERT INTO members (id, name) VALUES (?, ?)",
        [(1, "Alice"), (2, "Bob")],
    )
    conn.commit()
    conn.close()
    return db_path


@pytest.fixture
def no_bot_run(monkeypatch):
    """Patches commands.Bot.run so importing LedBotCode never actually tries to
    connect to Discord. Returns the mock so a test can assert it wasn't called
    (pinning the `if __name__ == "__main__":` guard against regression).
    """
    from discord.ext import commands
    from unittest.mock import Mock

    mock_run = Mock()
    monkeypatch.setattr(commands.Bot, "run", mock_run)
    return mock_run


@pytest.fixture
def temp_functions_db(tmp_path, monkeypatch, request):
    """Point Functions' module-level connection, and the path GP_databases
    opens, at a throwaway database so tests never touch the real
    DatabaseLedBot.db. The roster counters are module state too, so they're
    reset for each test.

    If a test ever swaps sys.modules['Functions'] without restoring it, this
    would patch the fresh module while the test file still calls the one it
    imported -- whose connection is the real database. That happened once; the
    check below makes it fail loudly rather than write to live data. It
    compares against the requesting test module's own binding, so conftest
    itself never has to import Functions.
    """
    import Functions
    held = getattr(request.module, "Functions", Functions)
    assert held is Functions, (
        "sys.modules['Functions'] was replaced by an earlier test and not restored"
    )

    db_path = tmp_path / "functions.db"
    conn = sqlite3.connect(str(db_path))
    monkeypatch.setattr(Functions, "conn", conn)
    monkeypatch.setattr(Functions, "c", conn.cursor())
    # GP_databases opens its own connection on DB_PATH; without this, one
    # called with no db_path would write to the real database.
    monkeypatch.setattr(Functions, "DB_PATH", str(db_path))
    monkeypatch.setattr(Functions, "weekly_job_depth", 0)
    monkeypatch.setattr(Functions, "roster_generation", 0)
    yield db_path
    conn.close()
