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
