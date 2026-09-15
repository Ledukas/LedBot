"""Tests for scripts/backup_db.py."""

import os
import sqlite3

import pytest

from scripts import backup_db


class TestRunBackup:
    def test_creates_timestamped_file(self, tmp_sqlite_db, tmp_path):
        backups_dir = tmp_path / "backups"
        result = backup_db.run_backup(tmp_sqlite_db, backups_dir)
        assert result.exists()
        assert result.parent == backups_dir
        assert result.name.startswith("DatabaseLedBot_")

    def test_backup_is_a_valid_independent_copy(self, tmp_sqlite_db, tmp_path):
        backups_dir = tmp_path / "backups"
        result = backup_db.run_backup(tmp_sqlite_db, backups_dir)

        conn = sqlite3.connect(str(result))
        rows = conn.execute("SELECT name FROM members ORDER BY id").fetchall()
        conn.close()
        assert rows == [("Alice",), ("Bob",)]

        # Mutate the source after backup -- the backup should be unaffected.
        source_conn = sqlite3.connect(str(tmp_sqlite_db))
        source_conn.execute("INSERT INTO members (id, name) VALUES (3, 'Carl')")
        source_conn.commit()
        source_conn.close()

        conn = sqlite3.connect(str(result))
        rows_after = conn.execute("SELECT name FROM members ORDER BY id").fetchall()
        conn.close()
        assert rows_after == [("Alice",), ("Bob",)]

    def test_missing_source_raises(self, tmp_path):
        missing = tmp_path / "nope.db"
        with pytest.raises(FileNotFoundError):
            backup_db.run_backup(missing, tmp_path / "backups")

    def test_creates_backups_dir_if_missing(self, tmp_sqlite_db, tmp_path):
        backups_dir = tmp_path / "does" / "not" / "exist"
        result = backup_db.run_backup(tmp_sqlite_db, backups_dir)
        assert result.exists()


class TestPruneOldBackups:
    @staticmethod
    def make_backups(dir_path, n):
        dir_path.mkdir(parents=True, exist_ok=True)
        paths = []
        for i in range(n):
            p = dir_path / f"DatabaseLedBot_{i}.db"
            p.write_text("x")
            os.utime(p, (i, i))  # distinct, increasing mtimes for deterministic ordering
            paths.append(p)
        return paths

    def test_fewer_than_retention_deletes_nothing(self, tmp_path):
        backups_dir = tmp_path / "backups"
        self.make_backups(backups_dir, 3)
        deleted = backup_db.prune_old_backups(backups_dir, retention=5)
        assert deleted == []
        assert len(list(backups_dir.glob("DatabaseLedBot_*.db"))) == 3

    def test_more_than_retention_deletes_oldest(self, tmp_path):
        backups_dir = tmp_path / "backups"
        paths = self.make_backups(backups_dir, 5)  # mtimes 0..4, paths[0] oldest
        deleted = backup_db.prune_old_backups(backups_dir, retention=2)
        remaining = sorted(p.name for p in backups_dir.glob("DatabaseLedBot_*.db"))
        assert remaining == sorted([paths[3].name, paths[4].name])
        assert {p.name for p in deleted} == {paths[0].name, paths[1].name, paths[2].name}

    def test_retention_zero_deletes_everything(self, tmp_path):
        backups_dir = tmp_path / "backups"
        self.make_backups(backups_dir, 3)
        deleted = backup_db.prune_old_backups(backups_dir, retention=0)
        assert len(deleted) == 3
        assert list(backups_dir.glob("DatabaseLedBot_*.db")) == []

    def test_empty_dir_returns_empty(self, tmp_path):
        backups_dir = tmp_path / "backups"
        backups_dir.mkdir()
        assert backup_db.prune_old_backups(backups_dir, retention=5) == []

    def test_non_matching_files_ignored(self, tmp_path):
        backups_dir = tmp_path / "backups"
        backups_dir.mkdir()
        (backups_dir / "not_a_backup.txt").write_text("x")
        (backups_dir / "DatabaseLedBot.db").write_text("x")  # no underscore+timestamp -- doesn't match the glob
        deleted = backup_db.prune_old_backups(backups_dir, retention=0)
        assert deleted == []
        assert (backups_dir / "not_a_backup.txt").exists()
        assert (backups_dir / "DatabaseLedBot.db").exists()


class TestMain:
    def test_success_path(self, tmp_sqlite_db, tmp_path, monkeypatch, capsys):
        backups_dir = tmp_path / "backups"
        monkeypatch.setattr(backup_db, "DB_PATH", tmp_sqlite_db)
        monkeypatch.setattr(backup_db, "BACKUPS_DIR", backups_dir)
        monkeypatch.setattr("sys.argv", ["backup_db.py"])

        exit_code = backup_db.main()

        assert exit_code == 0
        out = capsys.readouterr().out
        assert "Backup created" in out
        assert len(list(backups_dir.glob("DatabaseLedBot_*.db"))) == 1

    def test_failure_path_missing_db(self, tmp_path, monkeypatch, capsys):
        monkeypatch.setattr(backup_db, "DB_PATH", tmp_path / "nope.db")
        monkeypatch.setattr(backup_db, "BACKUPS_DIR", tmp_path / "backups")
        monkeypatch.setattr("sys.argv", ["backup_db.py"])

        exit_code = backup_db.main()

        assert exit_code == 1
        err = capsys.readouterr().err
        assert "Backup FAILED" in err

    def test_retention_argv_is_respected(self, tmp_sqlite_db, tmp_path, monkeypatch, capsys):
        backups_dir = tmp_path / "backups"
        TestPruneOldBackups.make_backups(backups_dir, 3)
        monkeypatch.setattr(backup_db, "DB_PATH", tmp_sqlite_db)
        monkeypatch.setattr(backup_db, "BACKUPS_DIR", backups_dir)
        monkeypatch.setattr("sys.argv", ["backup_db.py", "--retention", "1"])

        exit_code = backup_db.main()

        assert exit_code == 0
        # 3 pre-existing + 1 new = 4, retention=1 keeps only the newest.
        assert len(list(backups_dir.glob("DatabaseLedBot_*.db"))) == 1
