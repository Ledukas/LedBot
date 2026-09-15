"""Stand-alone SQLite backup for LedBot.

Uses SQLite's built-in online backup API, which produces a consistent
snapshot even while another process (the bot) has the database open.

Usage:
    python scripts/backup_db.py [--retention N]

The data only changes on the weekly Saturday-2AM GP cycle, so LedBotCode.py
calls run_backup() directly at the end of that job rather than this running
on its own separate schedule — a backup right after the data changes is more
useful than a fixed-clock one that would mostly just copy unchanged data.
The `!backup` Discord command also calls run_backup() for on-demand backups,
so there is a single source of truth for how backups are made. This script
is still runnable standalone by hand any time (e.g. over SSH) if ever needed.
"""

import argparse
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = REPO_ROOT / "DatabaseLedBot.db"
BACKUPS_DIR = REPO_ROOT / "Backups"
DB_STEM = "DatabaseLedBot"
DEFAULT_RETENTION = 52  # ~1 year of backups at the weekly cadence data actually changes


def run_backup(db_path: Path = DB_PATH, backups_dir: Path = BACKUPS_DIR) -> Path:
    """Create a timestamped backup of db_path inside backups_dir and return its path."""
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")

    backups_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    dest_path = backups_dir / f"{DB_STEM}_{timestamp}.db"

    source = sqlite3.connect(str(db_path))
    try:
        dest = sqlite3.connect(str(dest_path))
        try:
            source.backup(dest)
        finally:
            dest.close()
    finally:
        source.close()

    return dest_path


def prune_old_backups(backups_dir: Path = BACKUPS_DIR, retention: int = DEFAULT_RETENTION) -> list[Path]:
    """Delete all but the `retention` most recent backups. Returns the list of deleted paths."""
    backups = sorted(
        backups_dir.glob(f"{DB_STEM}_*.db"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    stale = backups[retention:]
    for path in stale:
        path.unlink()
    return stale


def main() -> int:
    parser = argparse.ArgumentParser(description="Back up DatabaseLedBot.db with rotation.")
    parser.add_argument(
        "--retention",
        type=int,
        default=DEFAULT_RETENTION,
        help=f"Number of backups to keep (default: {DEFAULT_RETENTION})",
    )
    args = parser.parse_args()

    try:
        # Referencing DB_PATH/BACKUPS_DIR here (rather than relying on
        # run_backup's own default arguments, which are bound once at import
        # time) means tests can monkeypatch these module constants and have
        # main() actually pick up the override.
        backup_path = run_backup(DB_PATH, BACKUPS_DIR)
        deleted = prune_old_backups(BACKUPS_DIR, retention=args.retention)
    except Exception as e:
        print(f"Backup FAILED: {e}", file=sys.stderr)
        return 1

    print(f"Backup created: {backup_path}")
    if deleted:
        print(f"Pruned {len(deleted)} old backup(s): {', '.join(p.name for p in deleted)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
