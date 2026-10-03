"""Stand-alone GP audit -- the same report !gp_audit posts, from a shell.

Usage:
    python scripts/gp_audit.py [--weeks N] [--hits N] [--min-history N] [--guild NAME]

Every threshold and every decision lives in logic.py, shared with the bot, so
this cannot drift from what gets posted on a Saturday; what is duplicated here
is the SELECT, deliberately. Functions.py is the natural home for that query
and holds its own copy, but importing it builds a live Google Sheets client and
reads .env at import time, which would cost this script the one property worth
having: it runs against a database copy on any machine, with no credentials.

The output is the embed's title and body verbatim, Markdown and all, so this
doubles as a preview of what a moderator will see.
"""

import argparse
import re
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import logic

REPO_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = REPO_ROOT / "DatabaseLedBot.db"

GP_COLUMN_RE = re.compile(r"^GP(\d{4})_(\d{2})_(\d{2})$")


def gp_snapshot_columns(cursor, table: str) -> list[str]:
    """Every weekly snapshot column of the table, oldest first."""
    names = [row[1] for row in cursor.execute(f"PRAGMA table_info({table})")]
    dated = [(m.groups(), n) for m, n in ((GP_COLUMN_RE.match(n), n) for n in names) if m]
    return [name for _, name in sorted(dated)]


def load_series(cursor, guild: str) -> dict[str, list[int | None]]:
    """Full weekly-gain history per current in-game member, oldest week first."""
    gained = logic.table_name(guild, "GP_gained")
    game = logic.table_name(guild, "game")
    columns = gp_snapshot_columns(cursor, gained)

    rows = cursor.execute(
        f"SELECT r.G_NAME, g.Name, {', '.join('g.' + c for c in columns)} "
        f"FROM {gained} AS g "
        f"JOIN {game} AS r ON r.G_ID = g.G_ID"
    )
    members = {}
    for current_name, gp_name, *gains in rows:
        label = current_name if current_name == gp_name else f"{current_name} (was {gp_name})"
        members[label] = [logic.parse_gp_value(value) for value in gains]
    return members


def main() -> int:
    parser = argparse.ArgumentParser(description="Flag unusual weekly GP gains for review.")
    parser.add_argument("--weeks", type=int, default=logic.GP_AUDIT_WEEKS,
                        help=f"Window to look back over (default: {logic.GP_AUDIT_WEEKS})")
    parser.add_argument("--hits", type=int, default=logic.GP_AUDIT_HITS,
                        help=f"Weeks above {logic.GP_GAIN_BAR} needed in that window "
                             f"(default: {logic.GP_AUDIT_HITS})")
    parser.add_argument("--min-history", type=int, default=logic.GP_AUDIT_MIN_HISTORY,
                        help=f"Skip members with fewer recorded weeks "
                             f"(default: {logic.GP_AUDIT_MIN_HISTORY})")
    parser.add_argument("--guild", choices=logic.GUILD_NAMES, help="Limit to one guild")
    args = parser.parse_args()

    if not DB_PATH.exists():
        print(f"Database not found: {DB_PATH}", file=sys.stderr)
        return 1

    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    cursor = conn.cursor()

    for guild in ([args.guild] if args.guild else logic.GUILD_NAMES):
        members = load_series(cursor, guild)
        sustained, spiked = logic.filter_gp_audit(
            members, weeks=args.weeks, hits=args.hits, min_history=args.min_history
        )
        print(f"\n{'-' * 72}")
        print(f"{len(members)} on the {guild} roster")
        body = logic.format_gp_audit(sustained, spiked, weeks=args.weeks, hits=args.hits)
        print(f"{guild} -- GP audit")
        print(body if body is not None else "Nobody flagged.")

    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
