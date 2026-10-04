from datetime import datetime, timedelta
import sqlite3
import inspect
import discord
import os
from dotenv import load_dotenv
import io
import json
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials
import asyncio
from contextlib import contextmanager

import logic
from scripts.backup_db import run_manual_backup

SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
CREDS = Credentials.from_service_account_file(
    "service_account.json",
    scopes=SCOPES
    )

load_dotenv()
# Relative on purpose: the bot runs from the repo root (systemd sets
# WorkingDirectory). GP_databases opens its own connection on the same path.
DB_PATH = 'DatabaseLedBot.db'
# WAL lets the weekly job and a moderator command touch the database at the
# same moment without hitting "database is locked".
conn = sqlite3.connect(DB_PATH)
conn.execute("PRAGMA journal_mode=WAL")
c = conn.cursor()

GP_prefix = 'GP'
# GP rank thresholds live in logic.RANK_THRESHOLDS (single source of truth).


WB_sheet_name = os.environ.get('WB_SHEET_NAME')
WB_spreadsheet_id = os.environ.get('WB_SPREADSHEET_ID')

def _sync_fetch_cell(spreadsheet_id: str, sheet_name: str, cell: str):
    """Blocking Google API call (runs in thread)."""
    service = build("sheets", "v4", credentials=CREDS)
    range_name = f"{sheet_name}!{cell}"

    result = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=range_name,
        valueRenderOption="FORMATTED_VALUE",
        dateTimeRenderOption="FORMATTED_STRING"
    ).execute()

    values = result.get("values", [])
    return logic.extract_cell_value(values)

async def get_cell_value(cell: str) -> str:
    # Errors propagate: caught here and returned as text, a failure was posted
    # under the "Boss Strategy" heading as though it were the strategy.
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(
        None,
        _sync_fetch_cell,
        WB_spreadsheet_id,
        WB_sheet_name,
        cell
    )

#get date
def get_date(today=None):
    """Column names for the week containing `today`: its Saturday's snapshot
    and the three before it."""
    today = today or datetime.today()
    saturday = today - timedelta(days=(today.weekday() - 5) % 7)
    column_name4, column_name3, column_name2, column_name1 = (
        logic.snapshot_column(saturday - timedelta(days=7 * weeks)) for weeks in (3, 2, 1, 0)
    )
    column_names_int = [column_name4, column_name3, column_name2, column_name1]
    return {
        "column_name1": column_name1,
        "column_name2": column_name2,
        "column_name3": column_name3,
        "column_name4": column_name4,
        "column_names": ['Name', 'G_ID', *column_names_int],
        "column_names_int": column_names_int,
    }

WEEKLY_RUNS_TABLE = 'weekly_runs'


def _ensure_weekly_runs_table():
    c.execute(
        f"CREATE TABLE IF NOT EXISTS {WEEKLY_RUNS_TABLE} "
        "(run_key TEXT PRIMARY KEY, started_at TEXT, ended_at TEXT, reminded_at TEXT, attempts INTEGER)"
    )
    columns = [row[1] for row in c.execute(f"PRAGMA table_info({WEEKLY_RUNS_TABLE})")]
    if 'ended_at' not in columns:
        # Every run recorded before this column existed did end; without the
        # backfill each one would be reported as interrupted.
        c.execute(f"ALTER TABLE {WEEKLY_RUNS_TABLE} ADD COLUMN ended_at TEXT")
        c.execute(f"UPDATE {WEEKLY_RUNS_TABLE} SET ended_at = started_at")
    if 'reminded_at' not in columns:
        c.execute(f"ALTER TABLE {WEEKLY_RUNS_TABLE} ADD COLUMN reminded_at TEXT")
    if 'attempts' not in columns:
        c.execute(f"ALTER TABLE {WEEKLY_RUNS_TABLE} ADD COLUMN attempts INTEGER")
    conn.commit()


def mark_weekly_runs_ended():
    _ensure_weekly_runs_table()
    c.execute(
        f"UPDATE {WEEKLY_RUNS_TABLE} SET ended_at = ? WHERE ended_at IS NULL",
        (datetime.now().isoformat(timespec='seconds'),),
    )
    conn.commit()


def interrupted_weekly_run():
    """(run_key, started_at, attempts) of a weekly run that started and never
    ended -- the process died mid-cycle, taking its unposted report with it --
    or None. Only meaningful when no weekly job is running in this process."""
    _ensure_weekly_runs_table()
    return c.execute(
        f"SELECT run_key, started_at, COALESCE(attempts, 1) FROM {WEEKLY_RUNS_TABLE} "
        "WHERE ended_at IS NULL AND started_at IS NOT NULL ORDER BY started_at DESC LIMIT 1"
    ).fetchone()


async def mark_weekly_run_started(run_key: str, started_at: datetime | None = None) -> None:
    """Record that an attempt at this GP week's cycle has started.

    Recorded at the start, not on success: the loop spaces its Saturday
    retries from this time, and because it is in the database a crash-restart
    can't retry any sooner. An upsert rather than INSERT OR REPLACE, which
    would wipe reminded_at along with the row. attempts lets a crash on every
    retry be reported once rather than on every restart.
    """
    _ensure_weekly_runs_table()
    c.execute(
        f"INSERT INTO {WEEKLY_RUNS_TABLE} (run_key, started_at, attempts) VALUES (?, ?, 1) "
        "ON CONFLICT(run_key) DO UPDATE SET started_at = excluded.started_at, ended_at = NULL, "
        "attempts = COALESCE(attempts, 0) + 1",
        (run_key, (started_at or datetime.now()).isoformat(timespec='seconds')),
    )
    conn.commit()


def mark_snapshot_reminded(run_key: str, now: datetime) -> None:
    """Record that the mod channel was reminded this week's snapshot is missing.

    When nothing was attempted -- the bot was down all Saturday -- this creates
    the row, ended and with no started_at, so it never reads as an interrupted
    run or as an attempt.
    """
    _ensure_weekly_runs_table()
    stamp = now.isoformat(timespec='seconds')
    c.execute(
        f"INSERT INTO {WEEKLY_RUNS_TABLE} (run_key, ended_at, reminded_at) VALUES (?, ?, ?) "
        "ON CONFLICT(run_key) DO UPDATE SET reminded_at = excluded.reminded_at",
        (run_key, stamp, stamp),
    )
    conn.commit()


def weekly_run_times(run_key: str) -> tuple[datetime | None, datetime | None]:
    """(last attempt's start, last reminder) for a GP week; None where never."""
    _ensure_weekly_runs_table()
    row = c.execute(
        f"SELECT started_at, reminded_at FROM {WEEKLY_RUNS_TABLE} WHERE run_key = ?", (run_key,)
    ).fetchone() or (None, None)
    return tuple(datetime.fromisoformat(value) if value else None for value in row)


# How many weekly jobs are running (a counter, not a flag: a moderator's
# !GP_weekly can overlap the scheduled run, and a flag would be cleared by
# whichever finished first while the other was still between its export and
# GP_databases). roster_generation is bumped whenever {guild}_game is written by
# anything other than the invite poll, so a poll whose roster read was already
# in flight can tell a fresher roster landed meanwhile and must not replace it.
weekly_job_depth = 0
roster_generation = 0


@contextmanager
def weekly_job():
    """Mark the weekly cycle as running while the body executes.

    The invite poll refreshes {guild}_game mid-week. The weekly job exports
    each guild's roster with an await per guild, then GP_databases reads them
    and the snapshot check reads them again, so a poll write landing in
    between would have this week's GP computed against a different roster
    from the one just exported.
    """
    global weekly_job_depth, roster_generation
    weekly_job_depth += 1
    roster_generation += 1
    try:
        yield
    finally:
        weekly_job_depth -= 1


SNAPSHOT_LOOKBACK_WEEKS = 52


async def latest_snapshot_day(today=None):
    """(a day in the latest week with a snapshot, that week's column).

    Walks back from this week to the newest one that was taken: this week is
    missing on Saturday before the 2 AM run, and so is last week when a whole
    Saturday failed and the next one hasn't filled it in yet. Falls back to
    this week if nothing within a year was taken.
    """
    today = today or datetime.today()
    day = today
    for _ in range(SNAPSHOT_LOOKBACK_WEEKS):
        run_key = (get_date(day))["column_name1"]
        if snapshot_taken(run_key):
            return day, run_key
        day -= timedelta(days=7)
    return today, (get_date(today))["column_name1"]


#get the dataframe
async def GP_dataframe(IOguild, today=None):
    """The guild's last four weeks of gains, up to the week containing `today`.

    Without `today`, the latest week that was taken -- so a missed Saturday
    doesn't make it ask for a column that was never written.
    """
    table_name_gained = logic.table_name(IOguild, 'GP_gained')
    if today is None:
        today, _ = await latest_snapshot_day()

    column_names_dict = get_date(today)
    column_names = column_names_dict["column_names"]
    column_names_int = column_names_dict["column_names_int"]
    
    query = f"SELECT {', '.join(column_names)} FROM {table_name_gained}"
    cursor = conn.execute(query)
    rows = cursor.fetchall()

    # Deliberately not wrapped in try/except: this used to catch, print, and
    # then fall through to `return monthly_gp_df`, which was never bound on the
    # failure path, so the caller got an UnboundLocalError instead of the real
    # error. Let it propagate to the caller, which reports it.
    return logic.build_gp_dataframe(rows, column_names, column_names_int, GP_prefix)

#GP roles and clammies for Aetherians
async def GP_roles(bot, monthly_gp_df):
    """Sync Aetherian rank roles and the Monthly Top role.

    Returns None when the sync completed, or a short reason string when it was
    skipped or only partly done, so the caller does not announce success for
    work that did not happen.
    """
    members_table = logic.table_name(logic.RANK_ROLE_GUILD, 'members')
    game_table = logic.table_name(logic.RANK_ROLE_GUILD, 'game')
    c.execute(f'''SELECT {members_table}.D_ID, {game_table}.GP
                FROM {game_table} JOIN {members_table}
                ON {game_table}.G_ID = {members_table}.G_ID''')
    result = c.fetchall()
    guild = bot.get_guild(809954021028134943)
    if guild is None:
        # Every role lookup below hangs off this; without the check they all
        # fail with an unhelpful AttributeError on None.
        print("GP_roles: Discord server not in cache, skipping the role sync")
        return "the Discord server was not in cache"
    # One member Discord refuses (a role above the bot's, say) used to abort
    # the sync for everyone after them, Monthly Top included.
    failures = []

    async def change(action, member, role):
        try:
            await action(role)
            return True
        except discord.HTTPException as e:
            print(f"GP_roles: could not update {member}: {e}")
            failures.append(f"{member.display_name}: {e}")
            return False

    for row in result:
        D_ID = row[0]
        GP = row[1]
        member = guild.get_member(D_ID)

        if member is None:
            continue

        target_role_name = logic.compute_gp_rank_role(GP, logic.RANK_THRESHOLDS)
        if target_role_name is None:
            continue

        role2give = discord.utils.get(guild.roles, name=target_role_name)
        if role2give is None:
            print(f"Role '{target_role_name}' does not exist in the Discord server, skipping")
            continue
        if role2give not in member.roles:
            if not await change(member.add_roles, member, role2give):
                continue
            # Only remove the immediately-adjacent lower rank (promotion-only sync,
            # matching the original behavior -- this doesn't demote members whose
            # GP has dropped, and doesn't strip every other rank role they hold).
            target_index = logic.RANK_ROLE_NAMES.index(target_role_name)
            if target_index > 0:
                lower_rank_name = logic.RANK_ROLE_NAMES[target_index - 1]
                role2remove = discord.utils.get(guild.roles, name=lower_rank_name)
                if role2remove is None:
                    print(f"Role '{lower_rank_name}' does not exist, leaving it in place")
                else:
                    await change(member.remove_roles, member, role2remove)

    # Rebind rather than dropna(inplace=True): this dataframe belongs to the
    # caller, which passes the same object on to red_gp afterwards. Mutating it
    # here dropped every row with an NA in any column from the low-GP report --
    # so a member who joined less than three weeks ago, and therefore has no
    # value in the oldest snapshot column, was silently never flagged.
    monthly_gp_df = monthly_gp_df.dropna()
    df_clammies = logic.filter_top_average(monthly_gp_df)
    list_clammies = df_clammies['G_ID'].tolist()
    clammy = discord.utils.get(guild.roles, name = 'Monthly Top')
    aeth_duck = discord.utils.get(guild.roles, name = 'Aetherian Duck')
    booster_duck = discord.utils.get(guild.roles, name = 'Booster (For DUCK)')
    if clammy is None:
        print("Role 'Monthly Top' does not exist in the Discord server, skipping the Monthly Top sync")
        return logic.format_role_sync_status(
            "rank roles were synced, but the 'Monthly Top' role does not exist", failures
        )
    # The duck pairing only makes sense when both roles resolve; without this,
    # a missing 'Aetherian Duck' would reach add_roles(None) and crash.
    sync_ducks = aeth_duck is not None and booster_duck is not None
    if not sync_ducks:
        print("'Aetherian Duck' or 'Booster (For DUCK)' does not exist, skipping the duck pairing")
    query = f"SELECT D_ID FROM {members_table} WHERE G_ID IN ({','.join(['?']*len(list_clammies))})"
    cursor = c.execute(query, list_clammies)
    rows = cursor.fetchall()
    d_ids = [row[0] for row in rows]

    #remove clammy
    members_with_clammy = [member for member in guild.members if clammy in member.roles]
    for member in members_with_clammy:
        if member.id not in d_ids:
            await change(member.remove_roles, member, clammy)
            if sync_ducks and booster_duck in member.roles:
                await change(member.remove_roles, member, aeth_duck)

    #give clammy
    for d_id in d_ids:
        member = guild.get_member(d_id)
        print(member)
        if member is None:
            print("member is None, error. Skipping")
            continue
        if clammy not in member.roles:
            await change(member.add_roles, member, clammy)
            if sync_ducks and booster_duck in member.roles:
                await change(member.add_roles, member, aeth_duck)

    return logic.format_role_sync_status(
        None if sync_ducks
        else "roles were synced, but the duck pairing was skipped (a duck role is missing)",
        failures,
    )
                
#red GP
def red_gp_report(monthly_gp_df, IOguild):
    """Members below the guild's weekly GP bar, as embeds; [] when nobody is.

    Named from {guild}_game rather than the GP table, whose names are frozen at
    the member's first snapshot -- the same reason _load_gp_series joins there.
    """
    Blacklist = [
        'bvK1B5ngXtgiw5MV95mE6BOP2rN2', #Ledukas, Aetherians
        '0dzrUrtCeOdllJBa8LXoYQCo4Fv1', #Ledukas, Pretherians
        '4EwZK8w84gR6ESP0YAjiXeP03n62', #Led-Bot, Aetherians
        'TQvhMJ1oAIfRXrvffVGN3Jy0Zdi1' #Led_Bot, Pretherians
        ]
    try:
        names_by_gid = dict(c.execute(
            f"SELECT G_ID, G_NAME FROM {logic.table_name(IOguild, 'game')}"
        ).fetchall())
        df_red_gp = logic.filter_red_gp(
            logic.current_names(monthly_gp_df, names_by_gid), IOguild, Blacklist
        )
        return logic.format_red_gp(IOguild, df_red_gp)
    except Exception as e:
        print("line: " + str(inspect.currentframe().f_lineno) + "\n error: " + str(e))
        return [logic.failure_embed(f"{IOguild} -- red GP failed", e)]

def _table_columns(cursor, table):
    return [row[1] for row in cursor.execute(f"PRAGMA table_info({table})")]


def _gp_snapshot_columns(table_name_gained):
    """Every weekly snapshot column of the table, oldest first."""
    return logic.sorted_snapshot_columns(_table_columns(c, table_name_gained))


def _measured_columns(cursor, IOguild):
    """The snapshot weeks whose {guild}_GP column holds values.

    By data, not by the column existing: the old GP_databases could fail after
    its first ALTER TABLE had autocommitted, leaving an all-NULL column that
    was never a measurement. _GP holds only measured totals -- a missed week
    spread by a catch-up is written to _GP_gained alone -- so this is also the
    record of which weeks were estimated. One scan: COUNT() of every column.
    """
    table = logic.table_name(IOguild, 'GP')
    columns = logic.sorted_snapshot_columns(_table_columns(cursor, table))
    if not columns:
        return set()
    counts = cursor.execute(
        f"SELECT {', '.join(f'COUNT({column})' for column in columns)} FROM {table}"
    ).fetchone()
    return {column for column, count in zip(columns, counts) if count}


def _is_measured(cursor, IOguild, column):
    """_measured_columns for a single week, without scanning every column."""
    table = logic.table_name(IOguild, 'GP')
    if column not in _table_columns(cursor, table):
        return False
    return cursor.execute(f"SELECT 1 FROM {table} WHERE {column} IS NOT NULL LIMIT 1").fetchone() is not None


def _previous_measured(measured, run_key):
    """The latest measured week before run_key, or None -- what this week's
    gain is measured against. A half-added all-NULL column is skipped."""
    target = logic.snapshot_date(run_key)
    earlier = [column for column in measured if logic.snapshot_date(column) < target]
    return max(earlier, key=logic.snapshot_date, default=None)


def _load_gp_series(IOguild):
    """Full weekly-gain history per current in-game member, oldest week first.

    Joined and labelled on {guild}_game rather than on the GP table. Joining by
    G_ID is what survives a character rename, and the GP tables keep whatever
    name the member had when GP_databases first inserted their row, so those
    names go stale -- reporting one would send a moderator looking for somebody
    who, under that name, is not in the guild list at all.

    The whole history is read, not just the audited window: logic.audit_member
    has to know which week is a member's first (that one is their lifetime GP,
    not a week's worth) and how many weeks they have in total.

    Returns (members, the series positions whose gain was spread over a
    missed snapshot).
    """
    table_name_gained = logic.table_name(IOguild, 'GP_gained')
    table_name_game = logic.table_name(IOguild, 'game')
    columns = _gp_snapshot_columns(table_name_gained)
    estimated_columns = logic.estimated_gain_columns(columns, _measured_columns(c, IOguild))

    rows = c.execute(
        f"SELECT r.G_NAME, g.Name, {', '.join('g.' + column for column in columns)} "
        f"FROM {table_name_gained} AS g "
        f"JOIN {table_name_game} AS r ON r.G_ID = g.G_ID"
    ).fetchall()

    members = {}
    for current_name, gp_name, *gains in rows:
        label = current_name if current_name == gp_name else f"{current_name} (was {gp_name})"
        members[label] = [logic.parse_gp_value(value) for value in gains]
    estimated = {index for index, column in enumerate(columns) if column in estimated_columns}
    return members, estimated


def gp_audit_report(IOguild):
    """Members whose weekly GP gains are worth a closer look, as embeds; []
    when nobody is flagged. The mirror of red_gp_report."""
    try:
        members, estimated = _load_gp_series(IOguild)
        sustained, spiked = logic.filter_gp_audit(members, estimated=estimated)
        return logic.section_embeds(IOguild, "GP audit", logic.format_gp_audit(sustained, spiked))
    except Exception as e:
        print("line: " + str(inspect.currentframe().f_lineno) + "\n error: " + str(e))
        return [logic.failure_embed(f"{IOguild} -- GP audit failed", e)]


def snapshot_taken(column_name):
    """Whether this week was measured for every guild: its _GP column holds
    values and _GP_gained has the column. GP_databases is one transaction, so
    False means nothing was written for the week."""
    return all(
        _is_measured(c, guild_name, column_name)
        and column_name in _gp_snapshot_columns(logic.table_name(guild_name, 'GP_gained'))
        for guild_name in logic.GUILD_NAMES
    )


def snapshot_status(IOguild, run_key, counts=True):
    """What one guild's snapshot for run_key holds, for the post-run check.

    Counts distinct game ids: Pretherians has a G_ID duplicated in _GP_gained.
    counts=False skips the roster and gain counts for a caller that only wants
    which weeks were estimated.
    """
    table_gained = logic.table_name(IOguild, 'GP_gained')
    table_game = logic.table_name(IOguild, 'game')
    measured = _measured_columns(c, IOguild)
    previous = _previous_measured(measured, run_key)
    status = {
        'guild': IOguild,
        'column': run_key,
        'previous': previous,
        'missed': logic.missed_snapshot_columns(previous, run_key),
        'present': run_key in measured and run_key in _gp_snapshot_columns(table_gained),
        'roster': 0,
        'recorded': 0,
        'zeros': 0,
        'negatives': 0,
    }
    if not counts:
        return status
    status['roster'] = c.execute(f"SELECT COUNT(DISTINCT G_ID) FROM {table_game}").fetchone()[0]
    if status['present']:
        status['recorded'], status['zeros'], status['negatives'] = c.execute(
            f"SELECT COUNT(DISTINCT CASE WHEN g.{run_key} IS NOT NULL THEN r.G_ID END), "
            f"COUNT(DISTINCT CASE WHEN CAST(g.{run_key} AS INTEGER) = 0 THEN r.G_ID END), "
            f"COUNT(DISTINCT CASE WHEN CAST(g.{run_key} AS INTEGER) < 0 THEN r.G_ID END) "
            f"FROM {table_game} AS r LEFT JOIN {table_gained} AS g ON g.G_ID = r.G_ID"
        ).fetchone()
    return status


def _load_link_snapshot(IOguild):
    """Everything the link commands reason about, in three full scans.

    All three tables are a few hundred to a thousand rows and carry no useful
    indexes, so scanning them costs nothing and saves every caller from
    re-deriving the same joins. Ids are normalized here, at the boundary, so
    nothing downstream ever compares two different storage classes.

    No WHERE clause on _members on purpose: deciding which rows matter is
    logic.find_link_conflicts' job, where it is covered by tests.
    """
    table_members = logic.table_name(IOguild, 'members')
    table_game = logic.table_name(IOguild, 'game')
    table_discord = logic.table_name(IOguild, 'discord')

    links = [
        {
            'rowid': row[0],
            'discord': row[1],
            'd_id': logic.normalize_discord_id(row[2]),
            'display': row[3],
            'g_id': logic.normalize_game_id(row[4]),
            'g_name': row[5],
        }
        for row in c.execute(
            f"SELECT rowid, Discord, D_ID, Display, G_ID, G_NAME FROM {table_members}"
        ).fetchall()
    ]

    live_characters = {}
    for g_name, g_id, gp in c.execute(f"SELECT G_NAME, G_ID, GP FROM {table_game}").fetchall():
        key = logic.normalize_game_id(g_id)
        if key is not None:
            live_characters[key] = (g_name, logic.parse_gp_value(gp))

    live_accounts = {
        d_id: display or discord_name
        for d_id, (display, discord_name) in _load_role_holders(IOguild).items()
    }

    return links, live_characters, live_accounts


def _load_role_holders(IOguild):
    """{d_id: (display, username)} for the guild role's holders -- the one
    reader of {guild}_discord, so whois and the links section agree on it."""
    holders = {}
    for d_id, discord_name, display in c.execute(
        f"SELECT D_ID, Discord, Display FROM {logic.table_name(IOguild, 'discord')}"
    ).fetchall():
        key = logic.normalize_discord_id(d_id)
        if key is not None:
            holders[key] = (display, discord_name)
    return holders


def links_report(IOguild, discord_guild=None):
    """Broken links and who is missing a link, role or character, as embeds;
    [] when everything is linked.

    Role holders come from {guild}_discord, which the weekly job and
    !sync_counters refresh. Whether a linked account without the role is still
    in the server is asked of Discord -- but only once the member cache is
    complete, since a missing member would otherwise read as having left.

    A load failure comes back as an error embed, never as [] -- otherwise a
    crash and a clean week would look identical in the mod channel.
    """
    try:
        links, live_characters, _ = _load_link_snapshot(IOguild)
        character_conflicts, account_conflicts = logic.find_link_conflicts(links, live_characters)
        server, live_names = None, {}
        if discord_guild is not None and discord_guild.chunked:
            server = _resolve_role_holders(discord_guild, IOguild, {
                row['d_id'] for row in logic.usable_links(links) if row['g_id'] in live_characters
            })
            # The names stored at !assign time go stale; these are what a mod
            # will find in the member list.
            for d_id in server:
                member = discord_guild.get_member(int(d_id))
                live_names[d_id] = logic.account_label(member.display_name, member.name)
        role_holders = {
            d_id: logic.account_label(display, discord_name)
            for d_id, (display, discord_name) in _load_role_holders(IOguild).items()
        }
        gaps = logic.link_gaps(links, live_characters, role_holders, server, live_names)
        return logic.section_embeds(
            IOguild, "links", logic.format_links(character_conflicts, account_conflicts, gaps),
        )
    except Exception as e:
        print("line: " + str(inspect.currentframe().f_lineno) + "\n error: " + str(e))
        return [logic.failure_embed(f"{IOguild} -- links failed", e)]


def _recent_gains(IOguild, g_ids):
    """The last few weekly gains for each of `g_ids`."""
    if not g_ids:
        return {}
    table_gained = logic.table_name(IOguild, 'GP_gained')
    columns = _gp_snapshot_columns(table_gained)[-logic.WHOIS_GAIN_WEEKS:]
    if not columns:
        return {}
    placeholders = ', '.join('?' for _ in g_ids)
    rows = c.execute(
        f"SELECT G_ID, {', '.join(columns)} FROM {table_gained} "
        f"WHERE G_ID IN ({placeholders})",
        tuple(g_ids),
    ).fetchall()
    return {
        logic.normalize_game_id(row[0]): [
            value for value in (logic.parse_gp_value(v) for v in row[1:]) if value is not None
        ]
        for row in rows
    }


def _search_links(IOguild, term):
    """Rows of the three tables matching `term` in any column.

    Every column is searched rather than guessing what kind of thing the term
    is. Game ids come in two formats and names can look like anything, so
    detection would be guesswork; searching everything cannot mis-detect.

    _game.G_NAME is searched as well as _members.G_NAME because the latter is
    frozen at !assign time -- 60 of the ~406 live link rows carry a name the
    character no longer uses, so searching only _members would fail to find
    somebody by the name they go by now.
    """
    table_members = logic.table_name(IOguild, 'members')
    table_game = logic.table_name(IOguild, 'game')
    table_discord = logic.table_name(IOguild, 'discord')

    pattern = logic.like_term(term)
    partial = logic.is_searchable_term(term)

    def where(partial_columns, exact_columns):
        """One WHERE clause and its parameters, built together.

        Deriving the placeholder count and the parameter list in the same loop
        is what keeps them from drifting: a term too short to search on drops
        the partial columns from both at once, rather than from the SQL in one
        place and the bindings in another.
        """
        clauses, params = [], []
        if partial:
            for column in partial_columns:
                clauses.append(f"({column} LIKE ? ESCAPE '\\')")
                params.append(pattern)
        for column in exact_columns:
            clauses.append(f"{column} = ?")
            params.append(term)
        if not clauses:
            return "0", ()
        return " OR ".join(clauses), tuple(params)

    clause, params = where(['G_NAME', 'Display', 'Discord'], ['G_ID', 'CAST(D_ID AS TEXT)'])
    member_rows = c.execute(
        f"SELECT G_ID, D_ID FROM {table_members} WHERE {clause}", params
    ).fetchall()

    clause, params = where(['G_NAME'], ['G_ID'])
    game_rows = c.execute(
        f"SELECT G_ID FROM {table_game} WHERE {clause}", params
    ).fetchall()

    clause, params = where(['Display', 'Discord'], ['CAST(D_ID AS TEXT)'])
    discord_rows = c.execute(
        f"SELECT D_ID FROM {table_discord} WHERE {clause}", params
    ).fetchall()

    g_ids = {logic.normalize_game_id(row[0]) for row in member_rows}
    g_ids |= {logic.normalize_game_id(row[0]) for row in game_rows}
    d_ids = {logic.normalize_discord_id(row[1]) for row in member_rows}
    d_ids |= {logic.normalize_discord_id(row[0]) for row in discord_rows}
    # Dropping None here is what keeps the five blank rows out of the IN lists
    # below; left in, they match each other and every answer grows a phantom.
    return g_ids - {None}, d_ids - {None}


async def whois(channel, term, discord_guild=None):
    """Look a member up by any of game name, game id, Discord name or id."""
    term = term.strip()
    if not term:
        await channel.send("Give something to search for: a game name, game id, Discord name or id.")
        return

    try:
        messages = []
        found_any = False
        for IOguild in logic.GUILD_NAMES:
            seed_g_ids, seed_d_ids = _search_links(IOguild, term)
            if not seed_g_ids and not seed_d_ids:
                continue

            links, live_characters, live_accounts = _load_link_snapshot(IOguild)
            matched = [
                row for row in links
                if row['g_id'] in seed_g_ids or row['d_id'] in seed_d_ids
            ]
            # Expand: every row touching a matched character or account, so the
            # cross-check a moderator is really asking for happens without them
            # having to run a second search.
            expanded_g = {row['g_id'] for row in matched} | seed_g_ids
            expanded_d = {row['d_id'] for row in matched} | seed_d_ids
            expanded = [
                row for row in links
                if row['g_id'] in expanded_g or row['d_id'] in expanded_d
            ]

            characters = {row['g_id'] for row in expanded if row['g_id']} | (
                seed_g_ids & live_characters.keys()
            )
            if len(characters) > logic.WHOIS_MAX_CHARACTERS:
                names = [
                    logic.character_name(live_characters.get(g_id)) or g_id
                    for g_id in characters
                ]
                messages.append(logic.format_whois_too_many(term, names))
                found_any = True
                continue

            role_holders = _resolve_role_holders(discord_guild, IOguild, expanded_d)
            gains = _recent_gains(IOguild, sorted(characters))

            records = logic.whois_characters(
                expanded, live_characters, live_accounts, role_holders, gains
            )
            unlinked_characters, unlinked_accounts = logic.whois_unlinked(
                {g_id: live_characters[g_id] for g_id in seed_g_ids if g_id in live_characters},
                {d_id: live_accounts[d_id] for d_id in seed_d_ids if d_id in live_accounts},
                {row['g_id'] for row in expanded},
                {row['d_id'] for row in expanded},
            )
            message = logic.format_whois(
                IOguild, records, unlinked_characters, unlinked_accounts
            )
            if message:
                messages.append(message)
                found_any = True
    except Exception as e:
        print("line: " + str(inspect.currentframe().f_lineno) + "\n error: " + str(e))
        await channel.send(f"Error looking up '{term}': {e}")
        return

    if not found_any:
        if not logic.is_searchable_term(term):
            # Say why rather than reporting an honest-looking "nothing found":
            # a short term only ever ran as an exact id match, so "no match" and
            # "not searched" look identical from the outside.
            await channel.send(
                f"'{term}' is too short to search on -- give at least "
                f"{logic.SEARCH_MIN_PARTIAL} characters, or an exact id."
            )
            return
        await channel.send(f"Nothing matches '{term}' in either guild.")
        return
    for message in messages:
        # Eight characters with several links each can pass 2000 characters.
        for part in logic.split_message(message):
            await channel.send(part)


def _resolve_role_holders(discord_guild, IOguild, d_ids):
    """Who still holds the guild role, asked of Discord rather than the database.

    {guild}_discord is rebuilt only weekly and by !members_discord and
    !sync_counters, so it can be days stale -- reading it would answer this
    question from an old snapshot. GP_roles and the giveaway cog already
    resolve membership this way.

    A Discord id missing from the member cache is left out entirely rather than
    reported as absent, so the report can say "unknown" instead of guessing.
    """
    if discord_guild is None:
        return {}
    holders = {}
    for d_id in d_ids:
        if d_id is None:
            continue
        member = discord_guild.get_member(int(d_id))
        if member is None:
            continue
        holders[d_id] = any(role.name == IOguild for role in member.roles)
    return holders


def _relink_write(IOguild, g_id, current_name, target):
    """Repoint one character's link rows. Synchronous, and must stay that way.

    Functions.conn is a single connection shared by every coroutine, and
    write_game_roster, the invite store and mark_weekly_run_started all commit
    on it. An await anywhere between the SELECT and the commit would let one of
    them commit this function's half-finished delete, so the whole transaction
    runs without yielding and the message is built by the caller afterwards.

    The delete names the rowids read a few lines above rather than repeating
    `WHERE G_ID = ?`. The predicate would be re-evaluated at delete time, so a
    row inserted in between would be removed without ever appearing in the
    report -- and that report is the only readable record of what went.
    """
    table_members = logic.table_name(IOguild, 'members')

    began = False
    try:
        conn.execute("BEGIN IMMEDIATE")
        began = True
    except sqlite3.OperationalError as e:
        # Only a transaction already open on the shared connection is tolerable
        # -- the work below still commits as one unit. "database is locked"
        # raises the same class and must not be swallowed: continuing would run
        # the SELECT without the write lock that stops a concurrent !assign
        # slipping a row in between it and the DELETE.
        if "within a transaction" not in str(e):
            raise

    try:
        rows = [
            {
                'rowid': row[0],
                'discord': row[1],
                'd_id': logic.normalize_discord_id(row[2]),
                'display': row[3],
                'g_name': row[5],
            }
            for row in c.execute(
                f"SELECT rowid, Discord, D_ID, Display, G_ID, G_NAME "
                f"FROM {table_members} WHERE G_ID = ? ORDER BY rowid",
                (g_id,),
            ).fetchall()
        ]

        delete_rowids, keep_rowid, need_insert = logic.plan_relink(rows, target['d_id'])
        removed = [row for row in rows if row['rowid'] in set(delete_rowids)]
        kept = next((row for row in rows if row['rowid'] == keep_rowid), None)

        if delete_rowids:
            placeholders = ', '.join('?' for _ in delete_rowids)
            c.execute(
                f"DELETE FROM {table_members} WHERE rowid IN ({placeholders})",
                tuple(delete_rowids),
            )

        inserted = None
        if need_insert:
            # The current in-game name, never whatever the moderator typed --
            # relinking by a former name would otherwise write that name back.
            c.execute(
                f"INSERT INTO {table_members} (Discord, D_ID, Display, G_ID, G_NAME) "
                f"VALUES (?,?,?,?,?)",
                (target['discord'], int(target['d_id']), target['display'], g_id, current_name),
            )
            inserted = target

        conn.commit()
    except Exception:
        # Only roll back a transaction this function opened. If one was already
        # in flight, rolling back would discard whatever the other coroutine had
        # pending as well.
        if began:
            conn.rollback()
        raise

    return removed, kept, inserted


def load_relink_candidates():
    """What resolve_relink_target needs, across both guilds.

    Lives here rather than in the command because assembling it is three table
    reads per guild, and the command layer does no SQL-shaped work anywhere
    else in this bot.

    Returns live characters keyed (guild, g_id), and an index of lowercased
    former names to the (guild, g_id) pairs that once used them -- a character
    renamed since !assign is otherwise unfindable by the name a moderator
    remembers.
    """
    live_characters = {}
    historical_names = {}
    for IOguild in logic.GUILD_NAMES:
        links, live, _ = _load_link_snapshot(IOguild)
        for g_id, entry in live.items():
            live_characters[(IOguild, g_id)] = entry
        for row in links:
            if row['g_id'] and row['g_name']:
                historical_names.setdefault(row['g_name'].lower(), []).append(
                    (IOguild, row['g_id'])
                )
    return live_characters, historical_names


async def relink(channel, IOguild, g_id, target):
    """Point a currently in-game character at the right Discord account.

    This is the only place in the bot that deletes anything. A superseded link
    row is genuinely lost -- it has to be, because GP_roles reads every row for
    a live character and would otherwise keep handing the rank role to the old
    account. The backup taken first makes that recoverable and the echoed rows
    make it legible; between them, nothing disappears silently.

    GP history is untouched either way: _GP and _GP_gained are keyed on G_ID
    and fed from {guild}_game, and nothing reads them through _members.
    """
    try:
        links, live_characters, _ = _load_link_snapshot(IOguild)
        if g_id not in live_characters:
            await channel.send(
                f"{g_id} is not currently in {IOguild} in game. "
                f"Run !members_game first if they only just joined."
            )
            return
        current_name = logic.character_name(live_characters[g_id])
    except Exception as e:
        print("line: " + str(inspect.currentframe().f_lineno) + "\n error: " + str(e))
        await channel.send(f"Error reading {IOguild} before relinking: {e}")
        return

    try:
        backup_name = run_manual_backup().name
    except Exception as e:
        print("line: " + str(inspect.currentframe().f_lineno) + "\n error: " + str(e))
        await channel.send(f"Refusing to relink: the safety backup failed ({e}).")
        return

    try:
        removed, kept, inserted = _relink_write(IOguild, g_id, current_name, target)
    except Exception as e:
        print("line: " + str(inspect.currentframe().f_lineno) + "\n error: " + str(e))
        await channel.send(f"Relink failed and was rolled back: {e}. Backup: `{backup_name}`")
        return

    # Read off the snapshot taken before the write rather than reloading: the
    # only rows this relink touched are the ones for g_id, which are excluded
    # here anyway, so a second scan of all three tables would answer the same.
    others = sorted(
        logic.character_name(live_characters.get(row['g_id'])) or row['g_id']
        for row in links
        if row['d_id'] == target['d_id'] and row['g_id'] != g_id and row['g_id'] in live_characters
    )
    warning = logic.relink_warning(IOguild, target['display'], others)

    await channel.send(logic.format_relink_result(
        IOguild, current_name, g_id, removed, kept, inserted,
        backup_name=backup_name, warning=warning,
    ))


INVITES_TABLE = 'invites'
INVITE_REPORTS_TABLE = 'invite_arrival_reports'

INVITE_COLUMNS = (
    'id', 'guild', 'invited_name', 'name_key', 'd_id', 'display', 'discord',
    'first_sent_at', 'sent_at', 'baseline', 'baseline_at', 'status', 'send_confirmed',
    'matched_g_id', 'matched_name', 'matched_at', 'method', 'superseded_by',
)


# The connection the invite tables were last ensured on. Keyed on the
# connection rather than a plain flag so a fresh database (tests swap one in
# per test) still gets its tables, while the live bot runs the DDL once.
_invite_tables_ready_on = None


def _ensure_invite_tables():
    """Invites are kept, never deleted: a finished one is the record that stops
    its matched character being handed to another invite."""
    global _invite_tables_ready_on
    if _invite_tables_ready_on is conn:
        return
    c.execute(
        f"CREATE TABLE IF NOT EXISTS {INVITES_TABLE} ("
        "id INTEGER PRIMARY KEY, guild TEXT NOT NULL, invited_name TEXT NOT NULL, "
        "name_key TEXT NOT NULL, d_id TEXT, display TEXT, discord TEXT, "
        "first_sent_at REAL NOT NULL, sent_at REAL NOT NULL, "
        "baseline TEXT NOT NULL, baseline_at REAL NOT NULL, "
        "status TEXT NOT NULL, send_confirmed INTEGER NOT NULL DEFAULT 1, "
        "matched_g_id TEXT, matched_name TEXT, matched_at REAL, method TEXT, "
        "superseded_by INTEGER)"
    )
    c.execute(
        f"CREATE TABLE IF NOT EXISTS {INVITE_REPORTS_TABLE} "
        "(guild TEXT NOT NULL, g_id TEXT NOT NULL, reported_at REAL NOT NULL, "
        "PRIMARY KEY (guild, g_id))"
    )
    conn.commit()
    _invite_tables_ready_on = conn


def _invite_from_row(row):
    invite = dict(zip(INVITE_COLUMNS, row))
    invite['baseline'] = set(json.loads(invite['baseline']))
    return invite


def load_invites(IOguild=None, statuses=None, since=None):
    _ensure_invite_tables()
    clauses, params = [], []
    if IOguild is not None:
        clauses.append("guild = ?")
        params.append(IOguild)
    if statuses:
        clauses.append(f"status IN ({', '.join('?' for _ in statuses)})")
        params.extend(statuses)
    if since is not None:
        clauses.append("sent_at >= ?")
        params.append(since)
    where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
    rows = c.execute(
        f"SELECT {', '.join(INVITE_COLUMNS)} FROM {INVITES_TABLE}{where} ORDER BY sent_at, id",
        params,
    ).fetchall()
    return [_invite_from_row(row) for row in rows]


def get_invite(invite_id):
    _ensure_invite_tables()
    row = c.execute(
        f"SELECT {', '.join(INVITE_COLUMNS)} FROM {INVITES_TABLE} WHERE id = ?", (invite_id,)
    ).fetchone()
    return _invite_from_row(row) if row else None


def find_cancellable_invite(IOguild, name):
    """The newest invite !uninvite can act on for this name, or None.

    Includes superseded invites: they block elimination for a week, so a
    moderator needs a way to clear one whose stray recipient isn't coming.
    """
    statuses = logic.INVITE_OPEN_STATUSES + (
        logic.INVITE_LINKED, logic.INVITE_NEEDS_ATTENTION, logic.INVITE_SUPERSEDED,
    )
    candidates = [
        invite for invite in load_invites(IOguild, statuses=statuses)
        if invite['name_key'] == logic.name_key(name)
    ]
    return candidates[-1] if candidates else None


def record_invite(IOguild, name, target, baseline, baseline_at, now, send_confirmed):
    """Record a sent !invite, merging or superseding per logic.plan_invite_record.

    `target` is {'d_id', 'display', 'discord'} or None when no member was
    given. Synchronous and committed as one unit, so an !invite can never leave
    a new row in place alongside the invite it was meant to replace.
    """
    _ensure_invite_tables()
    target = target or {}
    d_id = logic.normalize_discord_id(target.get('d_id'))
    plan = logic.plan_invite_record(
        load_invites(IOguild, statuses=logic.INVITE_OPEN_STATUSES), name, d_id, baseline
    )
    baseline_json = json.dumps(sorted(plan['baseline']))
    first_sent_at = plan['first_sent_at'] if plan['first_sent_at'] is not None else now
    merge = plan['merge_into']
    try:
        if merge:
            c.execute(
                f"UPDATE {INVITES_TABLE} SET invited_name = ?, d_id = ?, display = ?, discord = ?, "
                "sent_at = ?, first_sent_at = ?, baseline = ?, baseline_at = ?, status = ?, "
                "send_confirmed = ? WHERE id = ?",
                (name, d_id, target.get('display'), target.get('discord'), now, first_sent_at,
                 baseline_json, baseline_at, logic.INVITE_PENDING, int(send_confirmed), merge['id']),
            )
            invite_id, outcome = merge['id'], 'merged'
        else:
            c.execute(
                f"INSERT INTO {INVITES_TABLE} (guild, invited_name, name_key, d_id, display, "
                "discord, first_sent_at, sent_at, baseline, baseline_at, status, send_confirmed) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (IOguild, name, logic.name_key(name), d_id, target.get('display'),
                 target.get('discord'), first_sent_at, now, baseline_json, baseline_at,
                 logic.INVITE_PENDING, int(send_confirmed)),
            )
            invite_id, outcome = c.lastrowid, 'new'
        for old in plan['supersede']:
            c.execute(
                f"UPDATE {INVITES_TABLE} SET status = ?, superseded_by = ? WHERE id = ?",
                (logic.INVITE_SUPERSEDED, invite_id, old['id']),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return {
        'outcome': outcome,
        'invite_id': invite_id,
        'd_id': d_id,
        'previous_d_id': logic.normalize_discord_id(merge.get('d_id')) if merge else None,
        'superseded': [old['invited_name'] for old in plan['supersede']],
    }


def set_invite_status(invite_id, status, **fields):
    """Move an invite to `status`, optionally recording what it matched."""
    allowed = {'matched_g_id', 'matched_name', 'matched_at', 'method'}
    unknown = set(fields) - allowed
    if unknown:
        raise ValueError(f"Unknown invite fields: {sorted(unknown)}")
    assignments = ", ".join(["status = ?"] + [f"{name} = ?" for name in fields])
    c.execute(
        f"UPDATE {INVITES_TABLE} SET {assignments} WHERE id = ?",
        (status, *fields.values(), invite_id),
    )
    conn.commit()


def invite_claims(IOguild):
    """{g_id: when an invite last matched it} for this guild."""
    _ensure_invite_tables()
    return {
        g_id: matched_at
        for g_id, matched_at in c.execute(
            f"SELECT matched_g_id, MAX(matched_at) FROM {INVITES_TABLE} "
            "WHERE guild = ? AND matched_g_id IS NOT NULL GROUP BY matched_g_id",
            (IOguild,),
        ).fetchall()
    }


def reported_arrivals(IOguild, since):
    """Arrivals already reported as unmatched since `since`.

    Persisted because the poll re-evaluates every tick: kept in memory, every
    restart would re-post every unmatched arrival still inside the window.
    """
    _ensure_invite_tables()
    return {
        row[0] for row in c.execute(
            f"SELECT g_id FROM {INVITE_REPORTS_TABLE} WHERE guild = ? AND reported_at >= ?",
            (IOguild, since),
        ).fetchall()
    }


def mark_arrival_reported(IOguild, g_id, now):
    c.execute(
        f"INSERT OR REPLACE INTO {INVITE_REPORTS_TABLE} (guild, g_id, reported_at) VALUES (?, ?, ?)",
        (IOguild, g_id, now),
    )
    conn.commit()


def load_link_rows(IOguild):
    table = logic.table_name(IOguild, 'members')
    return [
        {
            'rowid': rowid,
            'd_id': logic.normalize_discord_id(d_id),
            'g_id': logic.normalize_game_id(g_id),
        }
        for rowid, d_id, g_id in c.execute(f"SELECT rowid, D_ID, G_ID FROM {table}").fetchall()
    ]


def link_for_invite(IOguild, g_id, current_name, target):
    """Link g_id to target's account for an invite, never deleting a row.

    Returns logic.plan_invite_link's (decision, other_accounts). Only an insert
    writes anything; a conflict leaves the table alone for a moderator's
    !relink. Synchronous for the reason on _relink_write: the read and the
    write must not have an await between them on the shared connection.
    """
    table = logic.table_name(IOguild, 'members')
    rows = [
        {'rowid': rowid, 'd_id': d_id}
        for rowid, d_id in c.execute(
            f"SELECT rowid, D_ID FROM {table} WHERE G_ID = ?", (g_id,)
        ).fetchall()
    ]
    decision, others = logic.plan_invite_link(rows, target['d_id'])
    if decision == 'insert':
        try:
            c.execute(
                f"INSERT INTO {table} (Discord, D_ID, Display, G_ID, G_NAME) VALUES (?,?,?,?,?)",
                (target.get('discord'), int(target['d_id']), target.get('display'), g_id, current_name),
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    return decision, others


async def promotions(bot, channel):
    try:
        guild = bot.get_guild(809954021028134943)
        if guild is None:
            await channel.send("Discord server not in cache, cannot check promotions right now.")
            return
        role = discord.utils.get(guild.roles, name="Promotions")
        if role is None:
            await channel.send("There is no 'Promotions' role, so nobody can be listed.")
            return
        # The latest week that was taken: this week's column doesn't exist
        # before the 2 AM run or after a missed Saturday.
        _, column_name1 = await latest_snapshot_day()
        promo_members = logic.table_name(logic.PROMOTION_GUILD, 'members')
        promo_gained = logic.table_name(logic.PROMOTION_GUILD, 'GP_gained')
        c.execute(f'''SELECT PM.D_ID, PM.G_ID, PGG.{column_name1}
        FROM {promo_members} AS PM
        JOIN {promo_gained} AS PGG ON PM.G_ID = PGG.G_ID
        WHERE CAST(PGG.{column_name1} AS INTEGER) >= {logic.PROMOTION_GP_REQUIREMENT}''')
        lines = []
        for d_id, _, GP in c.fetchall():
            member = guild.get_member(d_id)
            if member is None:
                print(f"Member with ID {d_id} not found in guild.")
                continue
            if role in member.roles:
                lines.append(f"{member.name.ljust(15)} | {GP}")
        if not lines:
            await channel.send('No members meet the requirements')
            return
        # Built in memory: the file on disk was left behind by any failure.
        text = "Discord name    | GP\n" + "\n".join(lines) + "\n"
        await channel.send(file=discord.File(io.BytesIO(text.encode()), filename='promo.txt'))
    except Exception as e:
        print(f"Error: {e}")
        await channel.send(f"Error: {e}")


    
def _ensure_game_table(table):
    """Create {guild}_game with the schema pandas' to_sql originally gave it.

    The shape matters beyond tidiness: assign reads this table with SELECT *
    and indexes the columns by position.
    """
    c.execute(
        f'CREATE TABLE IF NOT EXISTS "{table}" '
        '("index" INTEGER, "G_NAME" TEXT, "G_ID" TEXT, "GP" INTEGER)'
    )
    c.execute(f'CREATE INDEX IF NOT EXISTS "ix_{table}_index" ON "{table}" ("index")')


def write_game_roster(IOguild, rows, from_poll=False):
    """Replace {guild}_game with `rows` in a single transaction.

    This used to be pd.DataFrame(rows).to_sql(..., if_exists='replace'), which
    issues DROP, CREATE and CREATE INDEX as separately autocommitted statements
    and only then inserts inside a transaction -- so a power cut between them
    leaves the table empty or missing. That was a once-a-week exposure; with the
    invite poll refreshing the roster mid-week it would be one on every change.
    DELETE and INSERT here commit together or not at all.
    """
    global roster_generation
    table = logic.table_name(IOguild, 'game')
    _ensure_game_table(table)
    try:
        c.execute(f'DELETE FROM "{table}"')
        c.executemany(
            f'INSERT INTO "{table}" ("index", "G_NAME", "G_ID", "GP") VALUES (?, ?, ?, ?)',
            [(position, row['G_NAME'], row['G_ID'], row['GP']) for position, row in enumerate(rows)],
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    if not from_poll:
        roster_generation += 1


def _ensure_discord_table(table):
    """Create {guild}_discord with the schema pandas' to_sql originally gave it."""
    c.execute(
        f'CREATE TABLE IF NOT EXISTS "{table}" '
        '("index" INTEGER, "Discord" TEXT, "D_ID" INTEGER, "Display" TEXT)'
    )
    c.execute(f'CREATE INDEX IF NOT EXISTS "ix_{table}_index" ON "{table}" ("index")')


def write_discord_roster(IOguild, rows):
    """Replace {guild}_discord with `rows` in a single transaction -- the
    to_sql(if_exists='replace') it used to be autocommits its DROP and CREATE
    separately, which write_game_roster explains."""
    table = logic.table_name(IOguild, 'discord')
    _ensure_discord_table(table)
    try:
        c.execute(f'DELETE FROM "{table}"')
        c.executemany(
            f'INSERT INTO "{table}" ("index", "Discord", "D_ID", "Display") VALUES (?, ?, ?, ?)',
            [(position, row['Discord'], row['D_ID'], row['Display']) for position, row in enumerate(rows)],
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def game_roster_size(IOguild):
    table = logic.table_name(IOguild, 'game')
    _ensure_game_table(table)
    return c.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]


def refresh_discord_roster(IOguild, rows):
    """Write a freshly read role list unless it looks like a partial read.
    Returns why it was refused, or None once written."""
    table = logic.table_name(IOguild, 'discord')
    _ensure_discord_table(table)
    current = c.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
    problem = logic.validate_discord_rows(rows, current)
    if problem is None:
        write_discord_roster(IOguild, rows)
    return problem


def refresh_game_roster(IOguild, rows, generation):
    """Write a roster the invite poll just read, if it's safe and it changed.

    Returns what happened, for logging. Synchronous from the checks through
    the commit: nothing may await in here, or the weekly job could start
    between the check and the write. Keep it off executors for the same reason.
    """
    if weekly_job_depth > 0:
        return 'skipped: the weekly job is running'
    if generation != roster_generation:
        return 'skipped: a newer roster was written while this one was being read'
    table = logic.table_name(IOguild, 'game')
    try:
        current = c.execute(f'SELECT G_ID, G_NAME FROM "{table}"').fetchall()
    except sqlite3.OperationalError:
        current = []
    reason = logic.validate_roster_rows(rows, len(current))
    if reason:
        return f'refused: {reason}'
    if not logic.roster_changed(rows, current):
        return 'unchanged'
    write_game_roster(IOguild, rows, from_poll=True)
    return 'written'


async def GP_databases(run_key=None, db_path=None):
    """Take this week's snapshot: totals into {guild}_GP, gains into _GP_gained.

    One transaction for both guilds, on a connection that is always closed. It
    used to autocommit its first ALTER TABLE and roll the rest back on a
    failure, leaving a half-added column -- and the unclosed connection held
    the write lock while the error propagated.

    The gain is measured against the latest measured week, not the column
    exactly 7 days back, so a missed Saturday no longer breaks every run after
    it. When weeks were missed, the difference is spread evenly over them and
    this week (logic.spread_gain): totals stay exact and every report keeps
    working. Missed weeks go into _GP_gained only, so _GP keeps holding nothing
    but measured totals.

    run_key is passed in by the weekly job so the export, this and the check
    all agree on the week even if the run crosses midnight.
    """
    if run_key is None:
        run_key = (get_date())["column_name1"]
    db = sqlite3.connect(db_path or DB_PATH)
    try:
        db.execute("PRAGMA journal_mode=WAL")
        cursor = db.cursor()
        cursor.execute("BEGIN IMMEDIATE")
        try:
            for guild_name in logic.GUILD_NAMES:
                _take_snapshot(cursor, guild_name, run_key)
            db.commit()
        except Exception:
            db.rollback()
            raise
    finally:
        db.close()


def _take_snapshot(cursor, IOguild, run_key):
    table_gp = logic.table_name(IOguild, 'GP')
    table_gained = logic.table_name(IOguild, 'GP_gained')
    table_game = logic.table_name(IOguild, 'game')

    previous = _previous_measured(_measured_columns(cursor, IOguild), run_key)
    missed = logic.missed_snapshot_columns(previous, run_key)
    filled = [*missed, run_key]

    if run_key not in _table_columns(cursor, table_gp):
        cursor.execute(f"ALTER TABLE {table_gp} ADD COLUMN {run_key} TEXT")
    gained_columns = _table_columns(cursor, table_gained)
    for column in filled:
        if column not in gained_columns:
            cursor.execute(f"ALTER TABLE {table_gained} ADD COLUMN {column} TEXT")

    # Per table, and NOT EXISTS rather than NOT IN: one NULL G_ID makes NOT IN
    # match nothing, and Pretherians has an id in _GP that _GP_gained lacks.
    for table in (table_gp, table_gained):
        cursor.execute(
            f"INSERT INTO {table} (Name, G_ID) SELECT G_NAME, G_ID FROM {table_game} AS r "
            f"WHERE NOT EXISTS (SELECT 1 FROM {table} AS t WHERE t.G_ID = r.G_ID)"
        )
    cursor.execute(
        f"UPDATE {table_gp} SET {run_key} = "
        f"(SELECT GP FROM {table_game} WHERE {table_game}.G_ID = {table_gp}.G_ID)"
    )

    # Cleared first so a re-run leaves nothing from the last attempt behind,
    # for anyone who has since left.
    cursor.execute(f"UPDATE {table_gained} SET {', '.join(f'{column} = NULL' for column in filled)}")
    updates = {column: [] for column in filled}
    rows = cursor.execute(f"SELECT G_ID, {run_key}, {previous or 'NULL'} FROM {table_gp}").fetchall()
    for g_id, now_value, previous_value in rows:
        now_gp = logic.parse_gp_value(now_value)
        if now_gp is None:
            continue
        previous_gp = logic.parse_gp_value(previous_value)
        if previous_gp is None:
            # A first appearance stays `GPnow - 0` in this week alone, where
            # logic.observed_weeks knows to drop it, rather than lifetime GP
            # spread across weeks before they joined.
            updates[run_key].append((now_gp, g_id))
            continue
        for column, gain in zip(filled, logic.spread_gain(now_gp - previous_gp, len(filled))):
            updates[column].append((gain, g_id))
    for column, params in updates.items():
        cursor.executemany(f"UPDATE {table_gained} SET {column} = ? WHERE G_ID = ?", params)
