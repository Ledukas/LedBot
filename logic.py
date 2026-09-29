"""Pure logic extracted from LedBotCode.py / Functions.py / cogs/giveaway.py.

Every function here is a plain, deterministic computation with no Discord API
calls, no database connection, and no network access -- everything it needs
comes in as a parameter. That's what makes this module cheap and safe for
every test file (and LedBotCode.py/Functions.py/cogs/giveaway.py themselves)
to import: unlike those modules, importing logic.py has zero side effects.

A few of these were extracted specifically because doing so surfaced real
bugs in the original inline code -- see the docstring/comments on
compute_gp_rank_role, filter_red_gp, compute_remaining_to_rankup and
command_error_message. Those are noted in TODO.md as fixed.
"""

from datetime import datetime
import re

import pandas as pd
from discord.ext import commands


# ---------------------------------------------------------------------------
# Shared constants
# ---------------------------------------------------------------------------

# The Aetherian rank ladder, ascending. This lived in both LedBotCode.py and
# Functions.py, and the two copies fed different things: the flat threshold
# list drives the "GP needed to rank up" figure in !mygains, while the
# (threshold, role) pairs drive the roles actually assigned. A threshold
# changed in only one file would have had the bot quoting one scale and
# assigning on another, with nothing raising an error. Both are derived from
# this single table now, so they cannot disagree.
# The two IdleOn guilds. The Discord role name matches the guild name exactly,
# which is what members_discord relies on when it looks the role up.
GUILD_GIDS = {
    "Aetherians": "jSiitSSM7nO0HFuoVlsa",
    "Pretherians": "yuFnrJvPfK8ZdfFXHojg",
}
GUILD_NAMES = tuple(GUILD_GIDS)

# Two deliberate asymmetries between the guilds, named rather than left as bare
# literals so it is clear they are domain facts and not oversights: only
# Aetherians has the GP rank ladder and the Monthly Top role, and promotions
# run from Pretherians into Aetherians.
RANK_ROLE_GUILD = "Aetherians"
PROMOTION_GUILD = "Pretherians"

# The GP-gained bar a Pretherian must clear to be listed for promotion. Equal
# to the Aetherian red-GP threshold today, but a separate rule -- do not derive
# one from the other, or changing the activity report would silently move the
# promotion bar too.
PROMOTION_GP_REQUIREMENT = 400

# Each guild owns one table per kind, named "{guild}_{kind}".
TABLE_KINDS = ('members', 'discord', 'game', 'GP', 'GP_gained')


def table_name(guild_name: str, kind: str) -> str:
    """Build a per-guild table name, validating both halves.

    A table name cannot be passed as a SQL parameter, so every caller
    interpolates the guild name straight into the query string. Routing that
    through here is what keeps an arbitrary command argument -- IOguild comes
    from whatever a moderator typed -- out of the SQL, and it replaces the
    f-string and string-concatenation spellings that were scattered across
    LedBotCode.py, Functions.py and cogs/giveaway.py.
    """
    if guild_name not in GUILD_GIDS:
        raise ValueError(f"Unrecognized guild: {guild_name!r}")
    if kind not in TABLE_KINDS:
        raise ValueError(f"Unrecognized table kind: {kind!r}")
    return f"{guild_name}_{kind}"


RANK_THRESHOLDS: list[tuple[int, str]] = [
    (1000, 'Aetherian Knight'),
    (2500, 'Aetherian Hero'),
    (5000, 'Aetherian Demigod'),
    (10000, 'Aetherian Deity'),
    (25000, 'Aetherian Titan'),
    (50000, 'Aetherian Primordial'),
    (100000, 'True Aetherian'),
]
RANK_ROLE_NAMES = [name for _, name in RANK_THRESHOLDS]
GP_THRESHOLDS = [threshold for threshold, _ in RANK_THRESHOLDS]


# ---------------------------------------------------------------------------
# Extracted from Functions.py
# ---------------------------------------------------------------------------

def compute_gp_rank_role(gp: int, thresholds: list[tuple[int, str]]) -> str | None:
    """Return the name of the highest-tier role gp qualifies for, or None.

    thresholds must be an ascending list of (threshold, role_name) pairs.
    gp exactly equal to a threshold counts as reaching that tier (inclusive
    lower bound) -- the original inline if/elif chain in GP_roles used a
    strict '>' on every branch, so a member with GP exactly equal to a
    threshold (e.g. exactly 1000) matched no branch and silently got no role
    at all. This fixes that.
    """
    target = None
    for threshold, role_name in thresholds:
        if gp >= threshold:
            target = role_name
        else:
            break
    return target


def filter_top_average(df: pd.DataFrame, threshold: int = 649) -> pd.DataFrame:
    """Rows whose 'Average' column exceeds threshold (the 'Monthly Top' / clammy cutoff)."""
    return df[df['Average'] > threshold]


def build_gp_dataframe(
    rows: list[tuple],
    column_names: list[str],
    column_names_int: list[str],
    gp_prefix: str = 'GP',
) -> pd.DataFrame:
    """Build the per-guild GP dataframe: numeric coercion, year-agnostic column
    renaming, and the rolling Average column. Faithful port of the pure part
    of the original GP_dataframe (the SQL query stays with the caller).
    """
    df = pd.DataFrame(rows, columns=column_names)

    for column in column_names_int:
        df[column] = pd.to_numeric(df[column], errors='coerce')
    df[column_names_int] = df[column_names_int].fillna(pd.NA).astype('Int64')

    # Strip each column's own "GP{year}_" prefix (not a hardcoded year) since the
    # 4 columns here can legitimately span two different years near a year boundary
    # (e.g. "3 weeks ago" from early January falls in December of the prior year).
    new_column_names = {
        column: re.sub(rf'^{re.escape(gp_prefix)}\d{{4}}_', '', column)
        for column in column_names_int
    }
    df = df.rename(columns=new_column_names)

    df['Average'] = df.iloc[:, 2:].mean(axis=1)
    # Preserved from the original as-is (not removed): this line appears to be
    # dead code -- a nullable-Int64 mean of an all-NA row already yields pd.NA,
    # not the literal string 'NAType', so this .replace likely never matches
    # anything. Kept for behavioral fidelity rather than assumed safe to drop.
    df['Average'] = df['Average'].replace('NAType', pd.NA)
    df['Average'] = pd.to_numeric(df['Average'], errors='coerce')
    df['Average'] = df['Average'].round(1)
    return df


RED_GP_THRESHOLDS = {'Aetherians': 400, 'Pretherians': 140}


def filter_red_gp(df: pd.DataFrame, io_guild: str, blacklist: list[str]) -> pd.DataFrame:
    """Rows below the per-guild GP-gained threshold, excluding the blacklist.

    Raises ValueError for an io_guild outside RED_GP_THRESHOLDS -- the
    original inline version left its 'redGP' threshold variable undefined in
    this case, which surfaced as a confusing NameError several lines later
    instead of a clear error at the actual point of failure.
    """
    try:
        threshold = RED_GP_THRESHOLDS[io_guild]
    except KeyError:
        raise ValueError(f"Unrecognized io_guild: {io_guild!r}") from None

    filtered = df[df.iloc[:, 5] < threshold]
    filtered = filtered.dropna(subset=filtered.columns[4])
    filtered = filtered[~filtered['G_ID'].isin(blacklist)]
    filtered = filtered.drop('G_ID', axis=1)
    filtered = filtered.sort_values(filtered.columns[4])
    return filtered


# ---------------------------------------------------------------------------
# GP audit -- the mirror of red_gp: who is gaining implausibly much, rather
# than too little.
# ---------------------------------------------------------------------------

# Where the weekly tasks top out. It is the most common non-zero gain in the
# database by a factor of two over its neighbours (2311 weeks against 470 at
# 690), because every member who simply finishes the week lands exactly on it.
GP_TASK_CAP = 680

# The bar a single week must clear to count against a member. Deliberately
# above GP_TASK_CAP rather than on it: ordinary week-to-week variation carries
# people past the cap, so counting from the cap itself reports every diligent
# player in both guilds. Sustained gains above this need gold stopwatches, and
# those run out with the trophies that buy them -- which is the thing actually
# worth a second look.
GP_GAIN_BAR = 750

# A single week large enough to report on its own, checked strictly. 1200 is a
# common exact value (71 weeks across 28 members, against 3-13 at each
# neighbouring value), so landing on it is not the unusual part -- going past
# it is. Repeatedly landing on it is caught by the sustained count instead,
# since every such week also clears GP_GAIN_BAR.
GP_SPIKE_WEEK = 1200

# No player produces a week this size; it is the export mangling a row. The
# real record is 2410.
GP_IMPLAUSIBLE_WEEK = 5000

GP_AUDIT_WEEKS = 8
GP_AUDIT_HITS = 3
GP_AUDIT_MIN_HISTORY = 8


def parse_gp_value(value) -> int | None:
    """One stored weekly gain as an int, or None if it is missing or junk.

    The GP columns are added as TEXT (GP_databases has no schema to declare
    otherwise), so every read comes back as a string.
    """
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def observed_weeks(series: list[int | None]) -> list[int]:
    """Usable weekly gains from a member's full history, oldest first.

    Drops the first recorded week: Functions.GP_databases computes a gain
    against a missing previous snapshot as `GPnow - 0`, so a member's first
    week is their entire lifetime GP rather than a week's worth. Also drops
    anything above GP_IMPLAUSIBLE_WEEK, which covers the same artifact landing
    mid-history -- Pretherians has a duplicated row carrying one player's early
    history spliced onto another's join week, putting a 20300 in the middle of
    an otherwise ordinary series.
    """
    recorded = [value for value in series if value is not None]
    return [value for value in recorded[1:] if value <= GP_IMPLAUSIBLE_WEEK]


def latest_week(series: list[int | None]) -> int | None:
    """Gain in the newest snapshot, or None if there is no usable figure.

    Read off the newest column rather than off the member's last recorded
    week, so a member missing from the latest export is not reported on a
    figure that is several weeks old.
    """
    if not series:
        return None
    value = series[-1]
    if value is None or value > GP_IMPLAUSIBLE_WEEK:
        return None
    if all(earlier is None for earlier in series[:-1]):
        return None
    return value


def audit_member(series: list[int | None], weeks: int = GP_AUDIT_WEEKS) -> dict:
    """Both audit signals for one member's history."""
    history = observed_weeks(series)
    window = history[-weeks:]
    return {
        "history": len(history),
        "hits": sum(1 for value in window if value > GP_GAIN_BAR),
        "window": window,
        "latest": latest_week(series),
    }


def filter_gp_audit(
    members: dict[str, list[int | None]],
    weeks: int = GP_AUDIT_WEEKS,
    hits: int = GP_AUDIT_HITS,
    min_history: int = GP_AUDIT_MIN_HISTORY,
) -> tuple[list[tuple[int, str, list[int]]], list[tuple[int, str]]]:
    """Split members into (sustained, spiked), each sorted worst-first.

    The two signals catch different profiles and neither subsumes the other.
    Sustained wants a member above GP_GAIN_BAR in `hits` of the last `weeks`,
    counted across the window rather than consecutively -- gains collapse
    during in-game events, so a streak requirement lets anyone through on the
    back of one quiet week. Spiked wants a single newest week past
    GP_SPIKE_WEEK, because a lone enormous week between two ordinary ones never
    accumulates enough hits: the member whose record 2410 week prompted this
    sat at 2 of 6 under the sustained rule alone.

    min_history gates only the sustained side. It exists to keep a member with
    two weeks of history off a window-based count; a member three weeks into
    the guild posting an enormous week is exactly what the spike check is for.
    """
    audited = {name: audit_member(series, weeks) for name, series in members.items()}

    sustained = sorted(
        (
            (result["hits"], name, result["window"])
            for name, result in audited.items()
            if result["history"] >= min_history and result["hits"] >= hits
        ),
        key=lambda row: (-row[0], row[1]),
    )
    spiked = sorted(
        (
            (result["latest"], name)
            for name, result in audited.items()
            if result["latest"] is not None and result["latest"] > GP_SPIKE_WEEK
        ),
        key=lambda row: (-row[0], row[1]),
    )
    return sustained, spiked


def format_gp_audit(
    io_guild: str,
    sustained: list[tuple[int, str, list[int]]],
    spiked: list[tuple[int, str]],
    weeks: int = GP_AUDIT_WEEKS,
    hits: int = GP_AUDIT_HITS,
) -> str:
    """Render one guild's audit as a Discord message.

    Always names the figures that produced a flag. This report exists to start
    an investigation, not to conclude one, and a moderator cannot judge a name
    without the weeks behind it.

    A flagged member takes two lines, their weeks indented underneath, rather
    than one padded row. Names here can carry a '(was ...)' rename suffix, and
    the single-row version either truncated those mid-name or pushed the line
    wide enough to wrap in a phone's code block.
    """
    lines = [f"**{io_guild} -- GP audit**"]

    if sustained:
        lines.append(f"Above {GP_GAIN_BAR} in {hits}+ of the last {weeks} weeks:")
        block = "\n".join(
            f"{name}  --  {count} of {weeks}\n    "
            + ", ".join(str(value) for value in window)
            for count, name, window in sustained
        )
        lines.append(f"```\n{block}\n```")
    else:
        lines.append(f"Nobody above {GP_GAIN_BAR} in {hits}+ of the last {weeks} weeks.")

    if spiked:
        lines.append(f"Above {GP_SPIKE_WEEK} this week:")
        width = max(len(name) for _, name in spiked)
        block = "\n".join(
            f"{name:<{width}}  {value:,}" for value, name in spiked
        )
        lines.append(f"```\n{block}\n```")

    return "\n".join(lines)


def format_table_block(
    df: pd.DataFrame,
    first_col_width: int = 14,
    other_col_width: int = 7,
    separator: str = ' | ',
) -> str:
    """Render a dataframe as a left-padded, separator-joined monospace text
    block: one header line (from df.columns) followed by one line per row,
    each line ending in '\\n'. Shared by red_gp and mygains2, which used to
    duplicate this formatting independently.
    """
    columns = list(df.columns)
    header = str(columns[0]).ljust(first_col_width)
    for column in columns[1:]:
        header += separator + str(column).ljust(other_col_width)
    header += '\n'

    data = ""
    for _, row in df.iterrows():
        data += str(row.iloc[0]).ljust(first_col_width)
        for value in row.iloc[1:]:
            data += separator + str(value).ljust(other_col_width)
        data += '\n'

    return header + data


def extract_cell_value(values: list, default: str = "No strategy available yet") -> str:
    """Pull the first cell out of a Google Sheets values response, or default."""
    if values and values[0]:
        return values[0][0]
    return default


def build_game_members_rows(data: dict) -> list[dict]:
    """Turn a Firebase guild-members payload into row dicts ready for a dataframe."""
    rows = []
    for member_id, member_data in data.items():
        rows.append({
            'G_NAME': member_data['a'],
            'G_ID': member_id,
            'GP': member_data['e'],
        })
    return rows


# ---------------------------------------------------------------------------
# Member links -- the {guild}_members table mapping a Discord account to an
# in-game character, and the ways it goes wrong.
# ---------------------------------------------------------------------------

DISCORD_MENTION_RE = re.compile(r'^<@!?(\d+)>$')
LIKE_ESCAPE = '\\'

# Partial matches need enough to be worth searching on: a one-letter term
# matches 1408 of the ~1960 link rows, which is neither useful nor sendable.
# Exact matches on an id are always allowed, however short.
SEARCH_MIN_PARTIAL = 3

# Above this many distinct characters, report the names and stop rather than
# expanding each one -- the expanded form runs to several lines per character
# and Discord rejects a message over 2000 bytes.
WHOIS_MAX_CHARACTERS = 8
WHOIS_GAIN_WEEKS = 4

# Ordered worst-first, which is also the order they are reported in.
CONFLICT_SPLIT = 'split'          # one character, several Discord accounts
CONFLICT_DUPLICATE = 'duplicate'  # one character, same account more than once
CONFLICT_ORDER = (CONFLICT_SPLIT, CONFLICT_DUPLICATE)


def normalize_discord_id(value) -> str | None:
    """A Discord id as a canonical digit string, or None if it is not one.

    Accepts what a moderator can type (a raw id, `<@123>`, `<@!123>`) and what
    the database holds.

    Note what this is *not* for: `_members.D_ID` is declared INTEGER, so SQLite
    applies column affinity and `WHERE D_ID = ?` already matches whether the
    parameter is 123 or '123'. Plain equality lookups never needed this. What
    does need it is *grouping* -- affinity is not applied between two values of
    different storage class, so an integer 123 and a text '123' would fall into
    different groups and one account would be reported as two.
    """
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return str(value)
    text = str(value).strip()
    mention = DISCORD_MENTION_RE.match(text)
    if mention:
        return mention.group(1)
    return text if text.isdigit() else None


def normalize_game_id(value) -> str | None:
    """A game id, or None when there isn't one.

    Game ids come in two shapes -- 28-character Firebase uids and 23-character
    `steam_...` ones -- so this validates presence, not format. The None cases
    are the five blank rows in the live database (one all-'' row in Aetherians,
    one all-'' and three all-NULL in Pretherians). They have to be dropped
    before any grouping or IN list: left in, they match each other and every
    answer grows a phantom member built out of junk.
    """
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def like_term(term: str) -> str:
    """A LIKE pattern matching `term` anywhere, with wildcards escaped.

    Callers must pair this with ESCAPE '\\'. Not hygiene: 26 Aetherian and 31
    Pretherian live game names contain `_` or `%` (Sire_Vhal, Thomas_The_3rd),
    and unescaped `_` is a single-character wildcard.
    """
    escaped = (
        term.replace(LIKE_ESCAPE, LIKE_ESCAPE + LIKE_ESCAPE)
            .replace('%', LIKE_ESCAPE + '%')
            .replace('_', LIKE_ESCAPE + '_')
    )
    return f'%{escaped}%'


def is_searchable_term(term: str) -> bool:
    """Whether a term is long enough to run as a partial match."""
    return len(term.strip()) >= SEARCH_MIN_PARTIAL


def usable_links(link_rows: list[dict]) -> list[dict]:
    """Link rows with both ids present, ids normalized to canonical strings."""
    usable = []
    for row in link_rows:
        g_id = normalize_game_id(row.get('g_id'))
        d_id = normalize_discord_id(row.get('d_id'))
        if g_id is None or d_id is None:
            continue
        usable.append({**row, 'g_id': g_id, 'd_id': d_id})
    return usable


def find_link_conflicts(
    link_rows: list[dict],
    live_characters: dict,
) -> tuple[list[dict], list[dict]]:
    """Link rows that need a moderator's attention, worst first.

    `live_characters` maps a game id to whatever the caller wants to carry
    along for it (a name, or a (name, gp) pair); only its keys are used here.
    Passing it in rather than reading a table keeps the harmful/harmless
    decision testable with literals, and lets whois reuse this on a subset.

    Deliberately takes no set of live Discord accounts. None of the conflicts
    below needs one, which is what makes this immune to {guild}_discord being
    stale -- that table is rebuilt only by !members_discord and !sync_counters,
    both manual, so it can be weeks out of date.

    Rows for a character that has left the guild are not conflicts. They are
    the only record of who was once linked to what, and Functions.GP_roles
    joins *from* {guild}_game, so a departed character is never matched.
    """
    return conflicts_from_usable(usable_links(link_rows), live_characters)


def conflicts_from_usable(
    usable: list[dict],
    live_characters: dict,
) -> tuple[list[dict], list[dict]]:
    """find_link_conflicts for rows usable_links has already cleaned.

    Split out so a caller that has normalized the rows itself does not pay for
    it twice -- and, more to the point, so it cannot end up disagreeing with
    this function about which rows count.
    """
    by_character: dict[str, list[dict]] = {}
    for row in usable:
        if row['g_id'] not in live_characters:
            continue
        by_character.setdefault(row['g_id'], []).append(row)

    character_conflicts = []
    for g_id, rows in by_character.items():
        accounts: dict[str, int] = {}
        for row in rows:
            accounts[row['d_id']] = accounts.get(row['d_id'], 0) + 1
        if len(rows) < 2:
            continue
        character_conflicts.append({
            'kind': CONFLICT_SPLIT if len(accounts) > 1 else CONFLICT_DUPLICATE,
            'g_id': g_id,
            'name': character_name(live_characters.get(g_id)),
            'accounts': sorted(accounts.items()),
            'rows': rows,
        })

    by_account: dict[str, set] = {}
    for rows in by_character.values():
        for row in rows:
            by_account.setdefault(row['d_id'], set()).add(row['g_id'])

    account_conflicts = [
        {
            'd_id': d_id,
            'characters': sorted(
                character_name(live_characters.get(g_id)) or g_id for g_id in g_ids
            ),
        }
        for d_id, g_ids in by_account.items()
        if len(g_ids) > 1
    ]

    character_conflicts.sort(key=lambda c: (CONFLICT_ORDER.index(c['kind']), c['name'] or ''))
    account_conflicts.sort(key=lambda c: c['d_id'])
    return character_conflicts, account_conflicts


def character_name(entry) -> str | None:
    """The display name out of a live-character map entry, whatever its shape."""
    if entry is None:
        return None
    if isinstance(entry, (tuple, list)):
        return entry[0] if entry else None
    if isinstance(entry, dict):
        return entry.get('name')
    return entry


# Discord rejects a message over 2000 characters outright. Budgeting the body
# rather than capping the number of entries is what makes that safe regardless
# of how long the names in them turn out to be. One budget for the whole
# message, not one per block: two blocks each sized against the limit add up to
# twice the limit, and the headers, fences and "...and N more" lines are on top
# of that again.
DISCORD_MESSAGE_LIMIT = 2000
CONFLICT_REPORT_BUDGET = 1500


def _fit_entries(entries: list[list[str]], budget: int) -> tuple[list[str], int]:
    """As many whole entries as fit, plus how many were left out."""
    lines: list[str] = []
    used = 0
    for index, entry in enumerate(entries):
        size = sum(len(line) + 1 for line in entry)
        if used + size > budget and lines:
            return lines, len(entries) - index
        lines.extend(entry)
        used += size
    return lines, 0


def format_conflicts(
    io_guild: str,
    character_conflicts: list[dict],
    account_conflicts: list[dict],
    budget: int = CONFLICT_REPORT_BUDGET,
) -> str | None:
    """One guild's conflict report, or None when there is nothing to report.

    Returning None rather than a "nothing found" string is what lets the weekly
    job stay silent while the manual command still confirms it ran: whether
    there is anything to say is a fact about the data and belongs here, but
    what silence *means* depends on who asked, so the caller decides that.

    Entries are sorted worst-first, so when the budget truncates the report it
    is always the least urgent ones that fall off the end.
    """
    if not character_conflicts and not account_conflicts:
        return None

    lines = [f"**{io_guild} -- link conflicts**"]
    # Drawn down by each block, so the second one gets what the first left
    # rather than a second full allowance.
    remaining = budget

    if character_conflicts:
        entries = []
        for conflict in character_conflicts:
            name = conflict['name'] or conflict['g_id']
            if conflict['kind'] == CONFLICT_SPLIT:
                entry = [f"{name}  --  linked to {len(conflict['accounts'])} different accounts"]
                entry += [
                    f"    {d_id}   ({count} row{'s' if count > 1 else ''})"
                    for d_id, count in conflict['accounts']
                ]
                entry.append("    ^ each one is given the rank role; fix with !relink")
            else:
                d_id, count = conflict['accounts'][0]
                entry = [
                    f"{name}  --  {count} identical rows for {d_id}",
                    "    ^ harmless, a repeated !assign; tidy with !relink",
                ]
            entries.append(entry)

        body, omitted = _fit_entries(entries, remaining)
        lines.append("```\n" + "\n".join(body) + "\n```")
        remaining -= sum(len(line) + 1 for line in body)
        if omitted:
            lines.append(f"...and {omitted} more character(s); run !conflicts for one guild.")

    if account_conflicts:
        entries = [
            [f"{conflict['d_id']}  --  {', '.join(conflict['characters'])}"]
            for conflict in account_conflicts
        ]
        body, omitted = _fit_entries(entries, max(remaining, 0))
        lines.append(
            "One account on several live characters (may be a legitimate alt, "
            "but !mygains and !kick will pick one arbitrarily):"
        )
        lines.append("```\n" + "\n".join(body) + "\n```")
        if omitted:
            lines.append(f"...and {omitted} more account(s).")

    return "\n".join(lines)


def plan_relink(
    existing_rows: list[dict],
    target_d_id: str,
) -> tuple[list[int], int | None, bool]:
    """Which rows to delete, which to keep, and whether to insert.

    Keeps the lowest-rowid row already on the target account rather than
    deleting everything and inserting fresh. Two reasons: the surviving row is
    the original link, so its position in the append-only table still says
    roughly when it was made; and the common case -- a character assigned twice
    to the same account -- becomes a single delete that rewrites nothing.

    Returns rowids, never positions. The `index` column looks like a key and is
    not one: assign never sets it, so it is NULL on 745 of 939 Aetherian rows,
    and both rows of the one real duplicate in the live database have
    `index = NULL`. Deleting on it would hit unrelated rows or none at all.

    The triple is resolved before anything is written so that the delete set
    cannot accidentally include a row inserted moments earlier.
    """
    target = normalize_discord_id(target_d_id)
    if target is None:
        # Without this, an unusable target matches any row whose D_ID is also
        # unusable -- the blank rows -- so the junk row would be kept as though
        # it were correct and every real link deleted with nothing replacing it.
        raise ValueError(f"Not a usable Discord id to relink to: {target_d_id!r}")

    ordered = sorted(existing_rows, key=lambda row: row['rowid'])

    keep_rowid = None
    for row in ordered:
        if normalize_discord_id(row.get('d_id')) == target:
            keep_rowid = row['rowid']
            break

    delete_rowids = [row['rowid'] for row in ordered if row['rowid'] != keep_rowid]
    return delete_rowids, keep_rowid, keep_rowid is None


def assign_block_message(
    io_guild: str,
    game_name: str,
    existing_links: list[tuple],
    target_d_id=None,
) -> str | None:
    """Why !assign should refuse, or None to let it proceed.

    Distinguishes the two cases because they call for different things from a
    moderator: re-running !assign for somebody already linked is a no-op worth
    saying plainly, while a different account on the same character is the
    conflict !relink exists to resolve.
    """
    if not existing_links:
        return None

    target = normalize_discord_id(target_d_id)
    described = [
        f"{display or 'unknown'} ({normalize_discord_id(d_id)})"
        for d_id, display in existing_links
    ]
    linked_ids = {normalize_discord_id(d_id) for d_id, _ in existing_links}

    if target is not None and linked_ids == {target}:
        return (
            f"{game_name} is already linked to that account in {io_guild}. "
            f"Nothing to do."
        )
    return (
        f"{game_name} is already linked in {io_guild} to {', '.join(described)}. "
        f"Use !relink to point it at a different account."
    )


def whois_characters(
    link_rows: list[dict],
    live_characters: dict,
    live_accounts: dict,
    role_holders: dict,
    gains: dict,
) -> list[dict]:
    """One record per character, with every account linked to it.

    Grouped by character rather than by person. Following the links
    transitively would be the truer "one person" unit, but a single shared
    account chains dozens of unrelated characters into one blob that no longer
    fits in a Discord message; each link carries `also_linked` instead, which
    gives a moderator the same cross-check in a shape they can read.

    `role_holders` maps a Discord id to True/False resolved live from the
    Discord member cache, or omits it when unknown. It is not read from
    {guild}_discord, which nothing rebuilds automatically.
    """
    usable = usable_links(link_rows)

    characters_by_account: dict[str, set] = {}
    for row in usable:
        if row['g_id'] in live_characters:
            characters_by_account.setdefault(row['d_id'], set()).add(row['g_id'])

    by_character: dict[str, list[dict]] = {}
    for row in usable:
        by_character.setdefault(row['g_id'], []).append(row)

    conflicts, _ = conflicts_from_usable(usable, live_characters)
    kinds = {conflict['g_id']: conflict['kind'] for conflict in conflicts}

    records = []
    for g_id, rows in by_character.items():
        live = live_characters.get(g_id)
        name = character_name(live)
        gp = live[1] if isinstance(live, (tuple, list)) and len(live) > 1 else None

        # A character who has left has no current name, so fall back to the last
        # one recorded against them. "Who was this?" is the whole question being
        # asked about a departed character, and a bare game id does not answer it.
        last_known = None
        if name is None:
            for row in sorted(rows, key=lambda r: r['rowid'], reverse=True):
                if row.get('g_name'):
                    last_known = row['g_name']
                    break

        links = []
        for row in sorted(rows, key=lambda r: r['rowid']):
            d_id = row['d_id']
            others = sorted(
                character_name(live_characters.get(other)) or other
                for other in characters_by_account.get(d_id, set())
                if other != g_id
            )
            links.append({
                'd_id': d_id,
                'display': live_accounts.get(d_id) or row.get('display'),
                'name_at_link': row.get('g_name'),
                'stale_name': bool(name and row.get('g_name') and row['g_name'] != name),
                'in_discord': role_holders.get(d_id),
                'also_linked': others,
            })

        records.append({
            'g_id': g_id,
            'name': name,
            'last_known_name': last_known,
            'gp': gp,
            'in_game': g_id in live_characters,
            'gains': gains.get(g_id, []),
            'links': links,
            'kind': kinds.get(g_id),
        })

    records.sort(key=lambda r: (not r['in_game'], (r['name'] or r['g_id']).lower()))
    return records


def whois_unlinked(
    matched_characters: dict,
    matched_accounts: dict,
    linked_g_ids: set,
    linked_d_ids: set,
) -> tuple[list, list]:
    """Matches that have no link row at all, which is itself the answer.

    A character in the guild that was never assigned, or a role holder with no
    in-game name attached, is the most common thing a moderator is actually
    asking about. Reporting nothing for them would make the command look broken
    on exactly the cases it exists to explain.
    """
    characters = sorted(
        (character_name(entry) or g_id, g_id)
        for g_id, entry in matched_characters.items()
        if g_id not in linked_g_ids
    )
    accounts = sorted(
        (display or d_id, d_id)
        for d_id, display in matched_accounts.items()
        if d_id not in linked_d_ids
    )
    return characters, accounts


def format_whois(
    io_guild: str,
    characters: list[dict],
    unlinked_characters: list,
    unlinked_accounts: list,
    gain_weeks: int = WHOIS_GAIN_WEEKS,
) -> str | None:
    """One guild's whois answer, or None when nothing in it matched."""
    if not characters and not unlinked_characters and not unlinked_accounts:
        return None

    lines = [f"**{io_guild}**"]
    block = []

    for record in characters:
        if record['in_game']:
            name = record['name'] or record['g_id']
        else:
            last = record.get('last_known_name')
            name = f"{last} (last known name)" if last else record['g_id']
        status = "in game" if record['in_game'] else "NOT in the guild in game"
        if record['gp'] is not None:
            status += f", {record['gp']:,} GP"
        block.append(f"{name}  ({status})")
        block.append(f"    game id  {record['g_id']}")

        if record['gains']:
            shown = record['gains'][-gain_weeks:]
            # Counted from what is actually printed, not from gain_weeks: weeks
            # with no recorded figure are dropped, and labelling one value as
            # "last 4 wks" reads as four weeks of that number.
            recent = ", ".join(str(value) for value in shown)
            block.append(f"    last {len(shown)} wk{'s' if len(shown) != 1 else ''}  {recent}")

        for link in record['links']:
            if link['in_discord'] is True:
                where = "has the role"
            elif link['in_discord'] is False:
                where = "NOT in the server / no role"
            else:
                where = "role unknown"
            block.append(f"    discord  {link['display']} ({link['d_id']}) -- {where}")
            if link['stale_name']:
                block.append(f"        linked as '{link['name_at_link']}' -- renamed since")
            if link['also_linked']:
                block.append(f"        also linked to: {', '.join(link['also_linked'])}")

        if record['kind'] == CONFLICT_SPLIT:
            block.append("    !! several accounts on this character -- see !conflicts")
        elif record['kind'] == CONFLICT_DUPLICATE:
            block.append("    !! duplicate rows for the same account -- see !conflicts")
        block.append("")

    for name, g_id in unlinked_characters:
        block.append(f"{name}  (in game, no !assign link)")
        block.append(f"    game id  {g_id}")
        block.append("")

    for display, d_id in unlinked_accounts:
        block.append(f"{display}  ({d_id})  (has the role, no in-game name linked)")
        block.append("")

    lines.append("```\n" + "\n".join(block).rstrip() + "\n```")
    return "\n".join(lines)


def format_whois_too_many(term: str, names: list, limit: int = WHOIS_MAX_CHARACTERS) -> str:
    """Names only, when a term matched too many characters to expand."""
    shown = ", ".join(sorted(names)[:40])
    return (
        f"'{term}' matches {len(names)} characters, more than the {limit} this can "
        f"show in full. Narrow the search.\n```\n{shown}\n```"
    )


def resolve_relink_target(
    term: str,
    live_characters: dict,
    historical_names: dict,
) -> tuple[tuple | None, str | None]:
    """Find the one live character a relink term means, or explain why not.

    `live_characters` is keyed (guild, g_id) so a term can be resolved without
    the moderator naming a guild. `historical_names` maps a lowercased former
    name to the (guild, g_id) pairs that once used it.

    Tried in order: exact game id, current in-game name, then a former name.
    The former-name step is last and is the one that can be ambiguous -- 91
    names appear in both guilds' history -- so anything matching more than one
    live character returns a refusal listing them. It never picks one.
    """
    needle = term.strip()
    if not needle:
        return None, "Give a character name or game id to relink."

    exact = [key for key in live_characters if key[1] == needle]
    if len(exact) == 1:
        return exact[0], None

    lowered = needle.lower()
    by_name = [
        key for key, entry in live_characters.items()
        if (character_name(entry) or '').lower() == lowered
    ]
    if len(by_name) == 1:
        return by_name[0], None
    if len(by_name) > 1:
        return None, _ambiguous_relink(needle, by_name, live_characters)

    historical = [key for key in historical_names.get(lowered, []) if key in live_characters]
    unique = sorted(set(historical))
    if len(unique) == 1:
        return unique[0], None
    if len(unique) > 1:
        return None, _ambiguous_relink(needle, unique, live_characters)

    return None, (
        f"No character called '{needle}' is currently in either guild in game. "
        f"Check the spelling, or run !members_game first if they only just joined."
    )


def _ambiguous_relink(term: str, keys: list, live_characters: dict) -> str:
    described = "\n".join(
        f"    {guild}  {character_name(live_characters.get((guild, g_id))) or ''}  {g_id}"
        for guild, g_id in keys
    )
    # Only the game id is offered: !relink takes no guild argument, and telling
    # a moderator to name one sends them into a mis-parse where the guild name
    # binds to the character slot.
    return (
        f"'{term}' matches more than one live character. Re-run with the game id "
        f"of the one you mean:\n```\n{described}\n```"
    )


def format_relink_result(
    io_guild: str,
    character_name_now: str,
    g_id: str,
    removed_rows: list[dict],
    kept_row: dict | None,
    inserted: dict | None,
    backup_name: str | None = None,
    warning: str | None = None,
) -> str:
    """What relink did, printing every removed row in full.

    After the commit this message is the only readable copy of those rows, so
    it prints all five columns rather than a count. The backup named alongside
    is the recoverable copy.
    """
    lines = [f"**{io_guild} -- relinked {character_name_now}** (`{g_id}`)"]

    if removed_rows:
        block = "\n".join(
            f"rowid {row['rowid']}   {row.get('discord') or '?'}   {row.get('d_id')}   "
            f"{row.get('display') or '?'}   {row.get('g_name') or '?'}"
            for row in removed_rows
        )
        lines.append(f"Removed {len(removed_rows)} row(s):")
        lines.append(f"```\n{block}\n```")
    else:
        lines.append("Removed nothing.")

    if inserted:
        lines.append(
            f"Linked to **{inserted.get('display')}** (`{inserted.get('d_id')}`)."
        )
    elif kept_row is not None:
        lines.append(f"Kept the existing row (rowid {kept_row['rowid']}) -- it was already correct.")

    if warning:
        lines.append(warning)
    if backup_name:
        lines.append(f"Backup taken first: `{backup_name}`")
    return "\n".join(lines)


def relink_warning(io_guild: str, display: str, other_characters: list) -> str | None:
    """Flag an account that will now be on more than one live character."""
    if not other_characters:
        return None
    return (
        f"Note: {display} is also linked to {', '.join(sorted(other_characters))} "
        f"in {io_guild}. That may be a legitimate alt, but !mygains and !kick "
        f"will pick one of them arbitrarily."
    )


# ---------------------------------------------------------------------------
# Extracted from LedBotCode.py
# ---------------------------------------------------------------------------

def compute_remaining_to_rankup(gp: int, roles: list[int]) -> int | str:
    """GP still needed to reach the next rank, or "You have the final rank".

    Replaces a fragile original implementation (a descending, out-of-natural-
    order threshold list plus negative-index wraparound) that had two
    boundary bugs: GP below the lowest threshold silently returned 0 instead
    of the real gap to the first rank, and GP exactly equal to the top
    threshold returned 0 (implying "0 more needed") instead of the "final
    rank" message -- only GP *strictly above* the top threshold triggered it.
    """
    sorted_roles = sorted(roles)
    if gp >= sorted_roles[-1]:
        return "You have the final rank"
    for threshold in sorted_roles:
        if gp < threshold:
            return threshold - gp
    return "You have the final rank"  # unreachable given the check above; kept for safety


def filter_personal_gains(df: pd.DataFrame, g_id) -> pd.DataFrame:
    """Rows for a single G_ID, with the G_ID column dropped."""
    filtered = df[df['G_ID'] == g_id]
    return filtered.drop('G_ID', axis=1)


def find_missing(assigned: pd.Series, actual: pd.Series) -> pd.Series:
    """Values in assigned that don't appear in actual. Used for both the
    'assigned but not in discord' and 'assigned but not in game' diffs in
    sync_counters, which used to repeat this exact expression twice.
    """
    return assigned.loc[~assigned.isin(actual)]


def interpret_action_result(result_value: str | None, success_msg: str, failure_msg: str) -> str:
    """Map a Firebase cloud-function 'result' value to a Discord message.
    Shared by invite/kick, which used to duplicate this mapping independently.
    """
    if result_value is None:
        return failure_msg
    if result_value.lower() == "true":
        return success_msg
    return failure_msg


def assign_error_message(error: Exception) -> str:
    """Message for !assign's local error handler."""
    if isinstance(error, commands.MissingRequiredArgument):
        return "Missing required argument. Please provide all the necessary parameters."
    elif isinstance(error, commands.CommandInvokeError):
        return f"An error occurred while processing the command: {error.original}"
    elif isinstance(error, commands.UserNotFound):
        return "User not found."
    else:
        return "Command error"


# Ordered rather than a dict: MissingRequiredArgument is a subclass of
# UserInputError, so the more specific entry has to be matched first. A None
# message means "expected, but deliberately silent" -- a cooldown or a
# mistyped command name shouldn't produce a reply at all. MissingAnyRole is
# listed separately because it is NOT a subclass of MissingRole, so the
# has_any_role commands (wb-now, wb-next, gemdrop) used to fall through to
# the silent default when an unauthorized user tried them.
EXPECTED_COMMAND_ERRORS: list[tuple[type, str | None]] = [
    (commands.MissingRole, "You don't have the required permissions to use this command."),
    (commands.MissingAnyRole, "You don't have the required permissions to use this command."),
    (commands.MissingRequiredArgument, "Missing an argument!"),
    (commands.BotMissingPermissions, "The bot doesn't have the required permissions to run this command."),
    (commands.UserInputError, "There was an error in the input."),
    (commands.CommandOnCooldown, None),
    (commands.CommandNotFound, None),
]

UNEXPECTED_ERROR_MESSAGE = (
    "Something went wrong running that command. Ask a dev to check the bot logs."
)


def command_error_message(error: Exception) -> str | None:
    """Message for the global on_command_error handler, or None to send nothing.

    Anything not in EXPECTED_COMMAND_ERRORS is treated as a real bug rather
    than a predictable user mistake and gets a generic message. Previously
    every unlisted exception type returned None, so any command without its
    own .error handler -- which is all of them except assign -- failed with
    no Discord reply and no log line at all.

    Pair with is_unexpected_command_error to decide whether to also log a
    traceback; this function stays pure so it can be tested on its own.
    """
    for error_type, message in EXPECTED_COMMAND_ERRORS:
        if isinstance(error, error_type):
            return message
    return UNEXPECTED_ERROR_MESSAGE


def is_unexpected_command_error(error: Exception) -> bool:
    """True when error is not one of the anticipated user-facing cases, i.e.
    when it is worth writing a full traceback to the log."""
    return not any(
        isinstance(error, error_type) for error_type, _ in EXPECTED_COMMAND_ERRORS
    )


# The hour the weekly GP job runs, in the host's local time. Written once:
# the loop needs a cheap gate it can apply on every tick without touching the
# database, and should_run_weekly_gp needs the same condition, and those two
# disagreeing silently would stop the job from ever running.
WEEKLY_GP_HOUR = 2


def is_weekly_gp_window(now: datetime) -> bool:
    """Whether now falls in the weekly job's window: the 2 AM hour of a local
    Saturday. The cheap half of the gate, with no persistence lookup."""
    return is_saturday(now) and now.hour == WEEKLY_GP_HOUR


def should_run_weekly_gp(now: datetime, already_ran: bool) -> bool:
    """Whether the weekly GP job should fire on this tick.

    The loop polls local time every 15 minutes rather than using
    tasks.loop(time=...), because a naive time there is interpreted as UTC by
    discord.py while this check and Functions.get_date() both work in local
    time -- on a non-UTC host the two disagreed and the job ran hours from the
    intended 2 AM.

    already_ran must come from persistent storage, not a module global. Several
    ticks land inside the 2 AM hour, and the bot can restart inside it too --
    systemd runs it with Restart=on-failure -- so an in-memory marker is lost
    exactly when it is needed and the whole cycle runs a second time. That is
    the duplicate weekly report this loop exists to prevent.
    """
    if not is_weekly_gp_window(now):
        return False
    return not already_ran


def is_saturday(dt: datetime) -> bool:
    """Pins the "5 = Saturday" convention used by the weekly GP job."""
    return dt.weekday() == 5


# ---------------------------------------------------------------------------
# Extracted from cogs/giveaway.py
# ---------------------------------------------------------------------------

class GiveawayArgumentError(ValueError):
    """The giveaway arguments cannot be resolved into a guild filter plus a GP
    requirement. Carries a message written to be shown straight to the mod."""


def resolve_giveaway_args(
    guild_name: str | None,
    gp_required: int | None,
) -> tuple[str | None, int | None]:
    """Resolve the !giveaway command's overloaded guild_name/gp_required args.

    A bare numeric third argument (e.g. `!giveaway <msg> #chan 500`) is
    reinterpreted as a GP requirement with no guild filter. With neither given,
    gp_required defaults to 0, meaning no minimum.

    Naming a guild without a GP requirement raises GiveawayArgumentError. It
    used to leave gp_required as None, which the caller's SQL then compared
    against as NULL -- never true for any row -- so naming a guild silently
    disqualified every participant. Requiring the number is a deliberate choice
    over quietly defaulting it: for a giveaway, an argument that silently
    changes who can win is worse than one that asks the mod to be explicit.
    """
    if gp_required is None and guild_name and guild_name.isdigit():
        gp_required = int(guild_name)
        guild_name = None

    # "" arrives from an empty quoted argument. Downstream it is falsy and means
    # "both guilds", so normalize it to None rather than letting it through as a
    # guild name that matches nothing.
    if not guild_name:
        if gp_required is None:
            gp_required = 0
        return None, gp_required

    # Validate the guild here rather than letting a typo reach table_name() and
    # surface as a raw ValueError string. This function is where giveaway
    # arguments are resolved, so it is where they should be checked.
    if guild_name not in GUILD_GIDS:
        raise GiveawayArgumentError(
            f"'{guild_name}' is not one of our guilds. Use one of: {', '.join(GUILD_NAMES)}."
        )

    if gp_required is None:
        raise GiveawayArgumentError(
            f"Give a GP requirement when you name a guild, for example "
            f"`!giveaway <message id> #channel {guild_name} 500`. Use 0 for no minimum."
        )

    return guild_name, gp_required


def filter_eligible_participants(participants: list, winners: list) -> list:
    """Participants who haven't already won before (by .id)."""
    return [p for p in participants if p.id not in winners]
