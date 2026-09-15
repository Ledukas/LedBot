"""Pure logic extracted from LedBotCode.py / Functions.py / cogs/giveaway.py.

Every function here is a plain, deterministic computation with no Discord API
calls, no database connection, and no network access -- everything it needs
comes in as a parameter. That's what makes this module cheap and safe for
every test file (and LedBotCode.py/Functions.py/cogs/giveaway.py themselves)
to import: unlike those modules, importing logic.py has zero side effects.

A few of these were extracted specifically because doing so surfaced real
bugs in the original inline code -- see the docstring/comments on
compute_gp_rank_role, filter_red_gp, and compute_remaining_to_rankup. Those
are noted in TODO.md as fixed. command_error_message is the deliberate
exception: it preserves a known, still-open gap (see its docstring) rather
than silently fixing it here.
"""

from datetime import datetime
import re

import pandas as pd
from discord.ext import commands


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


def command_error_message(error: Exception) -> str | None:
    """Message for the global on_command_error handler, or None to send nothing.

    Deliberately preserves a known gap rather than fixing it here: there is
    no catch-all branch, so any exception type not explicitly listed below
    (not just CommandOnCooldown) returns None and the user sees no feedback
    at all. See TODO.md ("Global error handler silently swallows unrecognized
    exceptions") -- that's a separate, larger fix not yet chosen to be
    tackled; this function is pinned by a test that documents this exact gap.
    """
    if isinstance(error, commands.MissingRole):
        return "You don't have the required permissions to use this command."
    elif isinstance(error, commands.MissingRequiredArgument):
        return "Missing an argument!"
    elif isinstance(error, commands.BotMissingPermissions):
        return "The bot doesn't have the required permissions to run this command."
    elif isinstance(error, commands.UserInputError):
        return "There was an error in the input."
    elif isinstance(error, commands.CommandOnCooldown):
        return None
    return None


def is_saturday(dt: datetime) -> bool:
    """Pins the "5 = Saturday" convention used by the weekly GP job."""
    return dt.weekday() == 5


# ---------------------------------------------------------------------------
# Extracted from cogs/giveaway.py
# ---------------------------------------------------------------------------

def resolve_giveaway_args(
    guild_name: str | None,
    gp_required: int | None,
) -> tuple[str | None, int | None]:
    """Resolve the !giveaway command's overloaded guild_name/gp_required args.

    A bare numeric third argument (e.g. `!giveaway <msg> #chan 500`) is
    reinterpreted as a GP requirement with no guild filter. If neither is
    given, gp_required defaults to 0 (no minimum). Note: if a guild name IS
    given but gp_required is not, gp_required is returned unchanged as None
    (not defaulted to 0) -- this matches the original behavior faithfully,
    though it means the caller's SQL GP filter ends up comparing against
    NULL in that case, which is a pre-existing quirk, not something this
    extraction changes.
    """
    if gp_required is None and guild_name and guild_name.isdigit():
        gp_required = int(guild_name)
        guild_name = None
    if gp_required is None and guild_name is None:
        gp_required = 0
    return guild_name, gp_required


def filter_eligible_participants(participants: list, winners: list) -> list:
    """Participants who haven't already won before (by .id)."""
    return [p for p in participants if p.id not in winners]
