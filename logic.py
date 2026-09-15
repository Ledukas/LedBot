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
