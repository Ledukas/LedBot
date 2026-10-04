import discord
from discord import app_commands
from discord.ext import commands, tasks
from discord.ext.commands.bot import Bot
import pandas as pd
import sqlite3
import os
from dotenv import load_dotenv
import requests
import urllib3
import shlex
from datetime import datetime, timedelta, time
import inspect
import asyncio
import random
import sys
import traceback
import functools
# Aliased: `time` is already taken above by datetime.time.
from time import time as epoch_now

import Functions
import logic
from scripts.backup_db import run_backup

load_dotenv()

TOKEN = os.environ.get('TOKEN')
prefix = '!'
GP_prefix = 'GP'
separator = ' | '
bot = commands.Bot(command_prefix=prefix, intents=discord.Intents.all())
LedukasSpam_channelID = int(os.environ.get('SPAM_CHANNEL_ID'))
# Resolved in on_ready. Defined here so the `is None` guards that read it are
# correct on their own terms rather than relying on on_ready having run first.
LedukasSpam_channel = None
# Where the invite auto-link posts its welcome, and the two channels the
# welcome links to. Optional: a missing one skips the welcome with a note in
# the mod channel instead of stopping the bot at import.
WELCOME_POST_CHANNEL_ID = logic.parse_optional_id(os.environ.get('WELCOME_POST_CHANNEL_ID'))
WELCOME_INFO_CHANNEL_ID = logic.parse_optional_id(os.environ.get('WELCOME_INFO_CHANNEL_ID'))
ROLES_CHANNEL_ID = logic.parse_optional_id(os.environ.get('ROLES_CHANNEL_ID'))
email_a = os.environ.get('EMAIL_A')
email_p = os.environ.get('EMAIL_P')

# WAL lets the weekly job and a moderator command touch the database at the
# same moment without hitting "database is locked".
conn = sqlite3.connect('DatabaseLedBot.db')
conn.execute("PRAGMA journal_mode=WAL")

# GP rank thresholds live in logic.RANK_THRESHOLDS (single source of truth).

df_members_game = None

roles_to_remove = {
    # Both guild roles plus every rank role, top one included. 'True Aetherian'
    # used to be omitted here, so a kicked top-rank member kept it while every
    # other rank lost theirs; that was an oversight, not intent.
    'removeroles': list(logic.GUILD_NAMES) + logic.RANK_ROLE_NAMES,
    'giverole': 'Former Aetherian',
}
# !kick hands this out, so a returning member arrives carrying it; the invite
# auto-link takes it back off.
FORMER_ROLE_NAME = roles_to_remove['giverole']

# Only the email mapping is local -- the guild ids live in logic.GUILD_GIDS.
guild_emails = {"Aetherians": email_a, "Pretherians": email_p}
guilds_data = {
    name: {"gid": gid, "email": guild_emails[name]}
    for name, gid in logic.GUILD_GIDS.items()
}

# Load cogs
async def load_cogs():
    cogs_path = "cogs"  # The directory where your cogs are stored
    for filename in os.listdir(cogs_path):
        if filename.endswith(".py"):  # Only load Python files
            extension = f"{cogs_path}.{filename[:-3]}"  # Convert to module path
            try:
                await bot.load_extension(extension)
                print(f"Loaded cog: {extension}")
            except Exception as e:
                print(f"Failed to load cog {extension}: {e}")


#pd.set_option('display.max_rows', None)  

##---------------------------------------------  Commands
# a simple command to test if the bot is alive
@bot.command(name='Led')
@commands.has_role("Moderator")
async def test(ctx):
    await ctx.send(f'Bot!')

@bot.command()
@commands.has_role("Moderator")
async def led_stop(ctx):
    print("Shutting down...")
    await ctx.send("Shutting down...")
    await bot.close()
 
@bot.command(name='wb-now')
@commands.has_any_role(*logic.GUILD_NAMES, "Moderator", "Honorary Aetherian")
@commands.cooldown(1, 15, commands.BucketType.guild)
async def wb_now(ctx):
    try:
        async with ctx.typing():
            strategy = await Functions.get_cell_value('B4')
        
        await ctx.reply(f'**Current Week Boss Strategy:**\n\n{strategy}')
    except Exception as e:
        await ctx.reply('Error fetching current week boss strategy. Please try again later!')
        print(f"wb-now failed: {e}")

@bot.command(name='wb-next')
@commands.has_any_role(*logic.GUILD_NAMES, "Moderator", "Honorary Aetherian")
@commands.cooldown(1, 15, commands.BucketType.guild)
async def wb_next(ctx):
    try:
        async with ctx.typing():
            strategy = await Functions.get_cell_value('C2')
        
        await ctx.reply(f'**Next Week Boss Strategy:**\n\n{strategy}')
    except Exception as e:
        await ctx.reply('Error fetching next week boss strategy. Please try again later!')
        print(f"wb-next failed: {e}")

@bot.command()
@commands.has_any_role(*logic.GUILD_NAMES, "Moderator", "Honorary Aetherian")
@commands.cooldown(1, 15, commands.BucketType.guild)
async def gemdrop(ctx):
    message = (
        "Lava is doing a Gem(velope) Drop/Reset right now! "
        "Make sure you are in one of the following servers if it's a Gem(velope) Drop!\n\n"
        "Servers the drops can happen in: Carrot/Cake/Balloon/Pecunia/Dice. "
        "***Must be in W1 Town to get the drops!***\n"
        "<@&852898751219761152>"
    )
    await ctx.send(message)

#backup for the database
@bot.command(name="backup")
@commands.has_role("Moderator")
async def export_data(ctx):

    try:
        backup_path = run_backup()
        await ctx.send(f"Backup created: `{backup_path.name}`")
    except Exception as e:
        print(e)
        await ctx.send(f"Backup failed: {e}")

# export members from discord
@bot.command(name='members_discord')
@commands.has_role("Moderator")
async def members_discord(ctx):
    warnings, refreshed = refresh_discord_rosters()
    saved = f"Discord members exported: {', '.join(refreshed)}" if refreshed else None
    if warnings:
        await send_embeds(ctx, warnings, saved)
    else:
        await ctx.send(saved)


def refresh_discord_rosters(guild_names=logic.GUILD_NAMES):
    """Save who holds each guild role into {guild}_discord.

    Returns (warning embeds for anything not refreshed, the guilds that were).
    A refused or failed write leaves the previous list in place, which the
    links lists then use.
    """
    warnings, refreshed = [], []
    discord_guild = bot.get_guild(809954021028134943)
    for guild_name in guild_names:
        role = discord.utils.get(discord_guild.roles, name=guild_name) if discord_guild else None
        if discord_guild is None:
            problem = "the Discord server isn't available"
        elif role is None:
            problem = f"there is no '{guild_name}' role"
        else:
            try:
                problem = Functions.refresh_discord_roster(
                    guild_name, logic.discord_roster_rows(role.members)
                )
            except Exception as e:
                problem = f"{type(e).__name__}: {e}"
        if problem:
            warnings.append(logic.warning_embed(
                f"{guild_name} -- role list not refreshed",
                f"{problem}. The lists use the previous one.",
            ))
        else:
            refreshed.append(guild_name)
    return warnings, refreshed

#export members from the game
@bot.command(name='members_game')
@commands.has_role("Moderator")
async def members_guild(ctx):
    await export_game_rosters()
    await ctx.send("GP exported")


# Held from a roster read to its write, and by the weekly job through
# GP_databases: a !sync_counters export awaiting Firebase when the 2 AM run
# starts would otherwise land its older roster in the snapshot.
roster_export_lock = asyncio.Lock()


async def export_game_rosters(guild_names=logic.GUILD_NAMES):
    # The same non-blocking roster read and cached login the invite poll uses.
    # This was pyrebase, which is synchronous and froze the whole bot --
    # the invite poll included -- for the length of the export.
    for guild_name in guild_names:
        rows = await fetch_guild_roster(guild_name)
        problem = logic.validate_roster_rows(rows, Functions.game_roster_size(guild_name))
        if problem:
            raise ValueError(f"the {guild_name} roster wasn't saved: {problem}")
        Functions.write_game_roster(guild_name, rows)


#sync counters
@bot.command(name='sync_counters')
@commands.has_role("Moderator")
async def sync_counters(ctx, IOguild: str = None):
    """Refresh the in-game roster and who holds the guild roles, then show
    what needs linking. !conflicts shows the same without refreshing."""
    if await reject_unknown_guild(ctx, IOguild):
        return
    if Functions.weekly_job_depth:
        await ctx.send("A weekly run is in progress -- try again once it has finished.")
        return
    guild_names = [IOguild] if IOguild else list(logic.GUILD_NAMES)
    notes = []
    async with roster_export_lock:
        for guild_name in guild_names:
            try:
                await export_game_rosters([guild_name])
            except Exception as e:
                notes.append(logic.failure_embed(
                    f"{guild_name} -- in-game roster not refreshed", e,
                    f"The {guild_name} in-game lists below are from the previous roster.",
                ))
    notes.extend(refresh_discord_rosters(guild_names)[0])
    await send_guild_sections(ctx, IOguild, links_for, "everything is linked.", leading=notes)


def links_for(guild_name):
    return Functions.links_report(guild_name, bot.get_guild(809954021028134943))


#assign members in-game and discord
@bot.command(name='assign')
@commands.has_role("Moderator")
async def assign(ctx, IOguild, user_param, game_name):
    
    # Try to get user by ID
    try:
        user = await bot.fetch_user(int(user_param))
    except ValueError:
        # If the user_param is not a valid ID, try to get user by display name
        user = discord.utils.find(lambda u: u.name == user_param or u.display_name == user_param, ctx.guild.members)

    if user:
        
        if IOguild not in guilds_data:
            await ctx.send(f"'{IOguild}' is not one of our guilds. Use: {', '.join(guilds_data)}")
            return

        input_string = ctx.message.content
        args = shlex.split(input_string)
        game_name = args[3]
        table_name_members = logic.table_name(IOguild, 'members')
        table_name_game = logic.table_name(IOguild, 'game')
        
        c = conn.cursor()
        game = c.execute('SELECT * FROM ' + table_name_game + ' WHERE G_NAME = ?', (game_name,)).fetchone()
        if game is None:
            await ctx.send(
                f"No in-game member named '{game_name}' found in {IOguild}. "
                f"Check the spelling, or run !members_game first if they only just joined."
            )
            return
        # The LEFT JOIN is what lets the refusal name the account's current
        # display rather than the one frozen into _members at assign time,
        # which is stale on most rows. CAST on both sides because affinity is
        # not applied between two columns of different storage class.
        table_name_discord = logic.table_name(IOguild, 'discord')
        existing_links = c.execute(
            f"SELECT m.D_ID, COALESCE(d.Display, m.Display) "
            f"FROM {table_name_members} m "
            f"LEFT JOIN {table_name_discord} d "
            f"  ON CAST(d.D_ID AS TEXT) = CAST(m.D_ID AS TEXT) "
            f"WHERE m.G_ID = ?",
            (game[2],),
        ).fetchall()
        blocked = logic.assign_block_message(IOguild, game_name, existing_links, user.id)
        if blocked:
            await ctx.send(blocked)
            return

        c.execute('INSERT INTO ' + table_name_members + ' (Discord, D_ID, Display, G_ID, G_NAME) VALUES (?,?,?,?,?)',
                    (user.name + '#' + user.discriminator, user.id, user.display_name, game[2], game[1]))
        # Committed before replying. Held open across the await, this write lock
        # makes any other writer -- the invite poll refreshing the roster -- wait
        # on it synchronously, which blocks the loop so this coroutine can never
        # resume to commit; the other write fails after 5 s.
        conn.commit()

        await ctx.send(f"Assigned {game_name} to {user.display_name}, {user.id}")
    else:
        await ctx.send("User not found.")
        
    conn.commit()


HTTP_TIMEOUT = (5, 20)  # connect, read -- seconds


class FirebaseRequestError(Exception):
    """A request to the game's backend failed. The message is already redacted.

    maybe_sent is False only when the connection never opened -- the one case
    where a POST certainly did not reach the game.
    """

    def __init__(self, message, maybe_sent):
        super().__init__(message)
        self.maybe_sent = maybe_sent


async def _request(method, url, **kwargs):
    """A requests call run off the event loop, with a timeout and no secrets in errors.

    requests is synchronous, so calling it straight from an async handler blocks
    the whole bot for the round trip. It also had no timeout: a request that
    never answered held its thread forever, and since the invite poll's loop
    awaits each tick, one hung read would stop polling for good.

    Errors are re-raised with tokens stripped and the original chain dropped:
    requests puts the full URL in its messages, and the roster read carries the
    guild leader's token as ?auth=, which would otherwise reach both the mod
    channel and the journal.
    """
    loop = asyncio.get_running_loop()
    try:
        return await loop.run_in_executor(
            None, functools.partial(method, url, timeout=HTTP_TIMEOUT, **kwargs)
        )
    except requests.RequestException as e:
        raise FirebaseRequestError(
            logic.redact_secrets(f"{type(e).__name__}: {e}"),
            maybe_sent=not _never_connected(e),
        ) from None


def _never_connected(error):
    """Whether a request failed before any connection opened, so it can't have
    reached the server. Anything else -- a read timeout, a reset mid-response --
    may have been acted on.

    A DNS failure or refused connection arrives as requests.ConnectionError
    wrapping urllib3's NewConnectionError; treating it as "maybe sent" would
    record a phantom !invite that then blocks elimination for a week.
    """
    if isinstance(error, requests.ConnectTimeout):
        return True
    if isinstance(error, requests.ConnectionError) and error.args:
        cause = error.args[0]
        return isinstance(getattr(cause, 'reason', cause), urllib3.exceptions.NewConnectionError)
    return False


async def post_json(url, json=None, headers=None):
    return await _request(requests.post, url, json=json, headers=headers)


async def get_json(url, params=None):
    return await _request(requests.get, url, params=params)


SIGN_IN_URL = "https://identitytoolkit.googleapis.com/v1/accounts:signInWithPassword"
IGS_URL = "https://us-central1-idlemmo.cloudfunctions.net/igs"
ROSTER_URL = "https://idlemmo.firebaseio.com/_guild/{gid}/m.json"
SIGNIN_RETRY_SECONDS = 600

# guild -> (id token, expiry in epoch seconds). Tokens last an hour, so the
# invite poll signs in about once an hour rather than on every read.
_firebase_tokens = {}
# guild -> epoch seconds before which the invite poll must not sign in; forever
# after a credentials failure, until a manual command signs in successfully.
_poll_signin_paused_until = {}


class FirebaseSignInError(Exception):
    def __init__(self, message, permanent):
        super().__init__(message)
        self.permanent = permanent


async def firebase_sign_in(IOguild, from_poll=False, force=False):
    """(gid, id token) for a guild's leader account, cached until near expiry.

    The poll backs off after a failure so it can't lock the account that !kick
    and the weekly export also sign into: ten minutes for a transient error, and
    indefinitely for a credentials error, which retrying can never fix. Manual
    commands always try, and a manual success lifts the pause.
    """
    now = epoch_now()
    cached = _firebase_tokens.get(IOguild)
    if cached and not force and logic.token_is_fresh(now, cached[1]):
        return guilds_data[IOguild]["gid"], cached[0]
    if from_poll and now < _poll_signin_paused_until.get(IOguild, 0):
        raise FirebaseSignInError(
            f"signing in to {IOguild} is paused after an earlier failure", permanent=False
        )

    response = await post_json(
        f"{SIGN_IN_URL}?key={os.environ.get('FIRE_API')}",
        json={
            "email": guilds_data[IOguild]["email"],
            "password": os.environ.get('PASSWORD'),
            "returnSecureToken": True,
        },
    )
    try:
        data = response.json()
    except ValueError:
        data = {}
    token = data.get("idToken")
    if token:
        _firebase_tokens[IOguild] = (token, now + int(data.get("expiresIn", 3600)))
        _poll_signin_paused_until.pop(IOguild, None)
        return guilds_data[IOguild]["gid"], token

    message = (data.get("error") or {}).get("message") or f"HTTP {response.status_code}"
    permanent = logic.classify_signin_error(message) == 'permanent'
    if from_poll:
        _poll_signin_paused_until[IOguild] = float('inf') if permanent else now + SIGNIN_RETRY_SECONDS
    raise FirebaseSignInError(logic.redact_secrets(message), permanent=permanent)


async def guild_login(ctx, IOguild):
    """Resolve a guild name to its in-game id plus a Firebase id token.

    Returns (gid, id_token), or (None, None) after replying with the reason.
    Shared by invite and kick.
    """
    if IOguild not in guilds_data:
        await ctx.send(f"'{IOguild}' is not one of our guilds. Use: {', '.join(guilds_data)}")
        return None, None
    try:
        return await firebase_sign_in(IOguild)
    except (FirebaseSignInError, FirebaseRequestError) as e:
        # A failed login used to fall through and send "Bearer " with no token,
        # so a credentials problem surfaced as an unrelated failure further on.
        await ctx.send(f"Could not log in to {IOguild} in-game ({e}) -- check the bot's credentials.")
        return None, None


async def fetch_guild_roster(IOguild, from_poll=False):
    """The live in-game roster, as logic.build_game_members_rows rows.

    Plain REST with the cached token, off the event loop. Used by the weekly
    export and !members_game as well as the invite poll, so there is one way to
    read a roster and one sign-in path.
    """
    gid, token = await firebase_sign_in(IOguild, from_poll=from_poll)
    url = ROSTER_URL.format(gid=gid)
    response = await get_json(url, params={"auth": token})
    if response.status_code == 401:
        # A token can be revoked before it expires. One fresh sign-in, then stop.
        gid, token = await firebase_sign_in(IOguild, from_poll=from_poll, force=True)
        response = await get_json(url, params={"auth": token})
    if response.status_code != 200:
        raise FirebaseRequestError(
            f"reading the {IOguild} roster failed: HTTP {response.status_code}", maybe_sent=False
        )
    try:
        payload = response.json()
    except ValueError:
        payload = None
    if not isinstance(payload, dict):
        raise FirebaseRequestError(f"the {IOguild} roster came back empty", maybe_sent=False)
    return logic.build_game_members_rows(payload)


#command to send an invite
@bot.command(name='invite')
@commands.has_role("Moderator")
async def invite(ctx, IOguild: str = None, InviteName: str = None, *, member: str = None):
    """Invite someone in game, then link, role and welcome them when they join.

    The member is optional so the old `!invite <guild> <name>` still works; such
    an invite is watched for, but a join is reported with a ready !assign rather
    than linked, since there is no Discord account to link it to.
    """
    if IOguild is None or InviteName is None:
        await ctx.send("Usage: !invite <guild> <in-game name> [@member]")
        return
    if IOguild not in logic.GUILD_NAMES:
        await ctx.send(f"'{IOguild}' is not one of our guilds. Use: {', '.join(logic.GUILD_NAMES)}")
        return

    member_obj = None
    target = None
    if member:
        try:
            member_obj = await commands.MemberConverter().convert(ctx, member)
        except commands.BadArgument:
            await ctx.send(
                f"Couldn't find '{member}' in this server. They need to be here to be "
                f"linked -- or leave the member off to watch for the invite without linking."
            )
            return
        target = {
            'd_id': str(member_obj.id),
            'display': member_obj.display_name,
            'discord': member_obj.name + '#' + member_obj.discriminator,
        }

    # The baseline -- everyone already in the guild -- is read before the invite
    # goes out. Read after, an instant accept would put the invitee in their own
    # baseline, and they could never be matched.
    try:
        roster = await fetch_guild_roster(IOguild)
    except (FirebaseRequestError, FirebaseSignInError) as e:
        await ctx.send(
            f"Couldn't read the {IOguild} roster ({e}), so the invite wasn't sent -- "
            f"without it I can't tell who joins."
        )
        return
    # Only now: the roster read replaces a token that turns out to be revoked,
    # and the invite must go out with the current one.
    gid, id_token = await guild_login(ctx, IOguild)
    if gid is None:
        return
    baseline_at = epoch_now()
    baseline = {row['G_ID'] for row in roster}
    if any(logic.name_key(row['G_NAME']) == logic.name_key(InviteName) for row in roster):
        await ctx.send(f"'{InviteName}' is already in {IOguild}.")
        return

    warnings = []
    if target:
        names = {row['G_ID']: row['G_NAME'] for row in roster}
        already = sorted(
            names[row['g_id']] for row in Functions.load_link_rows(IOguild)
            if row['d_id'] == target['d_id'] and row['g_id'] in names
        )
        if already:
            warnings.append(
                f"Note: {member_obj.display_name} is already linked to {', '.join(already)} "
                f"in {IOguild}, so this would be a second character."
            )

    send_confirmed = True
    try:
        response = await post_json(
            IGS_URL,
            json={"data": {"gid": gid, "targetUsername": InviteName}},
            headers={"Authorization": "Bearer " + id_token},
        )
    except FirebaseRequestError as e:
        if not e.maybe_sent:
            await ctx.send(f"Couldn't reach the game, so the invite wasn't sent ({e}).")
            return
        # The request may well have reached the game even though no answer came
        # back. Dropping it would lose a real invite: once they accept, running
        # !invite again is refused as "already in the guild", so they'd never be
        # linked. The baseline predates the POST, so watching anyway is safe --
        # at worst it expires.
        send_confirmed = False
    else:
        try:
            payload = response.json()
        except ValueError:
            payload = None
        if not logic.action_succeeded(logic.parse_action_result(payload)):
            await ctx.send(
                f"The game refused the invite to '{InviteName}' -- check the in-game name, "
                f"and that {IOguild} has a free slot."
            )
            return

    record = Functions.record_invite(
        IOguild, InviteName, target, baseline, baseline_at, epoch_now(), send_confirmed
    )
    member_has_role = member_obj is not None and any(role.name == IOguild for role in member_obj.roles)
    await ctx.send(
        logic.format_invite_ack(
            IOguild, InviteName, member_obj.mention if member_obj else None,
            record, send_confirmed, member_has_role, warnings,
        ),
        allowed_mentions=discord.AllowedMentions.none(),
    )


@bot.command(name='invites')
@commands.has_role("Moderator")
async def invites(ctx):
    """Invites being watched for, plus recent ones that need a look."""
    now = epoch_now()
    # Superseded ones are listed because they still block elimination for a
    # week, and !uninvite is how they're cleared.
    shown = Functions.load_invites(
        statuses=logic.INVITE_OPEN_STATUSES + (
            logic.INVITE_LINKED, logic.INVITE_NEEDS_ATTENTION, logic.INVITE_SUPERSEDED,
        ),
        since=now - logic.INVITE_MEMORY_SECONDS,
    )
    await ctx.send(logic.format_invites_list(shown, now), allowed_mentions=discord.AllowedMentions.none())


@bot.command(name='uninvite')
@commands.has_role("Moderator")
async def uninvite(ctx, IOguild: str = None, *, name: str = None):
    """Stop watching for an invite -- above all, to stop a welcome going to the
    wrong person when the invite was tagged with the wrong member."""
    if IOguild is None or name is None:
        await ctx.send("Usage: !uninvite <guild> <in-game name>")
        return
    if IOguild not in logic.GUILD_NAMES:
        await ctx.send(f"'{IOguild}' is not one of our guilds. Use: {', '.join(logic.GUILD_NAMES)}")
        return
    found = Functions.find_cancellable_invite(IOguild, name)
    if found is None:
        await ctx.send(f"No open invite to '{name}' in {IOguild}.")
        return
    Functions.set_invite_status(found['id'], logic.INVITE_CANCELLED)
    await ctx.send(logic.format_uninvite(IOguild, found), allowed_mentions=discord.AllowedMentions.none())

#command to kick from guild
@bot.command(name='kick')
@commands.has_role("Moderator")
async def kick(ctx, IOguild, KickID):
    
    guild = bot.get_guild(809954021028134943)

    gid, id_token = await guild_login(ctx, IOguild)
    if gid is None:
        return

    # kick
    table_name_members = logic.table_name(IOguild, 'members')
    
    c = conn.cursor()
    c.execute(f"SELECT G_ID FROM {table_name_members} WHERE D_ID = ?", (KickID,))
    resultUID = c.fetchall()
    if not resultUID:
        await ctx.send(f"No member with Discord ID {KickID} is assigned in {IOguild}.")
        return
    resultUID = resultUID[0][0]
    guildData = {
        "data": {
            "uid": resultUID,
            "gid": gid
        }
    }
    headers = {
        "Authorization": "Bearer " + id_token
    }
    kick_uncertain = False
    try:
        response = await post_json("https://us-central1-idlemmo.cloudfunctions.net/gk", json=guildData, headers=headers)
    except FirebaseRequestError as e:
        if not e.maybe_sent:
            await ctx.send(f"Couldn't reach the game, so the kick wasn't sent ({e}).")
            return
        # No answer in time, but the kick may well have happened in game. Carry
        # on to the role cleanup rather than leave a kicked member with every
        # Discord role, and say plainly that the in-game result is unknown.
        response = None
        kick_uncertain = True

    payload = None
    if response is not None:
        try:
            payload = response.json()
        except ValueError:
            payload = None
    result_value = logic.parse_action_result(payload)
    c.execute(f"SELECT G_NAME FROM {table_name_members} WHERE D_ID = ?", (KickID,))
    Disp_result = c.fetchall()
    Disp_result = Disp_result[0][0]
    
    # The in-game kick has already happened at this point. The Discord role
    # cleanup below is a separate step that can fail on its own, and it used to
    # fail into a bare print while the moderator was still told the kick had
    # fully succeeded -- so a member could be removed in game yet silently keep
    # every Discord role. Track it and say so instead.
    role_warning = None
    if guild is None:
        role_warning = "the Discord server was not in cache"
    else:
        member = guild.get_member(int(KickID))
        if member is None:
            role_warning = f"{Disp_result} was not found in the Discord server"
        else:
            print(f"Found member to kick: {Disp_result}")
            try:
                role2give = discord.utils.get(guild.roles, name=roles_to_remove['giverole'])
                if role2give is None:
                    role_warning = f"the '{roles_to_remove['giverole']}' role does not exist"
                else:
                    await member.add_roles(role2give)

                for role in roles_to_remove['removeroles']:
                    role2remove = discord.utils.get(guild.roles, name=role)
                    if role2remove is not None and role2remove in member.roles:
                        await member.remove_roles(role2remove)
            except Exception as e:
                print(e)
                role_warning = str(e)

    message = logic.interpret_action_result(
        result_value,
        f"{Disp_result} has been kicked from {IOguild}",
        "Error, not kicked",
    )
    if kick_uncertain:
        message = (
            f"The game didn't answer in time, so {Disp_result} may or may not have been "
            f"kicked from {IOguild} -- check in game. Their Discord roles were updated as for a kick."
        )
    if role_warning:
        message += " (Discord roles were not updated: " + role_warning + ")"
    await ctx.send(message)

# a command to check gains
@bot.command(name='mygains')
async def mygains(ctx):
    if ctx.channel.id != 810014953477898240:
        await ctx.message.delete()
        return
    
    c = conn.cursor()
    user_did = ctx.author.id
    
    # One message per guild the member belongs to, in GUILD_NAMES order. This
    # was four hand-written branches covering every combination of two guilds.
    memberships = []
    for guild_name in logic.GUILD_NAMES:
        c.execute(
            f"SELECT * FROM {logic.table_name(guild_name, 'discord')} WHERE D_ID = ?",
            (str(user_did),),
        )
        if c.fetchone() is not None:
            memberships.append(guild_name)

    if not memberships:
        print("No GP gains recorded")
        await ctx.send("No GP gains recorded yet")
        return

    for guild_name in memberships:
        await ctx.send(await mygains2(guild_name, c, user_did))

async def mygains2(IOguild, c, user_did):
    
    table_name_members = logic.table_name(IOguild, 'members')
    table_name_game = logic.table_name(IOguild, 'game')
    
    monthly_gp_df = await Functions.GP_dataframe(IOguild)
    query = f"SELECT G_ID FROM {table_name_members} WHERE D_ID = ?"
    c.execute(query, (user_did,))
    user_gid = c.fetchall()
    # mygains decides membership from {guild}_discord, which is a different
    # population from {guild}_members: holding the Discord role does not mean
    # anyone has linked an in-game name yet. sync_counters exists to list
    # exactly these people, so this is a normal state, not a broken one.
    if not user_gid:
        return (
            f"You have the {IOguild} role but no in-game name linked yet -- "
            f"ask a moderator to run !assign for you."
        )
    personal_gains = logic.filter_personal_gains(monthly_gp_df, user_gid[0][0])
    # Header uses the guild name in place of the first column's real name
    # (e.g. "Name"), matching the original formatting exactly.
    personal_gains = personal_gains.rename(columns={personal_gains.columns[0]: IOguild})
    table_block = logic.format_table_block(personal_gains)

    user_gid = str(user_gid[0][0])
    query = f"SELECT GP FROM {table_name_game} WHERE G_ID = ?"
    c.execute(query, (user_gid,))
    user_gp = c.fetchall()
    if not user_gp:
        return (
            f"No current {IOguild} in-game data found for your linked name -- "
            f"you may no longer be in the guild in game."
        )

    remaining_points = logic.compute_remaining_to_rankup(int(user_gp[0][0]), logic.GP_THRESHOLDS)

    # Send the header and data as a message
    message = f"```{table_block}"
    if IOguild == logic.RANK_ROLE_GUILD:
        message += f"Total: {str(user_gp[0][0])}    GP needed to rank up: {remaining_points}```"
    else:
        message += f"Total: {str(user_gp[0][0])}```"
    return message

@bot.command(name='promotions')
@commands.has_role("Moderator")
async def promotions(ctx):
    await Functions.promotions(bot, LedukasSpam_channel)

@bot.command(name='whois')
@commands.has_role("Moderator")
async def whois(ctx, *, term: str = None):
    """Look a member up by game name, game id, Discord name or Discord id.

    One term, matched against every column rather than detected -- game ids
    come in two formats and names can look like anything, so guessing which
    kind of thing was typed would be guesswork that silently finds nothing.

    Replies where it was asked rather than in the mod channel: this is a
    lookup tool and the answer is wanted in the conversation that prompted it.
    Note that means the account-to-character mapping lands in whatever channel
    a moderator types it in.
    """
    if term is None:
        await ctx.send("Usage: !whois <game name | game id | discord name | discord id>")
        return
    await Functions.whois(ctx, term, discord_guild=ctx.guild)

@bot.command(name='conflicts')
@commands.has_role("Moderator")
async def conflicts(ctx, IOguild: str = None):
    """What needs linking, from the last refresh. Also part of the weekly report;
    !sync_counters refreshes first."""
    await send_guild_sections(ctx, IOguild, links_for, "everything is linked.")

@bot.command(name='relink')
@commands.has_role("Moderator")
async def relink(ctx, character: str = None, *, member: str = None):
    """Point an in-game character at the right Discord account.

    The guild is worked out from whichever roster the character is on, so it
    is not an argument; an ambiguous name is refused with the candidates
    rather than resolved by guessing.
    """
    if character is None or member is None:
        await ctx.send("Usage: !relink <game name | game id> <@member | discord id>")
        return

    try:
        live_characters, historical_names = Functions.load_relink_candidates()
    except Exception as e:
        await ctx.send(f"Error reading the guild rosters: {e}")
        return

    resolved, refusal = logic.resolve_relink_target(character, live_characters, historical_names)
    if resolved is None:
        await ctx.send(refusal)
        return
    guild_name, g_id = resolved

    # MemberConverter handles a mention, a raw id, a name or name#discriminator
    # in one; the fetch_user fallback is what still resolves somebody who has
    # left the server, which is exactly the case a relink is usually fixing.
    user = None
    try:
        user = await commands.MemberConverter().convert(ctx, member)
    except commands.BadArgument:
        target_id = logic.normalize_discord_id(member)
        if target_id is not None:
            try:
                user = await bot.fetch_user(int(target_id))
            except discord.NotFound:
                user = None
    if user is None:
        await ctx.send(f"Could not find a Discord user matching '{member}'.")
        return

    await Functions.relink(ctx, guild_name, g_id, {
        'discord': user.name + '#' + user.discriminator,
        'd_id': str(user.id),
        'display': getattr(user, 'display_name', user.name),
    })

@bot.command(name='gp_audit')
@commands.has_role("Moderator")
async def gp_audit(ctx, IOguild: str = None):
    """Members whose weekly GP gains are worth a closer look.

    Runs automatically as part of the weekly cycle; this is for checking
    between Saturdays, or re-reading last week's report without re-running it.
    Replies in the invoking channel rather than the mod channel, since a
    moderator asking for it on demand wants to see the answer where they asked.
    """
    await send_guild_sections(ctx, IOguild, Functions.gp_audit_report, "nobody flagged.")


async def reject_unknown_guild(ctx, IOguild):
    if IOguild is not None and IOguild not in logic.GUILD_NAMES:
        await ctx.send(f"Unknown guild '{IOguild}'. Pick one of: {', '.join(logic.GUILD_NAMES)}")
        return True
    return False


async def send_guild_sections(ctx, IOguild, build, clean_text, leading=()):
    """One reply for a per-guild report command: embeds for the guilds with
    something to report, a line for each that has nothing."""
    if await reject_unknown_guild(ctx, IOguild):
        return

    embeds, clean = list(leading), []
    for guild_name in ([IOguild] if IOguild else logic.GUILD_NAMES):
        section = build(guild_name)
        embeds.extend(section)
        if not section:
            clean.append(f"{guild_name}: {clean_text}")
    await send_embeds(ctx, embeds, "\n".join(clean) or None)

#baba pings
async def baba_ping():
    while True:
        try:
            now = datetime.now()
            if now.minute == 57:
                guild = bot.get_guild(809954021028134943)
                baba_role = discord.utils.get(guild.roles, name="Spiketrap") if guild else None
                baba_channel = bot.get_channel(1032916681569349632)
                if guild and baba_role and baba_channel:
                    await baba_channel.send(f"{baba_role.mention} The spiketrap is awaiting your death!")
                else:
                    print("baba_ping: guild/role/channel not found, skipping this hour")
                await asyncio.sleep(100)
            else:
                await asyncio.sleep(40)
        except Exception as e:
            # Never let a transient failure (rate limit, cache gap, etc.) kill this loop
            # permanently -- log it and keep going instead of letting the task die silently.
            print(f"baba_ping error: {e}")
            await asyncio.sleep(40)

#does weekly GP things
async def run_weekly_gp(ack_channel=None):
    """Run the weekly cycle with the invite poll held off {guild}_game.

    Wraps the whole cycle, not just the reporting part: the window that matters
    is between the roster export and GP_databases reading it, which sits
    before the cycle's own try block.
    """
    with Functions.weekly_job():
        try:
            await _run_weekly_gp(ack_channel)
        finally:
            # Not reached when the process dies mid-cycle: on_ready then finds
            # the run unended and says its report was lost.
            try:
                Functions.mark_weekly_runs_ended()
            except Exception as e:
                print(f"could not mark the weekly run ended: {e}", file=sys.stderr)


class WeeklyReportIncomplete(Exception):
    """The snapshot was taken and the report posted, but a section failed --
    which is why it must not be answered with a full !GP_weekly re-run."""


async def _run_weekly_gp(ack_channel):
    """One weekly GP cycle: refresh in-game GP, roll this week's snapshot
    columns, reassign rank roles, report low-GP members, implausible gains and
    link problems, and back up the DB.

    Everything is posted to the mod channel as one report, at the end, so the
    weekly history stays in one place no matter where !GP_weekly was typed.
    ack_channel is the invoking context for !GP_weekly (None for the scheduled
    run); it only hears that the run finished, and only when it is a different
    channel.
    """
    print("Starting weekly GP process")

    # Mark the week before doing any work, so a restart mid-cycle cannot run it
    # again, and so a manual !GP_weekly suppresses the scheduled run.
    run_key = (await Functions.get_date())["column_name1"]
    await Functions.mark_weekly_run_started(run_key)

    async with roster_export_lock:
        await export_game_rosters()
        await Functions.GP_databases()

    report = []
    failure = None
    try:
        # After the snapshot, not between it and the export: weekly_job()
        # keeps that window short, and GP_databases doesn't read this.
        try:
            report.extend(refresh_discord_rosters()[0])
        except Exception as e:
            failure = _record_failure(report, "Role lists not refreshed", e)
        for guild_name in logic.GUILD_NAMES:
            guild_failure = await collect_guild_report(guild_name, report, sync_roles=True)
            if failure is None:
                failure = guild_failure
        #await Functions.promotions(bot, LedukasSpam_channel)
    finally:
        # In a finally because GP_databases above has already written this
        # week's columns. A cycle that failed partway is exactly when a
        # snapshot matters most, so the backup must not be skipped with it.
        # No conn.commit() here: this module's connection performs no writes
        # during the cycle. Each writer commits its own -- write_game_roster
        # commits Functions.conn, and GP_databases commits the connection it
        # opens itself.
        print("weekly GP calculated")
        backup_name = None
        try:
            backup_name = run_backup().name
        except Exception as e:
            print(e)
            report.append(logic.failure_embed("Weekly backup failed", e))
        content, embeds = logic.finish_weekly_report(
            logic.weekly_report_heading(run_key), report, backup_name
        )
        # Nothing in this finally may raise: an exception here would replace the
        # one that brought us into it and hide why the cycle actually failed.
        posted = False
        try:
            await send_embeds(LedukasSpam_channel, embeds, content)
            posted = True
        except Exception as e:
            print(f"could not post the weekly report: {e}", file=sys.stderr)
            try:
                await LedukasSpam_channel.send(f"The weekly report could not be posted: {e}")
            except Exception as fallback_error:
                print(f"could not report that either: {fallback_error}", file=sys.stderr)

    # Raised only now, after the report is out, so the caller can say the run
    # had errors without suggesting a re-run that would redo the snapshot.
    if failure is not None:
        raise WeeklyReportIncomplete(f"{type(failure).__name__}: {failure}") from failure
    if ack_channel is not None and ack_channel.channel.id != LedukasSpam_channelID:
        if posted:
            await ack_channel.send(f"Weekly run finished -- the report is in <#{LedukasSpam_channelID}>.")
        else:
            await ack_channel.send("Weekly run finished, but the report could not be posted -- see the bot's log.")


async def collect_guild_report(guild_name, report, sync_roles, today=None):
    """Append one guild's sections of the weekly report to `report`.

    Returns the first exception that cost the guild a section, or None. A
    failed step becomes an error embed and the sections that don't depend on
    it still run -- Aetherians goes first, so a role-sync hiccup used to cost
    Pretherians its whole report.
    """
    failure = None
    syncs_roles = sync_roles and guild_name == logic.RANK_ROLE_GUILD
    try:
        monthly_gp_df = await Functions.GP_dataframe(guild_name, today)
    except Exception as e:
        skipped = ", rank roles not synced" if syncs_roles else ""
        failure = _record_failure(report, f"{guild_name} -- GP table failed{skipped}", e)
    else:
        # Only one guild runs the GP rank ladder and the Monthly Top role.
        if syncs_roles:
            try:
                roles_status = await Functions.GP_roles(bot, monthly_gp_df)
            except Exception as e:
                failure = _record_failure(report, f"{guild_name} -- rank roles failed", e)
            else:
                if roles_status is not None:
                    report.append(logic.warning_embed(
                        f"{guild_name} -- rank roles", f"Not fully synced: {roles_status}"
                    ))
        report.extend(Functions.red_gp_report(monthly_gp_df, guild_name))
    report.extend(Functions.gp_audit_report(guild_name))
    report.extend(links_for(guild_name))
    return failure


def _record_failure(report, title, error):
    traceback.print_exception(type(error), error, error.__traceback__)
    report.append(logic.failure_embed(title, error))
    return error


@bot.command(name='GP_weekly')
@commands.has_role("Moderator")
async def GP_weekly_man(ctx):
    if Functions.weekly_job_depth:
        await ctx.send("A weekly run is already in progress.")
        return
    await ctx.send(f"Weekly run started -- the report will be posted in <#{LedukasSpam_channelID}>.")
    try:
        await run_weekly_gp(ctx)
    except WeeklyReportIncomplete as e:
        await ctx.send(logic.format_weekly_incomplete(str(e)))


@bot.command(name='weekly_report')
@commands.has_role("Moderator")
async def weekly_report(ctx):
    """The latest week's report again, here, without running anything: no
    export, no snapshot, no role changes, no backup."""
    today = datetime.today()
    run_key = (await Functions.get_date(today))["column_name1"]
    if not Functions.snapshot_taken(run_key):
        # Saturday before the 2 AM run, or after a run that failed before
        # GP_databases: this week has no column yet, so show the last one.
        today -= timedelta(days=7)
        run_key = (await Functions.get_date(today))["column_name1"]
    report = []
    for guild_name in logic.GUILD_NAMES:
        await collect_guild_report(guild_name, report, sync_roles=False, today=today)
    content, embeds = logic.finish_weekly_report(
        logic.weekly_report_heading(run_key, preview=True), report, None
    )
    await send_embeds(ctx, embeds, content)


##---------------------------------------------  Functions

# Ticks every 15 minutes and acts only in the 2 AM hour on a local-time
# Saturday. It does NOT use tasks.loop(time=...): a naive datetime.time there is
# interpreted as UTC by discord.py, while this guard and Functions.get_date()
# both work in local time, so on any machine not set to UTC the two disagreed
# and the job ran hours away from the intended 2 AM -- on a UTC-5 host, Saturday
# 21:00 local. Polling local time keeps the schedule consistent with the dates
# the snapshot columns are named after, and stays correct across DST, which a
# fixed UTC offset captured at import would not.
#
# The weekly_runs marker that run_weekly_gp writes makes the several ticks
# inside the 2 AM hour, and a restart within it, run the job once.
# tasks.loop is still what runs it, for its start()/is_running() lifecycle: a
# gateway reconnect re-firing on_ready cannot spawn a second copy, which is what
# used to send the weekly report twice.
@tasks.loop(minutes=15)
async def gp_weekly_loop():
    now = datetime.now()
    if not logic.is_weekly_gp_window(now):
        return

    if LedukasSpam_channel is None:
        # Return before marking the week so a later tick can still run it once
        # the channel is cached.
        print("gp_weekly_loop: mod channel not available, skipping this tick", file=sys.stderr)
        return

    run_key = (await Functions.get_date())["column_name1"]
    if not logic.should_run_weekly_gp(now, await Functions.weekly_run_already_started(run_key)):
        return

    # Without this, any exception escaping run_weekly_gp stops the task for
    # good -- discord.ext.tasks does not reschedule after an unhandled error --
    # and the only sign would be a missing weekly report. Same permanent-death
    # mode baba_ping was hardened against.
    try:
        await run_weekly_gp()
    except WeeklyReportIncomplete as e:
        # Its traceback was already logged where it happened.
        print(f"weekly GP job finished with errors: {e}", file=sys.stderr)
        try:
            await LedukasSpam_channel.send(logic.format_weekly_incomplete(str(e)))
        except Exception as send_error:
            print(f"could not report weekly GP errors: {send_error}", file=sys.stderr)
    except Exception as e:
        print(f"weekly GP job failed: {e}", file=sys.stderr)
        traceback.print_exception(type(e), e, e.__traceback__)
        try:
            await LedukasSpam_channel.send(
                f"Weekly GP job failed: {e}. It will not retry automatically -- "
                f"run !GP_weekly once the cause is fixed."
            )
        except Exception as send_error:
            print(f"could not report weekly GP failure: {send_error}", file=sys.stderr)

##---------------------------------------------  Invite auto-link

# In memory on purpose: lost on restart, these only cost one early poll and one
# repeated connectivity note. Everything that must survive a restart -- the
# invites themselves, and which arrivals were already reported -- is in the DB.
_invite_last_polled = {}
_invite_poll_failing = {}


async def report_to_mods(text):
    if LedukasSpam_channel is None:
        print(f"(mod channel unavailable) {text}", file=sys.stderr)
        return
    await LedukasSpam_channel.send(text, allowed_mentions=discord.AllowedMentions.none())


async def send_embeds(destination, embeds, content=None):
    """Post a report in as few messages as Discord's embed limits allow,
    with `content` on the first. `destination` is a channel or a Context."""
    channel = getattr(destination, 'channel', destination)
    guild = getattr(channel, 'guild', None)
    if embeds and guild is not None and not channel.permissions_for(guild.me).embed_links:
        # Without it Discord drops the embeds and posts only the content line,
        # which would look like a report with nothing in it.
        notice = "I can't show this report here: I need the Embed Links permission in this channel."
        # The footer carries the weekly backup's name, the proof the job ran.
        footer = embeds[-1].footer.text
        await destination.send(
            "\n".join(line for line in (content, notice, footer) if line),
            allowed_mentions=discord.AllowedMentions.none(),
        )
        return
    messages = logic.pack_messages(embeds) or ([[]] if content else [])
    for index, batch in enumerate(messages):
        await destination.send(
            content if index == 0 else None,
            embeds=batch,
            allowed_mentions=discord.AllowedMentions.none(),
        )


@tasks.loop(seconds=10)
async def invite_poll_loop():
    # tasks.loop stops for good after an unhandled exception, and the only sign
    # would be invites that quietly never complete.
    try:
        await invite_poll_tick()
    except Exception as e:
        print(f"invite poll tick failed: {logic.redact_secrets(e)}", file=sys.stderr)
        traceback.print_exception(type(e), e, e.__traceback__)


@invite_poll_loop.before_loop
async def _invite_poll_wait_until_ready():
    await bot.wait_until_ready()


async def invite_poll_tick():
    if Functions.weekly_job_depth > 0:
        return
    now = epoch_now()

    for pending in Functions.load_invites(statuses=(logic.INVITE_PENDING,)):
        if logic.invite_age(now, pending['sent_at']) >= logic.INVITE_TRACK_SECONDS:
            Functions.set_invite_status(pending['id'], logic.INVITE_EXPIRED)
            await report_to_mods(logic.format_invite_expired(pending['guild'], pending))

    # Anything left `linked` was interrupted between writing the link and
    # finishing; this is what resumes it after a crash or restart.
    for linked in Functions.load_invites(statuses=(logic.INVITE_LINKED,)):
        await finish_invite(linked)

    pending = Functions.load_invites(statuses=(logic.INVITE_PENDING,))
    for IOguild in logic.GUILD_NAMES:
        guild_pending = [inv for inv in pending if inv['guild'] == IOguild]
        if not guild_pending:
            continue
        newest = max(inv['sent_at'] for inv in guild_pending)
        if not logic.should_poll_guild(now, newest, _invite_last_polled.get(IOguild)):
            continue
        _invite_last_polled[IOguild] = now
        # Isolated per guild, and reported on the first failure and on recovery
        # rather than every ten seconds.
        try:
            await poll_guild_invites(IOguild)
        except Exception as e:
            detail = logic.redact_secrets(e)
            print(f"invite poll for {IOguild} failed: {detail}", file=sys.stderr)
            if not _invite_poll_failing.get(IOguild):
                _invite_poll_failing[IOguild] = True
                if isinstance(e, FirebaseSignInError) and e.permanent:
                    note = ("The login was rejected, so I've stopped retrying -- any command "
                            "that logs in (e.g. !invite) will resume it once the password is fixed.")
                else:
                    note = "I'll keep trying and say when it works again."
                await report_to_mods(f"Can't read the {IOguild} roster to watch for invites: {detail}. {note}")
        else:
            if _invite_poll_failing.pop(IOguild, False):
                await report_to_mods(f"Reading the {IOguild} roster works again.")


async def poll_guild_invites(IOguild):
    """Read one guild's roster and settle whatever invites it resolves."""
    now = epoch_now()
    # Read before the roster: an invite recorded while the read is in flight
    # would have a baseline newer than the data, making earlier members look
    # like arrivals.
    invites = Functions.load_invites(
        IOguild, statuses=logic.INVITE_MATCHABLE_STATUSES, since=now - logic.INVITE_MEMORY_SECONDS
    )
    generation = Functions.roster_generation
    rows = await fetch_guild_roster(IOguild, from_poll=True)
    fetched_at = epoch_now()
    if Functions.weekly_job_depth > 0:
        return
    roster = {row['G_ID']: row['G_NAME'] for row in rows}

    result = logic.match_invites(
        invites, roster, Functions.load_link_rows(IOguild), Functions.invite_claims(IOguild), fetched_at
    )
    for action in result['actions']:
        await apply_invite_action(IOguild, action)

    already = Functions.reported_arrivals(IOguild, since=fetched_at - logic.INVITE_MEMORY_SECONDS)
    for arrival in result['unmatched']:
        if arrival['g_id'] in already:
            continue
        # Marked first: if the send fails, a missing report beats one that
        # repeats every ten seconds.
        Functions.mark_arrival_reported(IOguild, arrival['g_id'], fetched_at)
        await report_to_mods(logic.format_unmatched_arrival(IOguild, arrival))

    outcome = Functions.refresh_game_roster(IOguild, rows, generation)
    if outcome.startswith('refused'):
        print(f"{IOguild}_game not refreshed -- {outcome}", file=sys.stderr)


async def apply_invite_action(IOguild, action):
    invite = action['invite']
    # A !uninvite or a re-invite may have landed while the roster was being read.
    current = Functions.get_invite(invite['id'])
    if current is None or any(current[key] != invite[key] for key in ('status', 'sent_at', 'd_id')):
        return

    g_id, character = action['g_id'], action['name']
    matched = {
        'matched_g_id': g_id, 'matched_name': character,
        'matched_at': epoch_now(), 'method': action['method'],
    }
    kind = action['kind']
    if kind == logic.ACTION_LINK:
        decision, others = Functions.link_for_invite(IOguild, g_id, character, current)
        if decision == 'conflict':
            Functions.set_invite_status(invite['id'], logic.INVITE_CONFLICT, **matched)
            await report_to_mods(logic.format_invite_conflict(IOguild, current, character, g_id, others))
            return
        Functions.set_invite_status(invite['id'], logic.INVITE_LINKED, **matched)
        await finish_invite(Functions.get_invite(invite['id']))
    elif kind == logic.ACTION_CONFLICT:
        Functions.set_invite_status(invite['id'], logic.INVITE_CONFLICT, **matched)
        await report_to_mods(
            logic.format_invite_conflict(IOguild, current, character, g_id, action['other_accounts'])
        )
    elif kind == logic.ACTION_REPORT_SUPERSEDED:
        Functions.set_invite_status(invite['id'], logic.INVITE_REPORTED, **matched)
        await report_to_mods(logic.format_invite_superseded(IOguild, current, character, g_id))
    elif kind == logic.ACTION_REPORT_CANCELLED:
        Functions.set_invite_status(invite['id'], logic.INVITE_REPORTED, **matched)
        await report_to_mods(logic.format_invite_cancelled_arrival(IOguild, current, character, g_id))
    elif kind == logic.ACTION_REPORT_NO_MEMBER:
        Functions.set_invite_status(invite['id'], logic.INVITE_REPORTED, **matched)
        await report_to_mods(logic.format_invite_unlinkable(IOguild, current, character, g_id))


async def _invite_needs_attention(invite, character, reason):
    Functions.set_invite_status(invite['id'], logic.INVITE_NEEDS_ATTENTION)
    await report_to_mods(logic.format_invite_needs_attention(invite['guild'], invite, character, reason))


async def finish_invite(invite):
    """Give a linked member their role and the welcome, then mark the invite done.

    Re-run on every tick for any invite still `linked`, so a crash anywhere in
    here resumes. That retry is for crashes only: a Discord error that would
    simply happen again (Forbidden because the bot's role sits below the guild
    role, say) moves the invite to needs_attention after one report, instead of
    failing and reporting every ten seconds forever.

    The welcome is sent only when this call added the guild role. That makes it
    at-most-once across crashes -- a double public welcome is worse than a
    missed one -- and stops a second welcome when a mod already did it by hand.
    """
    IOguild = invite['guild']
    character = invite.get('matched_name')
    discord_guild = bot.get_guild(809954021028134943)
    if discord_guild is None:
        return
    d_id = int(invite['d_id'])

    member = discord_guild.get_member(d_id)
    if member is None:
        # The member cache is cold right after a restart, so ask Discord directly.
        try:
            member = await discord_guild.fetch_member(d_id)
        except discord.NotFound:
            print(f"WARNING: {IOguild}: linked {character} to {d_id}, who is not in the server",
                  file=sys.stderr)
            Functions.set_invite_status(invite['id'], logic.INVITE_DONE)
            await report_to_mods(logic.format_invite_member_gone(IOguild, invite, character))
            return
        except discord.HTTPException as e:
            await _invite_needs_attention(invite, character, f"couldn't look the member up ({e})")
            return

    role = discord.utils.get(discord_guild.roles, name=IOguild)
    if role is None:
        await _invite_needs_attention(invite, character, f"there is no '{IOguild}' role")
        return
    role_added = former_removed = False
    try:
        if role not in member.roles:
            await member.add_roles(role, reason=f"Joined {IOguild} via !invite")
            role_added = True
        former = discord.utils.get(discord_guild.roles, name=FORMER_ROLE_NAME)
        if former is not None and former in member.roles:
            await member.remove_roles(former, reason=f"Rejoined {IOguild}")
            former_removed = True
    except discord.HTTPException as e:
        await _invite_needs_attention(invite, character, f"couldn't change their roles ({e})")
        return

    welcome_note = None
    if role_added:
        # The welcome is the one public step, so re-check for an !uninvite that
        # landed while the roles were being changed.
        latest = Functions.get_invite(invite['id'])
        if latest is None or latest['status'] != logic.INVITE_LINKED:
            await report_to_mods(logic.format_invite_cancelled_midway(IOguild, invite, character))
            return
        welcome_note = await post_welcome(IOguild, member)
    Functions.set_invite_status(invite['id'], logic.INVITE_DONE)
    await report_to_mods(logic.format_invite_linked(
        IOguild, invite, character, invite['matched_g_id'], invite['method'],
        role_added, former_removed, welcome_note,
    ))


async def post_welcome(IOguild, member):
    """Post the welcome; returns None on success, or a note for the mod report."""
    if None in (WELCOME_POST_CHANNEL_ID, WELCOME_INFO_CHANNEL_ID, ROLES_CHANNEL_ID):
        return ("No welcome posted: WELCOME_POST_CHANNEL_ID, WELCOME_INFO_CHANNEL_ID and "
                "ROLES_CHANNEL_ID all need setting in .env.")
    channel = bot.get_channel(WELCOME_POST_CHANNEL_ID)
    if channel is None:
        return f"No welcome posted: channel {WELCOME_POST_CHANNEL_ID} wasn't found."
    try:
        await channel.send(
            logic.format_welcome(IOguild, member.id, WELCOME_INFO_CHANNEL_ID, ROLES_CHANNEL_ID),
            allowed_mentions=discord.AllowedMentions(everyone=False, roles=False, users=[member]),
        )
    except discord.HTTPException as e:
        return f"The welcome couldn't be posted ({e})."
    return None

##---------------------------------------------  Errors
# error messages for all commands
@assign.error
async def assign_error(ctx, error):
    await ctx.send(logic.assign_error_message(error))
@bot.event
async def on_command_error(ctx, error):
    # Log first, and log regardless of who replies: an unexpected error here is
    # a real bug, and until now nothing was written anywhere for it -- no
    # Discord message and no console output -- which is why so many failures
    # looked like the bot simply ignoring the command.
    if logic.is_unexpected_command_error(error):
        print(f"Unhandled error in command {ctx.command}:", file=sys.stderr)
        traceback.print_exception(type(error), error, error.__traceback__)

    # discord.py dispatches here *in addition to* a command's own @cmd.error
    # handler, so replying for a command that has one would double-message the
    # user (assign is the only one today).
    if ctx.command is not None and ctx.command.has_error_handler():
        return

    message = logic.command_error_message(error)
    if message is not None:
        await ctx.send(message)

        
baba_task = None
# gives a message in console once the bot goes live
@bot.event
async def on_ready():
    global baba_task
    print(f'Logged in with {bot.user.name} | {bot.user.id}')
    if baba_task is None:
        baba_task = 1
        bot.loop.create_task(baba_ping())
    global LedukasSpam_channel
    LedukasSpam_channel = bot.get_channel(LedukasSpam_channelID)
    if LedukasSpam_channel is None:
        print(f"WARNING: mod channel {LedukasSpam_channelID} not found; the weekly GP job cannot report")
    # Started only after the channel it writes to is resolved: an interval loop
    # runs its body immediately on start().
    if not gp_weekly_loop.is_running():
        gp_weekly_loop.start()
    if not invite_poll_loop.is_running():
        invite_poll_loop.start()
    if None in (WELCOME_POST_CHANNEL_ID, WELCOME_INFO_CHANNEL_ID, ROLES_CHANNEL_ID):
        print("WARNING: welcome channel ids are not all set in .env; invite auto-link "
              "will link and give roles but skip the welcome", file=sys.stderr)

    await load_cogs()
    await report_interrupted_weekly_run()


async def report_interrupted_weekly_run():
    # on_ready also fires on reconnects, including mid-run, when the run is
    # merely in progress rather than interrupted.
    if Functions.weekly_job_depth or LedukasSpam_channel is None:
        return
    try:
        interrupted = Functions.interrupted_weekly_run()
        if interrupted is not None:
            await report_to_mods(logic.format_interrupted_weekly_run(*interrupted))
            Functions.mark_weekly_runs_ended()
    except Exception as e:
        print(f"could not check for an interrupted weekly run: {e}", file=sys.stderr)

if __name__ == "__main__":
    bot.run(TOKEN)

    conn.close()
