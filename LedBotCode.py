import discord
from discord import app_commands
from discord.ext import commands, tasks
from discord.ext.commands.bot import Bot
import pandas as pd
import sqlite3
import os
import json
from dotenv import load_dotenv
import requests
import shlex
from datetime import datetime, timedelta, time
import inspect
import asyncio
import random
import sys
import traceback
import functools

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
    for guild_name in guilds_data.keys():     
        guild = ctx.guild # discord guild = server
        role = discord.utils.get(guild.roles, name=guild_name) # IOguild = IdleOnGuild. Must have a matching role name
        if role is None:
            await ctx.send(f"Role '{guild_name}' not found")
            return
        members = role.members
        role_members = [member for member in members if role in member.roles]
        data = {'Discord': [member.name + '#' + member.discriminator for member in role_members], 
                'D_ID': [int(member.id) for member in role_members],
                'Display': [member.display_name for member in role_members],}
        df = pd.DataFrame(data)
        # store the data in a database
        df.to_sql(logic.table_name(guild_name, 'discord'), conn, if_exists='replace')
        conn.commit()
    await ctx.send("Discord members exported")

#export members from the game
@bot.command(name='members_game')
@commands.has_role("Moderator")
async def members_guild(ctx):
    await Functions.GP_export(email_a, email_p)
    await ctx.send("GP exported")


#sync counters
@bot.command(name='sync_counters')
@commands.has_role("Moderator")
async def sync_counters(ctx):
    await members_discord(ctx)
    await members_guild(ctx)
    
    for guild_name in guilds_data.keys():  
    
        table_name_members = logic.table_name(guild_name, 'members')
        table_name_discord = logic.table_name(guild_name, 'discord')
        table_name_game = logic.table_name(guild_name, 'game')

        # Discord names for assigning
        with open('sync.txt', 'w') as f:
            f.write(f'*{guild_name}*\n')
            f.write('*Discord display names:\n')
        c = conn.cursor()
        try:
            c.execute('SELECT * FROM '+table_name_discord)
            rows = c.fetchall()
        except Exception as e:
            print("line: " + str(inspect.currentframe().f_lineno) + "\n error: " + str(e))
            await ctx.send(f"Could not read {table_name_discord}: {e}")
            return
        sync_discord_list = []
        for row in rows:
            try:
                value = row[2]
                results = c.execute('SELECT * FROM ' + table_name_members + ' WHERE D_ID = ?', (value,))
                if results.fetchone() == None:
                    sync_discord = row[3] + ", " + str(row[2])
                    sync_discord_list.append(sync_discord)
            except Exception as e:
                print("line: " + str(inspect.currentframe().f_lineno) + "\n error: " + str(e))
        with open('sync.txt', 'a') as f:
            for item in sync_discord_list: 
                try:
                    f.write(str(item)+'\n')
                except Exception as e:
                    print("line: " + str(inspect.currentframe().f_lineno) + "\n error: " + str(e))

        # Game names for assigning
        try:
            with open('sync.txt', 'a') as f:
                f.write('\n*In-game names:\n')
        except Exception as e:
                    print("line: " + str(inspect.currentframe().f_lineno) + "\n error: " + str(e))
        c.execute('SELECT * FROM '+table_name_game) 
        rows = c.fetchall()
        kickable_list = []
        for row in rows:
            value = row[2]
            results = c.execute('SELECT * FROM ' + table_name_members + ' WHERE G_ID = ?', (value,))
            if results.fetchone() == None:
                kickable = row[1]
                kickable_list.append(kickable)
        with open('sync.txt', 'a') as f:
            for item in kickable_list: 
                f.write(str(item)+'\n')

        # Preparing the dataframes for further comparison
        df_list = pd.read_sql_query("SELECT * FROM " + table_name_members, conn) # df_list- data from members table
        df_list_discord = df_list.loc[:, "D_ID"]
        df_list_game = df_list.loc[:, "G_ID"]
        df_discord = pd.read_sql_query("SELECT * FROM "+ table_name_discord, conn)
        df_discord = df_discord.loc[:, "D_ID"]
        df_game = pd.read_sql_query("SELECT * FROM "+ table_name_game, conn)
        df_game = df_game.loc[:, "G_ID"]

        # Game names, assigned but not in discord
        with open('sync.txt', 'a') as f:
            f.write('\n*Assigned but not in discord:\n')
        df_assigned_notindisocrd = logic.find_missing(df_list_discord, df_discord)
        for value in df_assigned_notindisocrd:
            temp = c.execute('SELECT G_ID FROM ' + table_name_members + ' WHERE D_ID = ?', (value,))
            game_id = c.fetchall()
            for value in game_id: 
                temp = c.execute('SELECT G_NAME from ' + table_name_game + ' WHERE G_ID =?', value)
                game_name = c.fetchall()
                for item in game_name:
                    with open('sync.txt', 'a') as f:
                        f.write(str(item)+'\n')

        # Discord accounts, assigned but not in game
        with open('sync.txt', 'a') as f:
            f.write('\n*Assigned but not in game:\n')
        df_assigned_notingame = logic.find_missing(df_list_game, df_game)
        for value in df_assigned_notingame:
            try:
                temp = c.execute('SELECT D_ID FROM ' + table_name_members + ' WHERE G_ID = ?', (value,))
                discord_id = c.fetchall()
            except Exception as e:
                print("line: " + str(inspect.currentframe().f_lineno) + "\n error: " + str(e))
                continue
            for value in discord_id:
                temp = c.execute('SELECT Display from ' + table_name_discord + ' WHERE D_ID = ?', value)
                discord_name = c.fetchall()
                for item in discord_name:
                    with open('sync.txt', 'a') as f:
                        f.write(str(item)+'\n')

        file = discord.File("sync.txt")
        await ctx.send(file=file)

        c.close()
        os.remove("sync.txt")
    await ctx.send("Sync files generated")
    
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
        c.execute('INSERT INTO ' + table_name_members + ' (Discord, D_ID, Display, G_ID, G_NAME) VALUES (?,?,?,?,?)',
                    (user.name + '#' + user.discriminator, user.id, user.display_name, game[2], game[1]))

        await ctx.send(f"Assigned {game_name} to {user.display_name}, {user.id}")
    else:
        await ctx.send("User not found.")
        
    conn.commit()


async def post_json(url, json=None, headers=None):
    """requests.post, run off the event loop.

    requests is synchronous, so calling it straight from an async command
    handler blocks the whole bot for the length of the round trip -- every
    other command stalls behind it, which on the Pi's connection is very
    noticeable. Functions.get_cell_value already offloads its blocking Google
    call the same way.
    """
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(
        None, functools.partial(requests.post, url, json=json, headers=headers)
    )


IDENTITY_TOOLKIT_URL = (
    "https://identitytoolkit.googleapis.com/v1/accounts:signInWithPassword"
    "?key=AIzaSyAU62kOE6xhSrFqoXQPv6_WHxYilmoUxDk"
)


async def guild_login(ctx, IOguild):
    """Resolve a guild name to its in-game id plus a fresh Firebase id token.

    Returns (gid, id_token), or (None, None) after replying with the reason.
    Shared by invite and kick, which used to carry their own copy of this.
    """
    if IOguild not in guilds_data:
        await ctx.send(f"'{IOguild}' is not one of our guilds. Use: {', '.join(guilds_data)}")
        return None, None

    login = {
        "email": guilds_data[IOguild]["email"],
        "password": os.environ.get('PASSWORD'),
        "returnSecureToken": True,
    }
    response = await post_json(IDENTITY_TOOLKIT_URL, json=login)
    id_token = response.json().get("idToken", "")
    if not id_token:
        # Previously this fell through and sent "Bearer " with no token, so a
        # credentials problem surfaced as an unrelated failure further on.
        await ctx.send(f"Could not log in to {IOguild} in-game -- check the bot's credentials.")
        return None, None

    return guilds_data[IOguild]["gid"], id_token


#command to send an invite
@bot.command(name='invite')
@commands.has_role("Moderator")
async def invite(ctx, IOguild, InviteName):

    gid, id_token = await guild_login(ctx, IOguild)
    if gid is None:
        return

    # send invite
    guildData = {
        "data": {
            "gid": gid,
            "targetUsername": InviteName
        }
    }
    headers = {
        "Authorization": "Bearer " + id_token
    }
    response = await post_json("https://us-central1-idlemmo.cloudfunctions.net/igs", json=guildData, headers=headers)
    
    print(response.status_code)
    print(response.content.decode())
    
    data = json.loads(response.content.decode())
    result_value = data["result"]
    
    await ctx.send(logic.interpret_action_result(
        result_value,
        "Invite sent. Let a moderator know when you join",
        "Error, invite not sent",
    ))
    
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
    response = await post_json("https://us-central1-idlemmo.cloudfunctions.net/gk", json=guildData, headers=headers)
    
    data = json.loads(response.content.decode())
    result_value = data["result"]
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
async def run_weekly_gp(ack_channel):
    """One weekly GP cycle: refresh in-game GP, roll this week's snapshot
    columns, reassign rank roles, report low-GP members, and back up the DB.

    ack_channel receives only the "GP exported" acknowledgement -- the invoking
    channel for !GP_weekly, the mod channel for the scheduled run. The reports
    themselves always go to the mod channel, so the weekly history stays in one
    place no matter where the command was typed. This used to be two
    near-identical copies that would have drifted apart the first time only one
    of them was updated.
    """
    print("Starting weekly GP process")
    await members_guild(ack_channel)
    await Functions.GP_databases()

    for guild_name in logic.GUILD_NAMES:
        monthly_gp_df = await Functions.GP_dataframe(guild_name)
        # Only one guild runs the GP rank ladder and the Monthly Top role.
        if guild_name == logic.RANK_ROLE_GUILD:
            await Functions.GP_roles(bot, monthly_gp_df)
            await LedukasSpam_channel.send("GP roles fixed!")
        await Functions.red_gp(LedukasSpam_channel, monthly_gp_df, guild_name)
    #await Functions.promotions(bot, LedukasSpam_channel)

    conn.commit()
    print("weekly GP calculated")

    # Data only changes on this weekly cycle, so back it up right after. A
    # separate always-on schedule would just copy the same unchanged data most
    # days.
    try:
        backup_path = run_backup()
        await LedukasSpam_channel.send(f"Weekly backup created: `{backup_path.name}`")
    except Exception as e:
        print(e)
        await LedukasSpam_channel.send(f"Weekly backup failed: {e}")


@bot.command(name='GP_weekly')
@commands.has_role("Moderator")
async def GP_weekly_man(ctx):
    await run_weekly_gp(ctx)


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
# _last_weekly_run_date makes the several ticks inside the 2 AM hour idempotent.
# tasks.loop is still what runs it, for its start()/is_running() lifecycle: a
# gateway reconnect re-firing on_ready cannot spawn a second copy, which is what
# used to send the weekly report twice.
_last_weekly_run_date = None


@tasks.loop(minutes=15)
async def gp_weekly_loop():
    global _last_weekly_run_date
    now = datetime.now()
    if not logic.should_run_weekly_gp(now, _last_weekly_run_date):
        return
    _last_weekly_run_date = now.date()

    # Without this, any exception escaping run_weekly_gp stops the task for
    # good -- discord.ext.tasks does not reschedule after an unhandled error --
    # and the only sign would be a missing weekly report. Same permanent-death
    # mode baba_ping was hardened against.
    try:
        await run_weekly_gp(LedukasSpam_channel)
    except Exception as e:
        print(f"weekly GP job failed: {e}", file=sys.stderr)
        traceback.print_exception(type(e), e, e.__traceback__)
        try:
            await LedukasSpam_channel.send(f"Weekly GP job failed: {e}")
        except Exception as send_error:
            print(f"could not report weekly GP failure: {send_error}", file=sys.stderr)

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
    if not gp_weekly_loop.is_running():
        gp_weekly_loop.start()
    global LedukasSpam_channel
    LedukasSpam_channel = bot.get_channel(LedukasSpam_channelID)
    
    await load_cogs()

if __name__ == "__main__":
    bot.run(TOKEN)

    conn.close()
