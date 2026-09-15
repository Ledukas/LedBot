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

conn = sqlite3.connect('DatabaseLedBot.db')

# GP roles:
role_1_knight = 1000
role_2_hero = 2500
role_3_demigod = 5000
role_4_deity = 10000
role_5_titan = 25000
role_6_primordial = 50000
role_7_true = 100000
GProles = [role_6_primordial, role_5_titan, role_4_deity, role_3_demigod, role_2_hero, role_1_knight, role_7_true]

df_members_game = None

roles_to_remove = {
    'removeroles': ['Pretherians', 'Aetherians', 'Aetherian Knight', 'Aetherian Hero', 'Aetherian Demigod', 'Aetherian Deity', 'Aetherian Titan', 'Aetherian Primordial'],
    'giverole': 'Former Aetherian',
}

guilds_data = {
    "Aetherians": {
        "gid": "jSiitSSM7nO0HFuoVlsa",
        "email": email_a
        },
    "Pretherians": {
        "gid": "yuFnrJvPfK8ZdfFXHojg",
        "email": email_p
        }
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
    await bot.logout()
 
@bot.command(name='wb-now')
@commands.has_any_role("Aetherians", "Pretherians", "Moderator", "Honorary Aetherian")
@commands.cooldown(1, 15, commands.BucketType.guild)
async def wb_now(ctx):
    try:
        async with ctx.typing():
            strategy = await Functions.get_cell_value('B4')
        
        await ctx.reply(f'**Current Week Boss Strategy:**\n\n{strategy}')
    except Exception:
        await ctx.reply('Error fetching current week boss strategy. Please try again later!')
        print(Exception)

@bot.command(name='wb-next')
@commands.has_any_role("Aetherians", "Pretherians", "Moderator", "Honorary Aetherian")
@commands.cooldown(1, 15, commands.BucketType.guild)
async def wb_next(ctx):
    try:
        async with ctx.typing():
            strategy = await Functions.get_cell_value('C2')
        
        await ctx.reply(f'**Next Week Boss Strategy:**\n\n{strategy}')
    except Exception:
        await ctx.reply('Error fetching next week boss strategy. Please try again later!')
        print(Exception)

@bot.command()
@commands.has_any_role("Aetherians", "Pretherians", "Moderator", "Honorary Aetherian")
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
        df.to_sql(guild_name+"_discord", conn, if_exists='replace')
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
    
        table_name_members = guild_name+'_members'
        table_name_discord = guild_name+'_discord'
        table_name_game = guild_name+'_game'

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
        
        input_string = ctx.message.content
        args = shlex.split(input_string)
        game_name = args[3]
        table_name_members = IOguild+'_members'
        table_name_game = IOguild+'_game'
        
        c = conn.cursor()
        game = c.execute('SELECT * FROM ' + table_name_game + ' WHERE G_NAME = ?', (game_name,)).fetchone()
        c.execute('INSERT INTO ' + table_name_members + ' (Discord, D_ID, Display, G_ID, G_NAME) VALUES (?,?,?,?,?)',
                    (user.name + '#' + user.discriminator, user.id, user.display_name, game[2], game[1]))

        await ctx.send(f"Assigned {game_name} to {user.display_name}, {user.id}")
    else:
        await ctx.send("User not found.")
        
    conn.commit()


#command to send an invite
@bot.command(name='invite')
@commands.has_role("Moderator")
async def invite(ctx, IOguild, InviteName):
    
    # login
    if IOguild is None:
        await ctx.send(f"No role given")
        return
    if IOguild == "Aetherians":
        gid = "jSiitSSM7nO0HFuoVlsa"
        email = email_a
    elif IOguild == "Pretherians": 
        gid = "yuFnrJvPfK8ZdfFXHojg"
        email = email_p
    else: 
        await ctx.send("That's not our guild!")
        return
    password = os.environ.get('PASSWORD')
    
    login = {
        "email": email,
        "password": password,
        "returnSecureToken": True
    }
    
    response = requests.post("https://identitytoolkit.googleapis.com/v1/accounts:signInWithPassword?key=AIzaSyAU62kOE6xhSrFqoXQPv6_WHxYilmoUxDk", json=login)
    loginResponse = response.json()
    
    # send invite
    guildData = {
        "data": {
            "gid": gid,
            "targetUsername": InviteName
        }
    }
    headers = {
        "Authorization": "Bearer " + loginResponse.get("idToken", "")
    }
    response = requests.post("https://us-central1-idlemmo.cloudfunctions.net/igs", json=guildData, headers=headers)
    
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
    
    # login
    if IOguild is None:
        await ctx.send(f"No role given")
        return
    if IOguild == "Aetherians":
        gid = "jSiitSSM7nO0HFuoVlsa"
        email = email_a
    elif IOguild == "Pretherians": 
        gid = "yuFnrJvPfK8ZdfFXHojg"
        email = email_p
    else: 
        await ctx.send("That's not our guild!")
        return
    
    password = os.environ.get('PASSWORD')
    
    login = {
        "email": email,
        "password": password,
        "returnSecureToken": True
    }
    
    response = requests.post("https://identitytoolkit.googleapis.com/v1/accounts:signInWithPassword?key=AIzaSyAU62kOE6xhSrFqoXQPv6_WHxYilmoUxDk", json=login)
    loginResponse = response.json()
    
    # kick
    table_name_members = IOguild+'_members'
    
    c = conn.cursor()
    c.execute(f"SELECT G_ID FROM {table_name_members} WHERE D_ID = ?", (KickID,))
    resultUID = c.fetchall()
    resultUID = resultUID[0][0]
    guildData = {
        "data": {
            "uid": resultUID,
            "gid": gid
        }
    }
    headers = {
        "Authorization": "Bearer " + loginResponse.get("idToken", "")
    }
    response = requests.post("https://us-central1-idlemmo.cloudfunctions.net/gk", json=guildData, headers=headers)
    
    data = json.loads(response.content.decode())
    result_value = data["result"]
    c.execute(f"SELECT G_NAME FROM {table_name_members} WHERE D_ID = ?", (KickID,))
    Disp_result = c.fetchall()
    Disp_result = Disp_result[0][0]
    
    if guild:
        member = guild.get_member(int(KickID))

        if member:
            print(f"Found member to kick: {Disp_result}")
        else:
            print("Member not found.")
    else:
        print("Guild not found.")
    try:
        role2give = discord.utils.get(guild.roles, name='Former Aetherian')
        await member.add_roles(role2give)
        
        for role in roles_to_remove['removeroles']:
            role2remove = discord.utils.get(guild.roles, name=role)
            if role2remove in member.roles:
                await member.remove_roles(role2remove)
    except Exception as e:
        print(e)
    
    await ctx.send(logic.interpret_action_result(
        result_value,
        f"{Disp_result} has been kicked from {IOguild}",
        "Error, not kicked",
    ))

# a command to check gains
@bot.command(name='mygains')
async def mygains(ctx):
    if ctx.channel.id != 810014953477898240:
        await ctx.message.delete()
        return
    
    c = conn.cursor()
    user_did = ctx.author.id
    
    c.execute("SELECT * FROM Aetherians_discord WHERE D_ID = ?", (str(user_did),))
    result_aetherians = c.fetchone()

    c.execute("SELECT * FROM Pretherians_discord WHERE D_ID = ?", (str(user_did),))
    result_pretherians = c.fetchone()

    if result_aetherians == None and result_pretherians == None:
        print("No GP gains recorded")
        message = "No GP gains recorded yet"
        await ctx.send(message)
    elif result_aetherians and not result_pretherians:
        IOguild = "Aetherians"
        message = await mygains2(IOguild, c, user_did)
        await ctx.send(message)
    elif not result_aetherians and result_pretherians:
        IOguild = "Pretherians"
        message = await mygains2(IOguild, c, user_did)
        await ctx.send(message)
    else:
        IOguild_aetherians = "Aetherians"
        message_aetherians = await mygains2(IOguild_aetherians, c, user_did)
        await ctx.send(message_aetherians)
        
        IOguild_pretherians = "Pretherians"
        message_pretherians = await mygains2(IOguild_pretherians, c, user_did)
        await ctx.send(message_pretherians)   

async def mygains2(IOguild, c, user_did):
    
    table_name_members = IOguild+'_members'
    table_name_game = IOguild+'_game'
    
    monthly_gp_df = await Functions.GP_dataframe(IOguild)
    query = f"SELECT G_ID FROM {table_name_members} WHERE D_ID = ?"
    c.execute(query, (user_did,))
    user_gid = c.fetchall()  
    personal_gains = logic.filter_personal_gains(monthly_gp_df, user_gid[0][0])
    # Header uses the guild name in place of the first column's real name
    # (e.g. "Name"), matching the original formatting exactly.
    personal_gains = personal_gains.rename(columns={personal_gains.columns[0]: IOguild})
    table_block = logic.format_table_block(personal_gains)

    user_gid = str(user_gid[0][0])
    query = f"SELECT GP FROM {table_name_game} WHERE G_ID = ?"
    c.execute(query, (user_gid,))
    user_gp = c.fetchall()

    remaining_points = logic.compute_remaining_to_rankup(int(user_gp[0][0]), GProles)

    # Send the header and data as a message
    message = f"```{table_block}"
    if IOguild == 'Aetherians':
        message += f"Total: {str(user_gp[0][0])}    GP needed to rank up: {remaining_points}```"
    elif IOguild == 'Pretherians':
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
async def GP_weekly_auto():

    print("Starting weekly GP process")
    await members_guild(LedukasSpam_channel) 
    print("test1")
    await Functions.GP_databases()
    print("test2")
    
    IOguild = "Aetherians"
    monthly_gp_df = await Functions.GP_dataframe(IOguild)
    await Functions.GP_roles(bot, monthly_gp_df)
    await LedukasSpam_channel.send("GP roles fixed!")
    await Functions.red_gp(LedukasSpam_channel, monthly_gp_df, IOguild)
    print("test3")
    
    IOguild = "Pretherians"
    monthly_gp_df = await Functions.GP_dataframe(IOguild)
    await Functions.red_gp(LedukasSpam_channel, monthly_gp_df, IOguild)
    #await Functions.promotions(bot, LedukasSpam_channel)
    print("test4")
    
    print("weekly GP calculated automatically")
    conn.commit()

    # Data only changes on this weekly cycle, so back it up right after — no need
    # for a separate always-on schedule (e.g. daily) that would just copy the same
    # unchanged data most days.
    try:
        backup_path = run_backup()
        await LedukasSpam_channel.send(f"Weekly backup created: `{backup_path.name}`")
    except Exception as e:
        print(e)
        await LedukasSpam_channel.send(f"Weekly backup failed: {e}")

@bot.command(name='GP_weekly')
@commands.has_role("Moderator")
async def GP_weekly_man(ctx):

    await members_guild(ctx) 
    await Functions.GP_databases()
    
    IOguild = "Aetherians"
    monthly_gp_df = await Functions.GP_dataframe(IOguild)
    await Functions.GP_roles(bot, monthly_gp_df)
    await LedukasSpam_channel.send("GP roles fixed!")
    await Functions.red_gp(LedukasSpam_channel, monthly_gp_df, IOguild)
    
    IOguild = "Pretherians"
    monthly_gp_df = await Functions.GP_dataframe(IOguild)
    await Functions.red_gp(LedukasSpam_channel, monthly_gp_df, IOguild)

    conn.commit()

    try:
        backup_path = run_backup()
        await LedukasSpam_channel.send(f"Weekly backup created: `{backup_path.name}`")
    except Exception as e:
        print(e)
        await LedukasSpam_channel.send(f"Weekly backup failed: {e}")



##---------------------------------------------  Functions

# Runs once every 24h at 2 AM local time; only actually does anything on Saturdays.
# Using tasks.loop (instead of a hand-rolled while-loop spawned via bot.loop.create_task)
# means the task has a proper start()/is_running() lifecycle, so a gateway reconnect
# re-firing on_ready can't silently spawn a second, duplicate copy of this loop
# (which is what used to cause the weekly GP report getting sent twice).
@tasks.loop(time=time(hour=2, minute=0))
async def gp_weekly_loop():
    if not logic.is_saturday(datetime.now()):
        return
    await GP_weekly_auto()

##---------------------------------------------  Errors
# error messages for all commands
@assign.error
async def assign_error(ctx, error):
    await ctx.send(logic.assign_error_message(error))
@bot.event
async def on_command_error(ctx, error):
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

    conn.close
