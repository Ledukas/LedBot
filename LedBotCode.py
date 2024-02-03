import discord
from discord.ext import commands
from discord.ext.commands.bot import Bot
import pandas as pd
import sqlite3
import os
import json
from dotenv import load_dotenv
import requests
import shlex
from datetime import datetime, timedelta
import inspect
import asyncio

import Functions

load_dotenv()

TOKEN = os.environ.get('TOKEN')
prefix = '!'
GP_prefix = 'GP'
separator = ' | '
bot = commands.Bot(command_prefix=prefix, intents=discord.Intents.all())
LedukasSpam_channelID = os.environ.get('SPAM_CHANNEL_ID')
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
GProles = [role_6_primordial, role_5_titan, role_4_deity, role_3_demigod, role_2_hero, role_1_knight]

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
        df_assigned_notindisocrd = df_list_discord.loc[~df_list_discord.isin(df_discord)]
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
        df_assigned_notingame = df_list_game.loc[~df_list_game.isin(df_game)]
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
async def assign(ctx, IOguild, user: discord.User, game_name):

    input_string = ctx.message.content
    args = shlex.split(input_string)
    game_name = args[3]
    value = game_name
    table_name_members = IOguild+'_members'
    table_name_game = IOguild+'_game'
    c = conn.cursor()
    game = c.execute('SELECT * FROM ' + table_name_game + ' WHERE G_NAME = ?', (value,)).fetchone()
    c.execute('INSERT INTO ' + table_name_members + ' (Discord, D_ID, Display, G_ID, G_NAME) VALUES (?,?,?,?,?)', (user.name + '#' + user.discriminator, user.id, user.display_name, game[2], game[1]))

    await ctx.send("Assignement succesfull")
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
    
    if result_value is None:
        await ctx.send("Error, invite not sent")
    elif result_value.lower() == "true":
        await ctx.send("Invite sent. Let me know when you join")
    else:
        await ctx.send("Error, invite not sent")
    
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
    
    if result_value is None:
        await ctx.send("Error, not kicked")
    elif result_value.lower() == "true":
        await ctx.send(f"{Disp_result} has been kicked from {IOguild}")
    else:
        await ctx.send("Error, not kicked")
 
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

    if not result_aetherians and not result_pretherians:
        print("No GP gains recorded")
        message = "No GP gains recorded yet"
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
    personal_gains = monthly_gp_df[monthly_gp_df['G_ID'] == user_gid[0][0]]
    personal_gains = personal_gains.drop('G_ID', axis=1)
    
    formatted_data = personal_gains.to_string(index=False)
    lines = formatted_data.split('\n')
    formatted_lines = []
    for line in lines:
        formatted_line = ' '.join(line.split()).replace(' ', separator)
        formatted_lines.append(formatted_line)
    formatted_data = '\n'.join(formatted_lines)

    header = IOguild.ljust(14)
    for column in personal_gains.columns[1:]:
        header += separator + column.ljust(7)
    header += '\n'

    # Construct the data string
    data = ""
    for _, row in personal_gains.iterrows():
        data += row[0].ljust(14)
        for value in row[1:]:
            data += separator + str(value).ljust(7)
        data += '\n'

    user_gid = str(user_gid[0][0])
    query = f"SELECT GP FROM {table_name_game} WHERE G_ID = ?"
    c.execute(query, (user_gid,))
    user_gp = c.fetchall()

    remaining_points = 0
    for i in range(len(GProles)):
        if int(user_gp[0][0]) >= GProles[i]:
            remaining_points = GProles[i - 1] - int(user_gp[0][0])
            break
    if remaining_points <0:
        remaining_points = "You have the final rank"
    
    # Send the header and data as a message
    message = f"```{header}{data}"
    if IOguild == 'Aetherians':
        message += f"Total: {str(user_gp[0][0])}    GP needed to rank up: {remaining_points}```"
    elif IOguild == 'Pretherians':
        message += f"Total: {str(user_gp[0][0])}```"
    return message

@bot.command(name='promotions')
@commands.has_role("Moderator")
async def promotions(ctx):
    await Functions.promotions(bot, LedukasSpam_channel)

#does weekly GP things
async def GP_weekly_auto():

    await members_guild(LedukasSpam_channel) 
    await Functions.GP_databases()
    
    IOguild = "Aetherians"
    monthly_gp_df = await Functions.GP_dataframe(IOguild)
    await Functions.GP_roles(bot, monthly_gp_df)
    await LedukasSpam_channel.send("GP roles fixed!")
    await Functions.red_gp(LedukasSpam_channel, monthly_gp_df, IOguild)
    
    IOguild = "Pretherians"
    monthly_gp_df = await Functions.GP_dataframe(IOguild)
    await Functions.red_gp(LedukasSpam_channel, monthly_gp_df, IOguild)
    #await Functions.promotions(bot, LedukasSpam_channel)
    
    print("weekly GP calculated automatically")
    conn.commit()

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



##---------------------------------------------  Functions

async def run_at_specific_time():
    while True:
        
        now = datetime.now()
        # Check if it's Saturday at 2 AM
        if now.weekday() == 5 and now.hour == 2 and now.minute == 0:
            await GP_weekly_auto()
            await asyncio.sleep(5000)    

        # Calculate the time until the next Saturday at 2 AM
        days_until_saturday = (5 - now.weekday()) % 7
        time_until_2_am = timedelta(hours=2) - timedelta(hours=now.hour, minutes=now.minute, seconds=now.second)
        total_time_until_next_run = timedelta(days=days_until_saturday) + time_until_2_am
        
        # Sleep until the next run time (Saturday at 2 AM)
        await asyncio.sleep(total_time_until_next_run.total_seconds())

##---------------------------------------------  Errors
# error messages for all commands
@assign.error
async def on_command_error(ctx, error):
    await ctx.send("Command error")

@bot.event
async def on_command_error(ctx, error):
    if isinstance(error, commands.MissingRole):
        await ctx.send("You don't have the required permissions to use this command.")
    elif isinstance(error, commands.MissingRequiredArgument):
        await ctx.send("Missing an argument!")
    elif isinstance(error, commands.BotMissingPermissions):
        await ctx.send("The bot doesn't have the required permissions to run this command.")
    elif isinstance(error, commands.UserInputError):
        await ctx.send("There was an error in the input.")
# gives a message in console once the bot goes live
@bot.event
async def on_ready():
    print(f'Logged in with {bot.user.name} | {bot.user.id}')
    bot.loop.create_task(run_at_specific_time())
    global LedukasSpam_channel
    LedukasSpam_channel = bot.get_channel(LedukasSpam_channelID)
bot.run(TOKEN)

conn.close