import discord
from discord.ext import commands
from discord.ext.commands.bot import Bot
import pandas as pd
import numpy as np
import sqlite3
import os
import json
from dotenv import load_dotenv
import requests
import pyrebase
import shlex
from datetime import datetime, timedelta
import inspect

load_dotenv()

TOKEN = os.environ.get('TOKEN')
prefix = '!'
GP_prefix = 'GP'
separator = ' | '
bot = commands.Bot(command_prefix=prefix, intents=discord.Intents.all())

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

#pd.set_option('display.max_rows', None)  

##---------------------------------------------  Commands
# a simple command to test if the bot is alive
@bot.command(name='Led')
@commands.has_role("Moderator")
async def test(ctx):
    await ctx.send(f'Bot!')

# export members from discord
@bot.command(name='members_discord')
@commands.has_role("Moderator")
async def members_discord(ctx, IOguild):
    if IOguild is None:
        await ctx.send(f"No role given")
        return
    guild = ctx.guild # discord guild = server
    role = discord.utils.get(guild.roles, name=IOguild) # IOguild = IdleOnGuild. Must have a matching role name
    if role is None:
        await ctx.send(f"Role '{IOguild}' not found")
        return
    members = role.members
    role_members = [member for member in members if role in member.roles]
    data = {'Discord': [member.name + '#' + member.discriminator for member in role_members], 
            'D_ID': [int(member.id) for member in role_members],
            'Display': [member.display_name for member in role_members],}
    df = pd.DataFrame(data)
    await ctx.send("Discord members exported")
    # store the data in a database
    df.to_sql(IOguild+"_discord", conn, if_exists='replace')
    conn.commit()

#export members from the game
@bot.command(name='members_game')
@commands.has_role("Moderator")
async def members_guild(ctx, IOguild):
    
    ## Configure the guild
    if IOguild is None:
        await ctx.send(f"No role given")
        return
    if IOguild == "Aetherians":
        gid = "jSiitSSM7nO0HFuoVlsa"
        email = os.environ.get('EMAIL_A')
    elif IOguild == "Pretherians": 
        gid = "yuFnrJvPfK8ZdfFXHojg"
        email = os.environ.get('EMAIL_P')
    else: 
        await ctx.send("That's not our guild!")
        return
    
    ## Configure Firebase
    config = {
        "apiKey": "AIzaSyAU62kOE6xhSrFqoXQPv6_WHxYilmoUxDk",
        "authDomain": "idlemmo.firebaseapp.com",
        "databaseURL": "https://idlemmo.firebaseio.com",
        "storageBucket": "idlemmo.appspot.com"
    }
    # Initialize Firebase
    firebase = pyrebase.initialize_app(config)
    
    # Get the authentication instance
    auth = firebase.auth()
    # Sign in with email and password
    password = os.environ.get('PASSWORD')
    user = auth.sign_in_with_email_and_password(email, password)
    # Get the ID token
    id_token = user['idToken']
    
    # Create a custom Firebase client with the ID token
    custom_app = pyrebase.initialize_app(config)
    db = custom_app.database()
    # Create an observable on the specified database location
    ref = db.child("_guild").child(gid).child("m")
    # Stream changes and register the stream handler
    data = ref.get(token=id_token).val()
    df_members_game = await process_guild_member_score(data)
    
    ## add member list to the database
    df_members_game.to_sql(IOguild+"_game", conn, if_exists='replace')
    conn.commit()
    await ctx.send("Game members exported")
#add members exported from the game to the database
async def process_guild_member_score(data):
    global df_members_game
    rows = []
    
    # Iterate over the members in the event data
    for member_id, member_data in data.items():
        # Extract the required fields ('a' and 'e') for each member
        a_value = member_data['a']
        e_value = member_data['e']
        
        # Create a new row with the values
        new_row = {'G_NAME': a_value, 'G_ID': member_id, 'GP': e_value}
        rows.append(new_row)    
    df_members_game = pd.DataFrame(rows)
    return df_members_game

#sync counters
@bot.command(name='sync_counters')
@commands.has_role("Moderator")
async def sync_counters(ctx, IOguild):
    await members_discord(ctx, IOguild)
    await members_guild(ctx, IOguild)
    
    table_name_members = IOguild+'_members'
    table_name_discord = IOguild+'_discord'
    table_name_game = IOguild+'_game'

    # Discord names for assigning
    with open('sync.txt', 'w') as f:
        f.write('*To sync:\n')
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
    await ctx.send("Sync file generated")
    await ctx.send(file=file)

    c.close()
    os.remove("sync.txt")
    
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
        email = os.environ.get('EMAIL_A')
    elif IOguild == "Pretherians": 
        gid = "yuFnrJvPfK8ZdfFXHojg"
        email = os.environ.get('EMAIL_P')
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
    
@bot.command(name='kick')
@commands.has_role("Moderator")
async def kick(ctx, IOguild, KickID):
    
    # login
    if IOguild is None:
        await ctx.send(f"No role given")
        return
    if IOguild == "Aetherians":
        gid = "jSiitSSM7nO0HFuoVlsa"
        email = os.environ.get('EMAIL_A')
    elif IOguild == "Pretherians": 
        gid = "yuFnrJvPfK8ZdfFXHojg"
        email = os.environ.get('EMAIL_P')
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
    c.execute(f"SELECT Display FROM {table_name_members} WHERE D_ID = ?", (KickID,))
    Disp_result = c.fetchall()
    Disp_result = Disp_result[0][0]
    if result_value is None:
        await ctx.send("Error, not kicked")
    elif result_value.lower() == "true":
        await ctx.send(f"{Disp_result} has been kicked from {IOguild}")
    else:
        await ctx.send("Error, not kicked")
    
#fix GP roles
@bot.command(name='GP_roles')
@commands.has_role("Moderator")
async def GP_roles(ctx, conn, c):
    c.execute('''SELECT Aetherians_members.D_ID, Aetherians_game.GP 
                 FROM Aetherians_game JOIN Aetherians_members 
                 ON Aetherians_game.G_ID = Aetherians_members.G_ID''')
    result = c.fetchall()
    guild = ctx.guild
    for row in result:
        D_ID = row[0]
        GP = row[1]
        member = guild.get_member(D_ID)

        if member is None:
            continue
        
        if GP > role_1_knight and GP < role_2_hero:
            role2give = discord.utils.get(ctx.guild.roles, name = 'Aetherian Knight')
            if role2give not in member.roles:
                await member.add_roles(role2give)

        elif GP > role_2_hero and GP < role_3_demigod:
            role2give = discord.utils.get(ctx.guild.roles, name = 'Aetherian Hero')
            if role2give not in member.roles:
                await member.add_roles(role2give)
                role2remove = discord.utils.get(ctx.guild.roles, name = 'Aetherian Knight')
                await member.remove_roles(role2remove)

        elif GP > role_3_demigod and GP < role_4_deity:
            role2give = discord.utils.get(ctx.guild.roles, name = 'Aetherian Demigod')
            if role2give not in member.roles:
                await member.add_roles(role2give)
                role2remove = discord.utils.get(ctx.guild.roles, name = 'Aetherian Hero')
                await member.remove_roles(role2remove)

        elif GP > role_4_deity and GP < role_5_titan:
            role2give = discord.utils.get(ctx.guild.roles, name = 'Aetherian Deity')
            if role2give not in member.roles:
                await member.add_roles(role2give)
                role2remove = discord.utils.get(ctx.guild.roles, name = 'Aetherian Demigod')
                await member.remove_roles(role2remove)

        elif GP > role_5_titan and GP < role_6_primordial:
            role2give = discord.utils.get(ctx.guild.roles, name = 'Aetherian Titan')
            if role2give not in member.roles:
                await member.add_roles(role2give)
                role2remove = discord.utils.get(ctx.guild.roles, name = 'Aetherian Deity')
                await member.remove_roles(role2remove)

        elif GP > role_6_primordial:
            role2give = discord.utils.get(ctx.guild.roles, name = 'Aetherian Primordial')
            if role2give not in member.roles:
                await member.add_roles(role2give)
                role2remove = discord.utils.get(ctx.guild.roles, name = 'Aetherian Titan')
                await member.remove_roles(role2remove)

    await ctx.send("GP roles fixed!")

# a command to check gains
@bot.command(name='mygains')
async def mygains(ctx):
    if ctx.channel.id != 810014953477898240:
        await ctx.message.delete()
        return
    
    c = conn.cursor()
    user_did = ctx.author.id
    
    # c.execute("SELECT * FROM Aetherians_discord WHERE D_ID = ?", (str(user_did),))
    # result = c.fetchone()
    # if result:
    #     IOguild = "Aetherians"
    #     message = await mygains2(IOguild, c, user_did)
    #     await ctx.send(message)
        
    # c.execute("SELECT * FROM Pretherians_discord WHERE D_ID = ?", (str(user_did),))
    # result = c.fetchone()
    # if result:
    #     IOguild = "Pretherians"
    #     message = await mygains2(IOguild, c, user_did)
    #     await ctx.send(message)
    
    c.execute("SELECT * FROM Aetherians_discord WHERE D_ID = ?", (str(user_did),))
    result_aetherians = c.fetchone()

    c.execute("SELECT * FROM Pretherians_discord WHERE D_ID = ?", (str(user_did),))
    result_pretherians = c.fetchone()

    if not result_aetherians and not result_pretherians:
        print("No GP gains recorded")
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
    
    monthly_gp_df = await GP_dataframe(IOguild)
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
    message += f"GP gained might be wrong until 08 05. My bad"
    return message
    
#does weekly GP things
@bot.command(name='GP_weekly')
@commands.has_role("Moderator")
async def GP_weekly(ctx, IOguild):
    
    c = conn.cursor()

    await fill_gp_table(ctx, c, IOguild)
    await fill_gp_gained_table(ctx, c, IOguild)
    monthly_gp_df = await GP_dataframe(IOguild)
    if IOguild == 'Aetherians':
        await GP_roles(ctx, conn, c)
        await clammies(ctx, c, monthly_gp_df)
    await red_gp(ctx, monthly_gp_df, IOguild)

    conn.commit()

##---------------------------------------------  Functions
#fill total GP table
async def fill_gp_table(ctx, c, IOguild):
    
    table_name_GP = IOguild+'_GP'
    table_name_game = IOguild+'_game'
    table_name_gianed = IOguild+'_GP_gained'
    #fills the GP table with total GP
    c.execute(f"PRAGMA table_info({table_name_GP})")
    columns = c.fetchall()
    
    column_names_dict = await get_date()
    column_name1 = column_names_dict["column_name1"]
    
    column_names = [col[1] for col in columns]
    if column_name1 in column_names:
        print(f"Column '{column_name1}' already exists in the {table_name_GP} table")
        await ctx.send("GP for this week already exists")
        return
    else:
        c.execute(f"ALTER TABLE {table_name_GP} ADD COLUMN {column_name1} TEXT")
        print(f"Column '{column_name1}' added to {table_name_GP}")

    c.execute(f'SELECT G_NAME, G_ID FROM {table_name_game} WHERE G_ID NOT IN (SELECT G_ID FROM {table_name_GP})')
    new_rows = c.fetchall()
    if new_rows:
        c.executemany(f'INSERT INTO {table_name_GP} (Name, G_ID) VALUES (?, ?)', new_rows)
        c.executemany(f'INSERT INTO {table_name_gianed} (Name, G_ID) VALUES (?, ?)', new_rows)
    c.execute(f"UPDATE {table_name_GP} SET {column_name1} = (SELECT GP FROM {table_name_game} WHERE {table_name_game}.G_ID = {table_name_GP}.G_ID)")
    conn.commit()
    await ctx.send("GP stored")

#fill GP gained table
async def fill_gp_gained_table(ctx, c, IOguild):
    table_name_GP = IOguild+'_GP'
    table_name_gianed = IOguild+'_GP_gained'
    
    c.execute(f"PRAGMA table_info({table_name_gianed})")
    columns = c.fetchall()
    
    column_names_dict = await get_date()
    column_name1 = column_names_dict["column_name1"]
    column_name2 = column_names_dict["column_name2"]
    
    column_names = [col[1] for col in columns]
    if column_name1 in column_names:
        print(f"Column '{column_name1}' already exists in the {table_name_gianed} table")
    else:
        c.execute(f"ALTER TABLE {table_name_gianed} ADD COLUMN {column_name1} TEXT")
        print(f"Column '{column_name1}' added to {table_name_gianed}")

    c.execute(f'SELECT Name, G_ID FROM {table_name_GP}')
    rows = c.fetchall()
    for row in rows:
        result = c.execute(f"SELECT {column_name1} FROM {table_name_GP} WHERE G_ID=?", (row[1],)).fetchone()
        if result is None or result[0] is None:
            continue
        GPnow = int(result[0])

        result = c.execute(f"SELECT {column_name2} FROM {table_name_GP} WHERE G_ID=?", (row[1],)).fetchone()
        if result is None or result[0] is None:
            GPold = 0
        else:
            GPold = int(result[0])
        GP_gained = GPnow - GPold
        c.execute(f"UPDATE {table_name_gianed} SET {column_name1} = ? WHERE G_ID = ?", (GP_gained, row[1]))
    conn.commit()
    await ctx.send("GP gained calculated")

#monthly GP gained with average dataframe creation
async def GP_dataframe(IOguild):
    table_name_gianed = IOguild+'_GP_gained'
    
    column_names_dict = await get_date()
    column_names = column_names_dict["column_names"]
    column_names_int = column_names_dict["column_names_int"]
    
    query = f"SELECT {', '.join(column_names)} FROM {table_name_gianed}"
    try:
        cursor = conn.execute(query)
        rows = cursor.fetchall()
    except Exception as e:
        print("line: " + str(inspect.currentframe().f_lineno) + "\n error: " + str(e))
    monthly_gp_df = pd.DataFrame(rows, columns=column_names)
    
    try:
        for column in column_names_int:
            monthly_gp_df[column] = pd.to_numeric(monthly_gp_df[column], errors='coerce')
        monthly_gp_df[column_names_int] = monthly_gp_df[column_names_int].fillna(pd.NA).astype('Int64')
    except Exception as e:
        print("line: " + str(inspect.currentframe().f_lineno) + "\n error: " + str(e))
    
    new_column_names = {column: column.replace('GP2023_', '') for column in column_names_int}
    monthly_gp_df = monthly_gp_df.rename(columns=new_column_names)
    
    try:
        monthly_gp_df['Average'] = monthly_gp_df.iloc[:, 2:].mean(axis=1)
        monthly_gp_df['Average'] = monthly_gp_df['Average'].replace('NAType', pd.NA)
        monthly_gp_df['Average'] = pd.to_numeric(monthly_gp_df['Average'], errors='coerce')
        monthly_gp_df['Average'] = monthly_gp_df['Average'].round(1)
    except Exception as e:
        print("line: " + str(inspect.currentframe().f_lineno) + "\nError: " + str(e))
    return monthly_gp_df


#clammies
async def clammies(ctx, c, monthly_gp_df):

    monthly_gp_df.dropna(inplace=True)
    df_clammies = monthly_gp_df[monthly_gp_df['Average'] > 649]
    list_clammies = df_clammies['G_ID'].tolist()
    guild = ctx.guild
    clammy = discord.utils.get(ctx.guild.roles, name = 'Monthly Top')
    aeth_duck = discord.utils.get(ctx.guild.roles, name = 'Aetherian Duck')
    booster_duck = discord.utils.get(ctx.guild.roles, name = 'Booster (For DUCK)')
    query = f"SELECT D_ID FROM Aetherians_members WHERE G_ID IN ({','.join(['?']*len(list_clammies))})"
    cursor = c.execute(query, list_clammies)
    rows = cursor.fetchall()
    d_ids = [row[0] for row in rows]

    #remove clammy
    members_with_clammy = [member for member in guild.members if clammy in member.roles]
    for member in members_with_clammy:
        if member.id not in d_ids:
            await member.remove_roles(clammy)
            if booster_duck in member.roles:
                await member.remove_roles(aeth_duck)

    #give clammy
    for d_id in d_ids:
        member = guild.get_member(d_id)
        if clammy not in member.roles:
            await member.add_roles(clammy)
            if booster_duck in member.roles:
                await member.add_roles(aeth_duck)

    await ctx.send("Clammies updated!")

#gives a list of people with red gp gained to chat
async def red_gp(ctx, monthly_gp_df, IOguild):
    
    if IOguild == 'Aetherians':
        redGP = 400
    elif IOguild == 'Pretherians':
        redGP = 140

    try:
        Blacklist = [
            'bvK1B5ngXtgiw5MV95mE6BOP2rN2', #Ledukas, Aetherians
            '0dzrUrtCeOdllJBa8LXoYQCo4Fv1', #Ledukas, Pretherians
            '4EwZK8w84gR6ESP0YAjiXeP03n62', #Led-Bot, Aetherians
            'TQvhMJ1oAIfRXrvffVGN3Jy0Zdi1' #Led_Bot, Pretherians
            ]
        df_red_gp = monthly_gp_df[monthly_gp_df.iloc[:, 5] < redGP]
        df_red_gp = df_red_gp.dropna(subset=df_red_gp.columns[4])
        df_red_gp = df_red_gp[~df_red_gp['G_ID'].isin(Blacklist)]
        df_red_gp = df_red_gp.drop('G_ID', axis=1)
        df_red_gp = df_red_gp.sort_values(df_red_gp.columns[4])
    except Exception as e:
        print("line: " + str(inspect.currentframe().f_lineno) + "\n error: " + str(e))

    # Convert dataframe to a formatted string
    formatted_data = df_red_gp.to_string(index=False)

    # Modify the string to align columns
    lines = formatted_data.split('\n')
    formatted_lines = []
    for line in lines:
        formatted_line = ' '.join(line.split()).replace(' ', separator)
        formatted_lines.append(formatted_line)
    formatted_data = '\n'.join(formatted_lines)
    
    with open('red.txt', 'w') as file:
        file.write(df_red_gp.columns.to_list()[0].ljust(14))  # Write the first column header
        for column in df_red_gp.columns[1:]:
            file.write(separator + column.ljust(7))  # Write the remaining column headers
        file.write('\n')

        for _, row in df_red_gp.iterrows():
            file.write(row[0].ljust(14))  # Write the first column values
            for value in row[1:]:
                file.write(separator + str(value).ljust(7))  # Write the remaining column values
            file.write('\n')
    
    file = discord.File('red.txt')
    await ctx.send(file=file)
    os.remove('red.txt')

async def get_date():
    # dates and column names:
    if datetime.today().weekday() == 5:
        day_temp = datetime.today()
    else: 
        days_since_saturday = (datetime.today().weekday() - 5) % 7
        day_temp = datetime.today() - timedelta(days=days_since_saturday)

    day = day_temp.strftime('%Y_%m_%d')
    one_week_ago = (day_temp - timedelta(days=7)).strftime('%Y_%m_%d')
    two_week_ago = (day_temp - timedelta(days=14)).strftime('%Y_%m_%d')
    three_week_ago = (day_temp - timedelta(days=21)).strftime('%Y_%m_%d')
    
    column_name1 = f"{GP_prefix}{day}"
    column_name2 = f"{GP_prefix}{one_week_ago}"
    column_name3 = f"{GP_prefix}{two_week_ago}"
    column_name4 = f"{GP_prefix}{three_week_ago}"
    column_names = ['Name', 'G_ID', column_name4, column_name3, column_name2, column_name1]
    column_names_int = [column_name4, column_name3, column_name2, column_name1]  
    
    column_names_dict = {
        "column_name1": column_name1,
        "column_name2": column_name2,
        "column_name3": column_name3,
        "column_name4": column_name4,
        "column_names": column_names,
        "column_names_int": column_names_int
    }
    
    return column_names_dict


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
bot.run(TOKEN)
conn.close