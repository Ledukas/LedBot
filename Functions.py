from datetime import datetime, timedelta
import pandas as pd
import sqlite3
import inspect
import discord
import os
import re
from dotenv import load_dotenv
import pyrebase
import aiohttp
import json
import time
from google.oauth2 import service_account
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials
import asyncio

import logic

SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
CREDS = Credentials.from_service_account_file(
    "service_account.json",
    scopes=SCOPES
    )

load_dotenv()
# WAL lets the weekly job and a moderator command touch the database at the
# same moment without hitting "database is locked".
conn = sqlite3.connect('DatabaseLedBot.db')
conn.execute("PRAGMA journal_mode=WAL")
c = conn.cursor()
password = os.environ.get('PASSWORD')

GP_prefix = 'GP'
separator = ' | '
# GP rank thresholds live in logic.RANK_THRESHOLDS (single source of truth).


WB_sheet_name = os.environ.get('WB_SHEET_NAME')
WB_spreadsheet_id = os.environ.get('WB_SPREADSHEET_ID')
GOOGLE_API_KEY = os.getenv('GOOGLESHEETS_API')
service = build('sheets', 'v4', developerKey=GOOGLE_API_KEY, cache_discovery=False)

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
    try:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None,
            _sync_fetch_cell,
            WB_spreadsheet_id,
            WB_sheet_name,
            cell
        )
    except Exception as error:
        print(f"Error fetching cell value: {error}")
        return "Failed to fetch boss strategy from spreadsheet"

#get date
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

#get the dataframe
async def GP_dataframe(IOguild):
    table_name_gained = logic.table_name(IOguild, 'GP_gained')
    
    column_names_dict = await get_date()
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
        return
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
            await member.add_roles(role2give)
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
                    await member.remove_roles(role2remove)

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
        return
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
            await member.remove_roles(clammy)
            if sync_ducks and booster_duck in member.roles:
                await member.remove_roles(aeth_duck)

    #give clammy
    for d_id in d_ids:
        member = guild.get_member(d_id)
        print(member)
        if member is None:
            print("member is None, error. Skipping")
            continue
        if clammy not in member.roles:
            await member.add_roles(clammy)
            if sync_ducks and booster_duck in member.roles:
                await member.add_roles(aeth_duck)
                
#red GP
async def red_gp(channel, monthly_gp_df, IOguild):

    Blacklist = [
        'bvK1B5ngXtgiw5MV95mE6BOP2rN2', #Ledukas, Aetherians
        '0dzrUrtCeOdllJBa8LXoYQCo4Fv1', #Ledukas, Pretherians
        '4EwZK8w84gR6ESP0YAjiXeP03n62', #Led-Bot, Aetherians
        'TQvhMJ1oAIfRXrvffVGN3Jy0Zdi1' #Led_Bot, Pretherians
        ]
    try:
        df_red_gp = logic.filter_red_gp(monthly_gp_df, IOguild, Blacklist)
    except Exception as e:
        # Bails out cleanly here instead of continuing on with an undefined
        # df_red_gp, which is what the old version did on any failure here.
        print("line: " + str(inspect.currentframe().f_lineno) + "\n error: " + str(e))
        await channel.send(f"Error building red-GP report: {e}")
        return

    with open('red.txt', 'w') as file:
        file.write(f'{IOguild} \n')
        file.write(logic.format_table_block(df_red_gp))

    file = discord.File('red.txt')
    await channel.send(file=file)
    os.remove('red.txt')

async def promotions(bot, channel):
    try:
        with open('promo.txt', 'w') as file:
            file.write(f"Discord name    | GP\n")
        column_names_dict = await get_date()
        column_name1 = column_names_dict["column_name1"]
        role = discord.utils.get(channel.guild.roles, name="Promotions")
        guild = bot.get_guild(809954021028134943)
        if guild is None:
            await channel.send("Discord server not in cache, cannot check promotions right now.")
            return
        promo_members = logic.table_name(logic.PROMOTION_GUILD, 'members')
        promo_gained = logic.table_name(logic.PROMOTION_GUILD, 'GP_gained')
        c.execute(f'''SELECT PM.D_ID, PM.G_ID, PGG.{column_name1}
        FROM {promo_members} AS PM
        JOIN {promo_gained} AS PGG ON PM.G_ID = PGG.G_ID
        WHERE PGG.{column_name1} >= {logic.PROMOTION_GP_REQUIREMENT}''')
        result = c.fetchall()
        found_any = False
        for item in result:
            member = guild.get_member(item[0])
            GP = item[2]
            if member is None:
                print(f"Member with ID {item[0]} not found in guild.")
                continue
            if role in member.roles:
                print(member)
                found_any = True
                with open('promo.txt', 'a') as file:
                    file.write(f"{member.name.ljust(15)} | {GP}\n")
        if not found_any:
            print('No members meet the requirements')
            await channel.send('No members meet the requirements')
        file = discord.File('promo.txt')
        await channel.send(file=file)
        os.remove('promo.txt')
    except Exception as e:
        print(f"Error: {e}")
        await channel.send(f"Error: {e}")


    
async def GP_export(email_a, email_p):

    emails = {"Aetherians": email_a, "Pretherians": email_p}
    guilds_data = {
        name: {"gid": gid, "email": emails[name]}
        for name, gid in logic.GUILD_GIDS.items()
    }

    ## Configure Firebase
    config = {
        "apiKey": os.environ.get('FIRE_API'),
        "authDomain": "idlemmo.firebaseapp.com",
        "databaseURL": "https://idlemmo.firebaseio.com",
        "storageBucket": "idlemmo.appspot.com"
    }
    # Initialize Firebase
    firebase = pyrebase.initialize_app(config)
    # Get the authentication instance
    auth = firebase.auth()

    #export members from the game
    for guild_name, guild_attributes in guilds_data.items():  
        # Sign in with email and password
        user = auth.sign_in_with_email_and_password(guild_attributes["email"], password)
        # Get the ID token
        id_token = user['idToken']

        # Create a custom Firebase client with the ID token
        custom_app = pyrebase.initialize_app(config)
        db = custom_app.database()
        # Create an observable on the specified database location
        ref = db.child("_guild").child(guild_attributes["gid"]).child("m")
        # Stream changes and register the stream handler
        data = ref.get(token=id_token).val()

        rows = logic.build_game_members_rows(data)
        df_members_game = pd.DataFrame(rows)

        ## add member list to the database
        df_members_game.to_sql(logic.table_name(guild_name, 'game'), conn, if_exists='replace')
        
    conn.commit()
    
async def GP_databases():
    conn = sqlite3.connect('DatabaseLedBot.db')
    conn.execute("PRAGMA journal_mode=WAL")
    c = conn.cursor()

    column_names_dict = await get_date()
    column_name1 = column_names_dict["column_name1"]
    column_name2 = column_names_dict["column_name2"]

    for guild_name in logic.GUILD_NAMES:

        table_name_GP = logic.table_name(guild_name, 'GP')
        table_name_game = logic.table_name(guild_name, 'game')
        table_name_gained = logic.table_name(guild_name, 'GP_gained')
    
        #fills the GP table with total GP
        c.execute(f"PRAGMA table_info({table_name_GP})")
        columns = c.fetchall()
        column_names = [col[1] for col in columns]
        if column_name1 in column_names:
            print(f"Column '{column_name1}' already exists in the {table_name_GP} table")
        else:
            c.execute(f"ALTER TABLE {table_name_GP} ADD COLUMN {column_name1} TEXT")
            print(f"Column '{column_name1}' added to {table_name_GP}")

        c.execute(f'SELECT G_NAME, G_ID FROM {table_name_game} WHERE G_ID NOT IN (SELECT G_ID FROM {table_name_GP})')
        new_rows = c.fetchall()
        if new_rows:
            c.executemany(f'INSERT INTO {table_name_GP} (Name, G_ID) VALUES (?, ?)', new_rows)
            c.executemany(f'INSERT INTO {table_name_gained} (Name, G_ID) VALUES (?, ?)', new_rows)
        c.execute(f"UPDATE {table_name_GP} SET {column_name1} = (SELECT GP FROM {table_name_game} WHERE {table_name_game}.G_ID = {table_name_GP}.G_ID)")


        #fill GP gained table
        c.execute(f"PRAGMA table_info({table_name_gained})")
        columns = c.fetchall()
    
        column_names = [col[1] for col in columns]
        if column_name1 in column_names:
            print(f"Column '{column_name1}' already exists in the {table_name_gained} table")
        else:
            c.execute(f"ALTER TABLE {table_name_gained} ADD COLUMN {column_name1} TEXT")
            print(f"Column '{column_name1}' added to {table_name_gained}")

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
            c.execute(f"UPDATE {table_name_gained} SET {column_name1} = ? WHERE G_ID = ?", (GP_gained, row[1]))

    conn.commit()
