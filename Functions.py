from datetime import datetime, timedelta
import pandas as pd
import sqlite3
import inspect
import discord
import os
from dotenv import load_dotenv
import pyrebase

load_dotenv()
conn = sqlite3.connect('DatabaseLedBot.db')
c = conn.cursor()
password = os.environ.get('PASSWORD')

GP_prefix = 'GP'
separator = ' | '
# GP roles:
role_1_knight = 1000
role_2_hero = 2500
role_3_demigod = 5000
role_4_deity = 10000
role_5_titan = 25000
role_6_primordial = 50000
GProles = [role_6_primordial, role_5_titan, role_4_deity, role_3_demigod, role_2_hero, role_1_knight]

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
    table_name_gained = IOguild+'_GP_gained'
    
    column_names_dict = await get_date()
    column_names = column_names_dict["column_names"]
    column_names_int = column_names_dict["column_names_int"]
    
    query = f"SELECT {', '.join(column_names)} FROM {table_name_gained}"
    cursor = conn.execute(query)
    rows = cursor.fetchall()
    monthly_gp_df = pd.DataFrame(rows, columns=column_names)
    
    for column in column_names_int:
        monthly_gp_df[column] = pd.to_numeric(monthly_gp_df[column], errors='coerce')
    monthly_gp_df[column_names_int] = monthly_gp_df[column_names_int].fillna(pd.NA).astype('Int64')
    
    new_column_names = {column: column.replace('GP2024_', '') for column in column_names_int}
    monthly_gp_df = monthly_gp_df.rename(columns=new_column_names)
    
    try:
        monthly_gp_df['Average'] = monthly_gp_df.iloc[:, 2:].mean(axis=1)
        monthly_gp_df['Average'] = monthly_gp_df['Average'].replace('NAType', pd.NA)
        monthly_gp_df['Average'] = pd.to_numeric(monthly_gp_df['Average'], errors='coerce')
        monthly_gp_df['Average'] = monthly_gp_df['Average'].round(1)
    except Exception as e:
        print("line: " + str(inspect.currentframe().f_lineno) + "\nError: " + str(e))
    return monthly_gp_df

#GP roles and clammies for Aetherians
async def GP_roles(bot, monthly_gp_df):
    c.execute('''SELECT Aetherians_members.D_ID, Aetherians_game.GP 
                 FROM Aetherians_game JOIN Aetherians_members 
                 ON Aetherians_game.G_ID = Aetherians_members.G_ID''')
    result = c.fetchall()
    guild = bot.get_guild(809954021028134943)
    for row in result:
        D_ID = row[0]
        GP = row[1]
        member = guild.get_member(D_ID)

        if member is None:
            continue
        
        if GP > role_1_knight and GP < role_2_hero:
            role2give = discord.utils.get(guild.roles, name='Aetherian Knight')
            if role2give not in member.roles:
                await member.add_roles(role2give)

        elif GP > role_2_hero and GP < role_3_demigod:
            role2give = discord.utils.get(guild.roles, name = 'Aetherian Hero')
            if role2give not in member.roles:
                await member.add_roles(role2give)
                role2remove = discord.utils.get(guild.roles, name = 'Aetherian Knight')
                await member.remove_roles(role2remove)

        elif GP > role_3_demigod and GP < role_4_deity:
            role2give = discord.utils.get(guild.roles, name = 'Aetherian Demigod')
            if role2give not in member.roles:
                await member.add_roles(role2give)
                role2remove = discord.utils.get(guild.roles, name = 'Aetherian Hero')
                await member.remove_roles(role2remove)

        elif GP > role_4_deity and GP < role_5_titan:
            role2give = discord.utils.get(guild.roles, name = 'Aetherian Deity')
            if role2give not in member.roles:
                await member.add_roles(role2give)
                role2remove = discord.utils.get(guild.roles, name = 'Aetherian Demigod')
                await member.remove_roles(role2remove)

        elif GP > role_5_titan and GP < role_6_primordial:
            role2give = discord.utils.get(guild.roles, name = 'Aetherian Titan')
            if role2give not in member.roles:
                await member.add_roles(role2give)
                role2remove = discord.utils.get(guild.roles, name = 'Aetherian Deity')
                await member.remove_roles(role2remove)

        elif GP > role_6_primordial:
            role2give = discord.utils.get(guild.roles, name = 'Aetherian Primordial')
            if role2give not in member.roles:
                await member.add_roles(role2give)
                role2remove = discord.utils.get(guild.roles, name = 'Aetherian Titan')
                await member.remove_roles(role2remove)
                
    monthly_gp_df.dropna(inplace=True)
    df_clammies = monthly_gp_df[monthly_gp_df['Average'] > 649]
    list_clammies = df_clammies['G_ID'].tolist()
    clammy = discord.utils.get(guild.roles, name = 'Monthly Top')
    aeth_duck = discord.utils.get(guild.roles, name = 'Aetherian Duck')
    booster_duck = discord.utils.get(guild.roles, name = 'Booster (For DUCK)')
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
                
#red GP
async def red_gp(channel, monthly_gp_df, IOguild):
    
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
        file.write(f'{IOguild} \n')
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
    await channel.send(file=file)
    os.remove('red.txt')

async def promotions(bot, channel):
    with open('promo.txt', 'w') as file:
        file.write(f"Discord name    | GP\n")
    column_names_dict = await get_date()
    column_name1 = column_names_dict["column_name1"]
    role = discord.utils.get(channel.guild.roles, name="Promotions")
    guild = bot.get_guild(809954021028134943)
    
    c.execute(f'''SELECT PM.D_ID, PM.G_ID, PGG.{column_name1}
    FROM Pretherians_members AS PM
    JOIN Pretherians_GP_gained AS PGG ON PM.G_ID = PGG.G_ID
    WHERE PGG.{column_name1} >= 400''')

    result = c.fetchall()
    for item in result:
        member = guild.get_member(item[0])
        GP = item[2]
        if member is None:
            return 'No members meet requirements'
        if role in member.roles:
            with open('promo.txt', 'a') as file:
                file.write(f"{member.name.ljust(15)} | {GP}\n")

    file = discord.File('promo.txt')
    await channel.send(file=file)
    os.remove('promo.txt')


    
async def GP_export(email_a, email_p):

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

        ## add member list to the database
        df_members_game.to_sql(guild_name+"_game", conn, if_exists='replace')
        
    conn.commit
    
async def GP_databases():
    conn = sqlite3.connect('DatabaseLedBot.db')
    c = conn.cursor()

    IOguild = {}
    IOguild[1] = "Aetherians"
    IOguild[2] = "Pretherians"

    column_names_dict = await get_date()
    column_name1 = column_names_dict["column_name1"]
    column_name2 = column_names_dict["column_name2"]

    for item in IOguild:

        table_name_GP = IOguild[item]+'_GP'
        table_name_game = IOguild[item]+'_game'
        table_name_gained = IOguild[item]+'_GP_gained'
    
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