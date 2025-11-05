import discord
from discord.ext import commands
from discord import app_commands
import random
import sqlite3
from datetime import datetime, timedelta
import json

WINNERS_FILE = "winners.json"

class Giveaway(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @commands.command(
        name="giveaway", 
        description="Run a giveaway by fetching reactions from a specific message."
    )
    @commands.has_role("Moderator")
    async def giveaway(self, ctx,
                        message_id: str, 
                        channel: discord.TextChannel, 
                        guild_name: str = None, 
                        gp_required: int = None):
        
        conn = sqlite3.connect('DatabaseLedBot.db')
        c = conn.cursor()
        
        try:
            
            with open(WINNERS_FILE, "r") as f:
                winners = json.load(f)
            print(f"Loaded winners: {winners}")
        except FileNotFoundError:
            winners = []
                
        try:
            if gp_required is None and guild_name and guild_name.isdigit():
                gp_required = int(guild_name)
                guild_name = None
            if gp_required is None and guild_name is None:
                gp_required = 0
            
            
            today = datetime.now()
            last_saturday = today - timedelta(days=(today.weekday() + 2) % 7)
            gp_column = f"GP{last_saturday.year}_{last_saturday.month:02}_{last_saturday.day:02}"
            print("test6")
            message_id = int(message_id)
            message = await channel.fetch_message(message_id)
            reaction = message.reactions[0]
            users = [user async for user in reaction.users() if not user.bot]

            # If no guild specified, treat it as if both guilds are selected
            guild_names = [guild_name] if guild_name else ["Aetherians", "Pretherians"]
            print("test7")
            final_participants = []
            for guild in guild_names:
                filtered_users = []
                for user in users:
                    member = ctx.guild.get_member(user.id)
                    if member and any(role.name == guild for role in member.roles):
                        filtered_users.append(member)
                print("test8")
                participant_ids = [member.id for member in filtered_users]
                
                members_table = f"{guild}_members"
                query1 = f"SELECT * FROM {members_table} WHERE D_ID IN ({','.join(['?' for _ in participant_ids])})"
                c.execute(query1, participant_ids)
                members_results = c.fetchall()
                g_ids = [row[4] for row in members_results]
                print("test9")
                gp_table = f"{guild}_GP_gained"
                query2 = f"SELECT * FROM {gp_table} WHERE G_ID IN ({','.join(['?' for _ in g_ids])}) AND {gp_column} IS NOT NULL AND CAST({gp_column} AS INTEGER) >= ?"
                c.execute(query2, g_ids + [gp_required])
                gp_results = c.fetchall()
                print("test10")
                for gp_row in gp_results:
                    g_id = gp_row[1]
                    member = next((m for m in members_results if m[4] == g_id), None)
                    if member:
                        discord_id = member[2]
                        participant = ctx.guild.get_member(discord_id)
                        if participant:
                            final_participants.append(participant)
                            
            print(f"Final participants: {[p.name for p in final_participants]}")
            eligible_participants = [p for p in final_participants if p.id not in winners]
            print(f"Eligible participants: {eligible_participants}")
            if not eligible_participants:
                await ctx.send("No eligible participants found.")
            winner = random.choice(eligible_participants)
            winners.append(winner.id)
            with open(WINNERS_FILE, "w") as f:
                json.dump(winners, f)

            await ctx.send(f"🎉 Congratulations {winner.mention}, you are the winner of the giveaway! 🎉")
        except Exception as e:
            await ctx.send(f"An error occurred: {e}")

async def setup(bot: commands.Bot):
    await bot.add_cog(Giveaway(bot))
