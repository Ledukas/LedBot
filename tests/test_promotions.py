"""The promotions section: Pretherians with the Promotions role who gained
enough GP in the week, listed in the weekly report and by !promotions."""

import asyncio
from datetime import datetime
from types import SimpleNamespace

import Functions
import logic

GUILD = logic.PROMOTION_GUILD
WEEK = "GP2026_10_10"
SATURDAY = datetime(2026, 10, 10, 12, 0)


def member(member_id, display, name):
    return SimpleNamespace(id=member_id, display_name=display, name=name)


def server(holders, chunked=True, role_name=logic.PROMOTION_ROLE):
    role = SimpleNamespace(name=role_name, members=holders)
    return SimpleNamespace(roles=[role], chunked=chunked)


def make_tables():
    Functions.c.execute(f'CREATE TABLE {GUILD}_members (Discord TEXT, D_ID INTEGER, Display TEXT, G_ID TEXT, G_NAME TEXT)')
    Functions.c.execute(f'CREATE TABLE {GUILD}_game ("index" INTEGER, G_NAME TEXT, G_ID TEXT, GP INTEGER)')
    Functions.c.execute(f'CREATE TABLE {GUILD}_GP_gained (Name TEXT, G_ID TEXT, {WEEK} TEXT)')
    Functions.c.executemany(f"INSERT INTO {GUILD}_members VALUES (?, ?, ?, ?, ?)", [
        ("kayn#0", 1, "Itsui", "A", "Lxrd_Kayn"),
        ("bho1337#0", 2, "Bho", "B", "Bho_One"),
        ("bho1337#0", 2, "Bho", "B", "Bho_One"),   # a repeated !assign
        ("low#0", 3, "Low", "C", "Lowly"),
        ("left#0", 4, "Left", "D", "Departed"),
        ("norole#0", 5, "NoRole", "E", "Unroled"),
    ])
    Functions.c.executemany(f'INSERT INTO {GUILD}_game VALUES (?, ?, ?, ?)', [
        (0, "Lxrd_Kayn", "A", 1), (1, "Bho_One", "B", 1), (2, "Lowly", "C", 1), (3, "Unroled", "E", 1),
    ])
    Functions.c.executemany(f"INSERT INTO {GUILD}_GP_gained VALUES (?, ?, ?)", [
        ("Lxrd_Kayn", "A", "950"), ("Bho_One", "B", "740"), ("Lowly", "C", "399"),
        ("Departed", "D", "1200"), ("Unroled", "E", "800"),
    ])


def report(discord_guild):
    return asyncio.run(Functions.promotions_report(discord_guild, SATURDAY))


ALL_HOLDERS = [member(1, "Itsui", "itsui"), member(2, "Bho", "bho1337"), member(3, "Low", "low"), member(4, "Left", "left")]


class TestSelectPromotions:
    def test_needs_the_role_and_the_gp(self):
        rows = [(1, "A", "400"), (2, "B", "399"), (3, "C", "900")]
        assert logic.select_promotions(rows, {"1": "one", "2": "two"}) == [("A", 400, "one")]

    def test_a_repeated_assign_is_listed_once(self):
        rows = [(1, "A", "500"), ("1", "A", "500")]
        assert logic.select_promotions(rows, {"1": "one"}) == [("A", 500, "one")]

    def test_most_gp_first(self):
        rows = [(1, "b", "500"), (1, "a", "900"), (1, "C", "500")]
        assert [name for name, _, _ in logic.select_promotions(rows, {"1": "x"})] == ["a", "b", "C"]

    def test_a_missing_gain_is_skipped(self):
        assert logic.select_promotions([(1, "A", None), (1, "B", "junk")], {"1": "x"}) == []


class TestFormatPromotions:
    def test_nobody_is_no_embeds(self):
        assert logic.format_promotions([]) == []

    def test_the_table(self):
        [embed] = logic.format_promotions([("Lxrd_Kayn", 950, "Itsui"), ("Bho_One", 740, "Bho (bho1337)")])
        assert embed.title == f"{GUILD} -- promotions, 400+ GP (2)"
        assert embed.description.split("\n")[1:5] == [
            "Character |  GP| Discord",
            "----------+----+--------",
            "Lxrd_Kayn | 950| Itsui",
            "Bho_One   | 740| Bho (bho1337)",
        ]

    def test_a_pipe_in_a_discord_name_cant_add_a_column(self):
        [embed] = logic.format_promotions([("Iarsis", 720, "Iarsis | The Notepad Guy")])
        assert embed.description.split("\n")[3] == "Iarsis    | 720| Iarsis ¦ The Notepad Guy"


class TestPromotionsReport:
    def test_role_holders_with_enough_gp_still_in_the_guild(self, temp_functions_db):
        make_tables()
        [embed] = report(server(ALL_HOLDERS))
        rows = embed.description.split("\n")[3:-1]
        assert rows == ["Lxrd_Kayn | 950| Itsui", "Bho_One   | 740| Bho (bho1337)"]

    def test_nobody_qualifying_is_silent(self, temp_functions_db):
        make_tables()
        assert report(server([member(3, "Low", "low")])) == []

    def test_problems_are_said_not_silent(self, temp_functions_db):
        make_tables()
        for discord_guild, reason in [
            (None, "isn't in the bot's cache"),
            (server(ALL_HOLDERS, role_name="Other"), "no 'Promotions' role"),
            (server(ALL_HOLDERS, chunked=False), "isn't fully loaded"),
        ]:
            [embed] = report(discord_guild)
            assert embed.colour.value == logic.WARNING_COLOR
            assert reason in embed.description

    def test_a_crash_is_an_error_embed(self, temp_functions_db):
        [embed] = report(server(ALL_HOLDERS))
        assert embed.title == f"{GUILD} -- promotions failed"
        assert embed.colour.value == logic.ERROR_COLOR
