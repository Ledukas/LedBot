"""The links section: who is missing a link, a role, or a character.

It replaced !sync_counters' sync.txt, and its four lists answer different
questions with different sources: role holders come from {guild}_discord,
which the weekly job and !sync_counters refresh, while whether a linked
account without the role is still in the server is asked of Discord.
"""

import asyncio
from types import SimpleNamespace

import pytest

import Functions
import logic

GUILD = "Aetherians"


def link(d_id, g_id, display=None, discord=None):
    return {'d_id': d_id, 'g_id': g_id, 'display': display, 'discord': discord}


def holders(*pairs):
    return {d_id: label for d_id, label in pairs}


class TestAccountLabel:
    def test_display_and_username_when_they_differ(self):
        assert logic.account_label("His Children", "hischildren#0") == "His Children (hischildren)"

    def test_display_alone_when_they_match(self):
        assert logic.account_label("KingBob531", "kingbob531#0") == "KingBob531"

    def test_a_legacy_discriminator_is_not_part_of_the_username(self):
        assert logic.account_label("Bob", "bobby#1234") == "Bob (bobby)"

    def test_falls_back_to_the_username(self):
        assert logic.account_label(None, "bobby#0") == "bobby"


class TestLinkGaps:
    LIVE = {"A": ("Alpha", 1), "B": ("Bravo", 1), "C": ("Charlie", 1)}

    def test_role_holders_and_characters_with_no_link(self):
        gaps = logic.link_gaps(
            [link("1", "A")], self.LIVE, holders(("1", "one"), ("2", "two")),
        )
        assert gaps['role_no_link'] == ["two"]
        assert gaps['game_no_link'] == ["Bravo", "Charlie"]

    def test_without_the_server_its_one_combined_list(self):
        gaps = logic.link_gaps([link("1", "A"), link("2", "B")], self.LIVE, holders(("1", "one")))
        assert gaps['no_role'] == ["Bravo"]
        assert gaps['no_role_in_server'] == gaps['no_role_left'] == []

    def test_the_server_splits_still_here_from_left(self):
        rows = [link("1", "A"), link("2", "B", "Bee", "bee#0"), link("3", "C")]
        server = {"1": True, "2": False}
        gaps = logic.link_gaps(rows, self.LIVE, holders(("1", "one")), server)
        assert gaps['no_role_in_server'] == ["Bravo  (Bee)"]
        assert gaps['no_role_left'] == ["Charlie"]
        assert gaps['no_role'] == []

    def test_a_split_with_one_holder_has_the_role(self):
        rows = [link("1", "A"), link("2", "A")]
        gaps = logic.link_gaps(rows, self.LIVE, holders(("2", "two")), {"1": False, "2": True})
        assert gaps['no_role_in_server'] == gaps['no_role_left'] == []

    def test_one_departed_and_one_current_character_is_not_missing_a_character(self):
        """The old sync_counters query matched any departed link row."""
        rows = [link("1", "GONE"), link("1", "A"), link("2", "GONE")]
        gaps = logic.link_gaps(rows, self.LIVE, holders(("1", "one"), ("2", "two")))
        assert gaps['role_no_character'] == ["two"]

    def test_junk_rows_link_nothing(self):
        rows = [link(None, "A"), link("", ""), link("2", None)]
        gaps = logic.link_gaps(rows, self.LIVE, holders(("2", "two")))
        assert gaps['game_no_link'] == ["Alpha", "Bravo", "Charlie"]
        assert gaps['role_no_link'] == ["two"]

    def test_sorted_ignoring_case(self):
        live = {"A": ("beta", 1), "B": ("Alpha", 1), "C": ("Gamma", 1)}
        assert logic.link_gaps([], live, {})['game_no_link'] == ["Alpha", "beta", "Gamma"]


class TestFormatLinks:
    def test_nothing_is_none(self):
        assert logic.format_links([], [], {key: [] for key, _ in logic.LINK_GAP_SECTIONS}) is None

    def test_only_non_empty_lists_get_a_heading(self):
        text = logic.format_links([], [], {'game_no_link': ["KingBob531"], 'role_no_link': []})
        assert "In game, no link:" in text and "KingBob531" in text
        assert "Has the role" not in text

    def test_no_game_ids(self):
        """The report used to print steam_7656... next to every name; !assign
        takes the name."""
        gaps = logic.link_gaps([], {"steam_76561198645315575": ("minetwaft", 1)}, {})
        text = logic.format_links([], [], gaps)
        assert "minetwaft" in text and "steam_" not in text

    def test_a_fence_in_a_display_name_cant_break_the_block(self):
        text = logic.format_links([], [], {'role_no_link': ["sneaky```name"]})
        assert text.count("```") % 2 == 0

    def test_every_list_full_at_once_fits_an_embed_and_ends_in_counts(self):
        rows = []
        for n in range(40):
            rows += [
                {'rowid': n * 3, 'd_id': 1000 + n, 'g_id': f"G{n}"},
                {'rowid': n * 3 + 1, 'd_id': 2000 + n, 'g_id': f"G{n}"},
                {'rowid': n * 3 + 2, 'd_id': 1000 + n, 'g_id': f"H{n}"},
            ]
        live = {}
        for n in range(40):
            live[f"G{n}"] = (f"character_number_{n}", 1)
            live[f"H{n}"] = (f"alt_character_{n}", 1)
        characters, accounts = logic.find_link_conflicts(rows, live)
        gaps = {key: [f"{key}_member_{n:03d}" for n in range(60)] for key, _ in logic.LINK_GAP_SECTIONS}
        text = logic.format_links(characters, accounts, gaps)
        assert characters and accounts
        assert len(text) < logic.EMBED_DESCRIPTION_LIMIT
        assert "the report is full" in text


class TestValidateDiscordRows:
    ROWS = [{'Discord': "a#0", 'D_ID': n, 'Display': "a"} for n in range(1, 11)]

    def test_a_normal_list_passes(self):
        assert logic.validate_discord_rows(self.ROWS, 10) is None

    @pytest.mark.parametrize("rows,current,reason", [
        ([], 10, "nobody holds the role"),
        (ROWS[:4], 10, "shrank from 10 to 4"),
        ([{'Discord': "a#0", 'D_ID': None, 'Display': "a"}], 0, "no Discord id"),
    ])
    def test_a_partial_read_is_refused(self, rows, current, reason):
        assert reason in logic.validate_discord_rows(rows, current)


def discord_rows(*ids):
    return [{'Discord': f"user{n}#0", 'D_ID': n, 'Display': f"User {n}"} for n in ids]


class TestDiscordRosterTable:
    TABLE = logic.table_name(GUILD, 'discord')

    def test_keeps_the_shape_to_sql_gave_it(self, temp_functions_db):
        Functions.write_discord_roster(GUILD, discord_rows(111))
        columns = [(r[1], r[2]) for r in Functions.c.execute(f'PRAGMA table_info("{self.TABLE}")')]
        assert columns == [("index", "INTEGER"), ("Discord", "TEXT"), ("D_ID", "INTEGER"), ("Display", "TEXT")]
        assert [r[1] for r in Functions.c.execute(f'PRAGMA index_list("{self.TABLE}")')] == [f"ix_{self.TABLE}_index"]
        assert Functions.c.execute(f'SELECT typeof(D_ID) FROM "{self.TABLE}"').fetchone() == ("integer",)

    def test_a_failure_mid_insert_leaves_the_previous_list(self, temp_functions_db):
        Functions.write_discord_roster(GUILD, discord_rows(1, 2))
        broken = discord_rows(3) + [{'Discord': "x#0", 'D_ID': {"not": "bindable"}, 'Display': "x"}]
        with pytest.raises(Exception):
            Functions.write_discord_roster(GUILD, broken)
        assert Functions.c.execute(f'SELECT D_ID FROM "{self.TABLE}" ORDER BY "index"').fetchall() == [(1,), (2,)]

    def test_a_halved_list_is_refused_and_nothing_changes(self, temp_functions_db):
        Functions.write_discord_roster(GUILD, discord_rows(*range(1, 11)))
        assert "shrank" in Functions.refresh_discord_roster(GUILD, discord_rows(1, 2))
        assert Functions.c.execute(f'SELECT COUNT(*) FROM "{self.TABLE}"').fetchone() == (10,)


def make_link_tables():
    Functions.c.execute(f'CREATE TABLE {GUILD}_members (Discord TEXT, D_ID INTEGER, Display TEXT, G_ID TEXT, G_NAME TEXT)')
    Functions.c.execute(f'CREATE TABLE {GUILD}_game ("index" INTEGER, G_NAME TEXT, G_ID TEXT, GP INTEGER)')
    Functions.write_discord_roster(GUILD, [])
    Functions.c.executemany(f"INSERT INTO {GUILD}_members VALUES (?, ?, ?, ?, ?)", [
        ("here#0", 1, "Here", "A", "Alpha"),
        ("gone#0", 2, "Gone", "B", "Bravo"),
    ])
    Functions.c.executemany(f'INSERT INTO {GUILD}_game VALUES (?, ?, ?, ?)', [
        (0, "Alpha", "A", 1), (1, "Bravo", "B", 1),
    ])


class FakeServer:
    def __init__(self, members, chunked=True):
        self.members, self.chunked = members, chunked

    def get_member(self, member_id):
        return self.members.get(member_id)


class TestLinksReport:
    def test_asks_discord_who_is_still_in_the_server(self, temp_functions_db):
        make_link_tables()
        server = FakeServer({1: SimpleNamespace(roles=[SimpleNamespace(name="Other")])})
        [embed] = Functions.links_report(GUILD, server)
        assert embed.title == f"{GUILD} -- links"
        assert "Linked, in the server without the role:\n```\nAlpha  (Here)" in embed.description
        assert "Linked, but left the Discord server:\n```\nBravo" in embed.description

    def test_an_incomplete_member_cache_doesnt_read_as_everyone_leaving(self, temp_functions_db):
        make_link_tables()
        [embed] = Functions.links_report(GUILD, FakeServer({}, chunked=False))
        assert "left the Discord server" not in embed.description
        assert "Linked, but without the role:" in embed.description


class FakeMember:
    def __init__(self, member_id, display):
        self.id, self.name, self.discriminator, self.display_name = member_id, f"user{member_id}", "0", display


@pytest.fixture
def bot(temp_functions_db, no_bot_run, monkeypatch):
    import LedBotCode
    return LedBotCode


def use_server(bot, monkeypatch, roles):
    server = SimpleNamespace(roles=roles)
    monkeypatch.setattr(bot.bot, "get_guild", lambda gid: server)


class TestRefreshDiscordRosters:
    def test_writes_each_guilds_role_holders(self, bot, monkeypatch):
        use_server(bot, monkeypatch, [
            SimpleNamespace(name=name, members=[FakeMember(n, f"Member {n}") for n in (1, 2)])
            for name in logic.GUILD_NAMES
        ])
        assert bot.refresh_discord_rosters() == []
        rows = Functions.c.execute(f'SELECT Discord, D_ID, Display FROM "{GUILD}_discord"').fetchall()
        assert rows == [("user1#0", 1, "Member 1"), ("user2#0", 2, "Member 2")]

    def test_a_missing_role_is_a_warning_and_leaves_the_table(self, bot, monkeypatch):
        Functions.write_discord_roster(GUILD, discord_rows(7))
        use_server(bot, monkeypatch, [])
        warnings = bot.refresh_discord_rosters()
        assert [w.title for w in warnings] == [f"{name} -- role list not refreshed" for name in logic.GUILD_NAMES]
        assert "no 'Aetherians' role" in warnings[0].description
        assert Functions.c.execute(f'SELECT D_ID FROM "{GUILD}_discord"').fetchall() == [(7,)]

    def test_a_shrunken_list_is_a_warning(self, bot, monkeypatch):
        Functions.write_discord_roster(GUILD, discord_rows(*range(1, 11)))
        use_server(bot, monkeypatch, [
            SimpleNamespace(name=name, members=[FakeMember(1, "One")]) for name in logic.GUILD_NAMES
        ])
        [warning] = bot.refresh_discord_rosters()
        assert warning.title == f"{GUILD} -- role list not refreshed" and "shrank" in warning.description
