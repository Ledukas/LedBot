"""The command paths that act on real members: !kick, the character lookup it
shares with !mygains, and the rank-role sync. Discord and the game are stubs."""

import asyncio
import sqlite3
from types import SimpleNamespace

import discord
import pandas as pd
import pytest

import Functions
import logic

GUILD = "Aetherians"


class FakeMember:
    def __init__(self, member_id, role_names=(), refuses=False):
        self.id = member_id
        self.display_name = f"user{member_id}"
        self.roles = [SimpleNamespace(name=name) for name in role_names]
        self.refuses = refuses

    def __str__(self):
        return self.display_name

    async def add_roles(self, role, **kwargs):
        if self.refuses:
            raise discord.Forbidden(SimpleNamespace(status=403, reason="Forbidden"), "Missing Permissions")
        self.roles.append(role)

    async def remove_roles(self, role, **kwargs):
        if self.refuses:
            raise discord.Forbidden(SimpleNamespace(status=403, reason="Forbidden"), "Missing Permissions")
        self.roles = [held for held in self.roles if held is not role]

    def role_names(self):
        return sorted(role.name for role in self.roles)


class FakeGuild:
    def __init__(self, role_names, members):
        self.roles = [SimpleNamespace(name=name) for name in role_names]
        self.members = list(members)
        for member in self.members:
            member.roles = [
                next(role for role in self.roles if role.name == held.name) for held in member.roles
            ]

    def get_member(self, member_id):
        return next((member for member in self.members if member.id == member_id), None)


class FakeContext:
    def __init__(self):
        self.sent = []

    async def send(self, text, **kwargs):
        self.sent.append(text)


def reply(status_code, payload):
    def json():
        if payload is None:
            raise ValueError("not JSON")
        return payload
    return SimpleNamespace(status_code=status_code, json=json)


def link_tables(db, links, in_game):
    db.execute(
        f'CREATE TABLE {GUILD}_members ("index" INTEGER, Discord TEXT, D_ID INTEGER, '
        f'Display TEXT, G_ID TEXT, G_NAME TEXT)'
    )
    db.execute(f'CREATE TABLE {GUILD}_game ("index" INTEGER, G_NAME TEXT, G_ID TEXT, GP INTEGER)')
    db.executemany(f"INSERT INTO {GUILD}_members (D_ID, G_ID, G_NAME) VALUES (?, ?, ?)", links)
    db.executemany(f"INSERT INTO {GUILD}_game (G_NAME, G_ID, GP) VALUES (?, ?, ?)", in_game)
    db.commit()


@pytest.fixture
def kick(no_bot_run, monkeypatch):
    import LedBotCode

    db = sqlite3.connect(":memory:")
    link_tables(
        db,
        links=[(1, 'old', 'OldName'), (1, 'new', 'NewName')],
        in_game=[('NewName', 'new', 1500)],
    )
    member = FakeMember(1, role_names=[GUILD, 'Aetherian Knight'])
    guild = FakeGuild(list(logic.GUILD_NAMES) + logic.RANK_ROLE_NAMES + ['Former Aetherian'], [member])
    posted = []
    state = SimpleNamespace(member=member, posted=posted, answer=reply(200, {"result": "true"}))

    async def guild_login(ctx, io_guild):
        return "gid", "token"

    async def post_json(url, json=None, headers=None):
        posted.append(json)
        if isinstance(state.answer, Exception):
            raise state.answer
        return state.answer

    monkeypatch.setattr(LedBotCode, "conn", db)
    monkeypatch.setattr(LedBotCode, "guild_login", guild_login)
    monkeypatch.setattr(LedBotCode, "post_json", post_json)
    monkeypatch.setattr(LedBotCode.bot, "get_guild", lambda guild_id: guild)

    def run(answer):
        state.answer = answer
        ctx = FakeContext()
        asyncio.run(LedBotCode.kick.callback(ctx, GUILD, "1"))
        return ctx.sent[-1]

    state.run = run
    state.error = LedBotCode.FirebaseRequestError
    yield state
    db.close()


class TestKick:
    def test_the_character_still_in_the_guild_is_the_one_kicked(self, kick):
        """The account's first link row is a character that left long ago."""
        kick.run(reply(200, {"result": "true"}))
        assert kick.posted == [{"data": {"uid": "new", "gid": "gid"}}]

    def test_a_kick_strips_the_guild_and_rank_roles(self, kick):
        message = kick.run(reply(200, {"result": "true"}))
        assert message == f"NewName has been kicked from {GUILD}"
        assert kick.member.role_names() == ['Former Aetherian']

    def test_a_refused_kick_leaves_the_roles_alone(self, kick):
        """It used to say "not kicked" and strip every role anyway."""
        message = kick.run(reply(200, {"result": "false"}))
        assert "was not kicked" in message
        assert kick.member.role_names() == sorted([GUILD, 'Aetherian Knight'])

    def test_a_rejected_token_is_a_refusal(self, kick):
        kick.run(reply(401, {"error": {"status": "UNAUTHENTICATED"}}))
        assert kick.member.role_names() == sorted([GUILD, 'Aetherian Knight'])

    def test_a_server_error_is_treated_as_possibly_kicked(self, kick):
        message = kick.run(reply(502, None))
        assert "may or may not have been" in message
        assert kick.member.role_names() == ['Former Aetherian']

    def test_a_timeout_after_connecting_is_treated_as_possibly_kicked(self, kick):
        message = kick.run(kick.error("ReadTimeout", maybe_sent=True))
        assert "may or may not have been" in message
        assert kick.member.role_names() == ['Former Aetherian']

    def test_a_request_that_never_left_changes_nothing(self, kick):
        message = kick.run(kick.error("ConnectTimeout", maybe_sent=False))
        assert "wasn't sent" in message
        assert kick.member.role_names() == sorted([GUILD, 'Aetherian Knight'])


class TestLinkedCharacter:
    @pytest.fixture
    def lookup(self, no_bot_run):
        import LedBotCode

        db = sqlite3.connect(":memory:")
        link_tables(
            db,
            links=[(1, 'old', 'Old'), (1, 'new', 'New'), (2, 'gone', 'Gone'), ('', '', '')],
            in_game=[('New', 'new', 5)],
        )
        yield lambda d_id: LedBotCode.linked_character(db.cursor(), GUILD, d_id)
        db.close()

    def test_prefers_the_character_in_the_guild(self, lookup):
        assert lookup(1) == ('new', 'New')

    def test_a_text_id_matches_the_integer_column(self, lookup):
        assert lookup("1") == ('new', 'New')

    def test_falls_back_to_a_departed_character(self, lookup):
        assert lookup(2) == ('gone', 'Gone')

    def test_no_link_row(self, lookup):
        assert lookup(3) is None


class TestGpRolesKeepsGoing:
    def sync(self, members, extra_roles=('Monthly Top', 'Aetherian Duck', 'Booster (For DUCK)')):
        c = Functions.conn.cursor()
        c.execute(
            f'CREATE TABLE {GUILD}_members (Discord TEXT, D_ID INTEGER, Display TEXT, G_ID TEXT, G_NAME TEXT)'
        )
        c.execute(f'CREATE TABLE {GUILD}_game (G_NAME TEXT, G_ID TEXT, GP INTEGER)')
        for member in members:
            g_id = f"g{member.id}"
            c.execute(f"INSERT INTO {GUILD}_members (D_ID, G_ID) VALUES (?, ?)", (member.id, g_id))
            c.execute(f"INSERT INTO {GUILD}_game VALUES (?, ?, ?)", (g_id, g_id, 1500))
        Functions.conn.commit()
        guild = FakeGuild(logic.RANK_ROLE_NAMES + list(extra_roles), members)
        # Every member averaging above the Monthly Top cutoff.
        frame = pd.DataFrame({
            'Name': [f"g{member.id}" for member in members],
            'G_ID': [f"g{member.id}" for member in members],
            'Average': [700.0 for _ in members],
        })
        bot = SimpleNamespace(get_guild=lambda guild_id: guild)
        return asyncio.run(Functions.GP_roles(bot, frame))

    def test_one_refused_member_does_not_stop_the_rest(self, temp_functions_db):
        """A Forbidden on the first member used to abort the sync for everyone
        after them, Monthly Top included."""
        members = [FakeMember(1, refuses=True), FakeMember(2), FakeMember(3)]
        status = self.sync(members)

        assert members[0].role_names() == []
        assert members[1].role_names() == ['Aetherian Knight', 'Monthly Top']
        assert members[2].role_names() == ['Aetherian Knight', 'Monthly Top']
        assert "role change(s) failed" in status
        assert "user1" in status

    def test_a_clean_sync_reports_nothing(self, temp_functions_db):
        assert self.sync([FakeMember(1), FakeMember(2)]) is None
