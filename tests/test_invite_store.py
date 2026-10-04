"""Tests for the invite auto-link's storage and Discord side, on a temp database.

The pure matching is in test_logic_invites.py. These cover what depends on
state: that re-invites never leave two live invites for one person, that the
roster is written atomically and never over the weekly job, that a link is
never deleted, that a Discord error which would simply repeat doesn't retry
forever, and that the guild leader's token can't leak into an error.
"""

import asyncio
import sqlite3
from types import SimpleNamespace

import discord
import pytest
import requests

import Functions
import logic

GUILD = "Aetherians"
T0 = 1_000_000.0
BOB = {'d_id': "111", 'display': "Bob", 'discord': "bob#0"}
CARL = {'d_id': "222", 'display': "Carl", 'discord': "carl#0"}


def rows(*pairs):
    return [{'G_ID': g_id, 'G_NAME': name, 'GP': 100} for g_id, name in pairs]


def make_members_table():
    Functions.c.execute(
        f"CREATE TABLE {logic.table_name(GUILD, 'members')} "
        '("index" INTEGER, Discord TEXT, D_ID INTEGER, Display TEXT, G_ID TEXT, G_NAME TEXT)'
    )


def live_invites(member_id):
    return [
        inv for inv in Functions.load_invites(GUILD, statuses=logic.INVITE_OPEN_STATUSES)
        if inv['d_id'] == member_id
    ]


class TestRecordInvite:
    def test_a_new_invite_is_pending(self, temp_functions_db):
        record = Functions.record_invite(GUILD, "Bob", BOB, {"A"}, T0, T0, True)
        stored = Functions.get_invite(record['invite_id'])
        assert record['outcome'] == 'new'
        assert stored['status'] == logic.INVITE_PENDING and stored['baseline'] == {"A"}

    def test_resending_merges_and_restarts_the_watch(self, temp_functions_db):
        first = Functions.record_invite(GUILD, "Bob", BOB, {"A", "X"}, T0, T0, True)
        again = Functions.record_invite(GUILD, "bob", BOB, {"A"}, T0 + 500, T0 + 500, True)
        stored = Functions.get_invite(first['invite_id'])
        assert again['outcome'] == 'merged' and again['invite_id'] == first['invite_id']
        assert stored['sent_at'] == T0 + 500 and stored['first_sent_at'] == T0
        assert stored['baseline'] == {"A"}

    def test_a_corrected_typo_supersedes_and_leaves_one_live_invite(self, temp_functions_db):
        """The user's requirement: never waiting on two accepts for one account."""
        typo = Functions.record_invite(GUILD, "Bbo", BOB, {"A"}, T0, T0, True)
        fixed = Functions.record_invite(GUILD, "Bob", BOB, {"A"}, T0 + 60, T0 + 60, True)
        assert fixed['superseded'] == ["Bbo"]
        assert Functions.get_invite(typo['invite_id'])['status'] == logic.INVITE_SUPERSEDED
        assert [inv['invited_name'] for inv in live_invites("111")] == ["Bob"]

    def test_retagging_the_right_member_moves_the_invite(self, temp_functions_db):
        Functions.record_invite(GUILD, "Bob", CARL, {"A"}, T0, T0, True)
        record = Functions.record_invite(GUILD, "Bob", BOB, {"A"}, T0 + 60, T0 + 60, True)
        assert record['outcome'] == 'merged'
        assert record['previous_d_id'] == "222" and record['d_id'] == "111"
        assert live_invites("222") == [] and len(live_invites("111")) == 1

    def test_a_cancelled_invite_is_never_merged_into(self, temp_functions_db):
        """!uninvite is how a wrong member gets stopped; re-inviting afterwards
        must start clean, not revive the cancelled row."""
        old = Functions.record_invite(GUILD, "Bob", CARL, {"A"}, T0, T0, True)
        Functions.set_invite_status(old['invite_id'], logic.INVITE_CANCELLED)
        new = Functions.record_invite(GUILD, "Bob", BOB, {"A"}, T0 + 60, T0 + 60, True)
        assert new['outcome'] == 'new' and new['invite_id'] != old['invite_id']
        assert Functions.get_invite(old['invite_id'])['status'] == logic.INVITE_CANCELLED

    def test_a_superseded_invite_can_be_cancelled(self, temp_functions_db):
        """It blocks elimination for a week, so !uninvite must be able to
        clear it when its stray recipient isn't coming."""
        typo = Functions.record_invite(GUILD, "Bbo", BOB, {"A"}, T0, T0, True)
        Functions.record_invite(GUILD, "Bob", BOB, {"A"}, T0 + 60, T0 + 60, True)
        found = Functions.find_cancellable_invite(GUILD, "Bbo")
        assert found is not None and found['id'] == typo['invite_id']
        assert found['status'] == logic.INVITE_SUPERSEDED

    def test_an_unconfirmed_send_is_recorded_as_such(self, temp_functions_db):
        record = Functions.record_invite(GUILD, "Bob", BOB, {"A"}, T0, T0, False)
        assert Functions.get_invite(record['invite_id'])['send_confirmed'] == 0


class TestInviteState:
    def test_unknown_status_fields_are_refused(self, temp_functions_db):
        record = Functions.record_invite(GUILD, "Bob", BOB, {"A"}, T0, T0, True)
        with pytest.raises(ValueError):
            Functions.set_invite_status(record['invite_id'], logic.INVITE_DONE, guild="Pretherians")

    def test_claims_hold_the_latest_match(self, temp_functions_db):
        first = Functions.record_invite(GUILD, "Bob", BOB, {"A"}, T0, T0, True)
        second = Functions.record_invite(GUILD, "Carl", CARL, {"A"}, T0, T0, True)
        Functions.set_invite_status(first['invite_id'], logic.INVITE_DONE, matched_g_id="X", matched_at=T0 + 1)
        Functions.set_invite_status(second['invite_id'], logic.INVITE_DONE, matched_g_id="X", matched_at=T0 + 9)
        assert Functions.invite_claims(GUILD) == {"X": T0 + 9}

    def test_arrival_reports_survive_a_restart(self, temp_functions_db, monkeypatch):
        """Persisted, or every restart would re-post every unmatched arrival."""
        Functions.record_invite(GUILD, "Bob", BOB, {"A"}, T0, T0, True)
        Functions.mark_arrival_reported(GUILD, "X", T0)
        reopened = sqlite3.connect(str(temp_functions_db))
        monkeypatch.setattr(Functions, "conn", reopened)
        monkeypatch.setattr(Functions, "c", reopened.cursor())
        try:
            assert Functions.reported_arrivals(GUILD, since=T0 - 1) == {"X"}
            assert Functions.reported_arrivals(GUILD, since=T0 + 1) == set()
        finally:
            reopened.close()


class TestWriteGameRoster:
    def test_keeps_the_schema_assign_reads_positionally(self, temp_functions_db):
        Functions.write_game_roster(GUILD, rows(("A", "Alpha")))
        table = logic.table_name(GUILD, 'game')
        columns = [(r[1], r[2]) for r in Functions.c.execute(f'PRAGMA table_info("{table}")')]
        assert columns == [("index", "INTEGER"), ("G_NAME", "TEXT"), ("G_ID", "TEXT"), ("GP", "INTEGER")]
        assert [r[1] for r in Functions.c.execute(f'PRAGMA index_list("{table}")')] == [f"ix_{table}_index"]
        assert Functions.c.execute(f'SELECT * FROM "{table}"').fetchall() == [(0, "Alpha", "A", 100)]

    def test_a_failure_mid_insert_leaves_the_previous_roster(self, temp_functions_db):
        """to_sql(replace) autocommitted its DROP and CREATE before inserting,
        so a failure part-way left the table empty."""
        Functions.write_game_roster(GUILD, rows(("A", "Alpha"), ("B", "Bravo")))
        broken = rows(("C", "Cee")) + [{'G_ID': "D", 'G_NAME': "Dee", 'GP': {"not": "bindable"}}]
        with pytest.raises(sqlite3.Error):
            Functions.write_game_roster(GUILD, broken)
        table = logic.table_name(GUILD, 'game')
        assert Functions.c.execute(f'SELECT G_ID FROM "{table}" ORDER BY "index"').fetchall() == [("A",), ("B",)]

    def test_only_writes_that_arent_the_polls_bump_the_generation(self, temp_functions_db):
        Functions.write_game_roster(GUILD, rows(("A", "Alpha")))
        assert Functions.roster_generation == 1
        Functions.write_game_roster(GUILD, rows(("A", "Alpha")), from_poll=True)
        assert Functions.roster_generation == 1


class TestRefreshGameRoster:
    START = rows(("A", "Alpha"), ("B", "Bravo"), ("C", "Cee"), ("D", "Dee"))

    @pytest.fixture
    def seeded(self, temp_functions_db):
        Functions.write_game_roster(GUILD, self.START)
        return Functions.roster_generation

    def test_a_change_is_written(self, seeded):
        assert Functions.refresh_game_roster(GUILD, self.START + rows(("E", "Eee")), seeded) == 'written'

    def test_an_unchanged_roster_writes_nothing(self, seeded):
        before = Functions.conn.total_changes
        assert Functions.refresh_game_roster(GUILD, self.START, seeded) == 'unchanged'
        assert Functions.conn.total_changes == before

    def test_refused_inside_the_weekly_job(self, seeded):
        with Functions.weekly_job():
            assert 'weekly job' in Functions.refresh_game_roster(GUILD, self.START + rows(("E", "E")), seeded)

    def test_refused_while_an_overlapping_weekly_job_is_still_running(self, seeded):
        """A counter, not a flag: the inner run finishing mustn't clear it
        while the outer one is still between its export and GP_databases."""
        with Functions.weekly_job():
            with Functions.weekly_job():
                pass
            outcome = Functions.refresh_game_roster(GUILD, self.START + rows(("E", "E")), Functions.roster_generation)
            assert 'weekly job' in outcome

    def test_refused_when_a_fresher_roster_landed_during_the_read(self, seeded):
        Functions.write_game_roster(GUILD, self.START)  # e.g. !members_game
        assert 'newer roster' in Functions.refresh_game_roster(GUILD, self.START + rows(("E", "E")), seeded)

    def test_a_roster_that_halved_is_refused(self, seeded):
        assert Functions.refresh_game_roster(GUILD, self.START[:1], seeded).startswith('refused')


class TestLinkForInvite:
    def members(self):
        table = logic.table_name(GUILD, 'members')
        return Functions.c.execute(f"SELECT D_ID, G_ID FROM {table} ORDER BY rowid").fetchall()

    def add_row(self, d_id, g_id):
        Functions.c.execute(
            f"INSERT INTO {logic.table_name(GUILD, 'members')} (D_ID, G_ID) VALUES (?, ?)", (d_id, g_id)
        )

    def test_a_fresh_joiner_is_inserted(self, temp_functions_db):
        make_members_table()
        assert Functions.link_for_invite(GUILD, "X", "Bob", BOB) == ('insert', [])
        assert self.members() == [(111, "X")]

    def test_an_existing_link_is_kept_even_with_a_duplicate(self, temp_functions_db):
        make_members_table()
        self.add_row(111, "X")
        self.add_row(111, "X")
        assert Functions.link_for_invite(GUILD, "X", "Bob", BOB) == ('keep', [])
        assert len(self.members()) == 2

    def test_another_account_is_a_conflict_and_nothing_is_deleted(self, temp_functions_db):
        """The bot never deletes a link row; !relink stays the only delete."""
        make_members_table()
        self.add_row(999, "X")
        assert Functions.link_for_invite(GUILD, "X", "Bob", BOB) == ('conflict', ["999"])
        assert self.members() == [(999, "X")]


# --- Discord side -----------------------------------------------------------

def http_error(cls, status):
    return cls(SimpleNamespace(status=status, reason="nope"), "denied")


class FakeChannel:
    def __init__(self):
        self.sent = []

    async def send(self, content=None, **kwargs):
        self.sent.append((content, kwargs))


class FakeMember:
    def __init__(self, member_id, roles=(), fail_roles=None):
        self.id = member_id
        self.roles = list(roles)
        self.fail_roles = fail_roles
        self.mention = f"<@{member_id}>"

    async def add_roles(self, role, reason=None):
        if self.fail_roles:
            raise self.fail_roles
        self.roles.append(role)

    async def remove_roles(self, role, reason=None):
        self.roles.remove(role)


class FakeGuild:
    def __init__(self, members, roles):
        self.members = {m.id: m for m in members}
        self.roles = roles

    def get_member(self, member_id):
        return self.members.get(member_id)

    async def fetch_member(self, member_id):
        raise http_error(discord.NotFound, 404)


@pytest.fixture
def bot_module(temp_functions_db, no_bot_run, monkeypatch):
    import LedBotCode
    mod_channel, welcome_channel = FakeChannel(), FakeChannel()
    monkeypatch.setattr(LedBotCode, "LedukasSpam_channel", mod_channel)
    monkeypatch.setattr(LedBotCode, "WELCOME_POST_CHANNEL_ID", 1)
    monkeypatch.setattr(LedBotCode, "WELCOME_INFO_CHANNEL_ID", 2)
    monkeypatch.setattr(LedBotCode, "ROLES_CHANNEL_ID", 3)
    monkeypatch.setattr(LedBotCode.bot, "get_channel", lambda cid: welcome_channel if cid == 1 else None)
    monkeypatch.setattr(LedBotCode, "_firebase_tokens", {})
    monkeypatch.setattr(LedBotCode, "_poll_signin_paused_until", {})
    return SimpleNamespace(module=LedBotCode, mod=mod_channel, welcome=welcome_channel)


def linked_invite():
    record = Functions.record_invite(GUILD, "Bob", BOB, {"A"}, T0, T0, True)
    Functions.set_invite_status(
        record['invite_id'], logic.INVITE_LINKED,
        matched_g_id="X", matched_name="Bob", matched_at=T0, method=logic.MATCH_BY_NAME,
    )
    return Functions.get_invite(record['invite_id'])


def use_guild(bot, monkeypatch, members):
    roles = [SimpleNamespace(name=GUILD), SimpleNamespace(name="Former Aetherian")]
    guild = FakeGuild(members, roles)
    monkeypatch.setattr(bot.module.bot, "get_guild", lambda gid: guild)
    return roles


class TestFinishInvite:
    def test_role_welcome_and_former_role_removed(self, bot_module, monkeypatch):
        member = FakeMember(111)
        guild_role, former = use_guild(bot_module, monkeypatch, [member])
        member.roles.append(former)
        invite = linked_invite()
        asyncio.run(bot_module.module.finish_invite(invite))

        assert guild_role in member.roles and former not in member.roles
        assert Functions.get_invite(invite['id'])['status'] == logic.INVITE_DONE
        [(text, kwargs)] = bot_module.welcome.sent
        assert text.startswith("<@111> ")
        assert kwargs['allowed_mentions'].users == [member]
        assert kwargs['allowed_mentions'].everyone is False

        # The mod report names the member too, but must not ping them there.
        [(report, report_kwargs)] = bot_module.mod.sent
        assert "<@111>" in report
        mentions = report_kwargs['allowed_mentions']
        assert mentions.users is False and mentions.roles is False and mentions.everyone is False

    def test_a_resumed_finish_does_not_welcome_twice(self, bot_module, monkeypatch):
        """Role already present -- a crash between role and welcome, or a mod
        who did it by hand. At-most-once: no second public welcome."""
        member = FakeMember(111)
        guild_role, _ = use_guild(bot_module, monkeypatch, [member])
        member.roles.append(guild_role)
        asyncio.run(bot_module.module.finish_invite(linked_invite()))
        assert bot_module.welcome.sent == []
        assert "welcome may not have been sent" in bot_module.mod.sent[0][0]

    def test_a_repeating_discord_error_is_reported_once_not_retried(self, bot_module, monkeypatch):
        """Forbidden -- the bot's role below the guild role -- would fail
        identically every tick. It parks the invite instead."""
        member = FakeMember(111, fail_roles=http_error(discord.Forbidden, 403))
        use_guild(bot_module, monkeypatch, [member])
        invite = linked_invite()
        asyncio.run(bot_module.module.finish_invite(invite))

        assert Functions.get_invite(invite['id'])['status'] == logic.INVITE_NEEDS_ATTENTION
        assert len(bot_module.mod.sent) == 1
        # The next tick only re-finishes `linked` invites, so nothing repeats.
        assert Functions.load_invites(statuses=(logic.INVITE_LINKED,)) == []

    def test_a_member_who_left_is_linked_but_not_welcomed(self, bot_module, monkeypatch):
        use_guild(bot_module, monkeypatch, [])
        invite = linked_invite()
        asyncio.run(bot_module.module.finish_invite(invite))
        assert Functions.get_invite(invite['id'])['status'] == logic.INVITE_DONE
        assert bot_module.welcome.sent == []
        assert "aren't in the Discord server" in bot_module.mod.sent[0][0]

    def test_an_uninvite_during_the_role_change_stops_the_welcome(self, bot_module, monkeypatch):
        member = FakeMember(111)
        use_guild(bot_module, monkeypatch, [member])
        invite = linked_invite()

        async def add_roles_then_uninvite(role, reason=None):
            member.roles.append(role)
            Functions.set_invite_status(invite['id'], logic.INVITE_CANCELLED)
        member.add_roles = add_roles_then_uninvite

        asyncio.run(bot_module.module.finish_invite(invite))
        assert bot_module.welcome.sent == []
        # The role was already given; the mod must hear so, since !uninvite
        # has just told them none would be.
        [(report, _)] = bot_module.mod.sent
        assert "role had already been given" in report


class FakeResponse:
    def __init__(self, data, status=200):
        self._data, self.status_code = data, status

    def json(self):
        return self._data


class TestSignIn:
    def calls(self, bot_module, monkeypatch, *responses):
        seen, queue = [], list(responses)

        async def fake_post(url, json=None, headers=None):
            seen.append(url)
            return queue.pop(0)
        monkeypatch.setattr(bot_module.module, "post_json", fake_post)
        return seen

    def test_the_token_is_cached(self, bot_module, monkeypatch):
        seen = self.calls(bot_module, monkeypatch, FakeResponse({'idToken': "t", 'expiresIn': "3600"}))
        asyncio.run(bot_module.module.firebase_sign_in(GUILD))
        asyncio.run(bot_module.module.firebase_sign_in(GUILD, from_poll=True))
        assert len(seen) == 1

    def test_a_bad_password_stops_the_poll_until_a_manual_success(self, bot_module, monkeypatch):
        """Retrying a credentials failure can't succeed, and every retry is a
        failed login on the account !kick and the weekly export share."""
        mod = bot_module.module
        seen = self.calls(
            bot_module, monkeypatch,
            FakeResponse({'error': {'message': "INVALID_PASSWORD"}}, 400),
            FakeResponse({'idToken': "t", 'expiresIn': "3600"}),
        )
        with pytest.raises(mod.FirebaseSignInError) as failure:
            asyncio.run(mod.firebase_sign_in(GUILD, from_poll=True))
        assert failure.value.permanent
        with pytest.raises(mod.FirebaseSignInError):
            asyncio.run(mod.firebase_sign_in(GUILD, from_poll=True))
        assert len(seen) == 1  # the paused poll didn't even try

        asyncio.run(mod.firebase_sign_in(GUILD))  # a manual command always tries
        assert GUILD not in mod._poll_signin_paused_until
        assert len(seen) == 2

    def test_a_transient_failure_pauses_the_poll_briefly(self, bot_module, monkeypatch):
        mod = bot_module.module
        self.calls(bot_module, monkeypatch, FakeResponse({'error': {'message': "TOO_MANY_ATTEMPTS_TRY_LATER"}}, 400))
        with pytest.raises(mod.FirebaseSignInError) as failure:
            asyncio.run(mod.firebase_sign_in(GUILD, from_poll=True))
        assert not failure.value.permanent
        assert mod._poll_signin_paused_until[GUILD] < float('inf')

    def test_a_weekly_retry_neither_sets_nor_obeys_the_brief_pause(self, bot_module, monkeypatch):
        """A blip in the weekly retry mustn't stall the invite poll, nor a poll
        blip the retry -- the retry schedule spaces it already."""
        mod = bot_module.module
        seen = self.calls(
            bot_module, monkeypatch,
            FakeResponse({'error': {'message': "TOO_MANY_ATTEMPTS_TRY_LATER"}}, 400),
            FakeResponse({'error': {'message': "TOO_MANY_ATTEMPTS_TRY_LATER"}}, 400),
            FakeResponse({'error': {'message': "TOO_MANY_ATTEMPTS_TRY_LATER"}}, 400),
        )
        with pytest.raises(mod.FirebaseSignInError):
            asyncio.run(mod.firebase_sign_in(GUILD, retry=True))
        assert GUILD not in mod._poll_signin_paused_until
        with pytest.raises(mod.FirebaseSignInError):
            asyncio.run(mod.firebase_sign_in(GUILD, from_poll=True))
        with pytest.raises(mod.FirebaseSignInError):
            asyncio.run(mod.firebase_sign_in(GUILD, retry=True))
        assert len(seen) == 3

    def test_a_weekly_retry_shares_the_bad_password_pause(self, bot_module, monkeypatch):
        mod = bot_module.module
        seen = self.calls(bot_module, monkeypatch, FakeResponse({'error': {'message': "INVALID_PASSWORD"}}, 400))
        with pytest.raises(mod.FirebaseSignInError):
            asyncio.run(mod.firebase_sign_in(GUILD, retry=True))
        assert mod._poll_signin_paused_until[GUILD] == float('inf')
        with pytest.raises(mod.FirebaseSignInError):
            asyncio.run(mod.firebase_sign_in(GUILD, retry=True))
        with pytest.raises(mod.FirebaseSignInError):
            asyncio.run(mod.firebase_sign_in(GUILD, from_poll=True))
        assert len(seen) == 1


class TestRequestErrors:
    def raise_from_get(self, monkeypatch, exc_type):
        def fake_get(url, params=None, timeout=None):
            raise exc_type(f"failed for {url}?auth={params['auth']}")
        monkeypatch.setattr(requests, "get", fake_get)

    def test_the_token_never_reaches_an_error(self, bot_module, monkeypatch):
        """requests puts the URL in its message, and the roster read passes the
        guild leader's token as ?auth=. Both the report and the traceback in
        the journal would carry it; the chain is dropped as well."""
        self.raise_from_get(monkeypatch, requests.ConnectionError)
        mod = bot_module.module
        with pytest.raises(mod.FirebaseRequestError) as failure:
            asyncio.run(mod.get_json("https://example/m.json", params={'auth': "eyJSECRET"}))
        assert "eyJSECRET" not in str(failure.value)
        assert failure.value.__cause__ is None and failure.value.__suppress_context__

    def test_a_dns_failure_counts_as_certainly_unsent(self, bot_module, monkeypatch):
        """requests wraps a failed lookup or refused connection as a
        ConnectionError around urllib3's NewConnectionError. Nothing was sent,
        so !invite must not record a phantom invite for it."""
        from urllib3.exceptions import MaxRetryError, NewConnectionError
        dns = requests.ConnectionError(
            MaxRetryError(None, "https://example", reason=NewConnectionError(None, "Failed to resolve"))
        )

        def fake_get(url, params=None, timeout=None):
            raise dns
        monkeypatch.setattr(requests, "get", fake_get)
        mod = bot_module.module
        with pytest.raises(mod.FirebaseRequestError) as failure:
            asyncio.run(mod.get_json("https://example", params={'auth': "t"}))
        assert failure.value.maybe_sent is False

    @pytest.mark.parametrize("exc_type,maybe_sent", [
        (requests.ConnectTimeout, False),
        (requests.ReadTimeout, True),
        (requests.ConnectionError, True),
    ])
    def test_only_a_connect_timeout_counts_as_certainly_unsent(self, bot_module, monkeypatch, exc_type, maybe_sent):
        """Anything else may have reached the game, so !invite records it rather
        than lose an invite that did go through."""
        self.raise_from_get(monkeypatch, exc_type)
        mod = bot_module.module
        with pytest.raises(mod.FirebaseRequestError) as failure:
            asyncio.run(mod.get_json("https://example", params={'auth': "t"}))
        assert failure.value.maybe_sent is maybe_sent
