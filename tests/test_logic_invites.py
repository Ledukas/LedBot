"""Tests for the invite auto-link logic in logic.py.

The game never says which account accepted an invite -- the only signal is a
new account id appearing in the roster -- so the matcher decides that from
circumstantial evidence, and most of these tests pin a case where the obvious
rule would link the wrong person. Several are regression tests for sequences
two design reviews found would do exactly that: a returning member handed to a
second invite, a typo'd invite reaching a stranger who then got welcomed as
someone else, and a crash leaving an invite stranded.
"""

import pytest

import logic

GUILD = "Aetherians"
BASE = {"A", "B"}
T0 = 1_000_000.0


def invite(i, name, d_id="111", baseline=BASE, status=logic.INVITE_PENDING,
           sent=T0, first=None, baseline_at=None, matched_g_id=None):
    return {
        'id': i, 'guild': GUILD, 'invited_name': name, 'name_key': logic.name_key(name),
        'd_id': d_id, 'display': f"member{i}", 'discord': f"member{i}#0",
        'baseline': set(baseline), 'baseline_at': sent if baseline_at is None else baseline_at,
        'status': status, 'sent_at': sent, 'first_sent_at': sent if first is None else first,
        'matched_g_id': matched_g_id, 'send_confirmed': 1,
    }


def roster(**arrivals):
    """The two baseline members plus whoever is passed as g_id=name."""
    return {"A": "Alpha", "B": "Bravo", **arrivals}


def match(invites, live, links=(), claims=None, fetched=T0 + 60):
    return logic.match_invites(invites, live, list(links), claims or {}, fetched)


def kinds(result):
    return [(a['kind'], a['method'], a['g_id']) for a in result['actions']]


class TestPollSchedule:
    @pytest.mark.parametrize("age,interval", [
        (0, 10), (119, 10), (120, 30), (599, 30), (600, 120), (3599, 120),
        (3600, 600), (86399, 600),
    ])
    def test_starts_fast_and_backs_off(self, age, interval):
        """Invitees usually accept within minutes, so the first two minutes
        are polled every ten seconds."""
        assert logic.invite_poll_interval(age) == interval

    def test_stops_at_24_hours(self):
        assert logic.invite_poll_interval(logic.INVITE_TRACK_SECONDS) is None

    def test_age_never_negative(self):
        """The Pi has no clock battery; before NTP syncs, now can be behind a
        stored time."""
        assert logic.invite_age(100.0, 500.0) == 0.0

    def test_first_poll_is_immediate(self):
        assert logic.should_poll_guild(T0, T0, None)

    def test_not_due_until_the_interval_passes(self):
        assert not logic.should_poll_guild(T0 + 5, T0, T0)
        assert logic.should_poll_guild(T0 + 10, T0, T0)

    def test_a_fresh_invite_speeds_the_guild_back_up(self):
        """Cadence follows the newest invite: an hour-old invite polls every two
        minutes, but a new one alongside it brings that back to ten seconds."""
        assert not logic.should_poll_guild(T0 + 3700, T0, T0 + 3690)
        assert logic.should_poll_guild(T0 + 3700, T0 + 3690, T0 + 3690)

    def test_clock_going_backwards_polls_rather_than_stalling(self):
        assert logic.should_poll_guild(T0, T0, T0 + 3600)

    def test_nothing_to_poll_after_expiry(self):
        assert not logic.should_poll_guild(T0 + logic.INVITE_TRACK_SECONDS, T0, None)


class TestSmallHelpers:
    def test_name_key_is_case_insensitive_and_trimmed(self):
        assert logic.name_key("  BoB ") == logic.name_key("bob")

    def test_token_freshness_leaves_a_margin(self):
        assert logic.token_is_fresh(T0, T0 + 3600)
        assert not logic.token_is_fresh(T0 + 3400, T0 + 3600)
        assert not logic.token_is_fresh(T0, None)

    @pytest.mark.parametrize("message", [
        "INVALID_PASSWORD", "INVALID_LOGIN_CREDENTIALS", "EMAIL_NOT_FOUND", "USER_DISABLED",
    ])
    def test_credential_failures_are_permanent(self, message):
        """Retrying these can't succeed; doing it from the poll would only pile
        failed logins onto the account !kick and the weekly export share."""
        assert logic.classify_signin_error(message) == 'permanent'

    @pytest.mark.parametrize("message", [
        "TOO_MANY_ATTEMPTS_TRY_LATER : Access to this account has been disabled", "HTTP 503", None,
    ])
    def test_other_failures_are_transient(self, message):
        assert logic.classify_signin_error(message) == 'transient'

    @pytest.mark.parametrize("payload,expected", [
        ({"result": "true"}, "true"), ({"result": True}, "True"),
        ({"error": {"message": "x"}}, None), (None, None), ("oops", None),
    ])
    def test_parse_action_result_never_raises(self, payload, expected):
        """invite and kick used to index data["result"] directly, so an error
        payload became a KeyError instead of a failure message."""
        assert logic.parse_action_result(payload) == expected

    def test_action_succeeded(self):
        assert logic.action_succeeded("true") and logic.action_succeeded("True")
        assert not logic.action_succeeded("false") and not logic.action_succeeded(None)

    @pytest.mark.parametrize("value,expected", [
        ("1481789094123143178", 1481789094123143178), (" 42 ", 42),
        (None, None), ("", None), ("abc", None), ("12a", None),
    ])
    def test_parse_optional_id(self, value, expected):
        """Optional so a missing .env entry skips the welcome instead of
        stopping the bot at import."""
        assert logic.parse_optional_id(value) == expected

    def test_redact_secrets(self):
        """requests puts the full URL in its exception text, and the roster read
        carries the guild leader's token as ?auth=."""
        text = ("HTTPSConnectionPool: /_guild/x/m.json?auth=eyJhbGciOi.abc.def&y=1 "
                "key=AIzaSy123 Authorization: Bearer eyJtoken")
        redacted = logic.redact_secrets(text)
        assert "eyJ" not in redacted and "AIzaSy123" not in redacted
        assert "auth=<redacted>" in redacted and "&y=1" in redacted


class TestPlanInviteRecord:
    def test_first_invite_is_new(self):
        plan = logic.plan_invite_record([], "Bob", "111", {"A"})
        assert plan['merge_into'] is None and plan['supersede'] == []
        assert plan['baseline'] == {"A"}

    def test_resending_after_a_rejection_merges(self):
        """Same member, same name: merge, keep the original first_sent_at, and
        intersect the baselines -- replacing the baseline could put an invitee
        who already arrived into their own baseline, and they'd never match."""
        old = invite(1, "Bob", baseline={"A", "B", "X"}, sent=T0 - 100)
        plan = logic.plan_invite_record([old], "bob", "111", {"A", "B"})
        assert plan['merge_into'] is old and plan['supersede'] == []
        assert plan['baseline'] == {"A", "B"}
        assert plan['first_sent_at'] == T0 - 100

    def test_correcting_a_typo_supersedes_the_old_invite(self):
        """Never two live invites for one account: a different name for the
        same member replaces the earlier invite."""
        old = invite(1, "Bbo", baseline={"A", "B"}, sent=T0 - 100)
        plan = logic.plan_invite_record([old], "Bob", "111", {"A", "B", "C"})
        assert plan['merge_into'] is None
        assert plan['supersede'] == [old]
        assert plan['baseline'] == {"A", "B"}
        assert plan['first_sent_at'] == T0 - 100

    def test_retagging_the_same_name_merges(self):
        """Same name, a different @member -- the wrong person was tagged. It
        merges, and the new account is recorded by the caller."""
        old = invite(1, "Bob", d_id="999")
        plan = logic.plan_invite_record([old], "Bob", "111", BASE)
        assert plan['merge_into'] is old

    def test_other_members_invites_are_left_alone(self):
        other = invite(1, "Carl", d_id="222")
        plan = logic.plan_invite_record([other], "Bob", "111", BASE)
        assert plan['merge_into'] is None and plan['supersede'] == []

    def test_an_invite_without_a_member_is_keyed_on_name_alone(self):
        other = invite(1, "Carl", d_id=None)
        plan = logic.plan_invite_record([other], "Bob", None, BASE)
        assert plan['merge_into'] is None and plan['supersede'] == []


class TestMatchByName:
    def test_the_invited_name_arriving_links(self):
        result = match([invite(1, "Bob")], roster(X="Bob"))
        assert kinds(result) == [(logic.ACTION_LINK, logic.MATCH_BY_NAME, "X")]

    def test_name_match_is_case_insensitive(self):
        result = match([invite(1, "Bob")], roster(X="BOB"))
        assert kinds(result)[0][1] == logic.MATCH_BY_NAME

    def test_a_prefix_is_not_a_match(self):
        """Exact names only: 'Bob' must not match 'Bobby' by name. (It may
        still be linked by elimination, which is flagged as such.)"""
        result = match([invite(1, "Bob"), invite(2, "Zed", d_id="222")], roster(X="Bobby"))
        assert result['actions'] == []

    def test_baseline_members_are_never_arrivals(self):
        """Someone already in the guild when the invite went out can't be its
        invitee, whatever they're called."""
        result = match([invite(1, "Bob", baseline={"A", "B", "X"})], roster(X="Bob"))
        assert result['actions'] == [] and result['unmatched'] == []

    def test_a_name_match_backed_by_another_invites_history_is_a_conflict(self):
        """X displays Bob's invited name but is already linked to the account
        another pending invite is for. That contradiction goes to a mod."""
        invites = [invite(1, "Bob", d_id="111"), invite(2, "Carl", d_id="222")]
        result = match(invites, roster(X="Bob"), links=[{'g_id': "X", 'd_id': "222"}])
        assert kinds(result) == [(logic.ACTION_CONFLICT, logic.MATCH_BY_NAME, "X")]

    def test_an_invite_with_no_member_is_reported_not_linked(self):
        result = match([invite(1, "Bob", d_id=None)], roster(X="Bob"))
        assert kinds(result) == [(logic.ACTION_REPORT_NO_MEMBER, logic.MATCH_BY_NAME, "X")]


class TestClaims:
    def test_a_matched_character_is_not_handed_to_a_second_invite(self):
        """The destructive sequence from the first review. Bob returns and is
        matched by name to his invite, which writes no new row because his old
        one already points at him. Without a claim he would still look like a
        fresh arrival, and the next tick would eliminate him into Carl's invite
        -- deleting Bob's link and welcoming Carl, who hadn't joined."""
        carl = invite(2, "Carl", d_id="222", sent=T0 + 10)
        claims = {"X": T0 + 30}  # Bob's invite matched X after Carl's was sent
        result = match([carl], roster(X="Bob"), links=[{'g_id': "X", 'd_id': "111"}], claims=claims)
        assert result['actions'] == []
        assert result['unmatched'] == []

    def test_a_claim_older_than_the_invite_does_not_block(self):
        """X was matched by some invite last month, left, and has now come back
        for this one. An old claim mustn't hide them."""
        result = match([invite(1, "Bob")], roster(X="Bob"), claims={"X": T0 - 86400 * 30})
        assert kinds(result)[0][2] == "X"

    def test_an_invite_that_already_matched_is_not_still_waiting(self):
        """Cancelling an invite after it linked doesn't make it outstanding
        again, so it can't block elimination for a week."""
        done_then_cancelled = invite(1, "Old", d_id="999", status=logic.INVITE_CANCELLED,
                                     matched_g_id="Q")
        result = match([done_then_cancelled, invite(2, "Bob")], roster(X="BobAlt"))
        assert kinds(result) == [(logic.ACTION_LINK, logic.MATCH_BY_ELIMINATION, "X")]


class TestMatchByHistory:
    def test_a_mod_assigning_by_hand_finishes_the_invite(self):
        """An arrival with a link row for the invite's account -- a mod ran
        !assign, or the bot crashed after writing the link -- completes the
        invite, so the role and welcome still happen."""
        result = match([invite(1, "Bob")], roster(X="SomethingElse"),
                       links=[{'g_id': "X", 'd_id': "111"}])
        assert kinds(result) == [(logic.ACTION_LINK, logic.MATCH_BY_HISTORY, "X")]

    def test_mixed_id_storage_still_counts_as_history(self):
        result = match([invite(1, "Bob", d_id="111")], roster(X="Other"),
                       links=[{'g_id': "X", 'd_id': 111}])
        assert kinds(result)[0][1] == logic.MATCH_BY_HISTORY

    def test_contested_history_is_reported_not_guessed(self):
        """X has rows for both invites' accounts: no way to tell whose it is."""
        invites = [invite(1, "Bob", d_id="111"), invite(2, "Carl", d_id="222")]
        links = [{'g_id': "X", 'd_id': "111"}, {'g_id': "X", 'd_id': "222"}]
        result = match(invites, roster(X="Other"), links=links)
        assert result['actions'] == []
        assert [u['g_id'] for u in result['unmatched']] == ["X"]


class TestElimination:
    def test_one_invite_one_fresh_arrival_links_by_elimination(self):
        """'Invited Bob, joined as BobAlt' -- the case the user asked for. Full
        auto, flagged in the report."""
        result = match([invite(1, "Bob")], roster(X="BobAlt"))
        assert kinds(result) == [(logic.ACTION_LINK, logic.MATCH_BY_ELIMINATION, "X")]

    def test_an_arrival_with_any_link_rows_is_never_eliminated(self):
        """Elimination may only ever insert. A returning member linked to some
        other account is a job for a moderator, not a guess."""
        result = match([invite(1, "Bob")], roster(X="Zed"), links=[{'g_id': "X", 'd_id': "999"}])
        assert result['actions'] == []
        assert result['unmatched'][0]['linked_to'] == ["999"]

    def test_junk_link_rows_do_not_block_elimination(self):
        """Blank and NULL-account rows aren't links; they mustn't make a fresh
        joiner look like a returning one."""
        junk = [{'g_id': "X", 'd_id': None}, {'g_id': "X", 'd_id': ""}]
        result = match([invite(1, "Bob")], roster(X="BobAlt"), links=junk)
        assert kinds(result)[0][1] == logic.MATCH_BY_ELIMINATION

    def test_two_outstanding_invites_means_no_guess(self):
        result = match([invite(1, "Bob"), invite(2, "Carl", d_id="222")], roster(X="Zed"))
        assert result['actions'] == []
        assert len(result['unmatched'][0]['invites']) == 2

    def test_two_arrivals_means_no_guess(self):
        result = match([invite(1, "Bob")], roster(X="Zed", Y="Yan"))
        assert result['actions'] == []
        assert {u['g_id'] for u in result['unmatched']} == {"X", "Y"}

    def test_a_name_match_can_free_the_other_invite_for_elimination(self):
        """Carl arrives under his invited name; that leaves Bob's invite and one
        arrival, so it resolves in the same pass."""
        invites = [invite(1, "Bob"), invite(2, "Carl", d_id="222")]
        result = match(invites, roster(X="BobAlt", Y="Carl"))
        assert sorted(kinds(result)) == sorted([
            (logic.ACTION_LINK, logic.MATCH_BY_NAME, "Y"),
            (logic.ACTION_LINK, logic.MATCH_BY_ELIMINATION, "X"),
        ])

    def test_an_expired_invite_is_never_eliminated_into(self):
        result = match([invite(1, "Bob", status=logic.INVITE_EXPIRED)], roster(X="BobAlt"))
        assert result['actions'] == []

    def test_a_memberless_invite_is_never_eliminated_into(self):
        result = match([invite(1, "Bob", d_id=None)], roster(X="BobAlt"))
        assert result['actions'] == []


class TestExpiredInvites:
    def test_a_recently_expired_invite_blocks_elimination(self):
        """Someone accepting an old invite after the watch ended must not be
        handed to whoever's invite is pending at the time."""
        old = invite(1, "Old", d_id="999", status=logic.INVITE_EXPIRED, sent=T0 - 86400 * 2)
        result = match([old, invite(2, "Bob")], roster(X="Stranger"))
        assert result['actions'] == []

    def test_a_late_invitee_is_still_matched_by_name(self):
        old = invite(1, "Old", d_id="999", status=logic.INVITE_EXPIRED, sent=T0 - 86400 * 2)
        result = match([old, invite(2, "Bob")], roster(X="Old"))
        assert kinds(result) == [(logic.ACTION_LINK, logic.MATCH_BY_NAME, "X")]
        assert result['actions'][0]['invite']['id'] == 1


class TestSupersededAndCancelled:
    def test_a_typo_that_reached_a_stranger_is_reported_not_linked(self):
        """The user's scenario: 'Bbo' was a real player and got the invite, the
        mod corrected it to 'Bob'. When the stranger accepts, they must not be
        linked to @Bob -- they're reported."""
        invites = [invite(1, "Bbo", status=logic.INVITE_SUPERSEDED, sent=T0 - 50), invite(2, "Bob")]
        result = match(invites, roster(X="Bbo"))
        assert kinds(result) == [(logic.ACTION_REPORT_SUPERSEDED, logic.MATCH_BY_NAME, "X")]

    def test_a_superseded_invite_blocks_elimination(self):
        """Otherwise the stranger, accepting under another character name,
        would simply be eliminated into @Bob's invite instead."""
        invites = [invite(1, "Bbo", status=logic.INVITE_SUPERSEDED, sent=T0 - 50), invite(2, "Bob")]
        result = match(invites, roster(X="Unrelated"))
        assert result['actions'] == []

    def test_the_live_invite_wins_a_name_shared_with_a_superseded_one(self):
        invites = [
            invite(1, "Bbo", d_id="111", status=logic.INVITE_SUPERSEDED, sent=T0 - 50),
            invite(2, "Bbo", d_id="222", sent=T0),
        ]
        result = match(invites, roster(X="Bbo"))
        assert kinds(result) == [(logic.ACTION_LINK, logic.MATCH_BY_NAME, "X")]
        assert result['actions'][0]['invite']['id'] == 2

    def test_the_same_person_under_a_replaced_name_finishes_after_assign(self):
        """Invited as 'BobAlt', corrected to 'Bob' for the same member; Bob's
        account accepts while showing 'BobAlt'. That is reported (it might have
        been a stranger), the mod confirms with !assign -- and the live invite
        must then finish. The report's claim used to hide the character from
        the live invite, which then expired as 'not accepted' and blocked
        elimination for a week."""
        live = invite(2, "Bob", d_id="111", sent=T0 + 60, first=T0)
        claims = {"X": T0 + 120}                          # recorded by the report
        after_assign = [{'g_id': "X", 'd_id': "111"}]     # the mod's !assign
        result = match([live], roster(X="BobAlt"), links=after_assign, claims=claims)
        assert kinds(result) == [(logic.ACTION_LINK, logic.MATCH_BY_HISTORY, "X")]

    def test_a_claim_still_hides_a_character_linked_to_someone_else(self):
        """The claim keeps doing its original job: a character linked to
        another account is not handed to this invite by history."""
        live = invite(2, "Carl", d_id="222", sent=T0 + 60, first=T0)
        result = match([live], roster(X="Bob"), links=[{'g_id': "X", 'd_id': "111"}],
                       claims={"X": T0 + 120})
        assert result['actions'] == []

    def test_a_cancelled_invitee_accepting_anyway_is_reported(self):
        """!uninvite can't withdraw the in-game invite, so they may still join."""
        cancelled = invite(1, "Bob", status=logic.INVITE_CANCELLED)
        result = match([cancelled], roster(X="Bob"))
        assert kinds(result) == [(logic.ACTION_REPORT_CANCELLED, logic.MATCH_BY_NAME, "X")]

    def test_a_cancelled_invite_does_not_block_elimination(self):
        """!uninvite is how a mod says 'not coming' and clears a stale invite
        -- the expiry message tells them so."""
        cancelled = invite(1, "Old", d_id="999", status=logic.INVITE_CANCELLED, sent=T0 - 100)
        result = match([cancelled, invite(2, "Bob")], roster(X="BobAlt"))
        assert kinds(result) == [(logic.ACTION_LINK, logic.MATCH_BY_ELIMINATION, "X")]


class TestRaceGuards:
    def test_an_invite_newer_than_the_roster_is_skipped(self):
        """Recorded while the roster read was in flight, its baseline is newer
        than the data and would make earlier members look like arrivals."""
        late = invite(1, "Bob", baseline_at=T0 + 100)
        result = match([late], roster(X="Bob"), fetched=T0 + 50)
        assert result['actions'] == [] and result['unmatched'] == []


class TestPlanInviteLink:
    def test_a_different_account_is_a_conflict(self):
        """A rejoin under a new Discord account: the bot never deletes, so a
        moderator runs !relink."""
        assert logic.plan_invite_link([{'d_id': 999}], "111") == ('conflict', ["999"])

    def test_same_account_duplicates_are_kept(self):
        """plan_relink would delete the duplicate, so treating 'would delete'
        as a conflict flagged every returning member who once had !assign run
        twice -- four characters in the live data."""
        rows = [{'d_id': 801512675003727894}, {'d_id': "801512675003727894"}]
        assert logic.plan_invite_link(rows, "801512675003727894") == ('keep', [])

    def test_junk_rows_alone_mean_insert(self):
        assert logic.plan_invite_link([{'d_id': None}, {'d_id': ''}], "111") == ('insert', [])

    def test_no_rows_means_insert(self):
        assert logic.plan_invite_link([], "111") == ('insert', [])

    def test_an_unusable_target_is_refused(self):
        with pytest.raises(ValueError):
            logic.plan_invite_link([], None)


class TestRosterHelpers:
    ROWS = [{'G_ID': "A", 'G_NAME': "Alpha", 'GP': 10}, {'G_ID': "B", 'G_NAME': "Bravo", 'GP': 20}]

    def test_a_gp_only_change_is_not_a_change(self):
        """GP ticks constantly; comparing it would rewrite the table on nearly
        every poll to store the same people."""
        assert not logic.roster_changed(self.ROWS, [("A", "Alpha"), ("B", "Bravo")])

    @pytest.mark.parametrize("stored", [
        [("A", "Alpha")],                                  # someone joined
        [("A", "Alpha"), ("B", "Bravo"), ("C", "Cee")],     # someone left
        [("A", "Alpha"), ("B", "OldName")],                 # a rename
    ])
    def test_membership_and_names_are(self, stored):
        assert logic.roster_changed(self.ROWS, stored)

    def test_a_good_roster_passes(self):
        assert logic.validate_roster_rows(self.ROWS, 2) is None

    @pytest.mark.parametrize("rows", [
        [],
        [{'G_ID': "", 'G_NAME': "x", 'GP': 1}],
        [{'G_ID': None, 'G_NAME': "x", 'GP': 1}],
        [{'G_ID': "A", 'G_NAME': " ", 'GP': 1}],
        [{'G_ID': "A", 'G_NAME': "x", 'GP': "1"}],
        [{'G_ID': "A", 'G_NAME': "x", 'GP': True}],
    ])
    def test_malformed_rosters_are_refused(self, rows):
        """A NULL game id reaching the GP tables would make GP_databases'
        NOT IN match nothing, silently ending new-member tracking."""
        assert logic.validate_roster_rows(rows, 0) is not None

    def test_a_roster_that_halved_is_refused(self):
        rows = [{'G_ID': str(n), 'G_NAME': f"n{n}", 'GP': 1} for n in range(99)]
        assert "shrank" in logic.validate_roster_rows(rows, 210)


class TestWelcome:
    def test_every_guild_has_a_template(self):
        assert set(logic.WELCOME_MESSAGES) == set(logic.GUILD_NAMES)

    @pytest.mark.parametrize("guild", logic.GUILD_NAMES)
    def test_placeholders_are_filled(self, guild):
        text = logic.format_welcome(guild, 123, 851497204099448912, 815059283040403478)
        assert text.startswith("<@123> ")
        assert "<#851497204099448912>" in text and "<#815059283040403478>" in text
        assert "{" not in text and "}" not in text
        assert len(text) < 2000

    def test_a_literal_brace_in_the_text_cannot_break_it(self, monkeypatch):
        """Filled with str.replace, not str.format, so an edit adding a brace
        can't turn every welcome into a KeyError."""
        monkeypatch.setitem(logic.WELCOME_MESSAGES, GUILD, "{member} hi {not a placeholder} :}")
        assert logic.format_welcome(GUILD, 1, 2, 3) == "<@1> hi {not a placeholder} :}"


class TestInviteMessages:
    def test_assign_suggestion_uses_the_numeric_id(self):
        """!assign parses int(user) first; a mention would fall through to a
        display-name lookup."""
        assert logic.suggest_assign_command(GUILD, "<@111>", "Bob") == "!assign Aetherians 111 Bob"

    def test_assign_suggestion_without_a_member_shows_a_placeholder(self):
        assert "<discord id>" in logic.suggest_assign_command(GUILD, None, "Bob")

    def test_an_unmatched_report_offers_a_command_per_invite(self):
        arrival = {'g_id': "X", 'name': "Zed", 'linked_to': [],
                   'invites': [invite(1, "Bob"), invite(2, "Carl", d_id="222")]}
        text = logic.format_unmatched_arrival(GUILD, arrival)
        assert "!assign Aetherians 111 Zed" in text and "!assign Aetherians 222 Zed" in text

    def test_a_conflict_says_the_role_and_welcome_are_then_manual(self):
        """A conflict never resumes, so after the suggested !relink nothing
        else happens automatically -- the report has to say so."""
        text = logic.format_invite_conflict(GUILD, invite(1, "Bob"), "Bob", "X", ["999"])
        assert "!relink X 111" in text and "by hand" in text

    def test_an_elimination_link_says_so(self):
        text = logic.format_invite_linked(GUILD, invite(1, "Bob"), "BobAlt", "X",
                                          logic.MATCH_BY_ELIMINATION, True, False, None)
        assert "elimination" in text and "!relink" in text

    def test_a_skipped_welcome_is_described_honestly(self):
        """Either a mod already did it by hand, or a crash landed between the
        role and the welcome -- the wording has to be true for both."""
        text = logic.format_invite_linked(GUILD, invite(1, "Bob"), "Bob", "X",
                                          logic.MATCH_BY_NAME, False, False, None)
        assert "welcome may not have been sent" in text

    def test_the_ack_names_a_replaced_invite(self):
        record = {'outcome': 'new', 'd_id': "111", 'previous_d_id': None, 'superseded': ["Bbo"]}
        text = logic.format_invite_ack(GUILD, "Bob", "@Bob", record, True, False)
        assert "Bbo" in text and "report" in text

    def test_the_ack_names_a_changed_member(self):
        record = {'outcome': 'merged', 'd_id': "222", 'previous_d_id': "111", 'superseded': []}
        text = logic.format_invite_ack(GUILD, "Bob", "@Carl", record, True, False)
        assert "@Carl" in text and "<@111>" in text

    def test_an_unconfirmed_send_says_so(self):
        record = {'outcome': 'new', 'd_id': "111", 'previous_d_id': None, 'superseded': []}
        text = logic.format_invite_ack(GUILD, "Bob", "@Bob", record, False, False)
        assert "may or may not" in text

    def test_the_invites_list_is_capped(self):
        many = [invite(n, f"name{n}", sent=T0 + n) for n in range(40)]
        text = logic.format_invites_list(many, T0 + 100)
        assert "newest 15 of 40" in text and len(text) < 2000

    def test_uninviting_a_linked_invite_points_at_relink(self):
        linked = invite(1, "Bob", status=logic.INVITE_LINKED)
        linked.update(matched_name="Bob", matched_g_id="X")
        assert "!relink" in logic.format_uninvite(GUILD, linked)


class TestConflictReportUnlinkedSection:
    def test_unlinked_characters_alone_make_a_report(self):
        text = logic.format_conflicts([], [], [("KingBob531", "X")])
        assert "KingBob531" in text

    def test_nothing_at_all_is_still_none(self):
        """The weekly job's silence depends on this."""
        assert logic.format_conflicts([], [], []) is None

    def test_unlinked_characters_come_from_usable_links(self):
        live = {"X": ("Linked", 1), "Y": ("NoAccount", 1), "Z": ("Unassigned", 1)}
        links = [{'g_id': "X", 'd_id': "1"}, {'g_id': "Y", 'd_id': None}]
        assert logic.unlinked_characters(links, live) == [("NoAccount", "Y"), ("Unassigned", "Z")]

    def test_all_three_sections_together_fit_an_embed(self):
        """A bad week -- conflicts, alts and unlinked characters at once --
        shares one budget, so it can't exceed Discord's limit."""
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
        unlinked = [(f"unassigned_character_{n}", f"U{n:025d}") for n in range(60)]
        text = logic.format_conflicts(characters, accounts, unlinked)
        assert characters and accounts
        assert len(text) < logic.EMBED_DESCRIPTION_LIMIT
