"""Tests for the {guild}_members link logic extracted into logic.py.

Most of these encode a fact about the live database that took measuring to
find, and that a plausible-looking change would quietly break: that `index` is
not a key, that 1200-odd rows are inert history rather than problems, that a
name stored at assign time goes stale, and that the same Discord id appears in
two storage classes. The docstrings carry the numbers so a future reader does
not have to re-derive them.
"""

import pytest

import logic


class TestNormalizeDiscordId:
    @pytest.mark.parametrize("value", [123, "123", " 123 ", "<@123>", "<@!123>"])
    def test_everything_a_moderator_can_type(self, value):
        assert logic.normalize_discord_id(value) == "123"

    @pytest.mark.parametrize("value", [None, "", "   ", "Bob", "12a", True, False])
    def test_junk_is_none(self, value):
        """The blank rows in _members land here -- one all-'' row in Aetherians,
        one all-'' and three all-NULL in Pretherians. They have to be dropped
        before any grouping, or they match each other and invent a member."""
        assert logic.normalize_discord_id(value) is None

    def test_int_and_str_agree(self):
        """The reason this exists. _members.D_ID is declared INTEGER so plain
        equality already works through affinity, but nothing applies affinity
        when GROUPing, and one integer row and one text row would otherwise be
        counted as two different accounts on the same character."""
        assert logic.normalize_discord_id(801512675003727894) == \
               logic.normalize_discord_id("801512675003727894")


class TestNormalizeGameId:
    @pytest.mark.parametrize("value", [
        "gV21OHOTjweE0000000000000000",      # 28-char Firebase uid
        "steam_76561198007143937",           # 23-char Steam form
    ])
    def test_both_live_formats_pass_through(self, value):
        """Both are legitimate, which is why the search never tries to work out
        what kind of thing a term is from its shape."""
        assert logic.normalize_game_id(value) == value

    @pytest.mark.parametrize("value", [None, "", "  "])
    def test_absent_is_none(self, value):
        assert logic.normalize_game_id(value) is None


class TestLikeTerm:
    def test_underscore_is_escaped(self):
        """26 Aetherian and 31 Pretherian live game names contain _ or %
        (Sire_Vhal, Thomas_The_3rd). Unescaped, _ is a single-character
        wildcard and the search silently matches the wrong people."""
        assert logic.like_term("Sire_Vhal") == r"%Sire\_Vhal%"

    def test_percent_is_escaped(self):
        assert logic.like_term("50%") == r"%50\%%"

    def test_backslash_is_escaped_first(self):
        assert logic.like_term("a\\b") == "%a\\\\b%"

    def test_plain_term_is_wrapped(self):
        assert logic.like_term("zoro") == "%zoro%"


class TestIsSearchableTerm:
    def test_short_terms_are_rejected(self):
        """'a' matches 1408 of the ~1960 link rows -- neither useful nor
        sendable."""
        assert not logic.is_searchable_term("a")
        assert not logic.is_searchable_term("ab")

    def test_three_characters_is_enough(self):
        assert logic.is_searchable_term("led")


def link(rowid, d_id, g_id, g_name="Someone", display="disp", discord="disp#0"):
    return {
        'rowid': rowid, 'd_id': d_id, 'g_id': g_id,
        'g_name': g_name, 'display': display, 'discord': discord,
    }


class TestFindLinkConflicts:
    def test_the_one_real_conflict_in_the_database(self):
        """Coantic, Pretherians, rowids 973/974: two rows, one account. The
        harmless kind -- a repeated !assign, not a rejoin. If this ever comes
        back classified `split`, the severity logic is wrong."""
        rows = [link(973, 801512675003727894, "X", "Coantic"),
                link(974, "801512675003727894", "X", "Coantic")]
        characters, accounts = logic.find_link_conflicts(rows, {"X": ("Coantic", 500)})
        assert len(characters) == 1
        assert characters[0]['kind'] == logic.CONFLICT_DUPLICATE
        assert characters[0]['accounts'] == [("801512675003727894", 2)]
        assert accounts == []

    def test_two_accounts_on_one_character_is_the_harmful_kind(self):
        """GP_roles joins _game to _members unqualified, so it hands the rank
        role out once per row -- a departed account gets a role computed from
        somebody else's GP."""
        rows = [link(1, 111, "X"), link(2, 222, "X")]
        characters, _ = logic.find_link_conflicts(rows, {"X": ("Someone", 1)})
        assert characters[0]['kind'] == logic.CONFLICT_SPLIT
        assert len(characters[0]['accounts']) == 2

    def test_harmful_sorts_before_harmless(self):
        rows = [link(1, 111, "A", "Alpha"), link(2, 111, "A", "Alpha"),
                link(3, 111, "B", "Bravo"), link(4, 222, "B", "Bravo")]
        characters, _ = logic.find_link_conflicts(
            rows, {"A": ("Alpha", 1), "B": ("Bravo", 1)}
        )
        assert [c['kind'] for c in characters] == \
               [logic.CONFLICT_SPLIT, logic.CONFLICT_DUPLICATE]

    def test_one_account_on_two_live_characters(self):
        rows = [link(1, 111, "A", "Alpha"), link(2, 111, "B", "Bravo")]
        _, accounts = logic.find_link_conflicts(
            rows, {"A": ("Alpha", 1), "B": ("Bravo", 1)}
        )
        assert accounts == [{'d_id': "111", 'characters': ["Alpha", "Bravo"]}]

    def test_blank_rows_are_dropped(self):
        """The five real ones: all-'' in both guilds, plus three all-NULL in
        Pretherians. Left in, they share a key and become a phantom member."""
        rows = [link(1, "", "", ""), link(2, None, None, None), link(3, 111, "")]
        characters, accounts = logic.find_link_conflicts(rows, {"": ("", 0)})
        assert (characters, accounts) == ([], [])

    def test_departed_characters_are_not_conflicts(self):
        """733 Aetherian and 821 Pretherian rows are in this state. They are the
        only record of who was once linked to what, and GP_roles joins FROM
        _game so they are never matched."""
        rows = [link(1, 111, "GONE"), link(2, 222, "GONE"), link(3, 333, "GONE")]
        characters, accounts = logic.find_link_conflicts(rows, {"LIVE": ("Here", 1)})
        assert (characters, accounts) == ([], [])

    def test_mixed_storage_is_one_account_not_two(self):
        rows = [link(1, 123, "X"), link(2, "123", "X")]
        characters, accounts = logic.find_link_conflicts(rows, {"X": ("Someone", 1)})
        assert characters[0]['kind'] == logic.CONFLICT_DUPLICATE
        assert accounts == []

    def test_a_single_row_is_never_a_conflict(self):
        characters, accounts = logic.find_link_conflicts(
            [link(1, 111, "X")], {"X": ("Someone", 1)}
        )
        assert (characters, accounts) == ([], [])


class TestFormatConflicts:
    def test_nothing_to_report_is_none(self):
        """The weekly job's silence depends on this exactly: None means send
        nothing, and only the manual command turns that into a line."""
        assert logic.format_conflicts([], []) is None

    def test_a_split_names_both_accounts(self):
        rows = [link(1, 111, "X"), link(2, 222, "X")]
        characters, accounts = logic.find_link_conflicts(rows, {"X": ("Someone", 1)})
        report = logic.format_conflicts(characters, accounts)
        assert "111" in report and "222" in report
        assert "!relink" in report

    def test_fits_in_an_embed(self):
        rows = []
        for n in range(30):
            rows += [link(n * 2, 1000 + n, f"G{n}"), link(n * 2 + 1, 2000 + n, f"G{n}")]
        live = {f"G{n}": (f"character_number_{n}", 1) for n in range(30)}
        characters, accounts = logic.find_link_conflicts(rows, live)
        report = logic.format_conflicts(characters, accounts)
        assert len(report) < logic.EMBED_DESCRIPTION_LIMIT

    def test_fits_with_both_kinds_at_once(self):
        """The case the single-block test above misses. Sizing each block
        against the limit separately lets the assembled message reach twice it:
        this shape measured 2208 characters before the budget was shared."""
        rows = []
        for n in range(40):
            rows += [
                link(n * 3, 1000 + n, f"G{n}", f"c{n}"),
                link(n * 3 + 1, 2000 + n, f"G{n}", f"c{n}"),   # -> a split
                link(n * 3 + 2, 1000 + n, f"H{n}", f"h{n}"),   # -> an account on two
            ]
        live = {}
        for n in range(40):
            live[f"G{n}"] = (f"character_number_{n}", 1)
            live[f"H{n}"] = (f"alt_character_{n}", 1)
        characters, accounts = logic.find_link_conflicts(rows, live)
        assert characters and accounts, "this fixture must produce both kinds"
        report = logic.format_conflicts(characters, accounts)
        assert len(report) < logic.EMBED_DESCRIPTION_LIMIT


class TestPlanRelink:
    def test_the_coantic_case_deletes_one_row_and_inserts_nothing(self):
        """Keeping the lower rowid preserves the original link and its place in
        the append-only table; deleting and re-inserting would rewrite a row
        that was already correct."""
        rows = [link(973, 801512675003727894, "X"), link(974, "801512675003727894", "X")]
        delete, keep, insert = logic.plan_relink(rows, "801512675003727894")
        assert (delete, keep, insert) == ([974], 973, False)

    def test_all_wrong_rows_are_replaced(self):
        rows = [link(5, 111, "X"), link(6, 222, "X")]
        delete, keep, insert = logic.plan_relink(rows, "333")
        assert (sorted(delete), keep, insert) == ([5, 6], None, True)

    def test_keeps_the_correct_row_among_wrong_ones(self):
        rows = [link(1, 999, "X"), link(2, 111, "X"), link(3, 888, "X")]
        delete, keep, insert = logic.plan_relink(rows, "111")
        assert (sorted(delete), keep, insert) == ([1, 3], 2, False)

    def test_no_existing_rows_just_inserts(self):
        assert logic.plan_relink([], "111") == ([], None, True)

    @pytest.mark.parametrize("target", [None, "", "not-an-id"])
    def test_an_unusable_target_is_refused(self, target):
        """Without this, an unusable target normalizes to None and matches the
        blank rows -- so the junk row is kept as though correct and every real
        link is deleted with nothing inserted to replace it."""
        rows = [link(1, "", "X"), link(2, 111, "X")]
        with pytest.raises(ValueError):
            logic.plan_relink(rows, target)

    def test_delete_targets_are_rowids_not_positions(self):
        """`index` looks like a key and is not one: assign never sets it, so it
        is NULL on 745 of 939 Aetherian rows and both rows of the live
        duplicate have index = NULL. Deleting on a position or on `index` would
        hit unrelated rows."""
        rows = [link(973, 111, "X"), link(974, 222, "X")]
        delete, _, _ = logic.plan_relink(rows, "111")
        assert delete == [974]
        assert 0 not in delete and 1 not in delete


class TestAssignBlockMessage:
    def test_unlinked_character_proceeds(self):
        assert logic.assign_block_message("Aetherians", "Bob", []) is None

    def test_same_account_is_a_no_op(self):
        message = logic.assign_block_message("Aetherians", "Bob", [(111, "Bobby")], 111)
        assert "already linked" in message
        assert "!relink" not in message

    def test_different_account_points_at_relink(self):
        message = logic.assign_block_message("Aetherians", "Bob", [(111, "Bobby")], 222)
        assert "!relink" in message
        assert "Bobby" in message and "111" in message

    def test_names_every_existing_link(self):
        message = logic.assign_block_message(
            "Aetherians", "Bob", [(111, "One"), (222, "Two")], 333
        )
        assert "One" in message and "Two" in message


class TestWhoisCharacters:
    def test_a_renamed_character_is_flagged_and_shown_by_its_current_name(self):
        """60 of the ~406 live link rows carry a name the character no longer
        uses (King_Vhal -> Sire_Vhal). Reporting the stored name would send a
        moderator looking for somebody who is not in the guild list."""
        rows = [link(1, 111, "X", g_name="King_Vhal")]
        records = logic.whois_characters(
            rows, {"X": ("Sire_Vhal", 900)}, {}, {}, {}
        )
        assert records[0]['name'] == "Sire_Vhal"
        assert records[0]['links'][0]['stale_name'] is True

    def test_cross_check_surfaces_a_second_character_on_the_same_account(self):
        """The whole point of the command: the conflict is found without the
        moderator running a second search."""
        rows = [link(1, 111, "A", "Alpha"), link(2, 111, "B", "Bravo")]
        records = logic.whois_characters(
            rows, {"A": ("Alpha", 1), "B": ("Bravo", 2)}, {}, {}, {}
        )
        alpha = next(r for r in records if r['name'] == "Alpha")
        assert alpha['links'][0]['also_linked'] == ["Bravo"]

    def test_a_departed_character_is_reported_not_dropped(self):
        records = logic.whois_characters([link(1, 111, "GONE")], {}, {}, {}, {})
        assert records[0]['in_game'] is False

    def test_role_membership_unknown_stays_unknown(self):
        """Absent from the Discord cache is not the same as absent from the
        server, and the report must not turn one into the other."""
        records = logic.whois_characters([link(1, 111, "X")], {"X": ("A", 1)}, {}, {}, {})
        assert records[0]['links'][0]['in_discord'] is None


class TestWhoisUnlinked:
    def test_an_unassigned_in_game_character_is_an_answer(self):
        """3 Aetherian and 5 Pretherian characters are in this state. 'In game,
        never assigned' is the most common thing a mod is actually asking."""
        characters, accounts = logic.whois_unlinked(
            {"X": ("KingBob531", 10)}, {}, set(), set()
        )
        assert characters == [("KingBob531", "X")]
        assert accounts == []

    def test_a_role_holder_with_no_link_is_an_answer(self):
        _, accounts = logic.whois_unlinked({}, {"111": "Someone"}, set(), set())
        assert accounts == [("Someone", "111")]

    def test_linked_ones_are_not_reported_as_unlinked(self):
        characters, accounts = logic.whois_unlinked(
            {"X": ("A", 1)}, {"111": "B"}, {"X"}, {"111"}
        )
        assert (characters, accounts) == ([], [])


class TestFormatWhois:
    def test_no_match_in_this_guild_is_none(self):
        """So the caller can say 'nothing anywhere' once for the term, rather
        than once per guild."""
        assert logic.format_whois("Aetherians", [], [], []) is None

    def test_fits_in_a_discord_message_at_the_cap(self):
        records = logic.whois_characters(
            [link(n, 1000 + n, f"G{n}", f"character_number_{n}")
             for n in range(logic.WHOIS_MAX_CHARACTERS)],
            {f"G{n}": (f"character_number_{n}", 99999) for n in range(logic.WHOIS_MAX_CHARACTERS)},
            {}, {},
            {f"G{n}": [1234, 5678, 9012, 3456] for n in range(logic.WHOIS_MAX_CHARACTERS)},
        )
        assert len(logic.format_whois("Aetherians", records, [], [])) < 2000

    def test_too_many_lists_names_instead_of_expanding(self):
        message = logic.format_whois_too_many("a", [f"name{n}" for n in range(40)])
        assert "Narrow the search" in message
        assert len(message) < 2000


class TestResolveRelinkTarget:
    LIVE = {
        ("Aetherians", "AAA"): ("Alpha", 100),
        ("Pretherians", "BBB"): ("Bravo", 200),
    }

    def test_exact_game_id(self):
        assert logic.resolve_relink_target("AAA", self.LIVE, {}) == (("Aetherians", "AAA"), None)

    def test_current_name_case_insensitively(self):
        assert logic.resolve_relink_target("alpha", self.LIVE, {}) == (("Aetherians", "AAA"), None)

    def test_a_former_name_resolves_to_the_live_character(self):
        historical = {"king_vhal": [("Aetherians", "AAA")]}
        assert logic.resolve_relink_target("King_Vhal", self.LIVE, historical) == \
               (("Aetherians", "AAA"), None)

    def test_a_former_name_live_in_both_guilds_is_refused(self):
        """91 names appear in both guilds' history through promotion churn, 17
        of them live somewhere today. Guessing would silently repoint the wrong
        person's link."""
        historical = {"shared": [("Aetherians", "AAA"), ("Pretherians", "BBB")]}
        resolved, refusal = logic.resolve_relink_target("shared", self.LIVE, historical)
        assert resolved is None
        assert "Alpha" in refusal and "Bravo" in refusal

    def test_a_character_not_in_game_is_refused(self):
        resolved, refusal = logic.resolve_relink_target("nobody", self.LIVE, {})
        assert resolved is None
        assert "!members_game" in refusal

    def test_an_empty_term_is_refused(self):
        resolved, refusal = logic.resolve_relink_target("   ", self.LIVE, {})
        assert resolved is None and refusal


class TestFormatRelinkResult:
    def test_every_removed_row_is_printed_in_full(self):
        """After the commit this message is the only readable copy of those
        rows, so it prints the columns rather than a count."""
        removed = [link(974, "801512675003727894", "X", "Coantic", "birb", "birb#0")]
        report = logic.format_relink_result(
            "Pretherians", "Coantic", "X", removed, None,
            {'d_id': "999", 'display': "New"}, backup_name="snap.db",
        )
        assert "974" in report
        assert "801512675003727894" in report
        assert "birb#0" in report and "Coantic" in report
        assert "snap.db" in report

    def test_a_kept_row_is_reported_as_kept(self):
        report = logic.format_relink_result(
            "Pretherians", "Coantic", "X", [], link(973, 111, "X"), None,
        )
        assert "973" in report and "already correct" in report


class TestRelinkWarning:
    def test_no_other_characters_is_silent(self):
        assert logic.relink_warning("Aetherians", "Someone", []) is None

    def test_an_alt_is_flagged_as_a_note_not_an_error(self):
        """An alt may well be legitimate -- there are zero cases today -- but
        mygains and kick pick one of them arbitrarily."""
        warning = logic.relink_warning("Aetherians", "Someone", ["Bravo"])
        assert "Bravo" in warning and "arbitrarily" in warning
