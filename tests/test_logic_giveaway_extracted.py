"""Tests for the logic.py functions extracted from cogs/giveaway.py."""

from types import SimpleNamespace

import pytest

import logic


class TestResolveGiveawayArgs:
    def test_both_none_defaults_gp_to_zero(self):
        assert logic.resolve_giveaway_args(None, None) == (None, 0)

    def test_numeric_guild_name_reinterpreted_as_gp_required(self):
        assert logic.resolve_giveaway_args("123", None) == (None, 123)

    def test_real_guild_name_leaves_gp_required_none(self):
        """Note: this does NOT default gp_required to 0 -- matches the
        original behavior exactly, even though it means the caller's SQL GP
        filter ends up comparing against NULL in this case (a pre-existing
        quirk this extraction preserves rather than fixes)."""
        assert logic.resolve_giveaway_args("Aetherians", None) == ("Aetherians", None)

    def test_gp_required_given_guild_name_none(self):
        assert logic.resolve_giveaway_args(None, 50) == (None, 50)

    def test_both_given_unchanged(self):
        assert logic.resolve_giveaway_args("Aetherians", 50) == ("Aetherians", 50)

    def test_empty_string_guild_name_is_not_treated_as_none(self):
        """"" is falsy, so it skips the digit-reinterpretation branch, and it
        also isn't `None`, so the "default gp_required to 0" branch doesn't
        fire either -- documents this real edge case rather than assuming
        "" behaves like None."""
        assert logic.resolve_giveaway_args("", None) == ("", None)


class TestFilterEligibleParticipants:
    @staticmethod
    def participant(id_):
        return SimpleNamespace(id=id_)

    def test_some_already_won(self):
        participants = [self.participant(1), self.participant(2), self.participant(3)]
        result = logic.filter_eligible_participants(participants, winners=[2])
        assert [p.id for p in result] == [1, 3]

    def test_no_winners_yet(self):
        participants = [self.participant(1), self.participant(2)]
        result = logic.filter_eligible_participants(participants, winners=[])
        assert [p.id for p in result] == [1, 2]

    def test_all_already_won(self):
        participants = [self.participant(1), self.participant(2)]
        result = logic.filter_eligible_participants(participants, winners=[1, 2])
        assert result == []

    def test_empty_participants(self):
        assert logic.filter_eligible_participants([], winners=[1, 2]) == []
