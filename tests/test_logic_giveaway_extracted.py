"""Tests for the logic.py functions extracted from cogs/giveaway.py."""

from types import SimpleNamespace

import pytest

import logic


class TestResolveGiveawayArgs:
    def test_both_none_defaults_gp_to_zero(self):
        assert logic.resolve_giveaway_args(None, None) == (None, 0)

    def test_numeric_guild_name_reinterpreted_as_gp_required(self):
        assert logic.resolve_giveaway_args("123", None) == (None, 123)

    def test_guild_name_without_gp_required_is_rejected(self):
        """Used to return ("Aetherians", None), which the caller compared
        against as SQL NULL -- never true for any row -- so naming a guild
        silently disqualified every participant."""
        with pytest.raises(logic.GiveawayArgumentError) as excinfo:
            logic.resolve_giveaway_args("Aetherians", None)
        assert "Aetherians" in str(excinfo.value)

    def test_gp_required_given_guild_name_none(self):
        assert logic.resolve_giveaway_args(None, 50) == (None, 50)

    def test_both_given_unchanged(self):
        assert logic.resolve_giveaway_args("Aetherians", 50) == ("Aetherians", 50)

    def test_zero_is_an_explicit_no_minimum(self):
        """0 is a real answer, not a missing one -- it must not be confused
        with None and rejected."""
        assert logic.resolve_giveaway_args("Aetherians", 0) == ("Aetherians", 0)

    def test_empty_string_guild_name_normalized_to_no_guild(self):
        """"" is falsy, and downstream that already means "both guilds", so it
        is normalized to None here rather than passed on as a guild name that
        matches nothing."""
        assert logic.resolve_giveaway_args("", None) == (None, 0)


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
