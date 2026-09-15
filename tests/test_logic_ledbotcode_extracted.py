"""Tests for the logic.py functions extracted from LedBotCode.py."""

from types import SimpleNamespace

import pandas as pd
import pytest
from discord.ext import commands

import logic

# Mirrors the real GProles ordering in LedBotCode.py/Functions.py: descending,
# with the top tier appended out of order at the end.
GProles = [50000, 25000, 10000, 5000, 2500, 1000, 100000]


class TestComputeRemainingToRankup:
    def test_below_first_threshold(self):
        """Bug this extraction fixed: used to silently return 0 instead of
        the real gap to the first rank."""
        assert logic.compute_remaining_to_rankup(500, GProles) == 500

    def test_between_two_thresholds(self):
        assert logic.compute_remaining_to_rankup(1500, GProles) == 1000  # next is 2500

    def test_exactly_at_a_middle_threshold(self):
        assert logic.compute_remaining_to_rankup(1000, GProles) == 1500  # next is 2500

    def test_exactly_at_top_threshold(self):
        """Bug this extraction fixed: used to return 0 (implying '0 more
        needed') instead of the final-rank message."""
        assert logic.compute_remaining_to_rankup(100000, GProles) == "You have the final rank"

    def test_above_top_threshold(self):
        assert logic.compute_remaining_to_rankup(150000, GProles) == "You have the final rank"

    def test_zero_gp(self):
        assert logic.compute_remaining_to_rankup(0, GProles) == 1000


class TestFilterPersonalGains:
    def test_matching_g_id(self):
        df = pd.DataFrame({'G_ID': ['g1', 'g2'], 'Name': ['A', 'B'], 'GP': [10, 20]})
        result = logic.filter_personal_gains(df, 'g1')
        assert result['Name'].tolist() == ['A']
        assert 'G_ID' not in result.columns

    def test_no_matching_g_id(self):
        df = pd.DataFrame({'G_ID': ['g1'], 'Name': ['A'], 'GP': [10]})
        result = logic.filter_personal_gains(df, 'g999')
        assert result.empty


class TestFindMissing:
    def test_some_missing(self):
        assigned = pd.Series([1, 2, 3])
        actual = pd.Series([2, 3])
        assert logic.find_missing(assigned, actual).tolist() == [1]

    def test_none_missing(self):
        assigned = pd.Series([1, 2])
        actual = pd.Series([1, 2, 3])
        assert logic.find_missing(assigned, actual).empty

    def test_all_missing(self):
        assigned = pd.Series([1, 2])
        actual = pd.Series([], dtype=int)
        assert logic.find_missing(assigned, actual).tolist() == [1, 2]

    def test_empty_assigned(self):
        assigned = pd.Series([], dtype=int)
        actual = pd.Series([1, 2])
        assert logic.find_missing(assigned, actual).empty


class TestInterpretActionResult:
    def test_none_is_failure(self):
        assert logic.interpret_action_result(None, "ok", "fail") == "fail"

    @pytest.mark.parametrize("value", ["true", "True", "TRUE"])
    def test_true_case_insensitive_is_success(self, value):
        assert logic.interpret_action_result(value, "ok", "fail") == "ok"

    @pytest.mark.parametrize("value", ["false", "maybe", ""])
    def test_other_values_are_failure(self, value):
        assert logic.interpret_action_result(value, "ok", "fail") == "fail"


class TestAssignErrorMessage:
    def test_missing_required_argument(self):
        param = SimpleNamespace(displayed_name=None, name="arg")
        error = commands.MissingRequiredArgument(param)
        assert logic.assign_error_message(error) == (
            "Missing required argument. Please provide all the necessary parameters."
        )

    def test_command_invoke_error_includes_original(self):
        error = commands.CommandInvokeError(ValueError("boom"))
        assert logic.assign_error_message(error) == (
            "An error occurred while processing the command: boom"
        )

    def test_user_not_found(self):
        error = commands.UserNotFound("SomeName")
        assert logic.assign_error_message(error) == "User not found."

    def test_unhandled_type_falls_to_generic_message(self):
        assert logic.assign_error_message(ValueError("whatever")) == "Command error"


class TestCommandErrorMessage:
    def test_missing_role(self):
        error = commands.MissingRole("Moderator")
        assert logic.command_error_message(error) == (
            "You don't have the required permissions to use this command."
        )

    def test_missing_required_argument(self):
        param = SimpleNamespace(displayed_name=None, name="arg")
        error = commands.MissingRequiredArgument(param)
        assert logic.command_error_message(error) == "Missing an argument!"

    def test_bot_missing_permissions(self):
        error = commands.BotMissingPermissions(["manage_roles"])
        assert logic.command_error_message(error) == (
            "The bot doesn't have the required permissions to run this command."
        )

    def test_user_input_error(self):
        error = commands.UserInputError("bad input")
        assert logic.command_error_message(error) == "There was an error in the input."

    def test_command_on_cooldown_sends_nothing(self):
        error = commands.CommandOnCooldown(None, 5.0, None)
        assert logic.command_error_message(error) is None

    def test_missing_any_role(self):
        """has_any_role raises MissingAnyRole, which is NOT a subclass of
        MissingRole -- so wb-now/wb-next/gemdrop used to fall through to the
        silent default whenever an unauthorized user tried them."""
        error = commands.MissingAnyRole(["Aetherians", "Pretherians"])
        assert logic.command_error_message(error) == (
            "You don't have the required permissions to use this command."
        )

    def test_command_not_found_sends_nothing(self):
        """A mistyped command name must stay silent, otherwise the bot would
        reply to every stray message beginning with the prefix."""
        assert logic.command_error_message(commands.CommandNotFound()) is None

    def test_unhandled_exception_type_gets_generic_message(self):
        """Previously returned None, so any command without its own .error
        handler failed with no Discord reply and no log line at all."""
        assert (
            logic.command_error_message(ValueError("some unexpected error"))
            == logic.UNEXPECTED_ERROR_MESSAGE
        )

    def test_command_invoke_error_gets_generic_message(self):
        """The wrapper discord.py puts around anything raised inside a command
        body -- the single most common real failure, and previously silent."""
        error = commands.CommandInvokeError(TypeError("'NoneType' is not subscriptable"))
        assert logic.command_error_message(error) == logic.UNEXPECTED_ERROR_MESSAGE


class TestIsUnexpectedCommandError:
    @pytest.mark.parametrize("error", [
        commands.MissingRole("Moderator"),
        commands.MissingAnyRole(["Aetherians"]),
        commands.BotMissingPermissions(["manage_roles"]),
        commands.UserInputError("bad input"),
        commands.CommandOnCooldown(None, 5.0, None),
        commands.CommandNotFound(),
    ])
    def test_anticipated_errors_are_not_logged(self, error):
        assert logic.is_unexpected_command_error(error) is False

    @pytest.mark.parametrize("error", [
        ValueError("boom"),
        TypeError("'NoneType' object is not subscriptable"),
        IndexError("list index out of range"),
        commands.CommandInvokeError(RuntimeError("boom")),
    ])
    def test_real_bugs_are_logged(self, error):
        assert logic.is_unexpected_command_error(error) is True


class TestIsSaturday:
    @pytest.mark.parametrize("weekday,expected", [
        (0, False), (1, False), (2, False), (3, False),
        (4, False), (5, True), (6, False),
    ])
    def test_all_weekdays(self, weekday, expected):
        from datetime import datetime, timedelta
        # 2026-08-24 is a Monday (weekday 0); walk forward to hit each weekday.
        monday = datetime(2026, 8, 24)
        dt = monday + timedelta(days=weekday)
        assert dt.weekday() == weekday
        assert logic.is_saturday(dt) == expected


class TestShouldRunWeeklyGp:
    """The loop polls local time every 15 minutes, so these pin both halves:
    it fires in the 2 AM hour on a Saturday, and only once per Saturday."""

    from datetime import datetime as _dt

    SATURDAY_2AM = _dt(2026, 9, 12, 2, 0)

    def test_fires_on_saturday_at_two(self):
        assert logic.should_run_weekly_gp(self.SATURDAY_2AM, None) is True

    def test_does_not_fire_on_other_days(self):
        from datetime import timedelta
        for offset in range(1, 7):
            day = self.SATURDAY_2AM + timedelta(days=offset)
            assert logic.should_run_weekly_gp(day, None) is False, day

    def test_does_not_fire_at_other_hours(self):
        for hour in (0, 1, 3, 12, 21, 23):
            when = self.SATURDAY_2AM.replace(hour=hour)
            assert logic.should_run_weekly_gp(when, None) is False, hour

    def test_only_runs_once_per_saturday(self):
        """Every tick inside the 2 AM hour would otherwise re-run the job --
        the duplicate weekly report this loop was rewritten to prevent."""
        assert logic.should_run_weekly_gp(self.SATURDAY_2AM, None) is True
        already = self.SATURDAY_2AM.date()
        for minute in (0, 15, 30, 45):
            when = self.SATURDAY_2AM.replace(minute=minute)
            assert logic.should_run_weekly_gp(when, already) is False, minute

    def test_runs_again_the_following_saturday(self):
        from datetime import timedelta
        last_week = (self.SATURDAY_2AM - timedelta(days=7)).date()
        assert logic.should_run_weekly_gp(self.SATURDAY_2AM, last_week) is True
