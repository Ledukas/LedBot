"""Tests for the logic.py functions extracted from LedBotCode.py."""

from datetime import datetime
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


class TestPickLinkedCharacter:
    def test_no_rows(self):
        assert logic.pick_linked_character([]) is None

    def test_a_departed_first_row_does_not_win_over_a_live_one(self):
        """!kick and !mygains took the account's first row, which for a member
        on their second character is one that left the guild."""
        rows = [('old', 'OldName', 0), ('new', 'NewName', 1)]
        assert logic.pick_linked_character(rows) == ('new', 'NewName')

    def test_first_live_row_wins_among_several(self):
        rows = [('gone', 'Gone', 0), ('a', 'A', 1), ('b', 'B', 1)]
        assert logic.pick_linked_character(rows) == ('a', 'A')

    def test_falls_back_to_the_oldest_row_when_none_is_live(self):
        rows = [('old', 'Old', 0), ('older', 'Older', 0)]
        assert logic.pick_linked_character(rows) == ('old', 'Old')

    def test_blank_rows_are_skipped(self):
        assert logic.pick_linked_character([('', '', 0), (None, None, 0)]) is None
        assert logic.pick_linked_character([('', '', 0), ('g1', 'A', 0)]) == ('g1', 'A')


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


class TestBabaPingDue:
    def test_only_in_the_ping_minute(self):
        assert logic.baba_ping_due(datetime(2026, 10, 3, 14, 57, 5), None)
        assert not logic.baba_ping_due(datetime(2026, 10, 3, 14, 56, 59), None)
        assert not logic.baba_ping_due(datetime(2026, 10, 3, 14, 58, 0), None)

    def test_a_second_tick_in_the_same_minute_does_not_ping_again(self):
        first = datetime(2026, 10, 3, 14, 57, 5)
        assert not logic.baba_ping_due(datetime(2026, 10, 3, 14, 57, 45), first)

    def test_pings_again_the_next_hour(self):
        first = datetime(2026, 10, 3, 14, 57, 5)
        assert logic.baba_ping_due(datetime(2026, 10, 3, 15, 57, 25), first)


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
    """From 2 AM Saturday until the snapshot is taken, retrying every 10
    minutes until 4 AM and every 30 until midnight -- timed from the last
    attempt's start, which comes from the database."""

    from datetime import datetime as _dt, timedelta as _td

    SATURDAY_2AM = _dt(2026, 9, 12, 2, 0)

    def test_fires_on_saturday_at_two(self):
        assert logic.should_run_weekly_gp(self.SATURDAY_2AM, False, None) is True

    def test_not_before_two(self):
        assert logic.should_run_weekly_gp(self.SATURDAY_2AM.replace(hour=1, minute=59), False, None) is False

    def test_a_bot_that_was_down_at_two_runs_when_it_comes_back(self):
        assert logic.should_run_weekly_gp(self.SATURDAY_2AM.replace(hour=11), False, None) is True

    def test_does_not_fire_on_other_days(self):
        for offset in range(1, 7):
            day = self.SATURDAY_2AM + self._td(days=offset)
            assert logic.should_run_weekly_gp(day, False, None) is False, day

    def test_a_taken_snapshot_never_reruns(self):
        """The duplicate weekly report this loop was rewritten to prevent."""
        for hour in range(2, 24):
            when = self.SATURDAY_2AM.replace(hour=hour)
            assert logic.should_run_weekly_gp(when, True, self.SATURDAY_2AM) is False, hour

    def test_retries_every_ten_minutes_until_four(self):
        last = self.SATURDAY_2AM
        assert logic.should_run_weekly_gp(last + self._td(minutes=5), False, last) is False
        assert logic.should_run_weekly_gp(last + self._td(minutes=10), False, last) is True

    def test_then_every_thirty_minutes(self):
        last = self.SATURDAY_2AM.replace(hour=5)
        assert logic.should_run_weekly_gp(last + self._td(minutes=20), False, last) is False
        assert logic.should_run_weekly_gp(last + self._td(minutes=30), False, last) is True

    def test_stops_at_midnight(self):
        last = self.SATURDAY_2AM.replace(hour=23, minute=40)
        assert logic.should_run_weekly_gp(last + self._td(minutes=30), False, last) is False

    def test_a_start_time_from_a_wrong_clock_does_not_block_retries(self):
        """The Pi has no clock battery; a marker written hours in the future
        must not hold the retries off for the rest of the day."""
        future = self.SATURDAY_2AM.replace(hour=9)
        assert logic.should_run_weekly_gp(self.SATURDAY_2AM.replace(hour=3), False, future) is True


class TestWeeklyRetrySchedule:
    from datetime import datetime as _dt

    def test_ten_minutes_then_thirty_then_stop(self):
        assert logic.weekly_retry_interval(0) == 600
        assert logic.weekly_retry_interval(2 * 3600 - 1) == 600
        assert logic.weekly_retry_interval(2 * 3600) == 1800
        assert logic.weekly_retry_interval(22 * 3600 - 1) == 1800
        assert logic.weekly_retry_interval(22 * 3600) is None

    def test_the_schedule_ends_at_midnight(self):
        assert logic.WEEKLY_GP_HOUR * 3600 + logic.WEEKLY_RETRY_SCHEDULE[-1][0] == 24 * 3600

    def test_the_window_opens_at_two_on_the_named_saturday(self):
        assert logic.weekly_window_opens("GP2026_09_12") == self._dt(2026, 9, 12, 2)


class TestSnapshotReminderDue:
    from datetime import datetime as _dt, timedelta as _td

    RUN_KEY = "GP2026_09_12"

    def test_not_while_saturday_retries_are_still_going(self):
        assert logic.snapshot_reminder_due(self._dt(2026, 9, 12, 23, 55), self.RUN_KEY, False, None) is False

    def test_from_sunday_midnight(self):
        assert logic.snapshot_reminder_due(self._dt(2026, 9, 13, 0, 0), self.RUN_KEY, False, None) is True

    def test_then_once_a_day(self):
        last = self._dt(2026, 9, 13, 0, 5)
        assert logic.snapshot_reminder_due(last + self._td(hours=23), self.RUN_KEY, False, last) is False
        assert logic.snapshot_reminder_due(last + self._td(hours=24), self.RUN_KEY, False, last) is True

    def test_never_once_taken(self):
        assert logic.snapshot_reminder_due(self._dt(2026, 9, 15), self.RUN_KEY, True, None) is False
