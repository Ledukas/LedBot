"""Tests for Functions.get_date() -- pure given "today", controlled here via freezegun."""

import asyncio

import pytest
from freezegun import freeze_time

import Functions


def run(coro):
    return asyncio.run(coro)


class TestGetDateResolvesMostRecentSaturday:
    @pytest.mark.parametrize("frozen_date,expected_saturday", [
        ("2026-08-17", "2026_08_15"),  # Monday
        ("2026-08-18", "2026_08_15"),  # Tuesday
        ("2026-08-19", "2026_08_15"),  # Wednesday
        ("2026-08-20", "2026_08_15"),  # Thursday
        ("2026-08-21", "2026_08_15"),  # Friday
        ("2026-08-22", "2026_08_22"),  # Saturday -- today itself, not a prior one
        ("2026-08-23", "2026_08_22"),  # Sunday
    ])
    def test_all_weekdays(self, frozen_date, expected_saturday):
        with freeze_time(frozen_date):
            result = run(Functions.get_date())
        assert result["column_name1"] == f"GP{expected_saturday}"


class TestGetDateYearBoundary:
    def test_three_and_two_weeks_ago_land_in_prior_year(self):
        # 2027-01-02 is a Saturday; 3/2 weeks back fall in December 2026.
        with freeze_time("2027-01-02"):
            result = run(Functions.get_date())

        assert result["column_name1"] == "GP2027_01_02"
        assert result["column_name2"] == "GP2026_12_26"
        assert result["column_name3"] == "GP2026_12_19"
        assert result["column_name4"] == "GP2026_12_12"

    def test_column_names_ordered_oldest_to_newest(self):
        with freeze_time("2027-01-02"):
            result = run(Functions.get_date())

        assert result["column_names"] == [
            "Name", "G_ID", "GP2026_12_12", "GP2026_12_19", "GP2026_12_26", "GP2027_01_02",
        ]
        assert result["column_names_int"] == [
            "GP2026_12_12", "GP2026_12_19", "GP2026_12_26", "GP2027_01_02",
        ]


class TestGetDateReturnShape:
    def test_has_exactly_the_expected_keys(self):
        with freeze_time("2026-08-22"):
            result = run(Functions.get_date())
        assert set(result.keys()) == {
            "column_name1", "column_name2", "column_name3", "column_name4",
            "column_names", "column_names_int",
        }
