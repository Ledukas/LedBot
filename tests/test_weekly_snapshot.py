"""The weekly snapshot surviving a missed Saturday, and the check after it.

GP_databases used to measure a week's gain against the column exactly seven
days back, so one missed Saturday raised `no such column` that week and every
week after it, each time leaving a half-added column behind. These pin the
replacement: the gain is measured against the latest *measured* week, missed
weeks are filled by spreading the difference evenly (totals exact), and the
whole snapshot is one transaction.
"""

import asyncio
import time
from datetime import date, datetime, timedelta

import pytest
from freezegun import freeze_time

import Functions
import logic
from scripts import gp_audit


# --- pure helpers -----------------------------------------------------------

class TestSnapshotColumns:
    def test_round_trip(self):
        assert logic.snapshot_column(date(2026, 10, 3)) == "GP2026_10_03"
        assert logic.snapshot_date("GP2026_10_03") == date(2026, 10, 3)

    @pytest.mark.parametrize("name", ["Name", "G_ID", "GP2026_13_40", "GP26_10_03"])
    def test_anything_else_is_not_a_snapshot(self, name):
        assert logic.snapshot_date(name) is None

    def test_sorted_by_date_ignoring_other_columns(self):
        names = ["Name", "GP2027_01_02", "G_ID", "GP2026_12_26"]
        assert logic.sorted_snapshot_columns(names) == ["GP2026_12_26", "GP2027_01_02"]


class TestMissedSnapshotColumns:
    def test_consecutive_weeks_miss_nothing(self):
        assert logic.missed_snapshot_columns("GP2026_09_26", "GP2026_10_03") == []

    def test_the_saturdays_in_between(self):
        assert logic.missed_snapshot_columns("GP2026_09_12", "GP2026_10_03") == [
            "GP2026_09_19", "GP2026_09_26",
        ]

    def test_across_a_year_boundary(self):
        assert logic.missed_snapshot_columns("GP2026_12_19", "GP2027_01_02") == ["GP2026_12_26"]

    def test_no_previous_snapshot(self):
        assert logic.missed_snapshot_columns(None, "GP2026_10_03") == []

    def test_an_odd_early_column_still_yields_saturdays(self):
        """Aetherians_GP starts with a Friday column, GP2023_03_31."""
        assert logic.missed_snapshot_columns("GP2023_03_31", "GP2023_04_15") == [
            "GP2023_04_01", "GP2023_04_08",
        ]


class TestSpreadGain:
    @pytest.mark.parametrize("total,weeks", [(1361, 2), (2041, 3), (0, 4), (-5, 2), (7, 1)])
    def test_sums_exactly(self, total, weeks):
        parts = logic.spread_gain(total, weeks)
        assert len(parts) == weeks and sum(parts) == total

    def test_the_remainder_goes_to_the_latest_weeks(self):
        assert logic.spread_gain(2042, 3) == [680, 681, 681]

    def test_one_week_is_the_whole_gain(self):
        assert logic.spread_gain(680, 1) == [680]


class TestEstimatedGainColumns:
    def test_missed_weeks_and_the_catch_up_week(self):
        gained = ["GP2026_09_19", "GP2026_09_26", "GP2026_10_03", "GP2026_10_10"]
        measured = ["GP2026_09_12", "GP2026_09_19", "GP2026_10_03", "GP2026_10_10"]
        assert logic.estimated_gain_columns(gained, measured) == {"GP2026_09_26", "GP2026_10_03"}

    def test_the_first_column_has_nothing_to_be_measured_against(self):
        assert logic.estimated_gain_columns(["GP2023_04_01"], ["GP2023_03_31", "GP2023_04_01"]) == {
            "GP2023_04_01",
        }


# --- the post-run check -----------------------------------------------------

def status(**overrides):
    base = {
        'guild': "Aetherians", 'column': "GP2026_10_03", 'previous': "GP2026_09_26", 'missed': [],
        'present': True, 'roster': 200, 'recorded': 200, 'zeros': 10, 'negatives': 0,
    }
    base.update(overrides)
    return base


class TestSnapshotProblems:
    def test_a_normal_week_is_fine(self):
        assert logic.snapshot_problems(status(), True) == []

    def test_members_with_nothing_recorded(self):
        assert logic.snapshot_problems(status(recorded=188), True) == [
            "12 of 200 in-game members have no GP recorded.",
        ]

    def test_half_on_zero_is_not_yet_a_stale_roster(self):
        assert logic.snapshot_problems(status(zeros=100), True) == []

    def test_mostly_zeros_suggests_a_retake_only_on_saturday(self):
        [saturday] = logic.snapshot_problems(status(zeros=101), True)
        [later] = logic.snapshot_problems(status(zeros=101), False)
        assert "may not have refreshed" in saturday and "!GP_weekly" in saturday
        assert "!GP_weekly" not in later

    def test_a_missing_column(self):
        assert logic.snapshot_problems(status(present=False, recorded=0), True) == [
            "This week's snapshot column is missing.",
        ]


class TestSnapshotNote:
    def test_both_guilds(self):
        statuses = [status(), status(guild="Pretherians", roster=204, recorded=204)]
        assert logic.format_snapshot_note(statuses) == (
            "Snapshot: Aetherians 200/200, Pretherians 204/204"
        )

    def test_negatives_are_noted_not_flagged(self):
        assert logic.format_snapshot_note([status(negatives=1)]) == (
            "Snapshot: Aetherians 200/200 (1 negative)"
        )

    def test_lateness(self):
        assert logic.format_snapshot_note([status()], timedelta(hours=7, minutes=5)).endswith(
            "-- taken 7h late"
        )
        assert logic.format_snapshot_note([status()], timedelta(days=4, hours=10)).endswith(
            "-- taken 4 days late"
        )

    def test_within_the_hour_is_not_late(self):
        """The loop ticks every 5 minutes, so the first attempt can land a
        little after 2 AM."""
        assert logic.snapshot_lateness("GP2026_10_03", datetime(2026, 10, 3, 2, 55)) is None
        assert logic.snapshot_lateness("GP2026_10_03", datetime(2026, 10, 3, 9, 0)) == timedelta(hours=7)


class TestEstimatedWeeksText:
    def test_nothing_missed(self):
        assert logic.format_estimated_weeks([status()]) is None

    def test_both_guilds_missing_the_same_week_is_one_sentence(self):
        statuses = [
            status(guild=guild, previous="GP2026_09_19", missed=["GP2026_09_26"])
            for guild in logic.GUILD_NAMES
        ]
        text = logic.format_estimated_weeks(statuses)
        assert text.startswith("No snapshot was taken for 9/26. The gain since 9/19 was spread evenly")
        assert "over 9/26 and 10/3" in text and "Aetherians" not in text

    def test_guilds_that_differ_are_named(self):
        statuses = [
            status(previous="GP2026_09_19", missed=["GP2026_09_26"]),
            status(guild="Pretherians", previous="GP2026_09_12", missed=["GP2026_09_19", "GP2026_09_26"]),
        ]
        lines = logic.format_estimated_weeks(statuses).split("\n")
        assert lines[0].startswith("Aetherians: ")
        assert lines[1].startswith("Pretherians: No snapshot was taken for 9/19 and 9/26.")
        assert "over 9/19, 9/26 and 10/3" in lines[1]


class TestFinishWeeklyReportWithANote:
    def test_the_note_joins_the_backup_in_the_footer(self):
        _, embeds = logic.finish_weekly_report(
            "**H**", [logic.report_embed("a", "x", 1)], "Database_2026.db", "Snapshot: A 1/1"
        )
        assert embeds[-1].footer.text == "Snapshot: A 1/1 · Backup: Database_2026.db"

    def test_a_quiet_week_carries_it_on_the_content_line(self):
        content, _ = logic.finish_weekly_report("**H**", [], "Database_2026.db", "Snapshot: A 1/1")
        assert content == "**H** -- nothing to report. Snapshot: A 1/1. Backup: `Database_2026.db`"


class TestMessages:
    def test_the_failure_message_states_the_cadence_that_applies(self):
        early = logic.format_weekly_failure("x", True, datetime(2026, 10, 3, 2, 0))
        late = logic.format_weekly_failure("x", True, datetime(2026, 10, 3, 11, 0))
        assert "every 10 minutes, then every 30 from 4 AM, until midnight" in early
        assert "every 30 minutes until midnight" in late

    def test_the_refusal_says_what_a_midweek_run_would_do(self):
        assert "overwrite Saturday's snapshot" in logic.format_gp_weekly_refused(True)
        assert "spreading the gain evenly" in logic.format_gp_weekly_refused(False)
        assert "`!GP_weekly force`" in logic.format_gp_weekly_refused(False)

    def test_the_reminder_never_suggests_a_midweek_run(self):
        for attempted, error, rejected in [(False, None, False), (True, "x", False), (True, None, True)]:
            text = logic.format_snapshot_reminder("GP2026_10_03", attempted, error, rejected)
            assert "!GP_weekly" not in text
            assert "next Saturday" in text


# --- GP_databases against a real database ----------------------------------

def seed(totals, roster, gained_columns=None):
    """Both guilds' tables, identically: {guild}_GP with `totals`
    ({column: {g_id: total}}), {guild}_GP_gained with the given columns, and
    {guild}_game from `roster` ({g_id: (name, gp)})."""
    columns = logic.sorted_snapshot_columns(totals)
    gained_columns = columns if gained_columns is None else gained_columns
    ids = sorted({g_id for week in totals.values() for g_id in week} | set(roster))
    db = Functions.conn
    for guild in logic.GUILD_NAMES:
        db.execute(f'CREATE TABLE {guild}_game ("index" INTEGER, G_NAME TEXT, G_ID TEXT, GP INTEGER)')
        db.executemany(
            f"INSERT INTO {guild}_game VALUES (?, ?, ?, ?)",
            [(i, name, g_id, gp) for i, (g_id, (name, gp)) in enumerate(roster.items())],
        )
        db.execute(f"CREATE TABLE {guild}_GP (Name TEXT, G_ID TEXT{''.join(f', {c} TEXT' for c in columns)})")
        db.executemany(
            f"INSERT INTO {guild}_GP VALUES ({', '.join('?' * (len(columns) + 2))})",
            [(g_id, g_id, *(str(totals[c][g_id]) if g_id in totals[c] else None for c in columns))
             for g_id in ids],
        )
        db.execute(
            f"CREATE TABLE {guild}_GP_gained (Name TEXT, G_ID TEXT{''.join(f', {c} TEXT' for c in gained_columns)})"
        )
        db.executemany(
            f"INSERT INTO {guild}_GP_gained VALUES ({', '.join('?' * (len(gained_columns) + 2))})",
            [(g_id, g_id, *('500' for _ in gained_columns)) for g_id in ids],
        )
    db.commit()


def snapshot(run_key):
    asyncio.run(Functions.GP_databases(run_key))


def gained(column, guild="Aetherians"):
    return {
        g_id: logic.parse_gp_value(value)
        for g_id, value in Functions.c.execute(f"SELECT G_ID, {column} FROM {guild}_GP_gained")
    }


def columns(table):
    return [row[1] for row in Functions.c.execute(f"PRAGMA table_info({table})")]


class TestGpDatabases:
    def test_a_normal_week(self, temp_functions_db):
        seed({"GP2026_09_26": {"A": 1000, "B": 2000}}, {"A": ("Alpha", 1680), "B": ("Beta", 2500)})
        snapshot("GP2026_10_03")
        assert gained("GP2026_10_03") == {"A": 680, "B": 500}

    def test_a_missed_week_is_spread_into_gp_gained_only(self, temp_functions_db):
        seed({"GP2026_09_19": {"A": 1000}}, {"A": ("Alpha", 2361)})
        snapshot("GP2026_10_03")
        assert (gained("GP2026_09_26")["A"], gained("GP2026_10_03")["A"]) == (680, 681)
        for guild in logic.GUILD_NAMES:
            assert "GP2026_09_26" not in columns(f"{guild}_GP")
            assert "GP2026_09_26" in columns(f"{guild}_GP_gained")

    def test_the_week_after_a_catch_up_is_normal(self, temp_functions_db):
        """The regression: the old code broke every week after a missed one."""
        seed({"GP2026_09_19": {"A": 1000}}, {"A": ("Alpha", 2361)})
        snapshot("GP2026_10_03")
        Functions.c.execute("UPDATE Aetherians_game SET GP = 3041")
        Functions.conn.commit()
        snapshot("GP2026_10_10")
        assert gained("GP2026_10_10")["A"] == 680

    def test_rerunning_a_catch_up_respreads_and_clears_a_leaver(self, temp_functions_db):
        seed({"GP2026_09_19": {"A": 1000, "B": 1000}}, {"A": ("Alpha", 2000), "B": ("Beta", 2000)})
        snapshot("GP2026_10_03")
        Functions.c.execute("DELETE FROM Aetherians_game WHERE G_ID = 'B'")
        Functions.c.execute("UPDATE Aetherians_game SET GP = 2400")
        Functions.conn.commit()
        snapshot("GP2026_10_03")
        assert (gained("GP2026_09_26")["A"], gained("GP2026_10_03")["A"]) == (700, 700)
        assert (gained("GP2026_09_26")["B"], gained("GP2026_10_03")["B"]) == (None, None)

    def test_someone_who_joined_during_the_gap_gets_lifetime_gp_once(self, temp_functions_db):
        """Left in one column, where observed_weeks drops a first week."""
        seed({"GP2026_09_19": {"A": 1000}}, {"A": ("Alpha", 2361), "C": ("Cee", 5000)})
        snapshot("GP2026_10_03")
        assert (gained("GP2026_09_26")["C"], gained("GP2026_10_03")["C"]) == (None, 5000)

    def test_the_very_first_snapshot(self, temp_functions_db):
        """Used to crash: there was no column seven days back to read."""
        seed({}, {"A": ("Alpha", 1500)})
        snapshot("GP2026_10_03")
        assert gained("GP2026_10_03") == {"A": 1500}

    def test_a_null_game_id_does_not_stop_new_members(self, temp_functions_db):
        """NOT IN against a column holding a NULL matches nothing at all."""
        seed({"GP2026_09_26": {"A": 1000}}, {"A": ("Alpha", 1680), "N": ("New", 300)})
        for guild in logic.GUILD_NAMES:
            Functions.c.execute(f"DELETE FROM {guild}_GP WHERE G_ID = 'N'")
            Functions.c.execute(f"DELETE FROM {guild}_GP_gained WHERE G_ID = 'N'")
            Functions.c.execute(f"INSERT INTO {guild}_GP (Name, G_ID) VALUES ('', NULL)")
            Functions.c.execute(f"INSERT INTO {guild}_GP_gained (Name, G_ID) VALUES ('', NULL)")
        Functions.conn.commit()
        snapshot("GP2026_10_03")
        assert gained("GP2026_10_03")["N"] == 300

    def test_an_id_missing_from_one_table_is_added_to_that_table(self, temp_functions_db):
        """Pretherians has an id in _GP that _GP_gained lacks."""
        seed({"GP2026_09_26": {"A": 1000}}, {"A": ("Alpha", 1680)})
        Functions.c.execute("DELETE FROM Aetherians_GP_gained WHERE G_ID = 'A'")
        Functions.conn.commit()
        snapshot("GP2026_10_03")
        assert gained("GP2026_10_03")["A"] == 680

    def test_a_half_added_column_is_skipped_and_stays_estimated(self, temp_functions_db):
        """How the old code left the database when it failed: 9/26 missing,
        and an all-NULL 10/03 in _GP from the ALTER that autocommitted."""
        seed({"GP2026_09_19": {"A": 1000}}, {"A": ("Alpha", 3041)})
        for guild in logic.GUILD_NAMES:
            Functions.c.execute(f"ALTER TABLE {guild}_GP ADD COLUMN GP2026_10_03 TEXT")
        Functions.conn.commit()

        snapshot("GP2026_10_10")
        assert [gained(column)["A"] for column in ("GP2026_09_26", "GP2026_10_03", "GP2026_10_10")] == [
            680, 680, 681,
        ]
        assert Functions.snapshot_taken("GP2026_10_10") is True
        assert Functions.snapshot_taken("GP2026_10_03") is False

        snapshot("GP2026_10_03")
        assert Functions.snapshot_taken("GP2026_10_03") is True

    def test_an_early_column_only_in_one_table_does_not_change_prev(self, temp_functions_db):
        seed({"GP2023_03_31": {"A": 10}, "GP2026_09_26": {"A": 1000}}, {"A": ("Alpha", 1680)},
             gained_columns=["GP2023_06_24", "GP2026_09_26"])
        snapshot("GP2026_10_03")
        assert gained("GP2026_10_03")["A"] == 680

    def test_a_failure_leaves_nothing_and_releases_the_lock(self, temp_functions_db, monkeypatch):
        """One transaction: a failure on the second guild rolls back the first
        guild's columns too, and the connection is closed, not left holding
        the write lock that stalled mark_weekly_runs_ended for 5 seconds."""
        seed({"GP2026_09_19": {"A": 1000}}, {"A": ("Alpha", 2361)})
        real = Functions._take_snapshot

        def fails_on_the_second_guild(cursor, guild, run_key):
            real(cursor, guild, run_key)
            if guild == logic.GUILD_NAMES[1]:
                raise RuntimeError("boom")
        monkeypatch.setattr(Functions, "_take_snapshot", fails_on_the_second_guild)

        with pytest.raises(RuntimeError, match="boom"):
            snapshot("GP2026_10_03")
        for guild in logic.GUILD_NAMES:
            assert "GP2026_10_03" not in columns(f"{guild}_GP")
            assert "GP2026_09_26" not in columns(f"{guild}_GP_gained")
        started = time.monotonic()
        Functions.mark_weekly_runs_ended()
        assert time.monotonic() - started < 1


class TestLatestTakenWeek:
    """GP_dataframe without a day reads the newest week that was taken, so a
    missed Saturday doesn't make it ask for a column that was never written."""

    WEEKS = {"GP2026_08_29": {"A": 0}, "GP2026_09_05": {"A": 680},
             "GP2026_09_12": {"A": 1360}, "GP2026_09_19": {"A": 2040}}

    def test_walks_back_past_two_missing_weeks(self, temp_functions_db):
        """9/26 failed all day; it is Saturday 10/03 before the 2 AM run."""
        seed(self.WEEKS, {"A": ("Alpha", 2720)})
        with freeze_time("2026-10-03 01:00:00"):
            day, run_key = asyncio.run(Functions.latest_snapshot_day())
            frame = asyncio.run(Functions.GP_dataframe("Aetherians"))
        assert run_key == "GP2026_09_19"
        assert list(frame.columns)[2:6] == ["08_29", "09_05", "09_12", "09_19"]

    def test_this_week_once_it_is_taken(self, temp_functions_db):
        seed(self.WEEKS, {"A": ("Alpha", 2720)})
        snapshot("GP2026_10_03")
        with freeze_time("2026-10-03 09:00:00"):
            _, run_key = asyncio.run(Functions.latest_snapshot_day())
        assert run_key == "GP2026_10_03"


class TestAttempts:
    def test_each_attempt_is_counted(self, temp_functions_db):
        for _ in range(3):
            asyncio.run(Functions.mark_weekly_run_started("GP2026_10_03"))
        assert Functions.interrupted_weekly_run()[2] == 3

    def test_rows_from_before_the_column_count_as_one(self, temp_functions_db):
        Functions.c.execute("CREATE TABLE weekly_runs (run_key TEXT PRIMARY KEY, started_at TEXT, ended_at TEXT)")
        Functions.c.execute("INSERT INTO weekly_runs VALUES ('GP2026_10_03', '2026-10-03T02:00:00', NULL)")
        assert Functions.interrupted_weekly_run()[2] == 1


class TestSnapshotStatus:
    def test_a_full_snapshot(self, temp_functions_db):
        seed({"GP2026_09_26": {"A": 1000, "B": 2000}}, {"A": ("Alpha", 1680), "B": ("Beta", 2000)})
        snapshot("GP2026_10_03")
        result = Functions.snapshot_status("Aetherians", "GP2026_10_03")
        assert (result['present'], result['roster'], result['recorded'], result['zeros']) == (True, 2, 2, 1)
        assert result['missed'] == []

    def test_a_member_with_nothing_recorded(self, temp_functions_db):
        seed({"GP2026_09_26": {"A": 1000}}, {"A": ("Alpha", 1680)})
        snapshot("GP2026_10_03")
        Functions.c.execute("INSERT INTO Aetherians_game VALUES (1, 'Late', 'L', 50)")
        result = Functions.snapshot_status("Aetherians", "GP2026_10_03")
        assert (result['roster'], result['recorded']) == (2, 1)

    def test_a_catch_up_reports_what_it_spread(self, temp_functions_db):
        seed({"GP2026_09_19": {"A": 1000}}, {"A": ("Alpha", 2361)})
        snapshot("GP2026_10_03")
        result = Functions.snapshot_status("Aetherians", "GP2026_10_03")
        assert (result['previous'], result['missed']) == ("GP2026_09_19", ["GP2026_09_26"])

    def test_a_week_not_taken(self, temp_functions_db):
        seed({"GP2026_09_26": {"A": 1000}}, {"A": ("Alpha", 1680)})
        assert Functions.snapshot_status("Aetherians", "GP2026_10_03")['present'] is False
        assert Functions.snapshot_taken("GP2026_10_03") is False


# --- the GP audit and estimated weeks --------------------------------------

class TestAuditWithEstimatedWeeks:
    def test_an_estimated_newest_week_is_never_a_spike(self):
        members = {"spread": [0, 300, 1500, 1500]}
        _, spiked = logic.filter_gp_audit(members, min_history=1, estimated={2, 3})
        assert spiked == []

    def test_estimated_weeks_count_but_are_marked(self):
        members = {"busy": [0] + [900] * 3 + [100] * 5}
        sustained, _ = logic.filter_gp_audit(members, weeks=8, hits=3, min_history=8, estimated={2, 3})
        [(hits, name, window)] = sustained
        assert hits == 3
        report = logic.format_gp_audit(sustained, [])
        assert "900, ~900, ~900, 100" in report
        assert "~ marks a week estimated from a missed snapshot." in report

    def test_no_legend_when_the_marked_entries_were_cut(self):
        """The budget drops later entries to '...and N more'; a legend for
        marks that aren't shown would point at nothing."""
        plain = [(8, f"member_number_{i}", [1500] * 8) for i in range(3)]
        marked = [(3, "zz_last", [logic.EstimatedGain(900)] * 3)]
        report = logic.format_gp_audit(plain + marked, [], budget=200)
        assert "~" not in report

    def test_the_bot_and_the_script_agree(self, temp_functions_db):
        seed(
            {"GP2026_09_12": {"A": 0}, "GP2026_09_19": {"A": 1000}},
            {"A": ("Alpha", 4600)},
        )
        snapshot("GP2026_10_03")
        members, estimated = Functions._load_gp_series("Aetherians")
        script_members, script_estimated = gp_audit.load_series(Functions.c, "Aetherians")
        assert (members, estimated) == (script_members, script_estimated)
        names = Functions._gp_snapshot_columns("Aetherians_GP_gained")
        assert {names[index] for index in estimated} == {"GP2026_09_12", "GP2026_09_26", "GP2026_10_03"}
        _, spiked = logic.filter_gp_audit(members, min_history=1, estimated=estimated)
        assert spiked == []
