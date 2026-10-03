"""Tests for the GP audit logic extracted into logic.py.

The boundary cases here are the ones the audit was actually built around, and
several encode decisions that cost real analysis to reach -- that 680 is a cap
players legitimately sit on rather than a tail value, that a dip-and-spike
profile never accumulates consecutive weeks, and that the export writes a
member's lifetime GP into their first weekly delta. A future tweak to any
threshold should have to break one of these on purpose.
"""

import pytest

import logic


class TestParseGpValue:
    @pytest.mark.parametrize("stored,expected", [
        ("680", 680),
        ("680.0", 680),
        (680, 680),
        ("0", 0),
        ("-120", -120),
    ])
    def test_reads_the_text_columns(self, stored, expected):
        assert logic.parse_gp_value(stored) == expected

    @pytest.mark.parametrize("stored", [None, "", "n/a"])
    def test_missing_or_junk_is_none(self, stored):
        assert logic.parse_gp_value(stored) is None


class TestObservedWeeks:
    def test_drops_the_first_recorded_week(self):
        """GP_databases computes a first gain as `GPnow - 0`, so week one is a
        member's whole lifetime GP rather than a week's worth."""
        assert logic.observed_weeks([None, 20300, 360, 380]) == [360, 380]

    def test_drops_an_implausible_week_mid_history(self):
        """Same artifact, landing mid-series via the duplicated Pretherians row
        that has no matching entry in Pretherians_GP at all."""
        assert logic.observed_weeks([370, 390, 20300, 360]) == [390, 360]

    def test_keeps_the_real_record_week(self):
        assert 2410 in logic.observed_weeks([500, 600, 2410, 700])

    def test_ignores_gaps(self):
        assert logic.observed_weeks([300, None, 400, None, 500]) == [400, 500]

    @pytest.mark.parametrize("series", [[], [None], [500]])
    def test_too_little_history_is_empty(self, series):
        assert logic.observed_weeks(series) == []


class TestLatestWeek:
    def test_reads_the_newest_column(self):
        assert logic.latest_week([300, 400, 1500]) == 1500

    def test_absent_from_the_latest_export_is_none(self):
        """Not reported on a figure that is several weeks old."""
        assert logic.latest_week([300, 1500, None]) is None

    def test_a_members_first_ever_snapshot_is_none(self):
        assert logic.latest_week([None, None, 20300]) is None

    def test_implausible_is_none(self):
        assert logic.latest_week([300, 400, 20300]) is None

    def test_empty_is_none(self):
        assert logic.latest_week([]) is None


class TestAuditMember:
    def test_counts_strictly_above_the_bar(self):
        """750 exactly does not count -- the bar is what a week must clear."""
        result = logic.audit_member([0] + [750, 751, 750, 760], weeks=4)
        assert result["hits"] == 2

    def test_the_task_cap_never_counts(self):
        """Every member who simply finishes their weekly tasks lands on 680.
        Counting them would report most of both guilds."""
        result = logic.audit_member([0] + [680] * 8, weeks=8)
        assert result["hits"] == 0

    def test_window_is_the_tail(self):
        result = logic.audit_member([0, 900, 900, 100, 100], weeks=2)
        assert result["window"] == [100, 100]
        assert result["hits"] == 0


class TestFilterGpAudit:
    def test_counts_across_the_window_not_consecutively(self):
        """Gains collapse during in-game events, so requiring a streak lets
        anyone through on the back of one quiet week."""
        members = {"spiky": [0, 900, 50, 900, 40, 900, 30, 60, 70]}
        sustained, _ = logic.filter_gp_audit(members, weeks=8, hits=3, min_history=4)
        assert [name for _, name, _ in sustained] == ["spiky"]

    def test_min_history_gates_the_sustained_side(self):
        members = {"newcomer": [0, 900, 900, 900]}
        sustained, _ = logic.filter_gp_audit(members, weeks=8, hits=3, min_history=8)
        assert sustained == []

    def test_min_history_does_not_gate_the_spike_side(self):
        """A member three weeks into the guild posting an enormous week is
        exactly what the spike check exists for."""
        members = {"newcomer": [0, 200, 1500]}
        _, spiked = logic.filter_gp_audit(members, min_history=8)
        assert spiked == [(1500, "newcomer")]

    def test_spike_bar_is_strict(self):
        """1200 is a common exact value (71 weeks across 28 members); landing
        on it is not the unusual part, going past it is."""
        members = {"on": [0, 300, 1200], "past": [0, 300, 1201]}
        _, spiked = logic.filter_gp_audit(members, min_history=1)
        assert [name for _, name in spiked] == ["past"]

    def test_the_two_signals_are_independent(self):
        """The profile that prompted this: quiet on average, occasional huge
        week. It trips the spike check while failing the sustained one."""
        members = {"quiet_spiker": [0] + [100] * 10 + [1500]}
        sustained, spiked = logic.filter_gp_audit(members, weeks=8, hits=3, min_history=8)
        assert sustained == []
        assert [name for _, name in spiked] == ["quiet_spiker"]

    def test_sorted_worst_first_then_by_name(self):
        members = {
            "b_three": [0] + [900] * 3 + [100] * 5,
            "a_three": [0] + [900] * 3 + [100] * 5,
            "four": [0] + [900] * 4 + [100] * 4,
        }
        sustained, _ = logic.filter_gp_audit(members, weeks=8, hits=3, min_history=8)
        assert [name for _, name, _ in sustained] == ["four", "a_three", "b_three"]

    def test_clean_guild_reports_nothing(self):
        members = {f"m{i}": [0] + [400, 680, 550, 620] * 3 for i in range(20)}
        sustained, spiked = logic.filter_gp_audit(members)
        assert (sustained, spiked) == ([], [])


class TestFormatGpAudit:
    def test_names_the_weeks_behind_each_flag(self):
        report = logic.format_gp_audit(
            [(3, "Someone", [900, 100, 900, 100, 900, 100, 50, 60])], []
        )
        assert "Someone" in report
        assert "900, 100, 900" in report

    def test_a_rename_suffix_survives_intact(self):
        """Truncating here would send a moderator looking for a name that does
        not exist in either the guild list or the database."""
        report = logic.format_gp_audit(
            [(3, "Kawaby (was TheOnlyVoid)", [1020, 630, 1200])], []
        )
        assert "Kawaby (was TheOnlyVoid)" in report

    def test_quiet_guild_is_none(self):
        """The weekly report's silence depends on this; the manual command
        turns it into a line."""
        assert logic.format_gp_audit([], []) is None

    def test_spike_section_only_appears_when_there_is_one(self):
        sustained = [(3, "Someone", [900, 900, 900])]
        assert "this week" not in logic.format_gp_audit(sustained, [])
        assert "this week" in logic.format_gp_audit([], [(1500, "Someone")])

    def test_fits_in_an_embed(self):
        sustained = [(8, f"member_number_{i}", [1500] * 8) for i in range(15)]
        spiked = [(1500, f"member_number_{i}") for i in range(15)]
        report = logic.format_gp_audit(sustained, spiked)
        assert len(report) < logic.EMBED_DESCRIPTION_LIMIT


class TestThresholdsHangTogether:
    def test_the_bar_sits_above_the_task_cap(self):
        assert logic.GP_GAIN_BAR > logic.GP_TASK_CAP

    def test_a_spike_week_also_counts_toward_the_sustained_rule(self):
        """What makes strict spike-checking safe: repeatedly landing on 1200
        is caught by the sustained count even though no single week trips the
        spike check."""
        assert logic.GP_SPIKE_WEEK > logic.GP_GAIN_BAR

    def test_no_real_week_reaches_the_implausible_ceiling(self):
        assert logic.GP_IMPLAUSIBLE_WEEK > 2410

    def test_the_window_is_long_enough_to_satisfy_the_hit_count(self):
        assert logic.GP_AUDIT_HITS <= logic.GP_AUDIT_WEEKS
