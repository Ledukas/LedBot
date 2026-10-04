"""Tests for the logic.py functions extracted from Functions.py."""

import pandas as pd
import pytest

import logic

THRESHOLDS = [
    (1000, 'Knight'),
    (2500, 'Hero'),
    (5000, 'Demigod'),
    (10000, 'Deity'),
    (25000, 'Titan'),
    (50000, 'Primordial'),
    (100000, 'True'),
]


class TestComputeGpRankRole:
    @pytest.mark.parametrize("gp,expected", [
        (1000, 'Knight'),
        (2500, 'Hero'),
        (5000, 'Demigod'),
        (10000, 'Deity'),
        (25000, 'Titan'),
        (50000, 'Primordial'),
        (100000, 'True'),
    ])
    def test_exact_threshold_is_inclusive(self, gp, expected):
        """The bug this extraction fixed: GP exactly on a threshold used to
        match no branch at all in the original strict '>' chain."""
        assert logic.compute_gp_rank_role(gp, THRESHOLDS) == expected

    @pytest.mark.parametrize("gp,expected", [
        (1500, 'Knight'),
        (3000, 'Hero'),
        (7500, 'Demigod'),
        (15000, 'Deity'),
        (30000, 'Titan'),
        (60000, 'Primordial'),
        (150000, 'True'),
    ])
    def test_value_inside_each_tier(self, gp, expected):
        assert logic.compute_gp_rank_role(gp, THRESHOLDS) == expected

    @pytest.mark.parametrize("gp", [0, -1, 999])
    def test_below_first_threshold_is_none(self, gp):
        assert logic.compute_gp_rank_role(gp, THRESHOLDS) is None

    def test_far_above_top_threshold(self):
        assert logic.compute_gp_rank_role(1_000_000, THRESHOLDS) == 'True'


class TestFilterTopAverage:
    def test_default_threshold_is_strict(self):
        df = pd.DataFrame({'Average': [600, 649, 650, 700]})
        result = logic.filter_top_average(df)
        assert result['Average'].tolist() == [650, 700]

    def test_custom_threshold(self):
        df = pd.DataFrame({'Average': [600, 649, 650, 700]})
        result = logic.filter_top_average(df, threshold=600)
        assert result['Average'].tolist() == [649, 650, 700]

    def test_empty_dataframe(self):
        df = pd.DataFrame({'Average': []})
        result = logic.filter_top_average(df)
        assert result.empty


class TestBuildGpDataframe:
    COLUMN_NAMES = ['Name', 'G_ID', 'GP2025_12_13', 'GP2025_12_20', 'GP2025_12_27', 'GP2026_01_03']
    COLUMN_NAMES_INT = ['GP2025_12_13', 'GP2025_12_20', 'GP2025_12_27', 'GP2026_01_03']

    def test_year_agnostic_column_stripping(self):
        """The 4 columns span a year boundary -- every column should still get
        its own 'GP{year}_' prefix stripped correctly, not just a single
        hardcoded/current year."""
        rows = [('Alice', 'g1', '100', '200', '300', '400')]
        df = logic.build_gp_dataframe(rows, self.COLUMN_NAMES, self.COLUMN_NAMES_INT, 'GP')
        assert list(df.columns) == ['Name', 'G_ID', '12_13', '12_20', '12_27', '01_03', 'Average']

    def test_numeric_coercion_and_average(self):
        rows = [('Alice', 'g1', '100', '200', '300', '400')]
        df = logic.build_gp_dataframe(rows, self.COLUMN_NAMES, self.COLUMN_NAMES_INT, 'GP')
        assert df.loc[0, 'Average'] == 250.0

    def test_all_na_row_has_na_average(self):
        rows = [('Bob', 'g2', None, None, None, None)]
        df = logic.build_gp_dataframe(rows, self.COLUMN_NAMES, self.COLUMN_NAMES_INT, 'GP')
        assert pd.isna(df.loc[0, 'Average'])

    def test_non_numeric_string_coerced_to_na(self):
        rows = [('Carl', 'g3', 'not-a-number', '200', '300', '400')]
        df = logic.build_gp_dataframe(rows, self.COLUMN_NAMES, self.COLUMN_NAMES_INT, 'GP')
        assert pd.isna(df.loc[0, '12_13'])


class TestFilterRedGp:
    @staticmethod
    def make_df():
        return pd.DataFrame({
            'Name': ['A', 'B', 'C', 'D'],
            'G_ID': ['g1', 'g2', 'g3', 'g4'],
            '3wk': [10, 10, 10, 10],
            '2wk': [10, 10, 10, 10],
            '1wk': [10, None, 10, 10],
            'now': [100, 100, 500, 100],
        })

    def test_aetherians_threshold_and_blacklist_and_dropna(self):
        df = self.make_df()
        result = logic.filter_red_gp(df, 'Aetherians', blacklist=['g4'])
        # C excluded (now=500 >= 400 threshold), B excluded (NA in 1wk col),
        # D excluded (blacklisted) -- only A remains.
        assert result['Name'].tolist() == ['A']
        assert 'G_ID' not in result.columns

    def test_pretherians_uses_lower_threshold(self):
        df = pd.DataFrame({
            'Name': ['A'],
            'G_ID': ['g1'],
            '3wk': [10], '2wk': [10], '1wk': [10],
            'now': [150],  # below Aetherians' 400 (qualifies) but above Pretherians' 140 (doesn't)
        })
        assert not logic.filter_red_gp(df, 'Aetherians', blacklist=[]).empty
        assert logic.filter_red_gp(df, 'Pretherians', blacklist=[]).empty

    def test_unrecognized_guild_raises(self):
        """The bug this extraction fixed: an unrecognized io_guild used to
        leave the threshold variable undefined, surfacing as a confusing
        NameError several lines later instead of a clear error here."""
        df = self.make_df()
        with pytest.raises(ValueError):
            logic.filter_red_gp(df, 'SomeOtherGuild', blacklist=[])


class TestFormatTableBlock:
    def test_header_and_rows(self):
        df = pd.DataFrame({'Name': ['Alice', 'Bob'], 'GP': [100, 200]})
        result = logic.format_table_block(df)
        expected = (
            'Name'.ljust(14) + ' | ' + 'GP'.ljust(7) + '\n'
            + 'Alice'.ljust(14) + ' | ' + '100'.ljust(7) + '\n'
            + 'Bob'.ljust(14) + ' | ' + '200'.ljust(7) + '\n'
        )
        assert result == expected

    def test_empty_dataframe_is_header_only(self):
        df = pd.DataFrame({'Name': [], 'GP': []})
        result = logic.format_table_block(df)
        assert result == 'Name'.ljust(14) + ' | ' + 'GP'.ljust(7) + '\n'

    def test_custom_widths_and_separator(self):
        df = pd.DataFrame({'X': ['a'], 'Y': [1]})
        result = logic.format_table_block(df, first_col_width=2, other_col_width=2, separator='-')
        assert result.splitlines()[0] == 'X'.ljust(2) + '-' + 'Y'.ljust(2)
        assert result.splitlines()[1] == 'a'.ljust(2) + '-' + '1'.ljust(2)


class TestExtractCellValue:
    def test_normal_value(self):
        assert logic.extract_cell_value([["hello"]]) == "hello"

    def test_empty_list(self):
        assert logic.extract_cell_value([]) == "No strategy available yet"

    def test_empty_first_row(self):
        assert logic.extract_cell_value([[]]) == "No strategy available yet"

    def test_row_with_empty_string_cell_returns_the_empty_string(self):
        # [[""]] is a non-empty outer list containing a non-empty row ([""]
        # has one element), so the truthiness checks both pass and the
        # (empty string) cell value itself is returned -- only a *missing*
        # row/value falls back to the default, not an empty string value.
        assert logic.extract_cell_value([[""]]) == ""

    def test_custom_default(self):
        assert logic.extract_cell_value([], default="custom") == "custom"


class TestBuildGameMembersRows:
    def test_multiple_members(self):
        data = {'m1': {'a': 'Name1', 'e': 100}, 'm2': {'a': 'Name2', 'e': 200}}
        rows = logic.build_game_members_rows(data)
        assert rows == [
            {'G_NAME': 'Name1', 'G_ID': 'm1', 'GP': 100},
            {'G_NAME': 'Name2', 'G_ID': 'm2', 'GP': 200},
        ]

    def test_empty_dict(self):
        assert logic.build_game_members_rows({}) == []

    def test_a_malformed_member_is_refused_by_validation_not_a_keyerror(self):
        rows = logic.build_game_members_rows({'m1': {'a': 'Name1', 'e': 100}, 'm2': {'e': 5}, 'm3': None})
        assert rows[1] == {'G_NAME': None, 'G_ID': 'm2', 'GP': 5}
        assert rows[2] == {'G_NAME': None, 'G_ID': 'm3', 'GP': None}
        assert logic.validate_roster_rows(rows, 0) == "a member has no name"


class TestSplitMessage:
    def test_a_short_message_is_untouched(self):
        assert logic.split_message("hello") == ["hello"]

    def test_every_part_fits_and_nothing_is_lost(self):
        body = [f"line {index} " + "x" * 40 for index in range(120)]
        text = "**Aetherians**\n```\n" + "\n".join(body) + "\n```"
        parts = logic.split_message(text)
        assert len(parts) > 1
        assert all(len(part) <= logic.DISCORD_MESSAGE_LIMIT for part in parts)
        kept = [line for part in parts for line in part.split("\n") if line.startswith("line ")]
        assert kept == body

    def test_a_cut_code_block_is_closed_and_reopened(self):
        text = "head\n```\n" + "\n".join("y" * 50 for _ in range(100)) + "\n```"
        for part in logic.split_message(text):
            assert part.count("```") == 2

    def test_one_overlong_line_is_cut(self):
        parts = logic.split_message("z" * 5000)
        assert all(len(part) <= logic.DISCORD_MESSAGE_LIMIT for part in parts)
        assert "".join(parts).replace("\n", "") == "z" * 5000


    def test_a_split_before_the_closing_fence_sends_no_empty_block(self):
        room = logic.DISCORD_MESSAGE_LIMIT - 8
        body = "x" * (room - len("```") - 2)
        parts = logic.split_message("```\n" + body + "\n```\n" + "tail " * 10)
        assert all(part.strip("`\n") for part in parts)
        assert all(part.count("```") % 2 == 0 for part in parts)


class TestKickOutcome:
    def test_a_true_result_is_done(self):
        assert logic.kick_outcome(200, {"result": "true"}) == logic.KICK_DONE

    def test_a_false_result_is_a_refusal(self):
        assert logic.kick_outcome(200, {"result": "false"}) == logic.KICK_REFUSED

    def test_a_client_error_is_a_refusal(self):
        assert logic.kick_outcome(401, {"error": {"status": "UNAUTHENTICATED"}}) == logic.KICK_REFUSED

    def test_a_server_error_or_unreadable_reply_is_unknown(self):
        """These used to read as "the game refused", leaving a member who may
        have been kicked with every Discord role."""
        assert logic.kick_outcome(502, None) == logic.KICK_UNKNOWN
        assert logic.kick_outcome(500, {"error": "internal"}) == logic.KICK_UNKNOWN
        assert logic.kick_outcome(200, None) == logic.KICK_UNKNOWN


class TestFormatRoleSyncStatus:
    def test_clean_sync_is_none(self):
        assert logic.format_role_sync_status(None, []) is None

    def test_failures_are_named_and_capped(self):
        status = logic.format_role_sync_status(None, [f"m{index}: Forbidden" for index in range(7)])
        assert status.startswith("7 role change(s) failed -- m0: Forbidden")
        assert status.endswith("and 2 more")

    def test_a_skipped_step_and_failures_are_both_reported(self):
        status = logic.format_role_sync_status("duck pairing skipped", ["a: Forbidden"])
        assert status == "duck pairing skipped; 1 role change(s) failed -- a: Forbidden"


class TestRankThresholdConstants:
    """RANK_THRESHOLDS used to be duplicated across LedBotCode.py and
    Functions.py, feeding the !mygains rank-up figure and the actual role
    assignment separately. These pin that everything is derived from one table
    and stays in the order compute_gp_rank_role relies on."""

    def test_thresholds_are_strictly_ascending(self):
        values = [threshold for threshold, _ in logic.RANK_THRESHOLDS]
        assert values == sorted(values)
        assert len(set(values)) == len(values)

    def test_derived_lists_match_the_table(self):
        assert logic.GP_THRESHOLDS == [t for t, _ in logic.RANK_THRESHOLDS]
        assert logic.RANK_ROLE_NAMES == [n for _, n in logic.RANK_THRESHOLDS]
        assert len(logic.GP_THRESHOLDS) == len(logic.RANK_ROLE_NAMES)

    def test_every_threshold_exactly_grants_its_own_rank(self):
        """The bug that started all this: a strict '>' meant GP exactly equal
        to a threshold matched no branch and granted no role."""
        for threshold, role_name in logic.RANK_THRESHOLDS:
            assert logic.compute_gp_rank_role(threshold, logic.RANK_THRESHOLDS) == role_name

    def test_rankup_figure_agrees_with_the_rank_table(self):
        """The two consumers must not disagree: just below a threshold, the
        gap reported is exactly the distance to the rank that threshold grants."""
        for threshold, _ in logic.RANK_THRESHOLDS[:-1]:
            assert logic.compute_remaining_to_rankup(threshold - 1, logic.GP_THRESHOLDS) == 1


class TestGuildConfig:
    def test_table_name_builds_expected_names(self):
        assert logic.table_name("Aetherians", "members") == "Aetherians_members"
        assert logic.table_name("Pretherians", "GP_gained") == "Pretherians_GP_gained"

    def test_unknown_guild_is_rejected(self):
        """IOguild is whatever a moderator typed, and it is interpolated
        directly into SQL because a table name cannot be a bound parameter."""
        with pytest.raises(ValueError):
            logic.table_name("Robert'); DROP TABLE members;--", "members")

    def test_unknown_table_kind_is_rejected(self):
        with pytest.raises(ValueError):
            logic.table_name("Aetherians", "not_a_table")

    def test_every_guild_has_a_red_gp_threshold(self):
        assert set(logic.RED_GP_THRESHOLDS) == set(logic.GUILD_NAMES)

    def test_asymmetric_guilds_are_real_guilds(self):
        assert logic.RANK_ROLE_GUILD in logic.GUILD_NAMES
        assert logic.PROMOTION_GUILD in logic.GUILD_NAMES

    def test_guild_names_match_the_gid_table(self):
        assert set(logic.GUILD_NAMES) == set(logic.GUILD_GIDS)
