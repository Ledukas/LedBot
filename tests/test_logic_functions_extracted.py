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
