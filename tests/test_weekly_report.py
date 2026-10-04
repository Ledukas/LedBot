"""The weekly report: one embedded post, quiet but never silent.

The pure half (formatting, chunking, packing) is tested against Discord's
limits directly. The orchestration half runs _run_weekly_gp with every step
that touches the network or the real database stubbed -- GP_databases opens
'DatabaseLedBot.db' by relative path, so the fixture also moves the working
directory into tmp_path in case a stub is ever missed.
"""

import asyncio
import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest
from freezegun import freeze_time

import Functions
import logic

WEEK = "GP2026_10_03"
REAL_GET_DATE = Functions.get_date
WEEKS = ["GP2026_09_12", "GP2026_09_19", "GP2026_09_26", WEEK]


def gp_frame(rows, weeks=WEEKS):
    """A dataframe shaped exactly as Functions.GP_dataframe builds it."""
    columns = ['Name', 'G_ID', *weeks]
    return logic.build_gp_dataframe(rows, columns, weeks, 'GP')


def red_frame(rows, guild="Aetherians"):
    return logic.filter_red_gp(gp_frame(rows), guild, [])


def descriptions(embeds):
    return [embed.description for embed in embeds]


def balanced(text):
    return text.count("```") % 2 == 0


# --- logic ------------------------------------------------------------------

class TestDateLabels:
    @pytest.mark.parametrize("column,label", [
        ("GP2026_10_03", "10/3"), ("08_29", "8/29"), ("09_05", "9/5"),
    ])
    def test_month_and_day_without_padding(self, column, label):
        assert logic.gp_date_label(column) == label

    def test_heading(self):
        assert logic.weekly_report_heading(WEEK) == "**Weekly report -- week ending 10/3**"
        assert logic.weekly_report_heading(WEEK, preview=True).startswith("**Report preview")


class TestCurrentNames:
    def test_a_renamed_member_shows_the_roster_name(self):
        df = gp_frame([("OldName", "A", 1, 2, 3, 4), ("Gone", "B", 1, 2, 3, 4)])
        renamed = logic.current_names(df, {"A": "NewName", "C": "Other"})
        assert list(renamed['Name']) == ["NewName", "Gone"]

    def test_a_blank_roster_name_keeps_the_stored_one(self):
        df = gp_frame([("Stored", "A", 1, 2, 3, 4)])
        assert list(logic.current_names(df, {"A": ""})['Name']) == ["Stored"]

    def test_the_shared_dataframe_is_not_mutated(self):
        """GP_roles and the red list are handed the same object."""
        df = gp_frame([("OldName", "A", 1, 2, 3, 4)])
        logic.current_names(df, {"A": "NewName"})
        assert list(df['Name']) == ["OldName"]


class TestFormatRedGp:
    def test_nobody_red_is_no_embeds(self):
        assert logic.format_red_gp("Aetherians", red_frame([("Fine", "A", 700, 700, 700, 700)])) == []

    def test_the_table(self):
        df = red_frame([
            ("Semaphore", "A", 610, 550, 610, 0),
            ("cioo4", "B", None, 340, 0, 10),
        ])
        [embed] = logic.format_red_gp("Aetherians", df)
        assert embed.title == "Aetherians -- red GP (2)"
        lines = embed.description.split("\n")
        assert lines[1] == "Name            9/12 9/19 9/26 10/3  avg"
        assert lines[2] == "Semaphore        610  550  610    0  443"
        assert lines[3] == "cioo4              -  340    0   10  117"
        assert balanced(embed.description)

    def test_lines_stay_phone_width(self):
        df = red_frame([("n" * 15, f"G{i}", 2410, 1660, 1200, 0) for i in range(5)])
        [embed] = logic.format_red_gp("Aetherians", df)
        assert max(len(line) for line in embed.description.split("\n")) <= 40

    def test_a_whole_guild_splits_into_valid_embeds(self):
        """An event week can put most of a guild on the list."""
        rows = [(f"member_{i:03d}_xx", f"G{i}", 300, 200, 100, 0) for i in range(210)]
        embeds = logic.format_red_gp("Aetherians", red_frame(rows))
        assert len(embeds) > 1
        assert embeds[0].title == "Aetherians -- red GP (210)"
        assert all(embed.title.endswith("(cont.)") for embed in embeds[1:])
        header = embeds[0].description.split("\n")[1]
        listed = []
        for text in descriptions(embeds):
            assert len(text) <= logic.EMBED_DESCRIPTION_LIMIT
            assert balanced(text)
            lines = text.split("\n")
            assert lines[1] == header
            listed += [line.split()[0] for line in lines[2:-1]]
        assert sorted(listed) == sorted(f"member_{i:03d}_xx" for i in range(210))


class TestGpAuditBudget:
    def test_a_whole_guild_flagged_ends_in_a_count_not_a_cut(self):
        sustained = [(8, f"member_number_{i:03d}", [1500] * 8) for i in range(150)]
        spiked = [(1500, f"member_number_{i:03d}") for i in range(150)]
        report = logic.format_gp_audit(sustained, spiked)
        assert len(report) < logic.EMBED_DESCRIPTION_LIMIT
        assert logic.TRUNCATED_NOTE not in report
        assert "...and 107 more member(s)." in report
        assert report.endswith("...and 149 more member(s).")
        assert balanced(report)


class TestReportEmbed:
    def test_a_short_body_is_untouched(self):
        assert logic.report_embed("t", "body", 1).description == "body"

    def test_an_over_long_block_is_cut_with_its_fence_closed(self):
        body = "heading\n```\n" + "\n".join("x" * 50 for _ in range(200)) + "\n```"
        text = logic.report_embed("t", body, 1).description
        assert len(text) <= logic.EMBED_DESCRIPTION_LIMIT
        assert balanced(text)
        assert text.endswith(logic.TRUNCATED_NOTE)

    def test_one_enormous_line_is_cut_not_dropped(self):
        text = logic.report_embed("t", "y" * 10000, 1).description
        assert len(text) <= logic.EMBED_DESCRIPTION_LIMIT
        assert text.startswith("yyyy")

    def test_a_failure_names_the_exception(self):
        embed = logic.failure_embed("Aetherians -- red GP failed", KeyError("G_ID"))
        assert embed.description == "KeyError: 'G_ID'"
        assert embed.colour.value == logic.ERROR_COLOR


def realistic_week():
    """Last week's shape: 40 + 14 red, a few flagged, 8 unlinked."""
    embeds = logic.format_red_gp("Aetherians", red_frame(
        [(f"aeth_member_{i:02d}", f"A{i}", 610, 550, 380, i) for i in range(40)]
    ))
    embeds += logic.section_embeds("Aetherians", "GP audit", logic.format_gp_audit(
        [(4, f"busy_member_{i}", [900, 760, 680, 1200, 900, 680, 800, 690]) for i in range(4)],
        [(1500, "busy_member_0")],
    ))
    embeds += logic.section_embeds("Aetherians", "links", logic.format_links(
        [], [], {'game_no_link': [f"unlinked_{i}" for i in range(3)],
                 'role_no_link': [f"discord_member_{i}" for i in range(3)]}
    ))
    embeds += logic.format_red_gp("Pretherians", red_frame(
        [(f"preth_member_{i:02d}", f"P{i}", 180, 150, 60, i) for i in range(14)], "Pretherians"
    ))
    embeds += logic.section_embeds("Pretherians", "links", logic.format_links(
        [], [], {'game_no_link': [f"unlinked_{i}" for i in range(5)],
                 'role_no_link': [f"discord_member_{i}" for i in range(5)],
                 'role_no_character': ["left_a", "left_b"]}
    ))
    return embeds


class TestPackMessages:
    def test_a_realistic_week_is_one_message(self):
        assert len(logic.pack_messages(realistic_week())) == 1

    def test_limits_hold_and_order_is_kept(self):
        rows = [(f"member_{i:03d}_xx", f"G{i}", 300, 200, 100, 0) for i in range(210)]
        embeds = logic.format_red_gp("Aetherians", red_frame(rows)) + realistic_week()
        messages = logic.pack_messages(embeds)
        assert len(messages) > 1
        for batch in messages:
            assert len(batch) <= logic.EMBEDS_PER_MESSAGE
            assert sum(len(embed) for embed in batch) <= logic.EMBED_TOTAL_LIMIT
        assert [embed for batch in messages for embed in batch] == embeds

    def test_eleven_small_embeds_need_two_messages(self):
        embeds = [logic.report_embed(str(i), "x", 1) for i in range(11)]
        assert [len(batch) for batch in logic.pack_messages(embeds)] == [10, 1]


class TestFinishWeeklyReport:
    def test_a_quiet_week_still_posts_its_heading_and_backup(self):
        content, embeds = logic.finish_weekly_report("**H**", [], "Database_2026.db")
        assert content == "**H** -- nothing to report. Backup: `Database_2026.db`"
        assert embeds == []

    def test_the_backup_rides_in_the_last_footer(self):
        report = [logic.report_embed("a", "x", 1), logic.report_embed("b", "y", 1)]
        content, embeds = logic.finish_weekly_report("**H**", report, "Database_2026.db")
        assert content == "**H**"
        assert embeds[-1].footer.text == "Backup: Database_2026.db"
        assert embeds[0].footer.text is None

    def test_no_backup_no_footer(self):
        _, embeds = logic.finish_weekly_report("**H**", [logic.report_embed("a", "x", 1)], None)
        assert embeds[0].footer.text is None


# --- builders ---------------------------------------------------------------

class TestBuilders:
    def test_a_missing_table_is_an_error_embed_not_silence(self, temp_functions_db):
        for build in (Functions.gp_audit_report, Functions.links_report):
            [embed] = build("Aetherians")
            assert embed.colour.value == logic.ERROR_COLOR
            assert "failed" in embed.title

    def test_the_red_list_uses_the_roster_name(self, temp_functions_db):
        Functions.c.execute('CREATE TABLE Aetherians_game ("index" INTEGER, G_NAME TEXT, G_ID TEXT, GP INTEGER)')
        Functions.c.execute("INSERT INTO Aetherians_game VALUES (0, 'NewName', 'A', 5000)")
        df = gp_frame([("OldName", "A", 100, 100, 100, 0)])
        [embed] = Functions.red_gp_report(df, "Aetherians")
        assert "NewName" in embed.description and "OldName" not in embed.description

    def test_a_formatter_failure_is_an_error_embed_too(self, temp_functions_db, monkeypatch):
        """Outside the builder's try it would escape the weekly loop's
        isolation and cost the other guild its report."""
        def broken(*args, **kwargs):
            raise ValueError("unexpected value")
        monkeypatch.setattr(Functions, "_load_gp_series", lambda guild: {})
        monkeypatch.setattr(Functions, "_load_link_snapshot", lambda guild: ([], {}, {}))
        monkeypatch.setattr(Functions, "_load_role_holder_labels", lambda guild: {})
        monkeypatch.setattr(logic, "format_gp_audit", broken)
        monkeypatch.setattr(logic, "format_links", broken)
        for build in (Functions.gp_audit_report, Functions.links_report):
            [embed] = build("Aetherians")
            assert embed.colour.value == logic.ERROR_COLOR

    def test_the_red_list_without_a_roster_is_an_error_embed(self, temp_functions_db):
        [embed] = Functions.red_gp_report(gp_frame([("A", "A", 1, 1, 1, 0)]), "Aetherians")
        assert embed.colour.value == logic.ERROR_COLOR


# --- orchestration ----------------------------------------------------------

MOD_CHANNEL_ID = 4242


class FakeChannel:
    def __init__(self, channel_id=MOD_CHANNEL_ID, embed_links=True, fail_embeds=False):
        self.id = channel_id
        self.sent = []
        self.guild = SimpleNamespace(me=object())
        self._embed_links = embed_links
        self._fail_embeds = fail_embeds

    def permissions_for(self, member):
        return SimpleNamespace(embed_links=self._embed_links)

    async def send(self, content=None, **kwargs):
        if self._fail_embeds and kwargs.get('embeds'):
            raise RuntimeError("400 Bad Request")
        self.sent.append((content, kwargs))


class FakeContext:
    def __init__(self, channel):
        self.channel = channel

    async def send(self, content=None, **kwargs):
        await self.channel.send(content, **kwargs)


def section(title):
    return [logic.report_embed(title, "x", 1)]


@pytest.fixture
def weekly(temp_functions_db, no_bot_run, monkeypatch, tmp_path):
    import LedBotCode
    monkeypatch.chdir(tmp_path)
    mod = FakeChannel()
    calls = []

    async def get_date(today=None):
        return {"column_name1": WEEK}

    def step(name):
        async def stub(*args, **kwargs):
            calls.append(name)
        return stub

    def refresh_discord_rosters():
        calls.append("refresh_discord")
        return list(state.discord_warnings)

    async def gp_dataframe(guild, today=None):
        state.dataframe_days.append(today)
        return f"df:{guild}"

    async def gp_roles(bot, df):
        return None

    def build(kind):
        def builder(*args):
            guild = next(arg for arg in args if arg in logic.GUILD_NAMES)
            calls.append(f"{kind}:{guild}")
            return state.sections.get(f"{kind}:{guild}", [])
        return builder

    state = SimpleNamespace(
        mod=mod, calls=calls, sections={}, dataframe_days=[], discord_warnings=[], module=LedBotCode,
    )
    monkeypatch.setattr(LedBotCode, "LedukasSpam_channel", mod)
    monkeypatch.setattr(LedBotCode, "LedukasSpam_channelID", MOD_CHANNEL_ID)
    monkeypatch.setattr(LedBotCode, "export_game_rosters", step("export"))
    monkeypatch.setattr(LedBotCode, "refresh_discord_rosters", refresh_discord_rosters)
    monkeypatch.setattr(LedBotCode, "run_backup", lambda: Path("Backups/Database_2026_10_03.db"))
    monkeypatch.setattr(Functions, "get_date", get_date)
    monkeypatch.setattr(Functions, "GP_databases", step("GP_databases"))
    monkeypatch.setattr(Functions, "GP_dataframe", gp_dataframe)
    monkeypatch.setattr(Functions, "GP_roles", gp_roles)
    monkeypatch.setattr(Functions, "snapshot_taken", lambda column: True)
    monkeypatch.setattr(Functions, "red_gp_report", build("red"))
    monkeypatch.setattr(Functions, "gp_audit_report", build("audit"))
    monkeypatch.setattr(Functions, "links_report", build("links"))
    return state


def run(state, ack=None):
    asyncio.run(state.module.run_weekly_gp(ack))


async def role_sync_crash(bot, df):
    raise RuntimeError("503 Service Unavailable")


class TestWeeklyRun:
    def test_a_quiet_week_posts_one_line(self, weekly):
        run(weekly)
        [(content, kwargs)] = weekly.mod.sent
        assert content == (
            "**Weekly report -- week ending 10/3** -- nothing to report. "
            "Backup: `Database_2026_10_03.db`"
        )
        assert kwargs['embeds'] == []

    def test_a_red_list_week_is_one_message_with_the_backup_in_the_footer(self, weekly):
        weekly.sections["red:Aetherians"] = section("Aetherians -- red GP (40)")
        weekly.sections["links:Pretherians"] = section("Pretherians -- links")
        run(weekly)
        [(content, kwargs)] = weekly.mod.sent
        assert content == "**Weekly report -- week ending 10/3**"
        titles = [embed.title for embed in kwargs['embeds']]
        assert titles == ["Aetherians -- red GP (40)", "Pretherians -- links"]
        assert kwargs['embeds'][-1].footer.text == "Backup: Database_2026_10_03.db"
        assert kwargs['allowed_mentions'].everyone is False

    def test_a_partial_role_sync_is_an_amber_embed(self, weekly, monkeypatch):
        async def partial(bot, df):
            return "the 'Monthly Top' role does not exist"
        monkeypatch.setattr(Functions, "GP_roles", partial)
        run(weekly)
        [embed] = weekly.mod.sent[0][1]['embeds']
        assert embed.title == "Aetherians -- rank roles"
        assert embed.colour.value == logic.WARNING_COLOR

    def test_a_role_sync_crash_costs_nobody_their_report(self, weekly, monkeypatch):
        monkeypatch.setattr(Functions, "GP_roles", role_sync_crash)
        weekly.sections["red:Pretherians"] = section("Pretherians -- red GP (14)")

        with pytest.raises(weekly.module.WeeklyReportIncomplete, match="503"):
            run(weekly)
        titles = [embed.title for embed in weekly.mod.sent[0][1]['embeds']]
        assert titles == ["Aetherians -- rank roles failed", "Pretherians -- red GP (14)"]
        assert "red:Aetherians" in weekly.calls

    def test_a_dataframe_failure_skips_only_what_needs_it(self, weekly, monkeypatch):
        async def gp_dataframe(guild, today=None):
            if guild == "Aetherians":
                raise KeyError("GP2026_10_03")
            return "df"
        monkeypatch.setattr(Functions, "GP_dataframe", gp_dataframe)

        with pytest.raises(weekly.module.WeeklyReportIncomplete):
            run(weekly)
        assert "red:Aetherians" not in weekly.calls
        for step in ("audit:Aetherians", "links:Aetherians",
                     "red:Pretherians", "audit:Pretherians", "links:Pretherians"):
            assert step in weekly.calls
        [embed] = weekly.mod.sent[0][1]['embeds']
        assert embed.title == "Aetherians -- GP table failed, rank roles not synced"

    def test_only_the_rank_role_guild_mentions_skipped_roles(self, weekly, monkeypatch):
        async def gp_dataframe(guild, today=None):
            raise KeyError("GP2026_10_03")
        monkeypatch.setattr(Functions, "GP_dataframe", gp_dataframe)
        with pytest.raises(weekly.module.WeeklyReportIncomplete):
            run(weekly)
        titles = [embed.title for embed in weekly.mod.sent[0][1]['embeds']]
        assert titles == [
            "Aetherians -- GP table failed, rank roles not synced",
            "Pretherians -- GP table failed",
        ]

    def test_a_failed_backup_is_reported(self, weekly, monkeypatch):
        def broken():
            raise OSError("disk full")
        monkeypatch.setattr(weekly.module, "run_backup", broken)
        run(weekly)
        [embed] = weekly.mod.sent[0][1]['embeds']
        assert embed.title == "Weekly backup failed"
        assert embed.footer.text is None

    def test_without_embed_links_it_says_so_and_keeps_the_backup_name(self, weekly):
        weekly.mod._embed_links = False
        weekly.sections["red:Aetherians"] = section("Aetherians -- red GP (40)")
        run(weekly)
        [(content, kwargs)] = weekly.mod.sent
        assert "Embed Links" in content
        assert "Backup: Database_2026_10_03.db" in content
        assert 'embeds' not in kwargs

    def test_a_rejected_post_falls_back_to_a_line(self, weekly):
        weekly.mod._fail_embeds = True
        weekly.sections["red:Aetherians"] = section("Aetherians -- red GP (40)")
        run(weekly)
        [(content, _)] = weekly.mod.sent
        assert content.startswith("The weekly report could not be posted")

    def test_a_finished_run_is_not_left_looking_interrupted(self, weekly):
        run(weekly)
        assert Functions.interrupted_weekly_run() is None

    def test_a_manual_run_elsewhere_is_acknowledged(self, weekly):
        elsewhere = FakeChannel(channel_id=1)
        run(weekly, FakeContext(elsewhere))
        [(content, _)] = elsewhere.sent
        assert content == f"Weekly run finished -- the report is in <#{MOD_CHANNEL_ID}>."

    def test_a_manual_run_is_not_told_an_unposted_report_was_posted(self, weekly):
        weekly.mod._fail_embeds = True
        weekly.sections["red:Aetherians"] = section("Aetherians -- red GP (40)")
        elsewhere = FakeChannel(channel_id=1)
        run(weekly, FakeContext(elsewhere))
        [(content, _)] = elsewhere.sent
        assert "could not be posted" in content

    def test_a_manual_run_in_the_mod_channel_is_not_acknowledged_twice(self, weekly):
        run(weekly, FakeContext(weekly.mod))
        assert len(weekly.mod.sent) == 1

    def test_a_failed_manual_run_is_not_acknowledged(self, weekly, monkeypatch):
        monkeypatch.setattr(Functions, "GP_roles", role_sync_crash)
        elsewhere = FakeChannel(channel_id=1)
        with pytest.raises(weekly.module.WeeklyReportIncomplete):
            run(weekly, FakeContext(elsewhere))
        assert elsewhere.sent == []


class TestDiscordRefreshInTheWeeklyRun:
    def test_it_runs_after_the_snapshot_and_before_the_sections(self, weekly):
        """Not between the export and GP_databases: weekly_job() keeps that
        window short, and the snapshot doesn't read the role list."""
        run(weekly)
        calls = weekly.calls
        assert calls.index("GP_databases") < calls.index("refresh_discord") < calls.index("red:Aetherians")

    def test_its_warnings_open_the_report(self, weekly):
        weekly.discord_warnings = [logic.warning_embed("Aetherians -- role list not refreshed", "x")]
        weekly.sections["links:Aetherians"] = section("Aetherians -- links")
        run(weekly)
        titles = [embed.title for embed in weekly.mod.sent[0][1]['embeds']]
        assert titles == ["Aetherians -- role list not refreshed", "Aetherians -- links"]


class TestSyncCounters:
    def test_refreshes_both_sides_then_posts_one_message(self, weekly):
        weekly.sections["links:Aetherians"] = section("Aetherians -- links")
        here = FakeChannel(channel_id=1)
        asyncio.run(weekly.module.sync_counters.callback(FakeContext(here), None))
        assert weekly.calls[:2] == ["export", "refresh_discord"]
        [(content, kwargs)] = here.sent
        assert content == "Pretherians: everything is linked."
        assert [embed.title for embed in kwargs['embeds']] == ["Aetherians -- links"]

    def test_a_failed_export_says_the_game_side_is_old_and_still_posts(self, weekly, monkeypatch):
        async def down():
            raise RuntimeError("Firebase unavailable")
        monkeypatch.setattr(weekly.module, "export_game_rosters", down)
        weekly.sections["links:Pretherians"] = section("Pretherians -- links")
        here = FakeChannel(channel_id=1)
        asyncio.run(weekly.module.sync_counters.callback(FakeContext(here), None))
        [(_, kwargs)] = here.sent
        failure, links = kwargs['embeds']
        assert failure.title == "In-game roster not refreshed"
        assert "previous roster" in failure.description
        assert links.title == "Pretherians -- links"
        assert "refresh_discord" in weekly.calls

    def test_refused_while_a_weekly_run_is_going(self, weekly, monkeypatch):
        """It writes {guild}_game, which the weekly job reads between its
        export and GP_databases."""
        monkeypatch.setattr(Functions, "weekly_job_depth", 1)
        here = FakeChannel(channel_id=1)
        asyncio.run(weekly.module.sync_counters.callback(FakeContext(here), None))
        [(content, _)] = here.sent
        assert content.startswith("A weekly run is in progress")
        assert weekly.calls == []

    def test_an_unknown_guild_is_refused_before_refreshing(self, weekly):
        here = FakeChannel(channel_id=1)
        asyncio.run(weekly.module.sync_counters.callback(FakeContext(here), "aetherians"))
        assert here.sent[0][0].startswith("Unknown guild")
        assert weekly.calls == []


class TestFailureAdvice:
    """A failure inside the report is not answered with a re-run: the snapshot
    is already taken, and re-running overwrites it with later totals."""

    def test_the_scheduled_run_says_not_to_rerun_and_logs_once(self, weekly, monkeypatch, capsys):
        monkeypatch.setattr(Functions, "GP_roles", role_sync_crash)
        with freeze_time("2026-10-03 02:10:00"):
            asyncio.run(weekly.module.gp_weekly_loop.coro())
        report, advice = [content for content, _ in weekly.mod.sent]
        assert report == "**Weekly report -- week ending 10/3**"
        assert "don't re-run !GP_weekly" in advice and "503" in advice
        assert capsys.readouterr().err.count("Traceback (most recent call last)") == 1

    def test_the_manual_run_is_told_the_same(self, weekly, monkeypatch):
        monkeypatch.setattr(Functions, "GP_roles", role_sync_crash)
        here = FakeChannel(channel_id=1)
        asyncio.run(weekly.module.GP_weekly_man.callback(FakeContext(here)))
        started, advice = [content for content, _ in here.sent]
        assert started.startswith("Weekly run started")
        assert "don't re-run !GP_weekly" in advice

    def test_a_second_manual_run_is_refused_while_one_is_running(self, weekly, monkeypatch):
        monkeypatch.setattr(Functions, "weekly_job_depth", 1)
        here = FakeChannel(channel_id=1)
        asyncio.run(weekly.module.GP_weekly_man.callback(FakeContext(here)))
        assert [content for content, _ in here.sent] == ["A weekly run is already in progress."]
        assert "export" not in weekly.calls and "GP_databases" not in weekly.calls


class TestInterruptedRun:
    def test_a_run_that_never_ended_is_reported_once(self, weekly):
        asyncio.run(Functions.mark_weekly_run_started(WEEK))
        asyncio.run(weekly.module.report_interrupted_weekly_run())
        asyncio.run(weekly.module.report_interrupted_weekly_run())
        [(content, _)] = weekly.mod.sent
        assert "week ending 10/3" in content and "never finished" in content

    def test_a_run_still_in_progress_is_not_reported(self, weekly, monkeypatch):
        """on_ready fires on reconnects, mid-run included."""
        asyncio.run(Functions.mark_weekly_run_started(WEEK))
        monkeypatch.setattr(Functions, "weekly_job_depth", 1)
        asyncio.run(weekly.module.report_interrupted_weekly_run())
        assert weekly.mod.sent == []

    def test_runs_recorded_before_the_column_existed_are_not_interrupted(self, temp_functions_db):
        Functions.c.execute("CREATE TABLE weekly_runs (run_key TEXT PRIMARY KEY, started_at TEXT)")
        Functions.c.execute("INSERT INTO weekly_runs VALUES ('GP2026_09_26', '2026-09-26T02:00:00')")
        assert Functions.interrupted_weekly_run() is None
        assert asyncio.run(Functions.weekly_run_already_started('GP2026_09_26'))


class TestManualReports:
    def test_the_preview_runs_nothing(self, weekly, monkeypatch):
        async def must_not_run(*args):
            raise AssertionError("the preview touched the roles")

        def must_not_back_up():
            raise AssertionError("the preview took a backup")
        monkeypatch.setattr(Functions, "GP_roles", must_not_run)
        monkeypatch.setattr(weekly.module, "run_backup", must_not_back_up)
        weekly.sections["red:Aetherians"] = section("Aetherians -- red GP (40)")
        here = FakeChannel(channel_id=1)

        asyncio.run(weekly.module.weekly_report.callback(FakeContext(here)))
        [(content, kwargs)] = here.sent
        assert content == "**Report preview -- week ending 10/3**"
        assert kwargs['embeds'][0].footer.text is None
        assert "export" not in weekly.calls and "GP_databases" not in weekly.calls

    def test_the_preview_before_the_saturday_run_shows_last_week(self, weekly, monkeypatch):
        monkeypatch.setattr(Functions, "get_date", REAL_GET_DATE)
        monkeypatch.setattr(Functions, "snapshot_taken", lambda column: column != "GP2026_10_10")
        here = FakeChannel(channel_id=1)
        with freeze_time("2026-10-10 01:30:00"):
            asyncio.run(weekly.module.weekly_report.callback(FakeContext(here)))
        [(content, _)] = here.sent
        assert content == "**Report preview -- week ending 10/3** -- nothing to report."
        assert {day.date().isoformat() for day in weekly.dataframe_days} == {"2026-10-03"}

    def test_gp_audit_says_which_guilds_are_clean_in_the_same_message(self, weekly):
        weekly.sections["audit:Aetherians"] = section("Aetherians -- GP audit")
        here = FakeChannel(channel_id=1)
        asyncio.run(weekly.module.gp_audit.callback(FakeContext(here), None))
        [(content, kwargs)] = here.sent
        assert content == "Pretherians: nobody flagged."
        assert [embed.title for embed in kwargs['embeds']] == ["Aetherians -- GP audit"]

    def test_conflicts_for_one_clean_guild(self, weekly):
        here = FakeChannel(channel_id=1)
        asyncio.run(weekly.module.conflicts.callback(FakeContext(here), "Pretherians"))
        assert here.sent == [("Pretherians: everything is linked.", here.sent[0][1])]
        assert weekly.calls == ["links:Pretherians"]

    def test_an_unknown_guild_is_refused_before_anything_runs(self, weekly):
        here = FakeChannel(channel_id=1)
        asyncio.run(weekly.module.conflicts.callback(FakeContext(here), "aetherians"))
        [(content, _)] = here.sent
        assert content.startswith("Unknown guild 'aetherians'")
        assert weekly.calls == []


# --- re-run semantics -------------------------------------------------------

def test_a_rerun_rereads_everyones_gp_at_that_moment(tmp_path, monkeypatch):
    """!GP_weekly after a failure overwrites this week's snapshot with GP as it
    stands at the re-run, not as it stood at 2 AM. GP_databases opens
    'DatabaseLedBot.db' by relative path, hence the chdir and the check."""
    monkeypatch.chdir(tmp_path)
    assert (Path.cwd() / "DatabaseLedBot.db").resolve().parent == tmp_path.resolve()

    columns = asyncio.run(Functions.get_date())
    this_week, last_week = columns["column_name1"], columns["column_name2"]
    db = sqlite3.connect("DatabaseLedBot.db")
    for guild in logic.GUILD_NAMES:
        db.execute(f'CREATE TABLE {guild}_game ("index" INTEGER, G_NAME TEXT, G_ID TEXT, GP INTEGER)')
        db.execute(f"CREATE TABLE {guild}_GP (Name TEXT, G_ID TEXT, {last_week} TEXT)")
        db.execute(f"CREATE TABLE {guild}_GP_gained (Name TEXT, G_ID TEXT)")
        db.execute(f"INSERT INTO {guild}_game VALUES (0, 'Someone', 'A', 1000)")
        db.execute(f"INSERT INTO {guild}_GP VALUES ('Someone', 'A', '900')")
        db.execute(f"INSERT INTO {guild}_GP_gained VALUES ('Someone', 'A')")
    db.commit()

    asyncio.run(Functions.GP_databases())
    db.execute("UPDATE Aetherians_game SET GP = 1500")
    db.commit()
    asyncio.run(Functions.GP_databases())

    total = db.execute(f"SELECT {this_week} FROM Aetherians_GP").fetchone()[0]
    gained = db.execute(f"SELECT {this_week} FROM Aetherians_GP_gained").fetchone()[0]
    db.close()
    assert (int(total), int(gained)) == (1500, 600)
