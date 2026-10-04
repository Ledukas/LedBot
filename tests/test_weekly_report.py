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
        monkeypatch.setattr(Functions, "_load_gp_series", lambda guild: ({}, set()))
        monkeypatch.setattr(Functions, "_load_link_snapshot", lambda guild: ([], {}, {}))
        monkeypatch.setattr(Functions, "_load_role_holders", lambda guild: {})
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

    def get_date(today=None):
        return {"column_name1": WEEK}

    def step(name):
        async def stub(*args, **kwargs):
            calls.append(name)
        return stub

    def refresh_discord_rosters(guild_names=logic.GUILD_NAMES):
        calls.append(f"refresh_discord:{','.join(guild_names)}")
        return list(state.discord_warnings), list(guild_names)

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

    async def export(guild_names=logic.GUILD_NAMES, retry=False):
        calls.append("export")
        state.export_retry.append(retry)
        if state.export_error is not None:
            raise state.export_error

    async def gp_databases(run_key=None, db_path=None):
        calls.append("GP_databases")
        state.taken.add(run_key)

    def snapshot_status(guild, run_key, counts=True):
        status = {
            'guild': guild, 'column': run_key, 'previous': None, 'missed': [], 'present': True,
            'roster': 1, 'recorded': 1, 'zeros': 0, 'negatives': 0,
        }
        status.update(state.snapshot.get(guild, {}))
        return status

    state = SimpleNamespace(
        mod=mod, calls=calls, sections={}, dataframe_days=[], discord_warnings=[], module=LedBotCode,
        taken={WEEK}, snapshot={}, export_error=None, export_retry=[],
    )
    monkeypatch.setattr(LedBotCode, "LedukasSpam_channel", mod)
    monkeypatch.setattr(LedBotCode, "LedukasSpam_channelID", MOD_CHANNEL_ID)
    # A contended lock binds to its event loop, and each test runs its own.
    monkeypatch.setattr(LedBotCode, "roster_export_lock", asyncio.Lock())
    monkeypatch.setattr(LedBotCode, "export_game_rosters", export)
    monkeypatch.setattr(LedBotCode, "refresh_discord_rosters", refresh_discord_rosters)
    monkeypatch.setattr(LedBotCode, "run_backup", lambda: Path("Backups/Database_2026_10_03.db"))
    monkeypatch.setattr(LedBotCode, "_weekly_last_error", {})
    monkeypatch.setattr(LedBotCode, "_poll_signin_paused_until", {})
    monkeypatch.setattr(Functions, "get_date", get_date)
    monkeypatch.setattr(Functions, "GP_databases", gp_databases)
    monkeypatch.setattr(Functions, "GP_dataframe", gp_dataframe)
    monkeypatch.setattr(Functions, "GP_roles", gp_roles)
    monkeypatch.setattr(Functions, "snapshot_taken", lambda column: column in state.taken)
    monkeypatch.setattr(Functions, "snapshot_status", snapshot_status)
    monkeypatch.setattr(Functions, "red_gp_report", build("red"))
    monkeypatch.setattr(Functions, "gp_audit_report", build("audit"))
    monkeypatch.setattr(Functions, "links_report", build("links"))
    return state


def run(state, ack=None):
    # At the run's own 2 AM, so the snapshot note has no lateness in it.
    with freeze_time("2026-10-03 02:00:00"):
        asyncio.run(state.module.run_weekly_gp(ack))


async def role_sync_crash(bot, df):
    raise RuntimeError("503 Service Unavailable")


SNAPSHOT_NOTE = "Snapshot: Aetherians 1/1, Pretherians 1/1"


class TestWeeklyRun:
    def test_a_quiet_week_posts_one_line(self, weekly):
        run(weekly)
        [(content, kwargs)] = weekly.mod.sent
        assert content == (
            "**Weekly report -- week ending 10/3** -- nothing to report. "
            f"{SNAPSHOT_NOTE}. Backup: `Database_2026_10_03.db`"
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
        assert kwargs['embeds'][-1].footer.text == f"{SNAPSHOT_NOTE} · Backup: Database_2026_10_03.db"
        assert kwargs['allowed_mentions'].everyone is False

    def test_snapshot_problems_open_the_report(self, weekly):
        weekly.snapshot["Pretherians"] = {'recorded': 10, 'zeros': 9, 'roster': 10}
        weekly.sections["red:Aetherians"] = section("Aetherians -- red GP (40)")
        run(weekly)
        titles = [embed.title for embed in weekly.mod.sent[0][1]['embeds']]
        assert titles == ["Pretherians -- snapshot check", "Aetherians -- red GP (40)"]

    def test_estimated_weeks_are_explained(self, weekly):
        for guild in logic.GUILD_NAMES:
            weekly.snapshot[guild] = {'previous': "GP2026_09_19", 'missed': ["GP2026_09_26"]}
        run(weekly)
        [embed] = weekly.mod.sent[0][1]['embeds']
        assert embed.title == "Missed weeks estimated"
        assert "spread evenly over 9/26 and 10/3" in embed.description

    def test_a_crash_in_the_check_is_an_error_not_silence(self, weekly, monkeypatch):
        def broken(guild, run_key):
            raise sqlite3.OperationalError("no such table")
        monkeypatch.setattr(Functions, "snapshot_status", broken)
        with pytest.raises(weekly.module.WeeklyReportIncomplete):
            run(weekly)
        [embed] = weekly.mod.sent[0][1]['embeds']
        assert embed.title == "Snapshot check failed"
        assert embed.footer.text == "Backup: Database_2026_10_03.db"

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
        assert embed.footer.text == SNAPSHOT_NOTE

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
        refresh = "refresh_discord:Aetherians,Pretherians"
        assert calls.index("GP_databases") < calls.index(refresh) < calls.index("red:Aetherians")

    def test_its_warnings_open_the_report(self, weekly):
        weekly.discord_warnings = [logic.warning_embed("Aetherians -- role list not refreshed", "x")]
        weekly.sections["links:Aetherians"] = section("Aetherians -- links")
        run(weekly)
        titles = [embed.title for embed in weekly.mod.sent[0][1]['embeds']]
        assert titles == ["Aetherians -- role list not refreshed", "Aetherians -- links"]


class TestRosterExportLock:
    def test_the_weekly_export_waits_for_a_sync_counters_export(self, weekly):
        """A !sync_counters export awaiting Firebase at 2 AM would otherwise
        land its older roster between the weekly export and the snapshot."""
        async def scenario():
            await weekly.module.roster_export_lock.acquire()
            job = asyncio.create_task(weekly.module.run_weekly_gp())
            for _ in range(5):
                await asyncio.sleep(0)
            assert "export" not in weekly.calls
            weekly.module.roster_export_lock.release()
            await job
        asyncio.run(scenario())
        assert weekly.calls.index("export") < weekly.calls.index("GP_databases")

    def test_a_refresh_crash_after_the_snapshot_is_not_a_rerun(self, weekly, monkeypatch):
        def broken(guild_names=logic.GUILD_NAMES):
            raise AttributeError("roles")
        monkeypatch.setattr(weekly.module, "refresh_discord_rosters", broken)
        weekly.sections["red:Aetherians"] = section("Aetherians -- red GP (40)")
        with pytest.raises(weekly.module.WeeklyReportIncomplete):
            run(weekly)
        titles = [embed.title for embed in weekly.mod.sent[0][1]['embeds']]
        assert titles == ["Role lists not refreshed", "Aetherians -- red GP (40)"]


class TestSyncCounters:
    def test_refreshes_both_sides_then_posts_one_message(self, weekly):
        weekly.sections["links:Aetherians"] = section("Aetherians -- links")
        here = FakeChannel(channel_id=1)
        asyncio.run(weekly.module.sync_counters.callback(FakeContext(here), None))
        assert weekly.calls[:3] == ["export", "export", "refresh_discord:Aetherians,Pretherians"]
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
        *failures, links = kwargs['embeds']
        assert [f.title for f in failures] == [
            f"{name} -- in-game roster not refreshed" for name in logic.GUILD_NAMES
        ]
        assert "The Pretherians in-game lists below are from the previous roster." in failures[1].description
        assert links.title == "Pretherians -- links"
        assert "refresh_discord:Aetherians,Pretherians" in weekly.calls

    def test_only_the_guild_that_failed_is_called_stale(self, weekly, monkeypatch):
        async def export(guild_names):
            if guild_names == ["Pretherians"]:
                raise RuntimeError("timed out")
        monkeypatch.setattr(weekly.module, "export_game_rosters", export)
        here = FakeChannel(channel_id=1)
        asyncio.run(weekly.module.sync_counters.callback(FakeContext(here), None))
        titles = [embed.title for embed in here.sent[0][1]['embeds']]
        assert titles == ["Pretherians -- in-game roster not refreshed"]

    def test_one_guild_refreshes_only_that_guild(self, weekly, monkeypatch):
        exported = []

        async def export(guild_names):
            exported.extend(guild_names)
        monkeypatch.setattr(weekly.module, "export_game_rosters", export)
        here = FakeChannel(channel_id=1)
        asyncio.run(weekly.module.sync_counters.callback(FakeContext(here), "Pretherians"))
        assert exported == ["Pretherians"]
        assert "refresh_discord:Pretherians" in weekly.calls
        assert not any(call.endswith(":Aetherians") for call in weekly.calls)

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
        weekly.taken.clear()
        with freeze_time("2026-10-03 02:10:00"):
            asyncio.run(weekly.module.gp_weekly_loop.coro())
        report, advice = [content for content, _ in weekly.mod.sent]
        assert report == "**Weekly report -- week ending 10/3**"
        assert "don't re-run !GP_weekly" in advice and "503" in advice
        assert capsys.readouterr().err.count("Traceback (most recent call last)") == 1

    def test_the_manual_run_is_told_the_same(self, weekly, monkeypatch):
        monkeypatch.setattr(Functions, "GP_roles", role_sync_crash)
        here = FakeChannel(channel_id=1)
        with freeze_time("2026-10-03 10:00:00"):
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


def tick(weekly, when):
    with freeze_time(when):
        asyncio.run(weekly.module.gp_weekly_loop.coro())


class TestScheduledRetries:
    """From 2 AM Saturday the loop runs the job until the snapshot is taken:
    every 10 minutes until 4 AM, every 30 until midnight."""

    def test_a_failed_first_attempt_is_posted_with_retry_advice(self, weekly):
        weekly.taken.clear()
        weekly.export_error = RuntimeError("Firebase unavailable")
        tick(weekly, "2026-10-03 02:00:00")
        [(content, _)] = weekly.mod.sent
        assert "Firebase unavailable" in content and "retry every 10 minutes" in content

    def test_a_retry_runs_ten_minutes_later_not_five(self, weekly):
        weekly.taken.clear()
        weekly.export_error = RuntimeError("Firebase unavailable")
        tick(weekly, "2026-10-03 02:00:00")
        weekly.export_error = None
        tick(weekly, "2026-10-03 02:05:00")
        assert weekly.calls.count("export") == 1
        tick(weekly, "2026-10-03 02:10:00")
        assert "GP_databases" in weekly.calls
        assert weekly.mod.sent[-1][0].startswith("**Weekly report -- week ending 10/3**")

    def test_a_retry_failure_is_only_logged(self, weekly, capsys):
        weekly.taken.clear()
        weekly.export_error = RuntimeError("Firebase unavailable")
        tick(weekly, "2026-10-03 02:00:00")
        tick(weekly, "2026-10-03 02:10:00")
        assert weekly.calls.count("export") == 2
        assert len(weekly.mod.sent) == 1
        assert capsys.readouterr().err.count("weekly GP job failed") == 2

    def test_retries_sign_in_as_retries(self, weekly):
        """So a rejected password pauses them, instead of each retry adding a
        failed login to the guild account."""
        weekly.taken.clear()
        weekly.export_error = RuntimeError("Firebase unavailable")
        tick(weekly, "2026-10-03 02:00:00")
        tick(weekly, "2026-10-03 02:10:00")
        assert weekly.export_retry == [False, True]

    def test_a_first_failure_after_four_says_every_thirty_minutes(self, weekly):
        weekly.taken.clear()
        weekly.export_error = RuntimeError("Firebase unavailable")
        tick(weekly, "2026-10-03 11:00:00")
        [(content, _)] = weekly.mod.sent
        assert "retry every 30 minutes until midnight" in content

    def test_the_sections_read_the_snapshots_own_week(self, weekly):
        """A run can cross midnight into a Saturday; the sections must not
        switch to the new week's columns."""
        with freeze_time("2026-10-10 00:05:00"):
            asyncio.run(weekly.module.run_weekly_gp(run_key=WEEK))
        assert {day.date().isoformat() for day in weekly.dataframe_days} == {"2026-10-03"}

    def test_a_late_success_says_how_late(self, weekly):
        """The bot was down at 2 AM and came back at 9."""
        weekly.taken.clear()
        tick(weekly, "2026-10-03 09:00:00")
        [(content, _)] = weekly.mod.sent
        assert f"{SNAPSHOT_NOTE} -- taken 7h late." in content

    def test_nothing_runs_while_a_job_is_running(self, weekly, monkeypatch):
        weekly.taken.clear()
        monkeypatch.setattr(Functions, "weekly_job_depth", 1)
        tick(weekly, "2026-10-03 02:00:00")
        assert weekly.calls == [] and weekly.mod.sent == []


class TestSnapshotReminder:
    """After the Saturday retries, a missing snapshot is a daily reminder until
    the next Saturday's run spreads the gap."""

    def test_nothing_runs_on_sunday_and_the_reminder_fires_daily(self, weekly):
        weekly.taken.clear()
        tick(weekly, "2026-10-04 00:05:00")
        tick(weekly, "2026-10-04 12:00:00")
        assert weekly.calls == []
        [(content, _)] = weekly.mod.sent
        assert content.startswith("No GP snapshot was taken for the week ending 10/3.")
        assert "The bot wasn't running on Saturday." in content
        tick(weekly, "2026-10-05 00:05:00")
        assert len(weekly.mod.sent) == 2

    def test_it_carries_the_last_error(self, weekly):
        weekly.taken.clear()
        weekly.export_error = RuntimeError("Firebase unavailable")
        tick(weekly, "2026-10-03 23:40:00")
        tick(weekly, "2026-10-04 00:05:00")
        assert "the last error: Firebase unavailable" in weekly.mod.sent[-1][0]

    def test_it_says_when_the_login_was_rejected(self, weekly):
        weekly.taken.clear()
        weekly.module._poll_signin_paused_until["Aetherians"] = float('inf')
        asyncio.run(Functions.mark_weekly_run_started(WEEK))
        tick(weekly, "2026-10-04 00:05:00")
        assert "The guild login was rejected" in weekly.mod.sent[-1][0]

    def test_a_restart_does_not_repost_it(self, weekly, temp_functions_db, monkeypatch):
        weekly.taken.clear()
        tick(weekly, "2026-10-04 00:05:00")
        restarted = sqlite3.connect(str(temp_functions_db))
        monkeypatch.setattr(Functions, "conn", restarted)
        monkeypatch.setattr(Functions, "c", restarted.cursor())
        monkeypatch.setattr(weekly.module, "_weekly_last_error", {})
        try:
            tick(weekly, "2026-10-04 00:10:00")
        finally:
            restarted.close()
        assert len(weekly.mod.sent) == 1


class TestManualWeeklyRun:
    def test_refused_midweek(self, weekly):
        here = FakeChannel(channel_id=1)
        with freeze_time("2026-10-07 12:00:00"):
            asyncio.run(weekly.module.GP_weekly_man.callback(FakeContext(here)))
        [(content, _)] = here.sent
        assert content.startswith("!GP_weekly only runs on Saturdays")
        assert "overwrite Saturday's snapshot" in content
        assert weekly.calls == []

    def test_a_refusal_is_not_a_weekly_job(self, weekly):
        """Entering weekly_job() would discard an in-flight invite-poll roster
        read and turn !sync_counters away while the refusal is sent."""
        depths = []

        class Watching(FakeChannel):
            async def send(self, content=None, **kwargs):
                depths.append(Functions.weekly_job_depth)
                await super().send(content, **kwargs)

        generation = Functions.roster_generation
        with freeze_time("2026-10-07 12:00:00"):
            asyncio.run(weekly.module.GP_weekly_man.callback(FakeContext(Watching(channel_id=1))))
        assert depths == [0]
        assert Functions.roster_generation == generation

    def test_force_runs_midweek_and_the_note_says_how_late(self, weekly):
        here = FakeChannel(channel_id=1)
        with freeze_time("2026-10-07 12:00:00"):
            asyncio.run(weekly.module.GP_weekly_man.callback(FakeContext(here), "force"))
        assert "GP_databases" in weekly.calls
        assert "taken 4 days late" in weekly.mod.sent[0][0]

    def test_saturday_needs_no_force(self, weekly):
        here = FakeChannel(channel_id=1)
        with freeze_time("2026-10-03 15:00:00"):
            asyncio.run(weekly.module.GP_weekly_man.callback(FakeContext(here)))
        assert "GP_databases" in weekly.calls

    def test_an_unknown_option_gets_usage(self, weekly):
        here = FakeChannel(channel_id=1)
        asyncio.run(weekly.module.GP_weekly_man.callback(FakeContext(here), "now"))
        assert here.sent[0][0] == "Usage: !GP_weekly [force]"
        assert weekly.calls == []

    def test_it_holds_the_job_counter_before_its_first_await(self, weekly):
        """Otherwise a retry tick could start a second cycle while the
        "started" reply is in flight."""
        depths = []

        class Watching(FakeChannel):
            async def send(self, content=None, **kwargs):
                depths.append(Functions.weekly_job_depth)
                await super().send(content, **kwargs)

        with freeze_time("2026-10-03 15:00:00"):
            asyncio.run(weekly.module.GP_weekly_man.callback(FakeContext(Watching(channel_id=1))))
        assert depths[0] >= 1

    def test_a_failure_says_why_and_whether_the_week_was_saved(self, weekly):
        weekly.taken.clear()
        weekly.export_error = RuntimeError("Firebase unavailable")
        here = FakeChannel(channel_id=1)
        with freeze_time("2026-10-03 15:00:00"):
            asyncio.run(weekly.module.GP_weekly_man.callback(FakeContext(here)))
        assert here.sent[-1][0] == (
            "Weekly run failed: Firebase unavailable. Nothing was saved for this week."
        )


class TestMembersGame:
    def test_refused_while_a_weekly_run_is_going(self, weekly, monkeypatch):
        """It writes {guild}_game, which the snapshot check reads after the
        export lock is released."""
        monkeypatch.setattr(Functions, "weekly_job_depth", 1)
        here = FakeChannel(channel_id=1)
        asyncio.run(weekly.module.members_guild.callback(FakeContext(here)))
        assert here.sent[0][0].startswith("A weekly run is in progress")
        assert weekly.calls == []


class TestInterruptedRun:
    def test_a_run_that_never_ended_is_reported_once(self, weekly):
        asyncio.run(Functions.mark_weekly_run_started(WEEK))
        asyncio.run(weekly.module.report_interrupted_weekly_run())
        asyncio.run(weekly.module.report_interrupted_weekly_run())
        [(content, _)] = weekly.mod.sent
        assert "week ending 10/3" in content and "never finished" in content

    def test_a_retry_that_died_again_is_logged_not_posted(self, weekly, capsys):
        """A crash on every attempt would otherwise post a notice per restart,
        up to one every ten minutes all Saturday."""
        weekly.taken.clear()
        asyncio.run(Functions.mark_weekly_run_started(WEEK))
        Functions.mark_weekly_runs_ended()
        asyncio.run(Functions.mark_weekly_run_started(WEEK))
        with freeze_time("2026-10-03 03:00:00"):
            asyncio.run(weekly.module.report_interrupted_weekly_run())
        assert weekly.mod.sent == []
        assert "interrupted again (attempt 2)" in capsys.readouterr().err
        assert Functions.interrupted_weekly_run() is None

    def test_no_promise_of_retries_after_a_rejected_login(self, weekly):
        weekly.taken.clear()
        weekly.module._poll_signin_paused_until["Aetherians"] = float('inf')
        asyncio.run(Functions.mark_weekly_run_started(WEEK))
        with freeze_time("2026-10-03 03:00:00"):
            asyncio.run(weekly.module.report_interrupted_weekly_run())
        [(content, _)] = weekly.mod.sent
        assert "retrying" not in content

    @pytest.mark.parametrize("taken,now,expected", [
        (True, "2026-10-03 03:00:00", "The snapshot was saved"),
        (False, "2026-10-03 03:00:00", "I'll keep retrying until midnight"),
        (False, "2026-10-05 09:00:00", "Next Saturday's run will spread"),
    ])
    def test_it_says_what_happens_next(self, weekly, taken, now, expected):
        if not taken:
            weekly.taken.clear()
        asyncio.run(Functions.mark_weekly_run_started(WEEK))
        with freeze_time(now):
            asyncio.run(weekly.module.report_interrupted_weekly_run())
        [(content, _)] = weekly.mod.sent
        assert expected in content

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
        assert Functions.weekly_run_times('GP2026_09_26')[0] is not None


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

    columns = Functions.get_date()
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
