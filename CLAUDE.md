# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

LedBot is a single-purpose Discord bot (discord.py) for managing two IdleOn game guilds, "Aetherians" and "Pretherians". It tracks each guild's Discord membership against in-game guild membership pulled from IdleOn's Firebase backend, computes weekly GP (guild points) gains, assigns GP-based Discord roles, and runs a few utility commands (giveaways, weekly boss strategy lookup, member invites/kicks). Deployed on a Raspberry Pi via systemd (see `DEPLOY.md`); mods interact with it purely through Discord commands and never need shell/SSH access to the Pi.

## Running the bot / running tests

```
python LedBotCode.py                              # run the bot (needs .env + service_account.json, see below)
pip install -r requirements-dev.txt && pytest      # run the test suite (pytest + freezegun, see pytest.ini)
```

`requirements.txt` is the bot's actual runtime dependencies (install these on the Pi); `requirements-dev.txt` is test-only tooling that has no business being installed there. There's no linter/build step.

Requires a `.env` file in the repo root (not committed) with at minimum:

```
TOKEN=                 # Discord bot token
EMAIL_A=                # login email for the Aetherians in-game guild
EMAIL_P=                # login email for the Pretherians in-game guild
PASSWORD=                # shared IdleOn/Firebase login password
FIRE_API=                # Firebase apiKey for pyrebase
GOOGLESHEETS_API=        # Google Sheets API key (used with an API-key-only client)
SPAM_CHANNEL_ID=        # Discord channel ID the bot posts weekly reports/pings to
WB_SPREADSHEET_ID=        # Google Sheet ID for weekly boss strategy
WB_SHEET_NAME=
WB_SPREADSHEET_URL=
```

A `service_account.json` (Google service account credentials, scoped to `spreadsheets.readonly`) must also be present in the repo root — it's used for the authenticated Sheets read in `Functions.get_cell_value`, separate from the API-key-only client built at module load in `Functions.py`.

State lives in `DatabaseLedBot.db` (SQLite; gitignored, not committed — it holds real member data) and `winners.json` (giveaway winner history, also gitignored). There's no migration tooling — tables are created/altered ad hoc by the code at runtime (see `Functions.GP_databases`, which does `ALTER TABLE ... ADD COLUMN` for each new weekly GP snapshot). `scripts/backup_db.py` takes timestamped snapshots into `Backups/` (gitignored) using SQLite's online backup API; it's triggered automatically at the end of the weekly GP job (see below) and on-demand via `!backup`, not on any independent schedule — the data only changes weekly, so a separate always-on backup schedule would mostly copy unchanged data.

## Architecture

- **`LedBotCode.py`** — entry point. Builds the `commands.Bot`, defines all top-level `!`-prefixed commands, the weekly/scheduled background tasks, and error handlers. Loads cogs from `cogs/` on `on_ready`. `bot.run(TOKEN)` is guarded under `if __name__ == "__main__":` specifically so the module can be safely `import`ed (by tests, or anything else) without trying to connect to Discord as a side effect.
- **`Functions.py`** — the data/business-logic layer used by both `LedBotCode.py` and the cogs: Google Sheets access, GP dataframe construction, role assignment logic, and the Firebase (`pyrebase`) export of in-game guild member data. Opens its own module-level `sqlite3` connection separate from the one in `LedBotCode.py`, and builds a live Google Sheets API client + loads `service_account.json` at import time.
- **`logic.py`** — pure, side-effect-free functions extracted from `LedBotCode.py`/`Functions.py`/`cogs/giveaway.py` specifically so they're unit-testable without mocking Discord/DB/network (see its module docstring for the full list and rationale). When adding new business logic that doesn't inherently need Discord/DB/network access, prefer putting the pure computation here and calling it from the I/O-coupled caller, rather than writing it inline — that's the established pattern now, not a one-off refactor.
- **`cogs/`** — discord.py extensions loaded dynamically at startup (any `.py` file in the directory). Currently just `giveaway.py`. Cogs open their own `sqlite3` connections rather than sharing the one from `LedBotCode.py`/`Functions.py`.
- **`scripts/backup_db.py`** — standalone (no Discord/bot dependency) database backup utility; see `DEPLOY.md`.
- **`scripts/gp_audit.py`** — standalone GP audit; prints the same report `!gp_audit` posts. See "GP audit" below.
- **`tests/`** — pytest suite covering `logic.py`, `Functions.get_date()`, and `scripts/backup_db.py`; `tests/conftest.py` has the shared fixtures. Run via `pytest` from the repo root (`pytest.ini` sets `pythonpath = .`).

### Per-guild configuration

Each IdleOn guild ("Aetherians" and "Pretherians") owns its own SQLite tables, named `{GuildName}_members`, `{GuildName}_discord`, `{GuildName}_game`, `{GuildName}_GP` and `{GuildName}_GP_gained`. There is no schema or ORM -- the names are built by string interpolation -- but every call site goes through `logic.table_name(guild, kind)`, which validates both halves. Use it rather than writing `IOguild + '_members'` inline: a table name cannot be a bound SQL parameter, so the guild name (which arrives from whatever a moderator typed) is interpolated directly into the query, and that validation is the only thing keeping arbitrary input out of it.

The guild list and ids live in `logic.GUILD_NAMES` / `logic.GUILD_GIDS`, and GP rank thresholds in `logic.RANK_THRESHOLDS` (with `RANK_ROLE_NAMES` and `GP_THRESHOLDS` derived from it). Guild-related code loops over `logic.GUILD_NAMES` rather than branching on the literal strings. Two genuine asymmetries between the guilds are named constants so they read as domain facts: `logic.RANK_ROLE_GUILD` (only Aetherians has the GP rank ladder and Monthly Top role) and `logic.PROMOTION_GUILD` (promotions run Pretherians into Aetherians). The only remaining guild literals are the two `.env` email mappings in `LedBotCode.py` and `Functions.GP_export`, where the environment variable names are per-guild by nature.

This was previously duplicated throughout -- parallel `if IOguild == "Aetherians"` branches, f-string table names at 16 sites, and GP thresholds defined separately in both `LedBotCode.py` and `Functions.py`. If you are adding guild-related logic, add it to the shared config rather than reintroducing a branch.

### Weekly GP cycle

GP tracking is snapshot-based, keyed by date-stamped columns (`GP{year}_{month}_{day}` for the most recent Saturday, computed in `Functions.get_date`):
1. `members_guild`/`Functions.GP_export` pulls current in-game GP totals from Firebase into `{Guild}_game`.
2. `Functions.GP_databases` adds this week's date column to `{Guild}_GP` (running total) and `{Guild}_GP_gained` (delta from the prior snapshot), backfilling new members.
3. `Functions.GP_roles` reassigns Aetherian rank roles based on current GP and manages the "Monthly Top" ("clammy") role based on 4-week rolling average GP.
4. `Functions.red_gp` reports members below a per-guild GP-gained threshold (400 for Aetherians, 140 for Pretherians) to the mod channel.
5. `Functions.gp_audit` reports the opposite end -- members gaining implausibly much -- to the mod channel. See below.
6. `scripts/backup_db.run_backup()` takes a timestamped DB snapshot, confirmed back to the mod channel.

This runs automatically via `gp_weekly_loop` (a `discord.ext.tasks.loop` ticking every 15 minutes and acting only in the 2 AM hour of a local-time Saturday, per `logic.should_run_weekly_gp`) or manually via the `!GP_weekly` command (Moderator-only). Two details are deliberate and easy to undo by accident. It is `tasks.loop` rather than a hand-rolled `while True` + `asyncio.sleep` loop, because the latter got duplicated on every gateway reconnect and made the weekly report send twice. And it polls local time rather than using `tasks.loop(time=...)`: a naive `datetime.time` there is interpreted as **UTC** by discord.py, while this gate and `Functions.get_date()` both work in local time, so on any non-UTC host the two disagree and the job runs hours from 2 AM. The 15-minute poll means several ticks fall inside the 2 AM hour, so the week is marked as started in a `weekly_runs` table (`Functions.mark_weekly_run_started` / `weekly_run_already_started`, keyed on this week's snapshot column name) and `should_run_weekly_gp` reads that flag. The marker is **in the database, not in memory**: the bot restarts, systemd runs it with `Restart=on-failure`, and an in-memory flag is lost exactly when it matters — a crash during the 2 AM job would restart inside the same hour and run the whole cycle again. `run_weekly_gp` writes the marker itself, before doing any work, so the manual `!GP_weekly` also suppresses the scheduled run, and a repeatable failure cannot become a restart loop (a failed run is reported to the mod channel for a moderator to re-run). The loop body also catches and reports its own exceptions, because `tasks.loop` stops permanently after an unhandled one. Both call the same `run_weekly_gp(ack_channel)`; `ack_channel` only decides where the "GP exported" acknowledgement goes, while the reports themselves always go to the mod channel.

### GP audit (cheat detection)

`Functions.gp_audit` flags members whose weekly gains are worth a moderator's attention, using two independent signals in `logic.filter_gp_audit`. It runs as part of the weekly cycle, on demand via `!gp_audit [guild]` (Moderator), and from a shell via `scripts/gp_audit.py`, which prints the Discord message verbatim.

The thresholds are absolute, not percentile-based: cheating is bounded by what the game physically allows, so a casual guild's top players are not suspicious merely for leading a casual guild. All four live in `logic.py` and were derived from the full history rather than guessed:

- **`GP_TASK_CAP = 680`** -- where the weekly tasks top out. It is the most common non-zero gain in the database by a factor of two over its neighbours (2311 weeks, against 470 at 690), because everyone who simply finishes the week lands exactly on it. Every comparison is therefore **strictly** greater; `>=` anywhere near this number reports most of both guilds.
- **`GP_GAIN_BAR = 750`** -- the bar a week must clear to count. Above the cap rather than on it, since ordinary variation carries people past 680.
- **`GP_SPIKE_WEEK = 1200`** -- a single week big enough to report alone. Also a common exact value (71 weeks across 28 members), so landing on it is not the unusual part; going past it is. Repeatedly landing on it is caught by the sustained count instead, since every such week clears `GP_GAIN_BAR`.
- **`GP_IMPLAUSIBLE_WEEK = 5000`** -- a data error, not a player. The real record is 2410.

The two signals are deliberately both present and neither subsumes the other. **Sustained** (above `GP_GAIN_BAR` in 3 of the last 8 weeks) counts *across* the window rather than consecutively, because gains collapse during in-game events and a streak requirement lets anyone through on one quiet week. **Spike** (newest week alone above `GP_SPIKE_WEEK`) exists because a lone enormous week between two ordinary ones never accumulates hits -- the member whose record 2410 week prompted this sat at 2 of 6 under the sustained rule alone. Historically the pair fires on about 4-5 members a week across both guilds.

Two details that look incidental but are not. `_load_gp_series` joins and labels on `{guild}_game`, not on the GP table: joining by `G_ID` is what survives a character rename, and the GP tables keep whatever name a member had when `GP_databases` first inserted their row, so reporting that name would send a moderator looking for somebody who no longer exists under it. And `logic.observed_weeks` drops each member's first recorded week, because `GP_databases` computes a gain against a missing previous snapshot as `GPnow - 0` -- that week is their lifetime GP, not a week's worth.

The SQL in `scripts/gp_audit.py` duplicates `Functions._load_gp_series` on purpose. Everything that *decides* anything is in `logic.py` and shared, but importing `Functions` builds a live Google Sheets client and reads `.env` at import time, which would cost the script its one useful property: running against a database copy on any machine with no credentials.

This is an investigation queue, not a verdict -- the report always prints the weeks behind a name so a moderator judges the data, and it goes to the mod channel only.

### Hardcoded IDs

The main Discord guild ID (`809954021028134943`) and several role/channel IDs are hardcoded inline throughout `LedBotCode.py` and `Functions.py` rather than pulled from `.env` (unlike `SPAM_CHANNEL_ID`, which is). Be aware of this when reading code that looks like it should be configurable but isn't.

### Permissions

Nearly all commands are gated with `@commands.has_role("Moderator")` or `@commands.has_any_role(...)`. There's no slash-command permission model in use despite `discord.app_commands` being imported — commands are plain prefix commands (`!`).
