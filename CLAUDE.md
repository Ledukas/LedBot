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
- **`tests/`** — pytest suite covering `logic.py`, `Functions.get_date()`, and `scripts/backup_db.py`; `tests/conftest.py` has the shared fixtures. Run via `pytest` from the repo root (`pytest.ini` sets `pythonpath = .`).

### Per-guild duplication pattern

Almost every piece of guild-related logic is duplicated per IdleOn guild ("Aetherians" and "Pretherians") rather than generalized, both in code (parallel `if IOguild == "Aetherians"` / `"Pretherians"` branches) and in SQLite table naming: each guild has its own `{GuildName}_members`, `{GuildName}_discord`, `{GuildName}_game`, `{GuildName}_GP`, and `{GuildName}_GP_gained` tables, built with Python f-strings (`table_name_members = IOguild+'_members'`, etc.) rather than a schema/ORM. When touching guild-related code, expect to mirror changes for both guild names, and check the sync/promotion/kick commands in `LedBotCode.py` and the role/GP functions in `Functions.py` accordingly. GP role thresholds (`role_1_knight` ... `role_7_true` / `GProles`) are also duplicated as module-level globals in both `LedBotCode.py` and `Functions.py`.

### Weekly GP cycle

GP tracking is snapshot-based, keyed by date-stamped columns (`GP{year}_{month}_{day}` for the most recent Saturday, computed in `Functions.get_date`):
1. `members_guild`/`Functions.GP_export` pulls current in-game GP totals from Firebase into `{Guild}_game`.
2. `Functions.GP_databases` adds this week's date column to `{Guild}_GP` (running total) and `{Guild}_GP_gained` (delta from the prior snapshot), backfilling new members.
3. `Functions.GP_roles` reassigns Aetherian rank roles based on current GP and manages the "Monthly Top" ("clammy") role based on 4-week rolling average GP.
4. `Functions.red_gp` reports members below a per-guild GP-gained threshold (400 for Aetherians, 140 for Pretherians) to the mod channel.
5. `scripts/backup_db.run_backup()` takes a timestamped DB snapshot, confirmed back to the mod channel.

This runs automatically via `gp_weekly_loop` (a `discord.ext.tasks.loop` ticking daily at 2 AM, gated to only act on Saturdays — deliberately `tasks.loop` rather than a hand-rolled `while True` + `asyncio.sleep` loop, since the latter used to get duplicated on every gateway reconnect and caused the weekly report to occasionally send twice) or manually via the `!GP_weekly` command (Moderator-only). Both call the same `run_weekly_gp(ack_channel)`; `ack_channel` only decides where the "GP exported" acknowledgement goes, while the reports themselves always go to the mod channel.

### Hardcoded IDs

The main Discord guild ID (`809954021028134943`) and several role/channel IDs are hardcoded inline throughout `LedBotCode.py` and `Functions.py` rather than pulled from `.env` (unlike `SPAM_CHANNEL_ID`, which is). Be aware of this when reading code that looks like it should be configurable but isn't.

### Permissions

Nearly all commands are gated with `@commands.has_role("Moderator")` or `@commands.has_any_role(...)`. There's no slash-command permission model in use despite `discord.app_commands` being imported — commands are plain prefix commands (`!`).
