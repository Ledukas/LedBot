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
FIRE_API=                # Firebase web apiKey, used for the REST sign-in
GOOGLESHEETS_API=        # Google Sheets API key (used with an API-key-only client)
SPAM_CHANNEL_ID=        # Discord channel ID the bot posts weekly reports/pings to
WB_SPREADSHEET_ID=        # Google Sheet ID for weekly boss strategy
WB_SHEET_NAME=
WB_SPREADSHEET_URL=
WELCOME_POST_CHANNEL_ID=  # optional: #aether-chair, where the invite auto-link posts welcomes
WELCOME_INFO_CHANNEL_ID=  # optional: the #welcome channel the welcome links to
ROLES_CHANNEL_ID=         # optional: the #roles channel the welcome links to
```

The three welcome ids are parsed with `logic.parse_optional_id`, so a missing one skips the welcome (with a note in the mod channel) instead of crashing at import the way `int(os.environ.get('SPAM_CHANNEL_ID'))` would.

A `service_account.json` (Google service account credentials, scoped to `spreadsheets.readonly`) must also be present in the repo root — it's used for the authenticated Sheets read in `Functions.get_cell_value`, separate from the API-key-only client built at module load in `Functions.py`.

State lives in `DatabaseLedBot.db` (SQLite; gitignored, not committed — it holds real member data) and `winners.json` (giveaway winner history, also gitignored). There's no migration tooling — tables are created/altered ad hoc by the code at runtime (see `Functions.GP_databases`, which does `ALTER TABLE ... ADD COLUMN` for each new weekly GP snapshot). `scripts/backup_db.py` takes timestamped snapshots into `Backups/` (gitignored) using SQLite's online backup API; it's triggered automatically at the end of the weekly GP job (see below) and on-demand via `!backup`, not on any independent schedule — the data only changes weekly, so a separate always-on backup schedule would mostly copy unchanged data.

## Architecture

- **`LedBotCode.py`** — entry point. Builds the `commands.Bot`, defines all top-level `!`-prefixed commands, the weekly/scheduled background tasks, and error handlers. Loads cogs from `cogs/` on `on_ready`. `bot.run(TOKEN)` is guarded under `if __name__ == "__main__":` specifically so the module can be safely `import`ed (by tests, or anything else) without trying to connect to Discord as a side effect.
- **`Functions.py`** — the data/business-logic layer used by both `LedBotCode.py` and the cogs: Google Sheets access, GP dataframe construction, role assignment logic, and writing the in-game roster to `{Guild}_game` (`write_game_roster`). The roster itself is read by `LedBotCode.fetch_guild_roster` over Firebase REST. Opens its own module-level `sqlite3` connection separate from the one in `LedBotCode.py`, and builds a live Google Sheets API client + loads `service_account.json` at import time.
- **`logic.py`** — pure, side-effect-free functions extracted from `LedBotCode.py`/`Functions.py`/`cogs/giveaway.py` specifically so they're unit-testable without mocking Discord/DB/network (see its module docstring for the full list and rationale). When adding new business logic that doesn't inherently need Discord/DB/network access, prefer putting the pure computation here and calling it from the I/O-coupled caller, rather than writing it inline — that's the established pattern now, not a one-off refactor.
- **`cogs/`** — discord.py extensions loaded dynamically at startup (any `.py` file in the directory). Currently just `giveaway.py`. Cogs open their own `sqlite3` connections rather than sharing the one from `LedBotCode.py`/`Functions.py`.
- **`scripts/backup_db.py`** — standalone (no Discord/bot dependency) database backup utility; see `DEPLOY.md`.
- **`scripts/gp_audit.py`** — standalone GP audit; prints the same report `!gp_audit` posts. See "GP audit" below.
- **`tests/`** — pytest suite covering `logic.py`, `scripts/backup_db.py`, and the database-backed parts of `Functions.py` (plus the invite auto-link's Discord side, with stub objects); `tests/conftest.py` has the shared fixtures. Run via `pytest` from the repo root (`pytest.ini` sets `pythonpath = .`). Tests that touch the database use the `temp_functions_db` fixture, which refuses to run if `sys.modules['Functions']` has been swapped -- a test that pops modules without restoring them once made the weekly-marker tests write to the real `DatabaseLedBot.db`. Use `monkeypatch.delitem(sys.modules, ...)`, never a bare `pop`.

### Per-guild configuration

Each IdleOn guild ("Aetherians" and "Pretherians") owns its own SQLite tables, named `{GuildName}_members`, `{GuildName}_discord`, `{GuildName}_game`, `{GuildName}_GP` and `{GuildName}_GP_gained`. There is no schema or ORM -- the names are built by string interpolation -- but every call site goes through `logic.table_name(guild, kind)`, which validates both halves. Use it rather than writing `IOguild + '_members'` inline: a table name cannot be a bound SQL parameter, so the guild name (which arrives from whatever a moderator typed) is interpolated directly into the query, and that validation is the only thing keeping arbitrary input out of it.

The guild list and ids live in `logic.GUILD_NAMES` / `logic.GUILD_GIDS`, and GP rank thresholds in `logic.RANK_THRESHOLDS` (with `RANK_ROLE_NAMES` and `GP_THRESHOLDS` derived from it). Guild-related code loops over `logic.GUILD_NAMES` rather than branching on the literal strings. Two genuine asymmetries between the guilds are named constants so they read as domain facts: `logic.RANK_ROLE_GUILD` (only Aetherians has the GP rank ladder and Monthly Top role) and `logic.PROMOTION_GUILD` (promotions run Pretherians into Aetherians). The only remaining guild literal is the `.env` email mapping in `LedBotCode.py` (`guild_emails`), where the environment variable names are per-guild by nature.

This was previously duplicated throughout -- parallel `if IOguild == "Aetherians"` branches, f-string table names at 16 sites, and GP thresholds defined separately in both `LedBotCode.py` and `Functions.py`. If you are adding guild-related logic, add it to the shared config rather than reintroducing a branch.

### Weekly GP cycle

GP tracking is snapshot-based, keyed by date-stamped columns (`GP{year}_{month}_{day}` for the most recent Saturday, computed in `Functions.get_date`):
1. `export_game_rosters` pulls current in-game GP totals from Firebase (`fetch_guild_roster`) into `{Guild}_game` (`Functions.write_game_roster`).
2. `Functions.GP_databases` adds this week's date column to `{Guild}_GP` (running total) and `{Guild}_GP_gained` (delta from the prior snapshot), backfilling new members.
3. `Functions.GP_roles` reassigns Aetherian rank roles based on current GP and manages the "Monthly Top" ("clammy") role based on 4-week rolling average GP.
4. `Functions.red_gp_report` lists members below a per-guild GP-gained threshold (400 for Aetherians, 140 for Pretherians), under their current `_game` names -- the GP tables' names are frozen at a member's first snapshot.
5. `Functions.gp_audit_report` lists the opposite end -- members gaining implausibly much. See below.
6. `Functions.conflicts_report` lists broken Discord-to-character links **and in-game characters with no link at all**. The unlinked list is the backstop for anyone the invite auto-link didn't catch. See below.
7. `scripts/backup_db.run_backup()` takes a timestamped DB snapshot.

**The output is one report**, posted to the mod channel at the end as embeds (`send_embeds`, packed by `logic.pack_messages` into as few messages as Discord's limits allow -- normally one). Steps 4-6 are builders that return embeds, `[]` when there is nothing to say, so a section with nothing in it simply doesn't appear; success messages ("GP exported", "roles fixed", "backup created") are gone. But the report is **never silent**: a quiet week still posts its heading with "nothing to report", and the backup's name rides in the last embed's footer (or on the heading). Without that, a quiet week and a job that never ran -- bot down at 2 AM, mod channel not cached -- would look identical. The red list is the one unbounded section (an event week can put most of a guild on it), so `logic.format_red_gp` splits it across embeds rather than truncating; everything else goes through `logic.report_embed`, which cuts an over-long body as a safety net. `send_embeds` checks for **Embed Links** first: without it Discord drops the embeds and posts a bare heading, so it says so instead.

**One failure doesn't cost the rest of the report.** `collect_guild_report` turns a failed `GP_dataframe` or `GP_roles` into an error embed and carries on with the sections that don't depend on it (Aetherians runs first, so a role-sync error used to lose Pretherians' whole report). The first such exception is re-raised *after* the report is posted, so the failure message below still fires. A failed builder returns an error embed, never `[]` -- otherwise a crash and a clean section would look identical. `!weekly_report` re-posts the latest week's report in the invoking channel without running anything (no export, snapshot, roles or backup); it's how the layout gets checked before a Saturday.

**Re-running re-reads everyone's GP at that moment.** `GP_databases` `UPDATE`s this week's existing column from `_game`, so after a failed run, `!GP_weekly` overwrites the 2 AM snapshot with totals from whenever it is typed (pinned by `test_a_rerun_rereads_everyones_gp_at_that_moment`).

This runs automatically via `gp_weekly_loop` (a `discord.ext.tasks.loop` ticking every 15 minutes and acting only in the 2 AM hour of a local-time Saturday, per `logic.should_run_weekly_gp`) or manually via the `!GP_weekly` command (Moderator-only). Two details are deliberate and easy to undo by accident. It is `tasks.loop` rather than a hand-rolled `while True` + `asyncio.sleep` loop, because the latter got duplicated on every gateway reconnect and made the weekly report send twice. And it polls local time rather than using `tasks.loop(time=...)`: a naive `datetime.time` there is interpreted as **UTC** by discord.py, while this gate and `Functions.get_date()` both work in local time, so on any non-UTC host the two disagree and the job runs hours from 2 AM. The 15-minute poll means several ticks fall inside the 2 AM hour, so the week is marked as started in a `weekly_runs` table (`Functions.mark_weekly_run_started` / `weekly_run_already_started`, keyed on this week's snapshot column name) and `should_run_weekly_gp` reads that flag. The marker is **in the database, not in memory**: the bot restarts, systemd runs it with `Restart=on-failure`, and an in-memory flag is lost exactly when it matters — a crash during the 2 AM job would restart inside the same hour and run the whole cycle again. `run_weekly_gp` writes the marker itself, before doing any work, so the manual `!GP_weekly` also suppresses the scheduled run, and a repeatable failure cannot become a restart loop (a failed run is reported to the mod channel for a moderator to re-run). The loop body also catches and reports its own exceptions, because `tasks.loop` stops permanently after an unhandled one. Both call the same `run_weekly_gp`; the manual one passes its context as `ack_channel`, which only hears "Weekly run finished" when it is a different channel from the mod channel, where the report always goes.

### GP audit (cheat detection)

`Functions.gp_audit_report` flags members whose weekly gains are worth a moderator's attention, using two independent signals in `logic.filter_gp_audit`. It runs as part of the weekly cycle, on demand via `!gp_audit [guild]` (Moderator), and from a shell via `scripts/gp_audit.py`, which prints the embed's title and body verbatim.

The thresholds are absolute, not percentile-based: cheating is bounded by what the game physically allows, so a casual guild's top players are not suspicious merely for leading a casual guild. All four live in `logic.py` and were derived from the full history rather than guessed:

- **`GP_TASK_CAP = 680`** -- where the weekly tasks top out. It is the most common non-zero gain in the database by a factor of two over its neighbours (2311 weeks, against 470 at 690), because everyone who simply finishes the week lands exactly on it. Every comparison is therefore **strictly** greater; `>=` anywhere near this number reports most of both guilds.
- **`GP_GAIN_BAR = 750`** -- the bar a week must clear to count. Above the cap rather than on it, since ordinary variation carries people past 680.
- **`GP_SPIKE_WEEK = 1200`** -- a single week big enough to report alone. Also a common exact value (71 weeks across 28 members), so landing on it is not the unusual part; going past it is. Repeatedly landing on it is caught by the sustained count instead, since every such week clears `GP_GAIN_BAR`.
- **`GP_IMPLAUSIBLE_WEEK = 5000`** -- a data error, not a player. The real record is 2410.

The two signals are deliberately both present and neither subsumes the other. **Sustained** (above `GP_GAIN_BAR` in 3 of the last 8 weeks) counts *across* the window rather than consecutively, because gains collapse during in-game events and a streak requirement lets anyone through on one quiet week. **Spike** (newest week alone above `GP_SPIKE_WEEK`) exists because a lone enormous week between two ordinary ones never accumulates hits -- the member whose record 2410 week prompted this sat at 2 of 6 under the sustained rule alone. Historically the pair fires on about 4-5 members a week across both guilds.

Two details that look incidental but are not. `_load_gp_series` joins and labels on `{guild}_game`, not on the GP table: joining by `G_ID` is what survives a character rename, and the GP tables keep whatever name a member had when `GP_databases` first inserted their row, so reporting that name would send a moderator looking for somebody who no longer exists under it. And `logic.observed_weeks` drops each member's first recorded week, because `GP_databases` computes a gain against a missing previous snapshot as `GPnow - 0` -- that week is their lifetime GP, not a week's worth.

The SQL in `scripts/gp_audit.py` duplicates `Functions._load_gp_series` on purpose. Everything that *decides* anything is in `logic.py` and shared, but importing `Functions` builds a live Google Sheets client and reads `.env` at import time, which would cost the script its one useful property: running against a database copy on any machine with no credentials.

This is an investigation queue, not a verdict -- the report always prints the weeks behind a name so a moderator judges the data, and it goes to the mod channel only.

### Member links (`!whois`, `!relink`, `!conflicts`)

`{Guild}_members` maps a Discord account to an in-game character. It is **append-only**: rows are inserted by `!assign` and by the invite auto-link (`Functions.link_for_invite`, which only ever inserts or leaves a row alone), and **`!relink` is the only thing in the repo that ever deletes a row**. Keep it that way -- the auto-link reports a would-be delete for a moderator instead of doing it. Understanding three facts about it prevents most mistakes:

- **`_game` is rebuilt weekly**, and also refreshed mid-week by the invite poll whenever membership changes, but **`_discord` is not** -- only `!members_discord` and `!sync_counters` write it, both manual, so it can be weeks stale. Anything asking "does this account still hold the role" must ask Discord (`guild.get_member`), as `GP_roles` and the giveaway cog already do. `Functions._resolve_role_holders` is the shared helper. Conflict detection deliberately takes **no** set of live accounts, which makes it immune to this staleness.
- **Most rows are inert history.** Only ~206/200 of 939/1024 rows are current. The rest are the only record of who was once linked to what, and `GP_roles` joins *from* `_game`, so a departed character is never matched. They are not cleaned up.
- **Deleting a link row cannot lose GP.** `_GP`/`_GP_gained` are keyed on `G_ID` and fed from `_game`; nothing reads GP through `_members`.

Four traps, all of which have live instances and are pinned by tests in `tests/test_logic_member_links.py`:

1. **`rowid` is the only safe row handle.** The `index` column looks like a key but `assign` never sets it -- NULL on 745 of 939 Aetherian rows, 194 distinct values across all of them. Both rows of the one real duplicate have `index = NULL`. `logic.plan_relink` returns rowids for this reason.
2. **`NOT IN` is unsafe.** `_members.G_ID`/`D_ID` contain NULLs, so `WHERE x NOT IN (SELECT ...)` matches *nothing at all*. Use `NOT EXISTS`.
3. **`LIKE` wildcards are in real data** -- 26 Aetherian and 31 Pretherian live game names contain `_` or `%` (`Sire_Vhal`). `logic.like_term` escapes them; every caller must pair it with `ESCAPE '\'`.
4. **Five blank rows exist** (all-`''` in both guilds, three all-NULL in Pretherians). `logic.normalize_game_id`/`normalize_discord_id` return `None` for them and `usable_links` drops them, which has to happen *before* any `GROUP BY` or `IN` list -- left in, they match each other and invent a member.

Note on `logic.normalize_discord_id`: `D_ID` is declared `INTEGER`, so SQLite affinity already makes `WHERE D_ID = ?` work with either `123` or `'123'`. Plain lookups never needed normalizing. What does is **grouping** -- affinity is not applied between two values of different storage class, so an integer and a text row for the same account would be counted as two accounts on one character.

`!whois <term>` searches every column of all three tables rather than detecting what kind of thing the term is (game ids have two formats and names can be anything, so detection would be guesswork). It searches `_game.G_NAME` as well as `_members.G_NAME`, because the latter is frozen at `!assign` time and 60 of ~406 live rows are stale; a departed character is shown under its last known name. It reports characters in-game with no link row and role holders with no link row, since "never assigned" is usually the actual answer. Partial matches need 3+ characters (`a` alone matches 1408 rows); above 8 characters it lists names and stops. It cross-checks other rows sharing a `G_ID` or `D_ID` automatically -- the conflict is the thing being looked for, so it should never need a second command.

`!relink` is the repo's only delete, and `Functions._relink_write` must stay **synchronous**: `Functions.conn` is one connection shared by every coroutine, and `write_game_roster`/`GP_databases`/`mark_weekly_run_started` all commit on it, so an `await` between the `SELECT` and the `commit` would let one of them commit a half-finished delete. It deletes by the rowids read moments earlier, never by re-running `WHERE G_ID = ?`, so a row inserted in between cannot vanish unreported. `run_backup()` runs first and the removed rows are echoed in full -- between them the delete is both reversible and legible. `logic.plan_relink` keeps the lowest-rowid row already on the target account, so tidying a duplicate rewrites nothing.

`!conflicts` ranks `split` (several accounts on one character -- harmful, `GP_roles` gives each of them the rank role) above `duplicate` (a repeated `!assign`, harmless). One account on several live characters is reported as a note, not an error: an alt may be legitimate. `logic.format_conflicts` returns **`None`** when there is nothing -- whether there is anything to say is a fact about the data, but what silence *means* is the caller's policy: `Functions.conflicts_report` turns it into `[]`, the weekly report leaves the section out, and `!conflicts` says "no link conflicts found". Load errors come back as an error embed on both paths, or a crash and a clean week would look identical.

### Invite auto-link (`!invite`, `!invites`, `!uninvite`)

`!invite <guild> <name> [@member]` sends the in-game invite and then watches for the invitee; when they arrive it writes the link row, gives the guild role, removes `Former Aetherian` (which `!kick` hands out), and posts the welcome in #aether-chair. Everything is reported to the mod channel. Without `@member` the invite is watched but a match is reported with a ready `!assign`, never linked.

**The game never says who accepted an invite.** This was checked exhaustively: the `igs` response is only `{"result":"true"}`, other players' account data (`_uid/{id}`) is 401, the guild node has no invite or join records, and a real invite from a clean test guild wrote nothing anywhere. The only signal is a new account id appearing in `_guild/{gid}/m`. Everything in `logic.match_invites` is about attributing an arrival from that alone, and refusing when it can't be sure:

1. **Name** -- the arrival displays the invited name (casefolded). Character names are unique game-wide, so this is certain -- *but* the guild may display a different character of the invitee's account, which is why the other rules exist.
2. **Discord history** -- the arrival already has a link row for the invite's account. This covers returning members and a mod who `!assign`ed by hand, and it is also what finishes an invite after a crash, so there is no separate "resolved manually" path.
3. **Elimination** -- exactly one invite outstanding in the guild, exactly one arrival, and that arrival has **no usable link rows**, so it can only ever insert. Full auto including the public welcome (the user's decision), flagged in the mod channel; a wrong guess is fixed with `!relink`.
4. Anything else is reported once (persisted in `invite_arrival_reports`) with a ready `!assign` per plausible invite.

Invite state lives in an `invites` table (created ad hoc like `weekly_runs`) and rows are **never deleted**: a finished invite's `matched_g_id` is the claim that stops a character being handed to a second invite. That claim exists because of a real failure found in review -- a returning member matched by name writes no new row (theirs already exists), so without it they'd still look like a fresh arrival and the next tick would eliminate them into someone else's invite.

Rules that look arbitrary but aren't, each pinned in `tests/test_logic_invites.py`:
- **One live invite per `(guild, Discord account)`.** Re-inviting the same name merges (restarting the 24 h watch); a different name for the same member *supersedes* the old invite. A superseded invite can **never link**: a typo can hit a real stranger, who then holds a genuine invite. Its name turning up is reported, and it counts as outstanding so the stranger can't be eliminated into someone else's invite either.
- **Expired (24 h) and superseded invites stay outstanding for 7 days** for elimination purposes, so a late or stray acceptance isn't handed to whoever is pending. `!uninvite` (status `cancelled`) is how a mod clears one; a cancelled invite doesn't block elimination, but its name turning up is still reported, since the bot can't withdraw an in-game invite.
- **Baselines are taken before the `igs` POST and intersected on merge**, so an instant accept can never land in the invitee's own baseline. Invites are read before the roster GET, and one recorded after the read is skipped.
- **A POST that times out after connecting is recorded anyway** (`send_confirmed = 0`): it may have reached the game, and dropping it would lose the link forever once they accept (a re-`!invite` is refused as "already in the guild"). Only a connect timeout counts as certainly unsent.
- **`finish_invite` sends the welcome only if it added the guild role.** That makes the welcome at-most-once across crashes and stops a second one when a mod already did it by hand. A Discord error that would simply repeat (e.g. `Forbidden`) parks the invite in `needs_attention` after one report; only `NotFound` means the member is gone.

**Polling** is `invite_poll_loop`, a 10 s `tasks.loop` that only reads the roster when `logic.should_poll_guild` says so: 10 s for the first 2 minutes after the newest invite, backing off to 10 minutes, and stopping at 24 h. One read per guild covers every invite in it. It uses plain REST with a cached token (`fetch_guild_roster`, `firebase_sign_in`), the same path the weekly export and `!members_game` use -- the export used to go through pyrebase, which is synchronous and froze the whole bot for its duration. After a failed sign-in the *poll* backs off (10 min, or indefinitely for a bad password, until a manual command succeeds) so it can't lock the guild account `!kick` and the weekly export share; manual commands always try.

**The token must never reach a log or a channel.** The roster read passes it as `?auth=`, and `requests` puts the URL in its exception messages. `_request` re-raises every request error through `logic.redact_secrets` with the chain dropped (`from None`), so nothing downstream can leak it.

**`{guild}_game` is refreshed by the poll**, only when membership or names change, through `Functions.refresh_game_roster`: validated first (a NULL game id reaching the GP tables would make `GP_databases`' `NOT IN` silently stop adding members), and refused while the weekly job runs or if a newer roster was written during the read. **All `_game` writes go through `Functions.write_game_roster`**, `DELETE` + `INSERT` in one transaction -- `to_sql(if_exists='replace')` autocommits its `DROP`/`CREATE` separately, so a power cut between them left the table empty. `run_weekly_gp` is wrapped in `Functions.weekly_job()`, a depth counter (not a flag -- `!GP_weekly` can overlap the scheduled run) that holds the poll off between the export and `GP_databases` reading it.

### Hardcoded IDs

The main Discord guild ID (`809954021028134943`) and several role/channel IDs are hardcoded inline throughout `LedBotCode.py` and `Functions.py` rather than pulled from `.env` (unlike `SPAM_CHANNEL_ID`, which is). Be aware of this when reading code that looks like it should be configurable but isn't.

### Permissions

Nearly all commands are gated with `@commands.has_role("Moderator")` or `@commands.has_any_role(...)`. There's no slash-command permission model in use despite `discord.app_commands` being imported — commands are plain prefix commands (`!`).
