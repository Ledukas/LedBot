# Deploying LedBot on a Raspberry Pi

## One-time setup

The Pi pulls the code with a read-only **deploy key**: it can read this one repository and nothing else, and can't push. Create it as the account the bot runs as, then add the public key under the repository's Settings → Deploy keys (or `gh repo deploy-key add <file> --repo Ledukas/LedBot` from a machine logged in to GitHub), leaving "Allow write access" off:

```bash
ssh-keygen -t ed25519 -N "" -C "LedBot Pi deploy key" -f ~/.ssh/github_ledbot
printf 'Host github.com\n    IdentityFile ~/.ssh/github_ledbot\n    IdentitiesOnly yes\n' >> ~/.ssh/config
ssh-keyscan -t ed25519 github.com >> ~/.ssh/known_hosts
ssh-keygen -lf ~/.ssh/known_hosts   # github.com must be SHA256:+DiY3wvvV6TuJJhbpZisF/zLDA0zPMSvHdkr4UvCOqU
cat ~/.ssh/github_ledbot.pub        # this is what goes into Deploy keys
```

The key has no passphrase so pulls need no one at the keyboard; if the Pi is lost, deleting the deploy key on GitHub is all it takes to cut it off. Then:

```bash
git clone git@github.com:Ledukas/LedBot.git ~/LedBot
cd ~/LedBot
python3 -m venv venv
venv/bin/pip install -r requirements.txt
```

`requirements.txt` needs **Python 3.12 or newer** (check with `python3 --version`). Raspberry Pi OS Trixie ships 3.13; on Bookworm (3.11) the install fails on numpy.

Then place the two secret files in the repo root (never committed — see `.gitignore`):
- `.env` — see the variables listed in `CLAUDE.md`
- `service_account.json` — Google service account credentials for the boss-strategy sheet lookup

The three welcome-channel ids in `.env` (`WELCOME_POST_CHANNEL_ID`, `WELCOME_INFO_CHANNEL_ID`, `ROLES_CHANNEL_ID`) are optional: without them the bot still starts, and the invite auto-link still links members and gives roles, but skips the welcome post and says so in the mod channel. Add them when updating an existing Pi.

The weekly report is posted as embeds, so the bot needs the **Embed Links** permission in the mod channel (and wherever `!gp_audit`, `!conflicts`, `!sync_counters` or `!weekly_report` are used). Without it the bot posts a line saying so instead of the report.

## Installing the systemd unit

`systemd/ledbot.service` has `User=pi` and `/home/pi/LedBot` as placeholders — edit both to match your actual username and the path you cloned the repo to before installing.

```bash
sudo cp systemd/ledbot.service /etc/systemd/system/
sudo systemctl daemon-reload

# Make "time-sync.target" actually wait for NTP (the unit is ordered after it)
sudo systemctl enable systemd-time-wait-sync

# Start now and on every boot
sudo systemctl enable --now ledbot.service
```

**Why the time sync:** the Pi has no clock battery, so after a boot its clock can be hours out until NTP syncs. The invite auto-link times invites in wall-clock seconds, and an invite recorded on a wrong clock would look hours old and expire at once when the clock corrects; the weekly 2 AM job is exposed to the same jump. With `systemd-time-wait-sync` enabled the bot waits for a synced clock before starting. Side effect: after a boot with **no network**, the bot waits rather than starting — which is fine, since it can't do anything offline anyway.

After pulling a change to `systemd/ledbot.service`, re-run the `cp` and `daemon-reload` lines above before restarting.

## Everyday operations

```bash
# Bot status / logs
sudo systemctl status ledbot.service
journalctl -u ledbot.service -f

# Update to the latest main
cd ~/LedBot && git pull --ff-only && sudo systemctl restart ledbot.service
```

A restart is safe at any time. A week that's already taken isn't run again, and retries keep their schedule (it's kept in the database, not in memory). The one moment to avoid is the Saturday run itself, normally about a minute from 2 AM: interrupting it loses nothing -- it writes in one transaction -- but the report waits for the next start and a retry.

If `requirements.txt` changed, run `venv/bin/pip install -r requirements.txt` before the restart.

Only one copy of the bot may ever run: they share the Discord token, so a second copy (an old Pi, a test run on a PC) would answer every command twice and write its own snapshots.

## Backups

> The weekly job fires at 2 AM **in the Pi's local timezone**, so make sure the Pi's clock and timezone are set correctly (`timedatectl`). The GP snapshot columns are named after the local date, so a wrong timezone shifts which day a week's gains are attributed to.

The database only changes once a week (the Saturday 2 AM local-time GP job), so backups are triggered from inside that same job rather than on a separate always-on schedule — `run_weekly_gp()` calls `scripts/backup_db.run_backup()` right after committing that week's data and posts a confirmation (or failure) to the mod channel, no SSH needed to check. A mod can also trigger an on-demand backup any time via the `!backup` Discord command.

Backups land in `Backups/` as timestamped `.db` snapshots; the weekly ones (`DatabaseLedBot_…`) keep the most recent 52, about a year (`--retention` changes that for a run by hand); `!backup` and the safety backup before a `!relink` are named `DatabaseLedBot-manual_…` and keep their own most recent 20, so a burst of relinks can't push weekly history out. It's also runnable directly by hand if ever needed: `venv/bin/python3 scripts/backup_db.py`.
