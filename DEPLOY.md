# Deploying LedBot on a Raspberry Pi

## One-time setup

```bash
git clone <this repo> ~/LedBot
cd ~/LedBot
python3 -m venv venv
venv/bin/pip install -r requirements.txt
```

Then place the two secret files in the repo root (never committed — see `.gitignore`):
- `.env` — see the variables listed in `CLAUDE.md`
- `service_account.json` — Google service account credentials for the boss-strategy sheet lookup

The three welcome-channel ids in `.env` (`WELCOME_POST_CHANNEL_ID`, `WELCOME_INFO_CHANNEL_ID`, `ROLES_CHANNEL_ID`) are optional: without them the bot still starts, and the invite auto-link still links members and gives roles, but skips the welcome post and says so in the mod channel. Add them when updating an existing Pi.

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

# After pulling code changes
sudo systemctl restart ledbot.service
```

## Backups

> The weekly job fires at 2 AM **in the Pi's local timezone**, so make sure the Pi's clock and timezone are set correctly (`timedatectl`). The GP snapshot columns are named after the local date, so a wrong timezone shifts which day a week's gains are attributed to.

The database only changes once a week (the Saturday 2 AM local-time GP job), so backups are triggered from inside that same job rather than on a separate always-on schedule — `run_weekly_gp()` calls `scripts/backup_db.run_backup()` right after committing that week's data and posts a confirmation (or failure) to the mod channel, no SSH needed to check. A mod can also trigger an on-demand backup any time via the `!backup` Discord command.

Backups land in `Backups/` as timestamped `.db` snapshots; `scripts/backup_db.py` keeps the most recent 52 (about a year at weekly cadence) and prunes older ones automatically (see `--retention` to change that). It's also runnable directly by hand if ever needed: `venv/bin/python3 scripts/backup_db.py`.
