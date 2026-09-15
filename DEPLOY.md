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

## Installing the systemd unit

`systemd/ledbot.service` has `User=pi` and `/home/pi/LedBot` as placeholders — edit both to match your actual username and the path you cloned the repo to before installing.

```bash
sudo cp systemd/ledbot.service /etc/systemd/system/
sudo systemctl daemon-reload

# Start now and on every boot
sudo systemctl enable --now ledbot.service
```

## Everyday operations

```bash
# Bot status / logs
sudo systemctl status ledbot.service
journalctl -u ledbot.service -f

# After pulling code changes
sudo systemctl restart ledbot.service
```

## Backups

The database only changes once a week (the Saturday 2 AM GP job), so backups are triggered from inside that same job rather than on a separate always-on schedule — `run_weekly_gp()` calls `scripts/backup_db.run_backup()` right after committing that week's data and posts a confirmation (or failure) to the mod channel, no SSH needed to check. A mod can also trigger an on-demand backup any time via the `!backup` Discord command.

Backups land in `Backups/` as timestamped `.db` snapshots; `scripts/backup_db.py` keeps the most recent 52 (about a year at weekly cadence) and prunes older ones automatically (see `--retention` to change that). It's also runnable directly by hand if ever needed: `venv/bin/python3 scripts/backup_db.py`.
