# Mac jobs

## Nightly trades_archive rehearsal (`com.zleek.rehearsal`)

`zleek-nightly-rehearsal.sh` runs at 04:30 local time (launchd runs a missed
time once on wake; a Mac that was shut down skips it). It is **read-only on
the server** (`ls`/`stat` over ssh, `scp` from it): it fetches the newest
nightly dump, checks its size, runs
`infra/sql/migrations/rehearse_trades_archive.sh` on it in a throwaway
TimescaleDB 2.31.0 container, writes the report to
`~/zleek-rehearsal/reports/rehearsal-YYYY-MM-DD.txt` (and a copy on the
Desktop), pushes a one-line summary or the failure reason to the heartbeat's
ntfy topic, and deletes downloaded dumps older than 2 days. Needs Docker
Desktop (started if it isn't running), ~25 GB free, and ssh to the server
without a password prompt (key in the macOS keychain / ssh-agent).

The rehearsal code comes from `main` once it has the rehearsal script, until
then from `phase4/storage-trades-archive` (override: `ZLEEK_REHEARSAL_REF`).

Install (from the repo root, on the Mac):

    mkdir -p ~/bin ~/zleek-rehearsal ~/.zleek && chmod 700 ~/.zleek
    install -m 700 infra/mac/zleek-nightly-rehearsal.sh ~/bin/
    install -m 600 infra/ops/zleek-ntfy.sh ~/bin/
    # the heartbeat's ntfy topic, copied once from the server (only those two keys)
    ssh ubuntu@79.72.91.54 "grep -E '^NTFY_(URL|TOKEN)=' zleek/apps/worker/.env.heartbeat" > ~/.zleek/ntfy.env && chmod 600 ~/.zleek/ntfy.env && sed 's/=.*/=<set>/' ~/.zleek/ntfy.env
    cp infra/mac/com.zleek.rehearsal.plist ~/Library/LaunchAgents/
    launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.zleek.rehearsal.plist

Run once now, under launchd (as the nightly run will):

    launchctl kickstart -p gui/$(id -u)/com.zleek.rehearsal
    tail -f ~/zleek-rehearsal/rehearsal.log ~/zleek-rehearsal/launchd.log

Status / remove:

    launchctl print gui/$(id -u)/com.zleek.rehearsal | grep -E 'state|last exit'
    launchctl bootout gui/$(id -u)/com.zleek.rehearsal
