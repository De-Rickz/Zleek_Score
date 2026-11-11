# Measured sets (ROADMAP phase 4)

One file per UTC day, written by `recover_backfill_gap_2026-11.py --population day`
the first time that day is measured, and read back on every later run, so a day
always measures the same markets. Each line is `websocket <condition_id>` (the
top 20 by the day's USD volume in the Data API) or `backfill <condition_id>`
(the next 200). The `#` lines say when and how the set was ranked.

On the server the script writes to `~/zleek-measured-sets` (mounted at `/sets`);
copy new files here and commit them so the sets are kept with the repo:

    scp 'ubuntu@79.72.91.54:zleek-measured-sets/*.txt' apps/worker/scripts/measured-sets/

Don't edit a file after it has been used: delete it only to re-rank a day
(for example after a ranking bug), and record that in the ROADMAP.
