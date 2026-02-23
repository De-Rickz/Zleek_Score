import pandas as pd
from datetime import datetime, timezone

rows = [
    # pretend these are your DB rows
    {
        "asset_id": "A1",
        "ts": 1732046405123,
        "price": 0.47,
        "size_usd": 120.0,
    },
    {
        "asset_id": "A1",
        "ts": 1732046408450,
        "price": 0.48,
        "size_usd": 50.0,
    },
    {
        "asset_id": "A1",
        "ts": 1732046460123,
        "price": 0.46,
        "size_usd": 80.0,
    },
]
df = pd.DataFrame(rows)

dt = df.loc[0]["ts"]
ts = datetime.fromtimestamp(dt/1000,tz=timezone.utc)

print(dt)
print(ts)

print(datetime.now(timezone.utc))