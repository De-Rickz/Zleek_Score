import httpx
import json
import logging
from datetime import datetime
from typing import List, Optional
from pydantic import BaseModel, Field, validator

logger = logging.getLogger("Ingestion.Gamma")
BASE_URL = "https://gamma-api.polymarket.com"

# --- 1. Define the Schema (The Zleek way) ---
class MarketRow(BaseModel):
    id: str
    title: str = Field(alias="question")
    condition_id: str = Field(alias="conditionId")
    category: str = "Uncategorized"
    status: str = "open"
    liquidity_usd: float = Field(alias="liquidityNum", default=0.0)
    token_yes_id: Optional[str] = None
    token_no_id: Optional[str] = None
    updated_at: Optional[datetime] = None
    raw: str

    @validator("token_yes_id", pre=True, always=True)
    def parse_token_ids(cls, v, values, **kwargs):
        # This handles the complex logic you had in parse_json
        # 'v' is the value of the field being validated
        raw_ids = kwargs.get("data").get("clobTokenIds")
        if isinstance(raw_ids, str) and raw_ids.strip():
            try:
                ids = json.loads(raw_ids)
                if len(ids) == 2:
                    # Logic: yes is usually index 1, no is index 0
                    return ids[1] 
            except:
                pass
        return None

# --- 2. The Async Fetcher ---
async def fetch_markets(min_liquidity=10000, min_volume=1000) -> List[dict]:
    all_raw_markets = []
    offset = 0
    limit = 100
    
    # Using AsyncClient as a context manager is more efficient for multiple calls
    async with httpx.AsyncClient(base_url=BASE_URL, timeout=10.0) as client:
        while True:
            params = {
                "active": "true",
                "closed": "false",
                "liquidity_num_min": min_liquidity,
                "volume_num_min": min_volume,
                "order": "volume24hr",
                "ascending": "false",
                "limit": limit,
                "offset": offset
            }
            
            try:
                response = await client.get("/markets", params=params)
                response.raise_for_status()
                data = response.json()
                
                if not data:
                    break
                    
                all_raw_markets.extend(data)
                offset += limit
                logger.info(f"Fetched {len(all_raw_markets)} markets so far...")
                
            except Exception as e:
                logger.error(f"Error fetching from Gamma: {e}")
                break
                
    return all_raw_markets

# --- 3. The Parser ---
def parse_markets(raw_data: List[dict]) -> List[dict]:
    rows = []
    for m in raw_data:
        try:
            # We use Pydantic to do the heavy lifting of date parsing and ID extraction
            market_obj = MarketRow(**m, raw=json.dumps(m))
            rows.append(market_obj.dict())
        except Exception as e:
            logger.warning(f"Skipping market {m.get('id')} due to parse error: {e}")
    return rows