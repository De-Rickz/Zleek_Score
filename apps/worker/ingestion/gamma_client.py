import requests
from pydantic import BaseModel,ValidationError, validator
import json
from datetime import datetime
import asyncio

base_url = "https://gamma-api.polymarket.com/"

endpoint = "markets?status=open&order=id&ascending=false&closed=false"



print("Starting")
'''
class Market(BaseModel):
    id: str 
    title: str
    category: str | None = None
    status: str | None = 'open'
    liquidity: int | None
    token_yes_id: str | None
    token_no_id: str | None
    created_at: str | None
    updated_at: str | None
    raw: str | None
    
    '''

def fetch_markets(limit=200):
    """
    Fetch the list of markets from the Gamma API.

    Returns:
        dict: The response from the Gamma API in JSON format.
    """
    offset = 0
    all_items = []
    while True:
        lim_and_off = f"&limit={limit}&offset={offset}"
        url = base_url + endpoint + lim_and_off

        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        items = data
        
        if not items:
            break
        
        all_items.extend(data)


    
        offset += limit
    return all_items
def parse_json(response):
    rows = []
    for m in response:
      
        
        token_ids = m.get("clobTokenIds","")
        ids = []
        if isinstance(token_ids, list):
            # already parsed
            ids = token_ids
        elif isinstance(token_ids, str) and token_ids.strip():
            try:
                ids = json.loads(token_ids)
            except json.JSONDecodeError:
                ids = []
        else:
            ids = []

        if isinstance(ids, list) and len(ids) == 2:
            no_id, yes_id = ids
        else:
            no_id = yes_id = None
            
        events = m.get("events") or []
        first_event = events[0] if events else {}
        created_raw = m.get("createdAt")
        updated_raw = m.get("updatedAt")
        
        char = {
            'id': m.get('id',""),
            'title': m.get('question',""), 
            'condition_id': m.get('conditionId',""),
            'event_id': first_event.get("id", ""),
            'event_title': first_event.get("title", ""),
            'category': m.get("category") or "Uncategorized",
            'status': "open" if m.get('active') else "close",
            'liquidity_usd': m.get("liquidityNum",0),
            'token_yes_id': yes_id if len(ids) == 2 else "" ,
            'token_no_id': no_id if len(ids) == 2 else "",
            'created_at': datetime.strptime(created_raw, "%Y-%m-%dT%H:%M:%S.%f%z") if created_raw else None,
            'updated_at': datetime.strptime(updated_raw, "%Y-%m-%dT%H:%M:%S.%f%z") if updated_raw else None,
            'raw': json.dumps(m)
            
        }
        rows.append(char)

        print (char.get("updated_at"))

    
    return rows
    
def fetch_trades(limit=200):
    offset = 0
    all_items = []
    end = "trades?status=open&order=id&ascending=false&closed=false"
    while True:
        lim_and_off = f"&limit={limit}&offset={offset}"
        url = base_url + end + lim_and_off

        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        items = data
        
        if not items:
            break
        
        all_items.extend(data)


    
        offset += limit
    return all_items
    
        


if __name__ == "__main__":
    markets = fetch_markets()
    rows = parse_json(markets)
