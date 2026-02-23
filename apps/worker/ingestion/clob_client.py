import os
from dotenv import load_dotenv
from py_clob_client.client import ClobClient

load_dotenv(".env.local")

HOST = "https://clob.polymarket.com"
CHAIN_ID = 137

PRIVATE_KEY = os.getenv("POLYMARKET_WALLET_KEY")
FUNDER = os.getenv("FUNDER_ADDRESS")
SIGNATURE_TYPE = 1  # Magic/email proxy signatures

_client: ClobClient | None = None

def get_client() -> ClobClient:
    global _client
    if _client is not None:
        return _client

    if not PRIVATE_KEY:
        raise RuntimeError("POLYMARKET_WALLET_KEY missing")
    if SIGNATURE_TYPE == 1 and not FUNDER:
        raise RuntimeError("FUNDER_ADDRESS missing (required for signature_type=1)")

    c = ClobClient(
        HOST,
        key=PRIVATE_KEY,
        chain_id=CHAIN_ID,
        signature_type=SIGNATURE_TYPE,
        funder=FUNDER,
    )

    c.set_api_creds(c.create_or_derive_api_creds())
    _client = c
    return _client