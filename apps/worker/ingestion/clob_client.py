from py_clob_client.client import ClobClient

host: str = ""
key: str = ""
chain_id: int = 137

### Initialization of a client that trades directly from an EOA
client = ClobClient(host, key=key, chain_id=chain_id)

