# Fetch all transactions from the XRP ledger

... and insert them into Google BigQuery. 

You can invoke the script from a node enabled environment by setting these environment variables:

- `NODE`: the rippled node (`wss://...`) to connect to, default: **wss://s2.ripple.com**
- `LEDGER`: the ledger index to start fetching transactions from, default: **32570**

# Todo

- Test Meta/Amount (DEX) ... STRUTCs
- Instructions (links), intro Google BigQuery
- Google BigQuery auth path (env. var.)
- Script to create Table (schema) @ BigQuery