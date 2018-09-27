# Fetch all transactions from the XRP ledger

This code allows you to fetc all transactions from the XRP ledger and insert them into Google BigQuery. 

# The data is already READY TO USE 🎉!

The data is already available in a **PUBLIC dataset** at Google BigQuery in:

```
xrpledgerdata.fullhistory.transactions
```

So a working sample query with some stats would be:

```
SELECT 
  COUNT(1) as TxCount,
  MIN(LedgerIndex) as MinLedger,
  MAX(LedgerIndex) as MaxLedger,
  COUNT(DISTINCT LedgerIndex) as LedgersWithTxCount
FROM 
  xrpledgerdata.fullhistory.transactions
```

Starting Sept. 27 2018 the dataset will be backfilled from a [full history rippled node](https://twitter.com/WietseWind/status/1027957804429193216). Once up to date, I'll run a service that will add all new transactions to the dataset as well.

# Run and insert into your own Google BigQuery project

## Setup

If you want to insert the data in your own BigQuery project, download your credentials (JSON) from the Google Admin Console, and export the path to your credentials file:

### OSX 

```
export GOOGLE_APPLICATION_CREDENTIALS="[PATH]"
```

### Windows

Powershell:

```
$env:GOOGLE_APPLICATION_CREDENTIALS="[PATH]"
```

Command Prompt:

```
set GOOGLE_APPLICATION_CREDENTIALS=[PATH]
```

## OSX Sample

```
export GOOGLE_APPLICATION_CREDENTIALS="/home/user/Downloads/[FILE_NAME].json"
```

More information:
https://cloud.google.com/docs/authentication/getting-started

> Now modify `schema.js` (line 1-3) to point to your own projectId, dataset and table.

# Create schema

Run `node applySchema.js`

**WARNING!** If an existing table exists, the table, schema and data will be **REMOVED**!

# Insert data

You can invoke the script from a node enabled environment by setting these environment variables:

- `NODE`: the rippled node (`wss://...`) to connect to, default: **wss://s2.ripple.com**
- `LEDGER`: the ledger index to start fetching transactions from, default: **32570**
