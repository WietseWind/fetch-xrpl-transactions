# Fetch all transactions from the XRP ledger

... and insert them into MongoDB. 

The **docker-compose** config is included to run both the script and a mongodb server;

## Howto

1. Config the ledger-index to start with in the `docker-compose.yml` file (`LEDGER` environment variable for the `fetch` service).
2. Run `docker-compose up`.  
  The `mongo` service is configured (in `docker-compose.yml`) to map the MongoDB TCP port to your host at port `21337`.
3. All transactions will be stored in the database `xrpl`, in the collection `transactions`.

### MAKE SURE YOU HAVE YOUR FIREWALL SETUP! MONGO IS NOT CONFIGURED TO REQUIRE A USERNAME AND PASSWORD FOR INCOMING CONNECTIONS.

## Run with a custom config

You can invoke the script from a node enabled environment by setting these environment variables:

- `NODE`: the rippled node (`wss://...`) to connect to, default: **wss://s2.ripple.com**
- `LEDGER`: the ledger index to start fetching transactions from, default: **32570**
- `MONGO`: the mongo connections tring to connect to, default: **mongodb://mongo:27017/xrpl**

When you run using `docker-compose`, the default `NODE` (rippled node) will be **wss://rippled.xrptipbot.com**.

You can run the `Dockerfile` with `-e` flags fo start a node:latest-container.