const {
  PROJECT_ID,
  DATASET_NAME,
  LEDGER_TABLE_NAME,
} = require('./schema')

const XrplClient = require('xrpl-client').XrplClient
const BigQuery = require('@google-cloud/bigquery')
const bigquery = new BigQuery({ projectId: PROJECT_ID })

let Stopped = false

async function fetchLedger(client, ledgerIndex) {
  try {
    return await client.send({
      command: 'ledger',
      ledger_index: parseInt(ledgerIndex),
      transactions: false,
      expand: false,
    })
  } catch(e) {
    console.error('Ledger fetching error', e)
    throw e
  }
}

async function getLastDBLedger() {
  let result
  try {
    result = await bigquery.query({
      query: `SELECT MAX(LedgerIndex) as MaxLedger
              FROM ${PROJECT_ID}.${DATASET_NAME}.${LEDGER_TABLE_NAME}`,
      useLegacySql: false,
    })
  } catch(e) {
    console.error('Google BigQuery Error', e)
    throw e
  }

  return result[0][0].MaxLedger
}

async function insertDBLedger(ledgerResult) {
  try {
    await bigquery.dataset(DATASET_NAME).table(LEDGER_TABLE_NAME).insert([{
      LedgerIndex: parseInt(ledgerResult.ledger.ledger_index),
      hash: ledgerResult.ledger.hash,
      CloseTime: bigquery.timestamp(new Date(Date.parse(ledgerResult.ledger.close_time_human)).toISOString().replace('T', ' ').replace(/[^0-9]+$/, '')),
      CloseTimeTimestamp: ledgerResult.ledger.close_time,
      CloseTimeHuman: ledgerResult.ledger.close_time_human,
      TotalCoins: parseInt(ledgerResult.ledger.totalCoins),
      ParentHash: ledgerResult.ledger.parent_hash,
      AccountHash: ledgerResult.ledger.account_hash,
      TransactionHash: ledgerResult.ledger.transaction_hash,
      _InsertedAt: bigquery.timestamp(new Date()),
    }])
  } catch(err) {
    if (err && err.name === 'PartialFailureError') {
      if (err.errors && err.errors.length > 0) {
        console.error('Insert errors:')
        err.errors.forEach(err => console.dir(err, { depth: null }))
        throw err
      }
    } else {
      console.error('ERROR:', err)
      throw err
    }
  }
}

async function processLedger(client, lastLedger) {
  const ledgerResult = await fetchLedger(client, lastLedger + 1)
  await insertDBLedger(ledgerResult)
  lastLedger = ledgerResult.ledger_index
  console.log(`${lastLedger} inserted`)
  return lastLedger
}

async function main() {
  console.log('Fetch XRPL Ledger Info into Google BigQuery')

  // Determine start ledger. lastLedger represents the last ledger that we
  // _have_ stored. So lastLedger + 1 is the next ledger we need. The
  // commandline input is supposed to represent the _next_ ledger, so we need
  // to subtract one.
  let lastLedger = 0
  const cmdLineStartLedger = typeof process.env.LEDGER === 'undefined' ? 32570 : parseInt(process.env.LEDGER)
  const startLedgerDB = await getLastDBLedger()

  if (startLedgerDB >= cmdLineStartLedger) {
    console.log(`BigQuery History at ledger [ ${startLedgerDB} ], > StartLedger.\n  Forcing StartLedger at:\n  >>> ${startLedgerDB+1}\n\n`)
    lastLedger = startLedgerDB
  } else {
    console.log(`Starting at ledger ${cmdLineStartLedger}`)
    lastLedger = Math.max(cmdLineStartLedger - 1, 1)
  }

  // Setup client
  const xrplNodeUrl = typeof process.env.NODE === 'undefined' ? 'wss://s2.ripple.com' : process.env.NODE.trim()
  const client = new XrplClient(xrplNodeUrl)
  await client.ready()
  console.log('Connected to the XRPL')

  // Main loop
  let errored = false
  while (!Stopped) {
    try {
      lastLedger = await processLedger(client, lastLedger)
    } catch(e) {
      console.error('Error', e)
      errored = true
      break
    }
  }

  // Cleanup
  console.log('Disconnecting from ledger')
  await client.close()
  console.log('Disconnected')

  if (lastLedger > 0) {
    console.log(`\nLast ledger: [ ${lastLedger} ]\n\nRun your next job with ENV: "LEDGER=${lastLedger+1}"\n\n`)
  }

  process.exit(errored ? 1 : 0)
}

function onSigInt() {
  console.log(`\nGracefully shutting down from SIGINT (Ctrl+C)\n -- Wait for remaining BigQuery inserts and XRPL Connection close...`);
  Stopped = true
}

process.on('SIGINT', onSigInt)

main().then()
