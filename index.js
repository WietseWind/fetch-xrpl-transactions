const {
  PROJECT_ID,
  DATASET_NAME,
  TRANSACTION_TABLE_NAME,
  transactionSchema,
  CurrencyFields,
} = require('./schema')

const XrplClient = require('xrpl-client').XrplClient
const BigQuery = require('@google-cloud/bigquery')
const bigquery = new BigQuery({ projectId: PROJECT_ID })

let Stopped = false

async function getLastDBLedger() {
  let result
  try {
    result = await bigquery.query({
      query: `SELECT 
                COUNT(1) as TxCount,
                MIN(LedgerIndex) as MinLedger,
                MAX(LedgerIndex) as MaxLedger,
                COUNT(DISTINCT LedgerIndex) as LedgersWithTxCount
              FROM 
                ${PROJECT_ID}.${DATASET_NAME}.${TRANSACTION_TABLE_NAME}`,
      useLegacySql: false,
    })
  } catch(e) {
    console.log('Google BigQuery Error', e)
    throw e
  }

  return result[0][0].MaxLedger
}

async function fetchLedgerTransactions(client, ledgerIndex) {
  const result = await client.send({
    command: 'ledger',
    ledger_index: ledgerIndex,
    transactions: true,
    expand: false,
  })

  if (typeof result.ledger.transactions === 'undefined' || result.ledger.transactions.length === 0) {
    return { ledger_index: ledgerIndex, transactions: [] }
  }

  if (result.ledger.transactions.length <= 200) {
    const txResults = await client.send({
      command: 'ledger',
      ledger_index: ledgerIndex,
      transactions: true,
      expand: true,
    }, 10)
    return { ledger_index: ledgerIndex, transactions: txResults }
  }

  console.log(`<<< MANY TXS at ledger ${ledgerIndex}: [[ ${result.ledger.transactions.length} ]], processing per-tx...`)
  const txPromises = result.ledger.transactions.map((tx) => {
    return client.send({
      command: 'tx',
      transaction: tx,
    }, 10)
  })
  const txResults = (await Promise.all(txPromises)).filter((tx) => {
    return typeof tx.error === 'undefined' && typeof tx.meta !== 'undefined' && typeof tx.meta.TransactionResult !== 'undefined'
  }).map((tx) => {
    return Object.assign(tx, {
      metaData: tx.meta,
    })
  })
  console.log('>>> ALL TXS FETCHED:', txResults.length)
  return { ledger_index: ledgerIndex, transactions: txResults }
}

async function insertIntoDB(txs) {
  try {
    return await bigquery.dataset(DATASET_NAME).table(TRANSACTION_TABLE_NAME).insert(txs)
  } catch(err) {
    if (err && err.name === 'PartialFailureError') {
      if (err.errors && err.errors.length > 0) {
        console.log('Insert errors:')
        err.errors.forEach(err => console.dir(err, { depth: null }))
        throw err
      }
    } else {
      console.error('ERROR:', err)
      throw err
    }
  }
}

function formatTxForDB(tx) {
  const _Tx = {}

  // Auto mapping for 1:1 fields (non RECORD)
  transactionSchema.forEach(SchemaNode => {
    if (typeof Tx[SchemaNode.description] !== 'undefined' 
        && Tx[SchemaNode.description] !== null 
        && typeof Tx[SchemaNode.description] !== 'object' 
        && SchemaNode.description === SchemaNode.name
    ) {
      let Value = Tx[SchemaNode.description]
      if (typeof Value === 'string' && typeof SchemaNode.type !== 'STRING') {
        if (SchemaNode.type === 'INTEGER') {
          Value = parseInt(Value)
        }
        if (SchemaNode.type === 'FLOAT') {
          Value = parseFloat(Value)
        }
      }
      Object.assign(_Tx, {
        [SchemaNode.name]: Value,
      })
    }
    if (SchemaNode.description.match(/^metaData\./)
        && typeof Tx.metaData[SchemaNode.name] !== 'undefined' 
        && Tx.metaData[SchemaNode.name] !== null 
        && typeof Tx.metaData[SchemaNode.name] !== 'object' 
        && SchemaNode.name !== 'DeliveredAmount'
    ) {
      Object.assign(_Tx, {
        [SchemaNode.name]: Tx.metaData[SchemaNode.name],
      })
    }
  })

  if (typeof Tx.metaData.DeliveredAmount === 'undefined' && typeof Tx.metaData.delivered_amount !== 'undefined') {
    Tx.metaData.DeliveredAmount = Tx.metaData.delivered_amount
  }
  if (typeof Tx.metaData.DeliveredAmount !== 'undefined') {
    let DeliveredAmount = parseInt(Tx.metaData.DeliveredAmount)
    if (!isNaN(DeliveredAmount)) {
      Object.assign(_Tx, {
        DeliveredAmount: DeliveredAmount
      })
    }
  }

  if (typeof Tx.Memos !== 'undefined') {
    Object.assign(_Tx, {
      Memos: Tx.Memos.map(m => {
        let n = { Memo: {} }
        if (typeof m.Memo !== 'undefined') {
          if (typeof m.Memo.MemoData !== 'undefined') n.Memo.MemoData = m.Memo.MemoData
          if (typeof m.Memo.MemoFormat !== 'undefined') n.Memo.MemoData = m.Memo.MemoFormat
          if (typeof m.Memo.MemoType !== 'undefined') n.Memo.MemoData = m.Memo.MemoType
        }
        return n
      })
    })
  }

  if (Tx.NFTokenOffers != null) {
    _Tx.NFTokenOffers = Tx.NFTokenOffers
  }

  if (Tx.metaData != null) {
    _Tx.Metadata = JSON.stringify(Tx.metaData)
  }

  CurrencyFields.forEach(CurrencyField => {
    if (typeof Tx[CurrencyField] === 'string') {
      Object.assign(_Tx, {
        [CurrencyField + 'XRP']: parseInt(Tx[CurrencyField]),
      })
    }
    if (typeof Tx[CurrencyField] === 'object' && typeof Tx[CurrencyField].currency !== 'undefined') {
      Object.assign(_Tx, {
        [CurrencyField + 'DEX']: {
          currency: Tx[CurrencyField].currency,
          issuer: Tx[CurrencyField].issuer,
          value: parseFloat(Tx[CurrencyField].value),
        }
      })
    }
  })

  // Special handling for timestamps
  _Tx._InsertedAt = bigquery.timestamp(new Date())
  
  return _Tx
}

async function processLedger(client, lastLedger) {
  const ledgerResult = await fetchLedgerTransactions(client, lastLedger + 1)
  const txCount = ledgerResult.transactions.length
  console.log(`${txCount > 0 ? 'Transactions in' : ' '.repeat(15)} ${ledgerResult.ledger_index}: `, txCount > 0 ? txCount : '-')

  if (txCount === 0) {
    return ledgerResult.ledger_index
  }

  const txs = ledgerResult.transactions.map((tx) => {
    return Object.assign(formatTxForDB(tx), {
      LedgerIndex: ledgerResult.ledger_index,
    })
  })

  const bqResult = await insertIntoDB(txs)
  console.log(`Inserted rows`, bqResult)
  return ledgerResult.ledger_index
}

async function main() {
  console.log('Fetch XRPL transactions into Google BigQuery')

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
      console.error('Error:', e)
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
