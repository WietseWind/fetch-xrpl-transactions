const {
  PROJECT_ID,
  DATASET_NAME,
  LEDGER_TABLE_NAME,
} = require('./schema')

const XrplClient = require('xrpl-client').XrplClient
const BigQuery = require('@google-cloud/bigquery')
const bigquery = new BigQuery({ projectId: PROJECT_ID })

const XRPLNodeUrl = typeof process.env.NODE === 'undefined' ? 'wss://s2.ripple.com' : process.env.NODE.trim()
const StartLedger = typeof process.env.LEDGER === 'undefined' ? 32570 : parseInt(process.env.LEDGER)

console.log('Fetch XRPL Ledger Info into Google BigQuery')

const Client = new XrplClient(XRPLNodeUrl)

async function safeHalt() {
  try {
    await Client.close()
  } catch(e) {
    console.error('ERROR closing connection:', e)
  } finally {
    process.exit(1)
  }
}
  
Client.ready().then(() => {
  let Stopped = false
  let LastLedger = 0

  console.log('Connected to the XRPL')
  let retryTimeout = 60 * 60 * 12

  const fetchLedger = (ledger_index) => {
    return new Promise((resolve, reject) => {
      return Client.send({
        command: 'ledger',
        ledger_index: parseInt(ledger_index),
        transactions: false,
        expand: false
      }).then(Result => {
        resolve(Result)
        return
      }).catch(reject)
    })
  }

  const run = (ledger_index) => {
    return fetchLedger(ledger_index).then(Result => {
      console.log(`${Result.ledger_index}`)
      bigquery.dataset(DATASET_NAME).table(LEDGER_TABLE_NAME).insert([{
        LedgerIndex: parseInt(Result.ledger.ledger_index),
        hash: Result.ledger.hash,
        CloseTime: bigquery.timestamp(new Date(Date.parse(Result.ledger.close_time_human)).toISOString().replace('T', ' ').replace(/[^0-9]+$/, '')),
        CloseTimeTimestamp: Result.ledger.close_time,
        CloseTimeHuman: Result.ledger.close_time_human,
        TotalCoins: parseInt(Result.ledger.totalCoins),
        ParentHash: Result.ledger.parent_hash,
        AccountHash: Result.ledger.account_hash,
        TransactionHash: Result.ledger.transaction_hash,
        _InsertedAt: bigquery.timestamp(new Date()),
      }])
        .then(r => {
          console.log(`Inserted rows`, r)
          LastLedger = Result.ledger_index
        })
        .catch(err => {
          if (err && err.name === 'PartialFailureError') {
            if (err.errors && err.errors.length > 0) {
              console.log('Insert errors:')
              err.errors.forEach(err => console.dir(err, { depth: null }))
              return safeHalt()
            }
          } else {
            console.error('ERROR:', err)
            return safeHalt()
          }
        })
      
      if (Stopped) {
        return Client.close()
      }

      return run(ledger_index + 1)
    }).catch(e => {
      console.log(e)
      return safeHalt()
    })
  }

  console.log(`Starting at ledger [ ${StartLedger} ], \n  Checking last ledger in BigQuery...`)

  bigquery.query({
    query: `SELECT 
              MAX(LedgerIndex) as MaxLedger
            FROM 
              ${PROJECT_ID}.${DATASET_NAME}.${LEDGER_TABLE_NAME}`,
    useLegacySql: false, // Use standard SQL syntax for queries.
  }).then(r => {
    if (r[0][0].MaxLedger > StartLedger) {
      console.log(`BigQuery History at ledger [ ${r[0][0].MaxLedger} ], > StartLedger.\n  Forcing StartLedger at:\n  >>> ${r[0][0].MaxLedger+1}\n\n`)
      run(r[0][0].MaxLedger + 1)
    } else{
      run(StartLedger)
    }
  }).catch(e => {
    console.log('Google BigQuery Error', e)
    return safeHalt()
  })

  process.on('SIGINT', function() {
    console.log(`\nGracefully shutting down from SIGINT (Ctrl+C)\n -- Wait for remaining BigQuery inserts and XRPL Connection close...`);
  
    Stopped = true  
    if (LastLedger > 0) {
      console.log(`\nLast ledger: [ ${LastLedger} ]\n\nRun your next job with ENV: "LEDGER=${LastLedger+1}"\n\n`)
    }
  })
})
