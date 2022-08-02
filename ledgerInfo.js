const { projectId, datasetName } = require('./schema')

const Client = require('rippled-ws-client')
const BigQuery = require('@google-cloud/bigquery')
const bigquery = new BigQuery({ projectId: projectId })

const XRPLNodeUrl = typeof process.env.NODE === 'undefined' ? 'wss://s2.ripple.com' : process.env.NODE.trim()
const StartLedger = typeof process.env.LEDGER === 'undefined' ? 32570 : parseInt(process.env.LEDGER)

console.log('Fetch XRPL Ledger Info into Google BigQuery')
  
new Client(XRPLNodeUrl).then(Connection => {
  let Stopped = false
  let LastLedger = 0

  console.log('Connected to the XRPL')
  let retryTimeout = 60 * 60 * 12

  const fetchLedger = (ledger_index) => {
    return new Promise((resolve, reject) => {
      return Connection.send({
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
      // console.log(Result)
      bigquery.dataset(datasetName).table('ledgers').insert([{
        LedgerIndex: parseInt(Result.ledger.ledger_index),
        hash: Result.ledger.hash,
        CloseTime: new Date(Date.parse(Result.ledger.close_time_human)).toISOString().replace('T', ' ').replace(/[^0-9]+$/, ''),
        CloseTimeTimestamp: Result.ledger.close_time,
        CloseTimeHuman: Result.ledger.close_time_human,
        TotalCoins: parseInt(Result.ledger.totalCoins),
        ParentHash: Result.ledger.parent_hash,
        AccountHash: Result.ledger.account_hash,
        TransactionHash: Result.ledger.transaction_hash
      }])
        .then(r => {
          console.log(`Inserted rows`, r)
          LastLedger = Result.ledger_index
          // process.exit(0)
        })
        .catch(err => {
          if (err && err.name === 'PartialFailureError') {
            if (err.errors && err.errors.length > 0) {
              console.log('Insert errors:')
              err.errors.forEach(err => console.dir(err, { depth: null }))
              process.exit(1)
            }
          } else {
            console.error('ERROR:', err)
            process.exit(1)
          }
        })

//      retryTimeout = 0
      
      if (Stopped) {
        return
      }

      return run(ledger_index + 1)
    }).catch(e => {
      console.log(e)
      process.exit(1)

      // retryTimeout += 500
//      if (retryTimeout > 5000) retryTimeout = 5000
      console.log(`Oops... Retry in ${retryTimeout / 1000} sec.`)
      setTimeout(() => {
        return run(ledger_index)
      }, retryTimeout * 1000)
    })
  }

  console.log(`Starting at ledger [ ${StartLedger} ], \n  Checking last ledger in BigQuery...`)

  bigquery.query({
    query: `SELECT 
              MAX(LedgerIndex) as MaxLedger
            FROM 
              ${projectId}.${datasetName}.ledgers`,
    useLegacySql: false, // Use standard SQL syntax for queries.
  }).then(r => {
    if (r[0][0].MaxLedger > StartLedger) {
      console.log(`BigQuery History at ledger [ ${r[0][0].MaxLedger} ], > StartLedger.\n  Forcing StartLedger at:\n  >>> ${r[0][0].MaxLedger+1}\n\n`)
      run(r[0][0].MaxLedger + 1)
    } else{
      run(StartLedger)
    }
  }).catch(e => {
    if (e.message.match(/Not found: Table xrpledgerdata:fullhistory.ledgers was not found/)) {
      console.log('>> Create table ...')

      const schema = [
        {
          name: "LedgerIndex",
          type: "INTEGER",
          mode: "NULLABLE",
          description: ""
        },
        {
          name: "hash",
          type: "STRING",
          mode: "NULLABLE",
          description: ""
        },
        {
          name: "CloseTime",
          type: "DATETIME",
          mode: "NULLABLE",
          description: ""
        },
        {
          name: "CloseTimeTimestamp",
          type: "INTEGER",
          mode: "NULLABLE",
          description: ""
        },
        {
          name: "CloseTimeHuman",
          type: "STRING",
          mode: "NULLABLE",
          description: ""
        },
        {
          name: "TotalCoins",
          type: "INTEGER",
          mode: "NULLABLE",
          description: ""
        },
        {
          name: "ParentHash",
          type: "STRING",
          mode: "NULLABLE",
          description: ""
        },
        {
          name: "AccountHash",
          type: "STRING",
          mode: "NULLABLE",
          description: ""
        },
        {
          name: "TransactionHash",
          type: "STRING",
          mode: "NULLABLE",
          description: ""
        }
      ]      
      bigquery.dataset(datasetName).createTable('ledgers', { schema: schema })
        .then(r => {
          console.log(` -- BigQuery Table ${r[0].id} created`)
          process.exit(0)
        })
    } else {
      console.log('Google BigQuery Error', e)
      process.exit(1)
    }
  })

  process.on('SIGINT', function() {
    console.log(`\nGracefully shutting down from SIGINT (Ctrl+C)\n -- Wait for remaining BigQuery inserts and XRPL Connection close...`);
  
    Stopped = true  
    Connection.close()
    if (LastLedger > 0) {
      console.log(`\nLast ledger: [ ${LastLedger} ]\n\nRun your next job with ENV: "LEDGER=${LastLedger+1}"\n\n`)
    }
  })
})
