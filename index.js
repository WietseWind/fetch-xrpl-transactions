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

const XRPLNodeUrl = typeof process.env.NODE === 'undefined' ? 'wss://s2.ripple.com' : process.env.NODE.trim()
const StartLedger = typeof process.env.LEDGER === 'undefined' ? 32570 : parseInt(process.env.LEDGER)

console.log('Fetch XRPL transactions into Google BigQuery')

const Client = new XrplClient(XRPLNodeUrl)
  
Client.ready().then(Connection => {
  let Stopped = false
  let LastLedger = 0

  console.log('Connected to the XRPL')
  let retryTimeout = 60 * 60 * 12

  const fetchLedgerTransactions = (ledger_index) => {
    return new Promise((resolve, reject) => {
      return Connection.send({
        command: 'ledger',
        ledger_index: parseInt(ledger_index),
        transactions: true,
        expand: false
      }, 10).then(Result => {
        if (typeof Result.ledger.transactions === 'undefined' || Result.ledger.transactions.length === 0) {
          // Do nothing
          resolve({ ledger_index: ledger_index, transactions: [] })
          return
        } else {
          if (Result.ledger.transactions.length > 200) {
            // Lots of data. Per TX
            console.log(`<<< MANY TXS at ledger ${ledger_index}: [[ ${Result.ledger.transactions.length} ]], processing per-tx...`)
            let transactions = Result.ledger.transactions.map(Tx => {
              return Connection.send({
                command: 'tx',
                transaction: Tx
              }, 10)
            })
            Promise.all(transactions).then(r => {
              let allTxs = r.filter(t => {
                return typeof t.error === 'undefined' && typeof t.meta !== 'undefined' && typeof t.meta.TransactionResult !== 'undefined'
              })
              console.log('>>> ALL TXS FETCHED:', allTxs.length)
              resolve({ ledger_index: ledger_index, transactions: allTxs.map(t => {
                return Object.assign(t, {
                  metaData: t.meta
                })
              }) })
              return
            })
          } else {
            // Fetch at once.
            resolve(new Promise((resolve, reject) => {
              Connection.send({
                command: 'ledger',
                ledger_index: parseInt(ledger_index),
                transactions: true,
                expand: true
              }, 10).then(Result => {
                resolve({ ledger_index: ledger_index, transactions: Result.ledger.transactions })
                return
              }).catch(reject)
            }))
          }
        }
        return
      }).catch(reject)
    })
  }

  const run = (ledger_index) => {
    return fetchLedgerTransactions(ledger_index).then(Result => {
      let txCount = Result.transactions.length
      console.log(`${txCount > 0 ? 'Transactions in' : ' '.repeat(15)} ${Result.ledger_index}: `, txCount > 0 ? txCount : '-')

      if (txCount > 0) {
        let Transactions = Result.transactions.map(Tx => {
          let _Tx = {
            LedgerIndex: Result.ledger_index
          }
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
                [SchemaNode.name]: Value
              })
            }
            if (SchemaNode.description.match(/^metaData\./)
                && typeof Tx.metaData[SchemaNode.name] !== 'undefined' 
                && Tx.metaData[SchemaNode.name] !== null 
                && typeof Tx.metaData[SchemaNode.name] !== 'object' 
                && SchemaNode.name !== 'DeliveredAmount'
            ) {
              Object.assign(_Tx, {
                [SchemaNode.name]: Tx.metaData[SchemaNode.name]
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
                [CurrencyField + 'XRP']: parseInt(Tx[CurrencyField])
              })
            }
            if (typeof Tx[CurrencyField] === 'object' && typeof Tx[CurrencyField].currency !== 'undefined') {
              Object.assign(_Tx, {
                [CurrencyField + 'DEX']: {
                  currency: Tx[CurrencyField].currency,
                  issuer: Tx[CurrencyField].issuer,
                  value: parseFloat(Tx[CurrencyField].value)
                }
              })
            }
          })

          // Special handling for timestamps
          _Tx._InsertedAt = bigquery.timestamp(new Date())
          
          return _Tx
        })
        
        // console.dir(Transactions[0], { depth: null })
        // process.exit(1)

        bigquery.dataset(DATASET_NAME).table(TRANSACTION_TABLE_NAME).insert(Transactions)
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
      }

      // retryTimeout = 0
      
      if (Stopped) {
        return
      }

      return run(ledger_index + 1)
    }).catch(e => {
      console.log(e)
      process.exit(1)

      // retryTimeout += 500
      // if (retryTimeout > 5000) retryTimeout = 5000
      console.log(`Oops... Retry in ${retryTimeout / 1000} sec.`)
      setTimeout(() => {
        return run(ledger_index)
      }, retryTimeout * 1000)
    })
  }

  console.log(`Starting at ledger [ ${StartLedger} ], \n  Checking last ledger in BigQuery...`)

  bigquery.query({
    query: `SELECT 
              COUNT(1) as TxCount,
              MIN(LedgerIndex) as MinLedger,
              MAX(LedgerIndex) as MaxLedger,
              COUNT(DISTINCT LedgerIndex) as LedgersWithTxCount
            FROM 
              ${PROJECT_ID}.${DATASET_NAME}.${TRANSACTION_TABLE_NAME}`,
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
    process.exit(1)
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
