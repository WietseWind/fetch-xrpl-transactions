const { schema, projectId, datasetName, tableName, CurrencyFields } = require('./schema')

const Client = require('rippled-ws-client')
const BigQuery = require('@google-cloud/bigquery')
const bigquery = new BigQuery({ projectId: projectId })

const XRPLNodeUrl = typeof process.env.NODE === 'undefined' ? 'wss://s2.ripple.com' : process.env.NODE.trim()
const StartLedger = typeof process.env.LEDGER === 'undefined' ? 32570 : parseInt(process.env.LEDGER)

console.log('Fetch XRPL transactions into Google BigQuery')
  
new Client(XRPLNodeUrl).then(Connection => {
  let Stopped = false
  let LastLedger = 0

  console.log('Connected to the XRPL')
  let retryTimeout = 0

  const fetchLedgerTransactions = (ledger_index) => {
    return new Promise((resolve, reject) => {
      return Connection.send({
        command: 'ledger',
        ledger_index: parseInt(ledger_index),
        transactions: true,
        expand: true
      }).then(Result => {
        resolve({
          ledger_index: ledger_index,
          transactions: Result.ledger.transactions
        })
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
          schema.forEach(SchemaNode => {
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
              Memos: Tx.Memos
            })
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
          
          return _Tx
        })
        
        // console.dir(Transactions[0], { depth: null })
        // process.exit(1)

        bigquery.dataset(datasetName).table(tableName).insert(Transactions)
          .then(r => {
            console.log(`Inserted rows`, r)
            LastLedger = Result.ledger_index
            // process.exit(0)
          })
          .catch(err => {
            if (err && err.name === 'PartialFailureError') {
              if (err.errors && err.errors.length > 0) {
                console.log('Insert errors:')
                err.errors.forEach(err => console.error(err))
                process.exit(1)
              }
            } else {
              console.error('ERROR:', err)
              process.exit(1)
            }
          })
      }
      
      retryTimeout = 0
      
      // return // If only one
      return run(ledger_index + 1)
    }).catch(e => {
      console.log(e)
      process.exit(1)

      retryTimeout += 500
      if (retryTimeout > 5000) retryTimeout = 5000
      console.log(`Oops... Retry in ${retryTimeout / 1000} sec.`)
      setTimeout(() => {
        return run(ledger_index)
      }, retryTimeout)
    })
  }

  run(StartLedger)
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