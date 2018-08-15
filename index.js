const MongoClient = require('mongodb').MongoClient
const Client = require('rippled-ws-client')
const XRPLNodeUrl = typeof process.env.NODE === 'undefined' ? 'wss://s2.ripple.com' : process.env.NODE.trim()
const StartLedger = typeof process.env.LEDGER === 'undefined' ? 32750 : parseInt(process.env.LEDGER)
const MongoServer = typeof process.env.MONGO === 'undefined' ? 'mongodb://mongo:27017/xrpl' : process.env.MONGO.trim()

new Client(XRPLNodeUrl).then(Connection => {
    console.log('Connected to the XRPL')
    let MongoCollection
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
                MongoCollection.insertMany(Result.transactions.map(Tx => {
                    // Add the ledger index to the virtual key `__ledgerIndex`
                    return Object.assign(Tx, {
                        __ledgerIndex: Result.ledger_index
                    })
                }), (err, res) => {
                    if (err) {
                        console.error(err)
                        process.exit(1)
                    }
                    console.log(`${res.insertedCount} documents inserted`)           
                })
            }
            retryTimeout = 0
            return run(ledger_index + 1)
        }).catch(() => {
            retryTimeout += 500
            if (retryTimeout > 5000) retryTimeout = 5000
            console.log(`Oops... Retry in ${retryTimeout / 1000} sec.`)
            setTimeout(() => {
                return run(ledger_index)
            }, retryTimeout)
        })
    }

    MongoClient.connect(MongoServer, { useNewUrlParser: true }, (err, MongoDb) => {
        console.log('Connected to MongoDB')
        
        if (err) {
            console.error(err)
            process.exit(1)
        }
        
        MongoCollection = MongoDb.db('xrpl').collection('transactions')
        run(StartLedger)
    })
})
