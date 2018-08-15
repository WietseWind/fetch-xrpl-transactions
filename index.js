const Client = require('rippled-ws-client')

const storeTransaction = (Tx) => {
    // Do something, eg. Mongo?
    return true
}

new Client('wss://rippled.xrptipbot.com').then(Connection => {
    const fetchLedgerTransactions = (ledger_index) => {
        return new Promise((resolve, reject) => {
            Connection.send({
                command: 'ledger',
                ledger_index: parseInt(ledger_index),
                transactions: true,
                expand: true
            }).then(Result => {
                resolve({
                    ledger_index: ledger_index,
                    transactions: Result.ledger.transactions
                })
            }).catch(reject)
        })
    }

    const run = (start_at_ledger) => {
        fetchLedgerTransactions(start_at_ledger).then(Result => {
            let txCount = Result.transactions.length
            console.log(`${txCount > 0 ? 'Transactions in' : ' '.repeat(15)} ${Result.ledger_index}: `, txCount > 0 ? txCount : '-')
            if (txCount > 0) {
                // Or insertMany, send the entire set to eg. mongo
                Result.transactions.forEach(storeTransaction)
            }
            // Todo: insert Result.transactions in somedb.
            run(start_at_ledger + 1)
        })
    }

    // run(32750) // First ledger
    run(20000000)
})