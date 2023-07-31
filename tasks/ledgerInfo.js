async function fetchLedger(clientSender, ledgerIndex) {
  try {
    return await clientSender({
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

async function insertDBLedger(ledgerResult, bigquery, dbDetails) {
  try {
    await bigquery.dataset(dbDetails.datasetName).table(dbDetails.tableName).insert([{
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

async function process(args) {
  const {
    clientSender,
    lastLedger,
    bigquery,
    dbDetails,
  } = args
  const ledgerResult = await fetchLedger(clientSender, lastLedger + 1)
  await insertDBLedger(ledgerResult, bigquery, dbDetails)
  lastLedger = ledgerResult.ledger_index
  console.log(`${lastLedger} inserted`)
  return lastLedger
}

async function getLastDBLedger(args) {
  const { bigquery, projectID, datasetName, tableName } = args
  let result
  try {
    result = await bigquery.query({
      query: `SELECT MAX(LedgerIndex) as MaxLedger
              FROM ${projectID}.${datasetName}.${tableName}`,
      useLegacySql: false,
    })
  } catch(e) {
    console.error('Google BigQuery Error', e)
    throw e
  }
  return result[0][0].MaxLedger
}

module.exports = {
  process,
  getLastDBLedger,
}
