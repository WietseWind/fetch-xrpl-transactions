const {
  transactionSchema,
  CurrencyFields,
} = require('../schema')

async function fetchLedgerTransactions(clientSender, ledgerIndex) {
  const result = await clientSender({
    command: 'ledger',
    ledger_index: ledgerIndex,
    transactions: true,
    expand: false,
  })

  if (typeof result.ledger.transactions === 'undefined' || result.ledger.transactions.length === 0) {
    return { ledger_index: ledgerIndex, transactions: [] }
  }

  if (result.ledger.transactions.length <= 200) {
    const txResults = await clientSender({
      command: 'ledger',
      ledger_index: ledgerIndex,
      transactions: true,
      expand: true,
    })
    return { ledger_index: ledgerIndex, transactions: txResults.ledger.transactions }
  }

  console.log(`<<< MANY TXS at ledger ${ledgerIndex}: [[ ${result.ledger.transactions.length} ]], processing per-tx...`)
  const txPromises = result.ledger.transactions.map((tx) => {
    return clientSender({
      command: 'tx',
      transaction: tx,
    })
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

async function insertIntoDB(txs, bigquery, dbDetails) {
  try {
    return await bigquery.dataset(dbDetails.datasetName).table(dbDetails.tableName).insert(txs)
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
    if (typeof tx[SchemaNode.description] !== 'undefined' 
        && tx[SchemaNode.description] !== null 
        && typeof tx[SchemaNode.description] !== 'object' 
        && SchemaNode.description === SchemaNode.name
    ) {
      let Value = tx[SchemaNode.description]
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
        && typeof tx.metaData[SchemaNode.name] !== 'undefined' 
        && tx.metaData[SchemaNode.name] !== null 
        && typeof tx.metaData[SchemaNode.name] !== 'object' 
        && SchemaNode.name !== 'DeliveredAmount'
    ) {
      Object.assign(_Tx, {
        [SchemaNode.name]: tx.metaData[SchemaNode.name],
      })
    }
  })

  if (typeof tx.metaData.DeliveredAmount === 'undefined' && typeof tx.metaData.delivered_amount !== 'undefined') {
    tx.metaData.DeliveredAmount = tx.metaData.delivered_amount
  }
  if (typeof tx.metaData.DeliveredAmount !== 'undefined') {
    let DeliveredAmount = parseInt(tx.metaData.DeliveredAmount)
    if (!isNaN(DeliveredAmount)) {
      Object.assign(_Tx, {
        DeliveredAmount: DeliveredAmount
      })
    }
  }

  if (typeof tx.Memos !== 'undefined') {
    Object.assign(_Tx, {
      Memos: tx.Memos.map(m => {
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

  if (tx.NFTokenOffers != null) {
    _Tx.NFTokenOffers = tx.NFTokenOffers
  }

  if (tx.metaData != null) {
    _Tx.Metadata = JSON.stringify(tx.metaData)
  }

  CurrencyFields.forEach(CurrencyField => {
    if (typeof tx[CurrencyField] === 'string') {
      Object.assign(_Tx, {
        [CurrencyField + 'XRP']: parseInt(tx[CurrencyField]),
      })
    }
    if (typeof tx[CurrencyField] === 'object' && typeof tx[CurrencyField].currency !== 'undefined') {
      Object.assign(_Tx, {
        [CurrencyField + 'DEX']: {
          currency: tx[CurrencyField].currency,
          issuer: tx[CurrencyField].issuer,
          value: parseFloat(tx[CurrencyField].value),
        }
      })
    }
  })
  
  return _Tx
}

async function process(args) {
  const {
    clientSender,
    lastLedger,
    bigquery,
    dbDetails,
  } = args
  const ledgerResult = await fetchLedgerTransactions(clientSender, lastLedger + 1)
  const txCount = ledgerResult.transactions.length
  console.log(`${txCount > 0 ? 'Transactions in' : ' '.repeat(15)} ${ledgerResult.ledger_index}: `, txCount > 0 ? txCount : '-')

  if (txCount === 0) {
    return ledgerResult.ledger_index
  }

  const txs = ledgerResult.transactions.map((tx) => {
    return Object.assign(formatTxForDB(tx), {
      LedgerIndex: ledgerResult.ledger_index,
      _InsertedAt: bigquery.timestamp(new Date()),
    })
  })

  const bqResult = await insertIntoDB(txs, bigquery, dbDetails)
  console.log(`Inserted rows`, bqResult)
  return ledgerResult.ledger_index
}

async function getLastDBLedger(args) {
  const { bigquery, projectID, datasetName, tableName } = args
  let result
  try {
    result = await bigquery.query({
      query: `SELECT 
                COUNT(1) as TxCount,
                MIN(LedgerIndex) as MinLedger,
                MAX(LedgerIndex) as MaxLedger,
                COUNT(DISTINCT LedgerIndex) as LedgersWithTxCount
              FROM 
                ${projectID}.${datasetName}.${tableName}`,
      useLegacySql: false,
    })
  } catch(e) {
    console.log('Google BigQuery Error', e)
    throw e
  }
  return result[0][0].MaxLedger
}

module.exports = {
  process,
  getLastDBLedger,
}
