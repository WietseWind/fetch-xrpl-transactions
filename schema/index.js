const {
  PROJECT_ID,
  DATASET_NAME,
  TRANSACTION_TABLE_NAME,
  LEDGER_TABLE_NAME,
} = require('./CONSTANTS')
const {
  transactionSchema,
  CurrencyFields,
} = require('./transactions')
const { ledgerSchema } = require('./ledgers')

module.exports = {
  PROJECT_ID: PROJECT_ID,
  DATASET_NAME: DATASET_NAME,
  TRANSACTION_TABLE_NAME: TRANSACTION_TABLE_NAME,
  LEDGER_TABLE_NAME: LEDGER_TABLE_NAME,
  transactionSchema: transactionSchema,
  ledgerSchema: ledgerSchema,
  CurrencyFields: CurrencyFields,
}
