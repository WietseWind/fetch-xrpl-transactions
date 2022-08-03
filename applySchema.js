const {
  PROJECT_ID,
  DATASET_NAME,
  TRANSACTION_TABLE_NAME,
  LEDGER_TABLE_NAME,
  transactionSchema,
  ledgerSchema,
} = require('./schema')

const BigQuery = require('@google-cloud/bigquery')
const bigquery = new BigQuery({ projectId: PROJECT_ID })

const createTable = async (name, schema) => {
  return new Promise((resolve, reject) => {
    bigquery.dataset(DATASET_NAME).createTable(name, { schema: schema })
      .then(r => {
        console.log(` -- BigQuery Table ${r[0].id} created`)
        resolve()
      })
      .catch(e => {
        reject(e)
      })
  })
}

const deleteTable = async (name) => {
  return new Promise((resolve, reject) => {
    bigquery.dataset(DATASET_NAME).table(name).delete().then(() => {
      console.log(` -- BigQuery Table ${name} removed`)
      resolve()
    }).catch(e => {
      if (e.errors[0].reason === 'notFound') {
        resolve()
      } else{
        reject(e)
      }
    })
  })
}

const recreateTable = async (name, schema) => {
  console.log(`Dropping and creating table [ ${name} ] in dataset [ ${DATASET_NAME} ] @ Google BigQuery`)

  await deleteTable(name)
  await createTable(name, schema)
}

(async () => {
  await Promise.all([
    recreateTable(TRANSACTION_TABLE_NAME, transactionSchema),
    recreateTable(LEDGER_TABLE_NAME, ledgerSchema),
  ])

  console.log(`Done\n`)
})()
