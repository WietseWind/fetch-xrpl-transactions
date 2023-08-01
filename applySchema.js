const {
  transactionSchema,
  ledgerSchema,
} = require('./schema')

const BigQuery = require('@google-cloud/bigquery')

async function createTable(name, schema, bigquery, dataset) {
  const result = await bigquery.dataset(dataset).createTable(name, { schema })
  console.log(` -- BigQuery Table ${result[0].id} created`)
}

async function deleteTable(name, bigquery, dataset) {
  try {
    await bigquery.dataset(dataset).table(name).delete()
    console.log(` -- BigQuery Table ${name} removed`)
  } catch(e) {
    if (e.errors[0].reason === 'notFound') {
      console.log(` Table ${name} doesn't yet exist - nothing to delete`)
    } else {
      throw e
    }
  }
}

async function recreateTable(name, schema, bigquery, dataset) {
  console.log(`Dropping and creating table [ ${name} ] in dataset [ ${dataset} ] @ Google BigQuery`)
  await deleteTable(name, bigquery, dataset)
  await createTable(name, schema, bigquery, dataset)
}

async function main() {
  const dbDetails = {
    projectID: process.env.PROJECT_ID?.trim(),
    datasetName: process.env.DATASET_NAME?.trim(),
    txTableName: process.env.TRANSACTION_TABLE_NAME?.trim(),
    ledgerTableName: process.env.LEDGER_TABLE_NAME?.trim(),
  }

  const hasInvalidValue = Object.values(dbDetails).find((value) => {
    return typeof value !== 'string' || value.length === 0
  }) !== undefined
  if (hasInvalidValue) {
    console.error('Invalid db args')
    process.exit(1)
  }

  const bigquery = new BigQuery({ projectId: dbDetails.projectID })

  await Promise.all([
    recreateTable(dbDetails.txTableName, transactionSchema, bigquery, dbDetails.datasetName),
    recreateTable(dbDetails.ledgerTableName, ledgerSchema, bigquery, dbDetails.datasetName),
  ])
  console.log(`Done\n`)
  process.exit(0)
}

main().then()
