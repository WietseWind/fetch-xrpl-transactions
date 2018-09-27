const { schema, projectId, datasetName, tableName, CurrencyFields } = require('./schema')

const BigQuery = require('@google-cloud/bigquery')
const bigquery = new BigQuery({ projectId: projectId })

console.log(`Create table [ ${tableName} ] in dataset [ ${datasetName} ] @ Google BigQuery`)

const createTable = async () => {
  return new Promise((resolve, reject) => {
    bigquery.dataset(datasetName).createTable(tableName, { schema: schema })
      .then(r => {
        console.log(` -- BigQuery Table ${r[0].id} created`)
        resolve()
      })
      .catch(e => {
        reject(e)
      })
  })
}

const deleteTable = async () => {
  return new Promise((resolve, reject) => {
    bigquery.dataset(datasetName).table(tableName).delete().then(() => {
      console.log(` -- BigQuery Table ${tableName} removed`)
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

(async () => {
  await deleteTable()
  await createTable()

  console.log(`Done\n`)
})()
