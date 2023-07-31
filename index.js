const {
  TaskRunner,
  ledgerInfoProcess,
  ledgerGetLastDBLedger,
  transactionInfoProcess,
  transactionGetLastDBLedger,
} = require('./tasks')

const BigQuery = require('@google-cloud/bigquery')
const XrplClient = require('xrpl-client').XrplClient

const XrplRequestOptions = {
  timeoutSeconds: 10,
}
const Task = new TaskRunner()

function onRequestStop() {
  console.log(`\nGracefully shutting down\n -- Wait for remaining BigQuery inserts and XRPL Connection close...`);
  Task.stop()
}

function getDBDetails() {
  const dbDetails = {
    projectID: process.env.PROJECT_ID?.trim(),
    datasetName: process.env.DATASET_NAME?.trim(),
    tableName: process.env.TABLE_NAME?.trim(),
  }
  const hasInvalidValue = Object.values(dbDetails).find((value) => {
    return typeof value !== 'string' || value.length === 0
  }) !== undefined
  if (hasInvalidValue) {
    return null
  }
  return dbDetails
}

function getModeDetails() {
  const mode = process.env.MODE?.trim()
  if (mode === 'ledgers') {
    return {
      processFunc: ledgerProcess,
      lastDBLedgerFunc = ledgerLastDBLedger,
      message: 'Fetch XRPL Ledger Info into Google BigQuery',
    }
  }
  if (mode === 'transactions') {
    return {
      processFunc: transactionProcess,
      lastDBLedgerFunc = transactionLastDBLedger,
      message: 'Fetch XRPL transactions into Google BigQuery',
    }
  }
  return null
}

async function getInitialLastLedger(dbFunc, bigquery, dbDetails) {
  // Determine start ledger. lastLedger represents the last ledger that we
  // _have_ stored. So lastLedger + 1 is the next ledger we need. The
  // commandline input is supposed to represent the _next_ ledger, so we need
  // to subtract one.
  const lastDBLedger = await dbFunc({ bigquery, ...dbDetails })
  const cmdLineStartLedger = typeof process.env.LEDGER === 'undefined' ? 32570 : parseInt(process.env.LEDGER)
  if (lastDBLedger >= cmdLineStartLedger) {
    console.log(`BigQuery History at ledger [ ${lastDBLedger} ], > StartLedger.\n  Forcing StartLedger at:\n  >>> ${lastDBLedger+1}\n\n`)
    return lastDBLedger
  }
  console.log(`Starting at ledger ${cmdLineStartLedger}`)
  return Math.max(cmdLineStartLedger - 1, 1)
}

async function main() {
  const dbDetails = getDBDetails()
  if (dbDetails == null) {
    console.error('One or more BigQuery parameters are invalid or were omitted')
    process.exit(1)
  }
  const bigquery = new BigQuery({ projectId: dbDetails.projectID })

  const modeDetails = getModeDetails()
  if (modeDetails == null) {
    console.error('Invalid mode')
    process.exit(1)
  }
  console.log(modeDetails.message)

  let lastLedger = await getInitialLastLedger(modeDetails.lastDBLedgerFunc, bigquery, dbDetails)

  // Setup client
  const xrplNodeUrl = typeof process.env.NODE === 'undefined' ? 'wss://s2.ripple.com' : process.env.NODE.trim()
  const client = new XrplClient(xrplNodeUrl)
  await client.ready()
  console.log('Connected to the XRPL')
  const clientSender = async function (args) => {
    return await client.send(args, XrplRequestOptions)
  }

  // main loop
  const task = async function () => {
    lastLedger = await modeDetails.processFunc({
      clientSender,
      lastLedger,
      bigquery,
      dbDetails,
    })
  }
  await Task.start(task)

  // Cleanup
  console.log('Disconnecting from ledger')
  await client.close()
  console.log('Disconnected')
  console.log(`\nLast ledger: [ ${lastLedger} ]\n\nRun your next job with ENV: "LEDGER=${lastLedger+1}"\n\n`)

  process.exit(Task.errored ? 1 : 0)
}

process.on('SIGINT', onRequestStop)
process.on('SIGTERM', onRequestStop)

main().then()
