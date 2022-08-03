const schema = [
  {
    name: "LedgerIndex",
    type: "INTEGER",
    mode: "NULLABLE",
    description: ""
  },
  {
    name: "hash",
    type: "STRING",
    mode: "NULLABLE",
    description: ""
  },
  {
    name: "CloseTime",
    type: "TIMESTAMP",
    mode: "NULLABLE",
    description: ""
  },
  {
    name: "CloseTimeTimestamp",
    type: "INTEGER",
    mode: "NULLABLE",
    description: ""
  },
  {
    name: "CloseTimeHuman",
    type: "STRING",
    mode: "NULLABLE",
    description: ""
  },
  {
    name: "TotalCoins",
    type: "INTEGER",
    mode: "NULLABLE",
    description: ""
  },
  {
    name: "ParentHash",
    type: "STRING",
    mode: "NULLABLE",
    description: ""
  },
  {
    name: "AccountHash",
    type: "STRING",
    mode: "NULLABLE",
    description: ""
  },
  {
    name: "TransactionHash",
    type: "STRING",
    mode: "NULLABLE",
    description: ""
  },
  {
    name: "_InsertedAt",
    type: "TIMESTAMP",
    mode: "NULLABLE",
    descriptpion: "When row was inserted",
  },
]      

module.exports = {
  ledgerSchema: schema,
}
