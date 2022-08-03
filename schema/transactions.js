/**
 * BigQuery Schema;
 *  Type:
 *      STRING
 *      BYTES
 *      INTEGER
 *      FLOAT
 *      NUMERIC
 *      BOOLEAN
 *      TIMESTAMP
 *      DATE
 *      TIME
 *      DATETIME
 *      GEOGRAPHY
 *      RECORD
 *           MODE: REPEATED
 *               fields: [ {}, {} ] 
 *   Mode:
 *      NULLABLE 
 *      (RECORD: REPEATED)
 */

const CurrencyFields = [
  'Amount',
  'TakerGets',
  'TakerPays',
  'SendMax',
  'LimitAmount',
  'SendMin',
  'DeliverMin',
  'NFTokenBrokerFee',
]

const TxTypeFields = [
  {
    TransactionType: "STRING",
    Account: "STRING",
    hash: "STRING",
    Destination: "STRING",
    Fee: "INTEGER",
    Flags: "INTEGER",
    DestinationTag: "INTEGER",
    SourceTag: "INTEGER",
    InvoiceID: "STRING",
    Sequence: "INTEGER",
  },
  {
    SigningPubKey: "STRING",
    TxnSignature: "STRING",
    AccountTxnID: "STRING",
    SignerQuorum: "INTEGER",
    ClearFlag: "INTEGER",
    Domain: "STRING",
    EmailHash: "STRING",
    MessageKey: "STRING",
    SetFlag: "INTEGER",
    TransferRate: "INTEGER",
    TickSize: "INTEGER",
    CheckID: "STRING",
    Expiration: "INTEGER",
    Authorize: "STRING",
    Owner: "STRING",
    OfferSequence: "INTEGER",
    Condition: "STRING",
    FinishAfter: "INTEGER",
    CancelAfter: "INTEGER",
    Fulfillment: "STRING",
    LastLedgerSequence: "INTEGER",
    SettleDelay: "INTEGER",
    RegularKey: "STRING",
    QualityIn: "INTEGER",
    QualityOut: "INTEGER",
    Amendment: "STRING",
    LedgerSequence: "INTEGER",
    BaseFee: "INTEGER",
    ReferenceFeeUnits: "INTEGER",
    ReserveBase: "INTEGER",
    ReserveIncrement: "INTEGER",
    NFTokenTaxon: "INTEGER",
    URI: "STRING",
    Issuer: "STRING",
    TransferFee: "INTEGER",
    NFTokenID: "STRING",
    NFTokenSellOffer: "STRING",
    NFTokenBuyOffer: "STRING",
  }
]

const schema = [
  {
    name: "LedgerIndex",
    type: "INTEGER",
    mode: "NULLABLE",
    description: "XRPL ledger index"
  },
  {
    name: "TransactionResult",
    type: "STRING",
    mode: "NULLABLE",
    description: "metaData.TransactionResult"
  },
  {
    name: "TransactionIndex",
    type: "INTEGER",
    mode: "NULLABLE",
    description: "metaData.TransactionIndex"
  },
  {
    name: "DeliveredAmount",
    type: "INTEGER",
    mode: "NULLABLE",
    description: "metaData.DeliveredAmount"
  },
  {
    name: "_InsertedAt",
    type: "TIMESTAMP",
    mode: "NULLABLE",
    description: "When row was inserted",
  },
  {
    name: "Memos",
    type: "RECORD",
    mode: "REPEATED",
    description: "Memos",
    fields: [
      {
        name: "Memo",
        type: "RECORD",
        mode: "NULLABLE",
        description: "Memos[].Memo",
        fields: [
          {
            name: "MemoData",
            type: "STRING",
            mode: "NULLABLE",
            description: "Memos[].Memo.MemoData",
          },
          {
            name: "MemoFormat",
            type: "STRING",
            mode: "NULLABLE",
            description: "Memos[].Memo.MemoFormat",
          },
          {
            name: "MemoType",
            type: "STRING",
            mode: "NULLABLE",
            description: "Memos[].Memo.MemoType",
          },
        ]
      },
    ]
  },
  {
    name: "SignerEntries",
    type: "RECORD",
    mode: "REPEATED",
    description: "SignerEntries",
    fields: [
      {
        name: "SignerEntry",
        type: "RECORD",
        mode: "NULLABLE",
        description: "SignerEntries[].SignerEntry",
        fields: [
          {
            name: "Account",
            type: "STRING",
            mode: "NULLABLE",
            description: "SignerEntries[].SignerEntry.Account",
          },
          {
            name: "SignerWeight",
            type: "INTEGER",
            mode: "NULLABLE",
            description: "SignerEntries[].SignerEntry.SignerWeight",
          },
        ]
      },
    ]
  },
  {
    name: "Signers",
    type: "RECORD",
    mode: "REPEATED",
    description: "Signers",
    fields: [
      {
        name: "SignerEntry",
        type: "RECORD",
        mode: "NULLABLE",
        description: "Signers[].Signer",
        fields: [
          {
            name: "Account",
            type: "STRING",
            mode: "NULLABLE",
            description: "Signers[].Signer.Account",
          },
          {
            name: "SigningPubKey",
            type: "STRING",
            mode: "NULLABLE",
            description: "Signers[].Signer.SigningPubKey",
          },
          {
            name: "TxnSignature",
            type: "STRING",
            mode: "NULLABLE",
            description: "Signers[].Signer.TxnSignature",
          },
        ]
     },
    ]
  },
  {
    name: "NFTokenOffers",
    type: "STRING",
    mode: "REPEATED",
    description: "NFTokenOffers",
  }
]

CurrencyFields.forEach(Field => {
  schema.unshift({
    name: `${Field}XRP`,
    type: "INTEGER",
    mode: "NULLABLE",
    description: Field
  })
})

Object.keys(TxTypeFields[0]).forEach(TxTypeField => {
  schema.unshift({
    name: TxTypeField,
    type: TxTypeFields[0][TxTypeField],
    mode: "NULLABLE",
    description: TxTypeField,
  })
})

Object.keys(TxTypeFields[1]).forEach(TxTypeField => {
  schema.push({
    name: TxTypeField,
    type: TxTypeFields[1][TxTypeField],
    mode: "NULLABLE",
    description: TxTypeField,
  })
})

CurrencyFields.forEach(Field => {
  schema.push({
    name: `${Field}DEX`,
    type: "RECORD",
    mode: "NULLABLE",
    description: Field,
    fields: [
      {
        name: "currency",
        type: "STRING",
        mode: "NULLABLE",
        description: "LimitAmount.currency",
      },
      {
        name: "issuer",
        type: "STRING",
        mode: "NULLABLE",
        description: "LimitAmount.issuer",
      },
      {
        name: "value",
        type: "FLOAT",
        mode: "NULLABLE",
        description: "LimitAmount.value"
      },
    ]
  })
})

module.exports = {
  transactionSchema: schema,
  CurrencyFields: CurrencyFields
}
