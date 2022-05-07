package pkg

import (
	"time"
)

type Blockchain int32

const (
	Blockchain_UNKNOWN  Blockchain = 0
	Blockchain_ETHEREUM Blockchain = 1
)

type Network int32

const (
	Network_UNKNOWN          Network = 0
	Network_ETHEREUM_MAINNET         = 1
)

// Identifier represents a generic struct of identifying properties
type Identifier struct {
	// A hash identifier
	Hash string
	// An index, sequence-based identifier (block height, sequence, index, etc)
	Index uint64
}

// Ledger represents a block or a ledger on a blockchain.
type Ledger struct {
	// Blockchain this ledger is part of
	Blockchain Blockchain
	// Blockchain network this ledger is a part of
	Network Network
	// Identification information for this ledger / block
	Identifier Identifier
	// Identifying information of a proceeding ledger in the blockchain
	PreviousLedger Identifier
	// Indicates whether this block is orphaned or not (default = false)
	Orphaned bool
	// Timestamp of when this ledger was created, as reported by the blockchain
	Timestamp time.Time
	// Transactions of the ledger
	Transactions []Transaction
}

// Transaction represents a transaction within a ledger or block on a blockchain
type Transaction struct {
	// Blockchain this transaction is part of
	Blockchain Blockchain
	// Blockchain network this transaction is a part of
	Network Network
	// Identification information of this transaction
	Identifier Identifier
	// Identifier of the ledger this transaction resides in
	Ledger Identifier
}
