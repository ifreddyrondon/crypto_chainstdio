package blockchain

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common/hexutil"

	"github.com/ifreddyrondon/crypto_chainstdio/pkg"
	"github.com/ifreddyrondon/crypto_chainstdio/pkg/jsonrpc"
)

type ethereumRequestMethod string

const (
	// JSON RPC method to get an ethereum block by its hash
	ethGetBlockByHashMethod ethereumRequestMethod = "eth_getBlockByHash"
	// JSON RPC method to get an ethereum block by its number
	ethGetBlockByNumberMethod ethereumRequestMethod = "eth_getBlockByNumber"

	// JSON RPC method to get number of block tip
	ethGetChainStatsMethods ethereumRequestMethod = "eth_blockNumber"
)

// EthereumBlock is an ethereum block
type EthereumBlock struct {
	Hash         EthereumHexString     `json:"hash"`
	ParentHash   EthereumHexString     `json:"parentHash"`
	Number       string                `json:"number"`
	Timestamp    string                `json:"timestamp"`
	Transactions []EthereumTransaction `json:"transactions"`
	BaseFee      string                `json:"baseFeePerGas"`
}

func (b EthereumBlock) toLedger() (pkg.Ledger, error) {
	ledger := pkg.Ledger{Blockchain: pkg.Blockchain_ETHEREUM}
	cleanedNumber, err := CleanHexString(b.Number)
	if err != nil {
		return ledger, fmt.Errorf("error cleaning hex block number string %s. err: %w", b.Number, err)
	}
	blockNumber, err := hexutil.DecodeUint64(cleanedNumber)
	if err != nil {
		return ledger, fmt.Errorf("error decoding block number hex %s. err: %w", cleanedNumber, err)
	}
	prevBlockNumber := blockNumber - 1

	// the genesis block's predecessor is itself
	if blockNumber == 0 {
		prevBlockNumber = 0
	}

	cleanedTimestamp, err := CleanHexString(b.Timestamp)
	if err != nil {
		return ledger, fmt.Errorf("error cleaning hex block timestamp string %s. err: %w", b.Timestamp, err)
	}
	blockTimestamp, err := hexutil.DecodeUint64(cleanedTimestamp)
	if err != nil {
		return ledger, fmt.Errorf("error decoding block timestamp hex %s. err: %w", cleanedTimestamp, err)
	}

	ledger.Identifier = pkg.Identifier{Hash: string(b.Hash), Index: blockNumber}
	ledger.PreviousLedger = pkg.Identifier{Hash: string(b.ParentHash), Index: prevBlockNumber}
	ledger.Timestamp = time.Unix(int64(blockTimestamp), 0).UTC()

	return ledger, nil
}

// EthereumHexString is a special string type to handle normalizing ETH hex strings during unmarshal
type EthereumHexString string

func (e *EthereumHexString) UnmarshalJSON(input []byte) error {
	var s string
	if err := json.Unmarshal(input, &s); err != nil {
		return err
	}
	s = strings.ToLower(s)

	*e = EthereumHexString(s)

	return nil
}

// EthereumTransaction is an ethereum transaction
type EthereumTransaction struct {
	BlockHash            EthereumHexString `json:"blockHash"`
	BlockNumber          string            `json:"blockNumber"`
	ChainID              string            `json:"chainId"`
	Hash                 EthereumHexString `json:"hash"`
	From                 EthereumHexString `json:"from"`
	To                   EthereumHexString `json:"to"`
	Input                string            `json:"input"`
	Value                string            `json:"value"`
	Nonce                string            `json:"nonce"`
	Gas                  string            `json:"gas"`
	GasPrice             string            `json:"gasPrice"`
	MaxPriorityFeePerGas string            `json:"maxPriorityFeePerGas"`
	MaxFeePerGas         string            `json:"maxFeePerGas"`
	Index                string            `json:"transactionIndex"`
	Type                 string            `json:"type"`
}

type Ethereum struct {
	nodesURL string
	network  pkg.Network
	c        *jsonrpc.Client
}

func NewEthereum(nodesURL string, network pkg.Network, c *http.Client) Ethereum {
	return Ethereum{
		c:        jsonrpc.New(c),
		nodesURL: nodesURL,
		network:  network,
	}
}

// Highest fetches the tip block
func (e Ethereum) Highest(ctx context.Context) (pkg.Ledger, error) {
	jsonreq := jsonrpc.Request{
		Method: string(ethGetChainStatsMethods),
		Params: []interface{}{},
	}

	var l pkg.Ledger
	req, err := jsonrpc.NewRequest(ctx, e.nodesURL, jsonreq)
	if err != nil {
		return l, fmt.Errorf("failed to create request: %w", err)
	}
	var resp jsonrpc.Response
	if err := e.do(req, &resp); err != nil {
		return pkg.Ledger{}, fmt.Errorf("error fetching latest block number: %w", err)
	}
	var hex string
	if err := json.Unmarshal(resp.Result, &hex); err != nil {
		return l, fmt.Errorf("error unmarshaling latest block number %s: %w", string(resp.Result), err)
	}
	cleanedResult, err := CleanHexString(hex)
	if err != nil {
		return l, fmt.Errorf("error cleaning hex block number %s. err: %w", hex, err)
	}
	height, err := hexutil.DecodeUint64(cleanedResult)
	if err != nil {
		return l, fmt.Errorf("error decoding block number hex %s. err: %w", cleanedResult, err)
	}
	return e.Ledger(ctx, pkg.Identifier{Index: height})
}

// Ledger fetches a ledger (block) by hash or by index
func (e Ethereum) Ledger(ctx context.Context, id pkg.Identifier) (pkg.Ledger, error) {
	l, err := e.Ledgers(ctx, id)
	if err != nil {
		return pkg.Ledger{}, fmt.Errorf("error getting block. %w", err)
	}
	return l[0], nil
}

// Ledgers fetches ledgers (blocks) by hash or by index
func (e Ethereum) Ledgers(ctx context.Context, ids ...pkg.Identifier) ([]pkg.Ledger, error) {
	var jsonreqs []jsonrpc.Request
	for _, id := range ids {
		var params []interface{}
		var method ethereumRequestMethod
		if id.Hash != "" {
			method = ethGetBlockByHashMethod
			params = []interface{}{id.Hash, true}
		} else {
			method = ethGetBlockByNumberMethod
			params = []interface{}{hexutil.EncodeUint64(id.Index), true}
		}
		jsonreqs = append(jsonreqs, jsonrpc.Request{Method: string(method), Params: params})
	}

	req, err := jsonrpc.NewBatchRequest(ctx, e.nodesURL, jsonreqs...)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	var responses []jsonrpc.Response
	if err := e.do(req, &responses); err != nil {
		return nil, fmt.Errorf("error fetching blocks: %w", err)
	}
	var result []pkg.Ledger
	for _, resp := range responses {
		var b EthereumBlock
		if err := json.Unmarshal(resp.Result, &b); err != nil {
			return nil, fmt.Errorf("error unmarshaling block %s: %w", string(resp.Result), err)
		}
		l, err := b.toLedger()
		if err != nil {
			return nil, fmt.Errorf("error parsing block to ledger. %w", err)
		}
		l.Network = e.network
		txs, err := e.transactionsFromBlock(b.Transactions)
		if err != nil {
			return nil, fmt.Errorf("error getting transactions from block. %w", err)
		}
		l.Transactions = txs
		result = append(result, l)
	}
	return result, nil
}

func (e Ethereum) transactionsFromBlock(transactions []EthereumTransaction) ([]pkg.Transaction, error) {
	txs := make([]pkg.Transaction, 0)
	if len(transactions) == 0 {
		return txs, nil
	}
	for _, tx := range transactions {
		cleanedBlockNumber, err := CleanHexString(tx.BlockNumber)
		if err != nil {
			return txs, fmt.Errorf("error cleaning hex transaction block numer %s. err: %w", tx.BlockNumber, err)
		}
		blockNumber, err := hexutil.DecodeUint64(cleanedBlockNumber)
		if err != nil {
			return txs, fmt.Errorf("error decoding transaction block number hex %s. err: %w", cleanedBlockNumber, err)
		}
		cleanedTxIndex, err := CleanHexString(tx.Index)
		if err != nil {
			return txs, fmt.Errorf("error cleaning hex transaction index %s. err: %w", tx.Index, err)
		}
		txIndex, err := hexutil.DecodeUint64(cleanedTxIndex)
		if err != nil {
			return txs, fmt.Errorf("error decoding transaction index hex %s. err: %w", txIndex, err)
		}
		t := pkg.Transaction{
			Blockchain: pkg.Blockchain_ETHEREUM,
			Network:    e.network,
			Identifier: pkg.Identifier{
				Hash:  string(tx.Hash),
				Index: txIndex,
			},
			Ledger: pkg.Identifier{
				Hash:  string(tx.BlockHash),
				Index: blockNumber,
			},
			From: string(tx.From),
			To:   string(tx.To),
		}
		txs = append(txs, t)
	}
	return txs, nil
}

func (e Ethereum) do(req *http.Request, result interface{}) error {
	resp, err := e.c.Client.Do(req)
	if err != nil {
		return err
	}
	defer func(Body io.ReadCloser) {
		_ = Body.Close()
	}(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("invalid status code response: %v", resp.StatusCode)
	}
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("error reading body: %w", err)
	}
	if err := json.Unmarshal(b, &result); err != nil {
		return fmt.Errorf("error unmarshaling response %s: %w", string(b), err)
	}
	return nil
}
