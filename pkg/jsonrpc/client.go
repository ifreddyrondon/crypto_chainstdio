package jsonrpc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
)

const (
	DefaultJSONRPCVersion = "2.0"
	DefaultJSONRPCID      = 1

	contentTypeHeader = "Content-Type"
	contentTypeVal    = "application/json"
)

var ErrorMissingReq = errors.New("missing request")

type (
	Request struct {
		Jsonrpc string        `json:"jsonrpc"`
		ID      int           `json:"id"`
		Method  string        `json:"method"`
		Params  []interface{} `json:"params"`
	}
	Response struct {
		Jsonrpc string          `json:"jsonrpc"`
		ID      int             `json:"id"`
		Error   *ResponseError  `json:"error"`
		Result  json.RawMessage `json:"result"`
	}
	ResponseError struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
		Meaning string `json:"meaning"`
	}
)

func NewRequest(ctx context.Context, url string, req Request) (*http.Request, error) {
	if req.Jsonrpc == "" {
		req.Jsonrpc = DefaultJSONRPCVersion
	}
	if req.ID == 0 {
		req.ID = DefaultJSONRPCID
	}
	b, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("error marshaling request: %w", err)
	}
	return http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(b))
}

func NewBatchRequest(ctx context.Context, url string, reqs ...Request) (*http.Request, error) {
	if len(reqs) == 0 {
		return nil, ErrorMissingReq
	}
	for i, r := range reqs {
		if r.Jsonrpc == "" {
			r.Jsonrpc = DefaultJSONRPCVersion
		}
		if r.ID == 0 {
			r.ID = i
		}
	}
	b, err := json.Marshal(reqs)
	if err != nil {
		return nil, fmt.Errorf("error marshaling request: %w", err)
	}
	return http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(b))
}

type jsonrpcRoundTripper struct {
	// base represents the original round tripper that's been wrapped
	base http.RoundTripper
}

func (rt *jsonrpcRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	req.Header.Set(contentTypeHeader, contentTypeVal)
	return rt.base.RoundTrip(req)
}

type Client struct {
	*http.Client
}

// New takes an HTTP client and wraps its RoundTripper with jsonrpc necessary changes.
func New(c *http.Client) *Client {
	if c.Transport == nil {
		c.Transport = http.DefaultTransport
	}

	c.Transport = &jsonrpcRoundTripper{base: c.Transport}
	return &Client{Client: c}
}
