package blockchain

import (
	"errors"
	"strings"
)

var ErrMissingHexPrefix = errors.New("string missing 0x prefix")

// has0xPrefix returns true if the string beings with `0x` or `0X`
func has0xPrefix(str string) bool {
	return len(str) >= 2 && str[0] == '0' && (str[1] == 'x' || str[1] == 'X')
}

// CleanHexString cleans up a hex string by removing all leading 0s
func CleanHexString(hex string) (string, error) {
	var cleaned string

	if !has0xPrefix(hex) {
		return "", ErrMissingHexPrefix
	}

	cleaned = hex[2:]

	if len(cleaned) > 1 && cleaned[0] == '0' {
		cleaned = strings.TrimLeft(hex[2:], "0")
	}

	// if trimming all left 0s returned an empty string
	// set clean to be "0" so that `hexutil` can properly parse
	if cleaned == "" {
		cleaned = "0"
	}

	return "0x" + cleaned, nil
}
