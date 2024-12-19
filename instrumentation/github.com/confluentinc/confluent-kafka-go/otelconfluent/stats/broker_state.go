package stats

import (
	"errors"
	"strings"
)

type BrokerState int64

const (
	INIT BrokerState = iota
	DOWN
	CONNECT
	AUTH
	APIVERSION_QUERY
	AUTH_HANDSHAKE
	UP
	UPDATE
)

const StateDescription string = `Possible States and their Values: INIT: 0, DOWN: 1, CONNECT: 2, AUTH: 3, APIVERSION_QUERY: 4, AUTH_HANDSHAKE: 5, UP: 6,UPDATE: 7`

func (s BrokerState) String() string {
	switch s {
	case INIT:
		return "INIT"
	case DOWN:
		return "DOWN"
	case CONNECT:
		return "CONNECT"
	case AUTH:
		return "AUTH"
	case APIVERSION_QUERY:
		return "APIVERSION_QUERY"
	case AUTH_HANDSHAKE:
		return "AUTH_HANDSHAKE"
	case UP:
		return "UP"
	case UPDATE:
		return "UPDATE"
	default:
		return "UNKNOWN"
	}
}

var stateMap = map[string]BrokerState{
	"INIT":             INIT,
	"DOWN":             DOWN,
	"CONNECT":          CONNECT,
	"AUTH":             AUTH,
	"APIVERSION_QUERY": APIVERSION_QUERY,
	"AUTH_HANDSHAKE":   AUTH_HANDSHAKE,
	"UP":               UP,
	"UPDATE":           UPDATE,
}

// BrokerStateFromString converts a string to its corresponding State
func BrokerStateFromString(stateStr string) (BrokerState, error) {
	state, exists := stateMap[strings.ToUpper(stateStr)]
	if !exists {
		return -1, errors.New("invalid state string")
	}
	return state, nil
}
