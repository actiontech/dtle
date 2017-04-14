package server

type RPCType byte

const (
	rpcUdup RPCType = iota
	rpcRaft
	rpcMultiplex
	rpcTLS
)
