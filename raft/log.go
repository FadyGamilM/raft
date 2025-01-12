package raft

type LogEntry struct {
	Cmd  string // TODO : i beleive we should either have a generic typr or sadly use interface or any
	Term int
}
