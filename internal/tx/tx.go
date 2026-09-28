package tx

import "time"

type ID string

type State string

const (
	StateInit      State = "INIT"
	StatePrepared  State = "PREPARED"
	StateCommitted State = "COMMITTED"
	StateAborted   State = "ABORTED"
)

type Vote int

const (
	VoteUnknown Vote = iota
	VoteYes
	VoteNo
)

type TransferRequest struct {
	ID          ID
	FromAccount string
	ToAccount   string
	Amount      int64 // cents
}

type WALRecord struct {
	TxID      ID
	Phase     string
	Action    string
	Account   string
	Amount    int64
	Timestamp time.Time
}
