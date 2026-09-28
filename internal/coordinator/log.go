package coordinator

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"twophasecommit/internal/tx"
)

var ErrDecisionNotFound = errors.New("transaction decision not found")

type DecisionRecord struct {
	TxID        tx.ID
	State       tx.State
	FromAccount string
	ToAccount   string
	Amount      int64
	Message     string
	CreatedAt   time.Time
}

type DecisionLog struct {
	db *sql.DB
}

func NewDecisionLog(db *sql.DB) *DecisionLog {
	return &DecisionLog{db: db}
}

func (l *DecisionLog) Save(ctx context.Context, outcome Outcome, req tx.TransferRequest) error {
	_, err := l.db.ExecContext(ctx,
		`INSERT INTO transaction_decisions (tx_id, state, from_account, to_account, amount, message)
		 VALUES ($1, $2, $3, $4, $5, $6)
		 ON CONFLICT (tx_id) DO UPDATE SET state = EXCLUDED.state, message = EXCLUDED.message`,
		string(outcome.TxID), string(outcome.State), req.FromAccount, req.ToAccount, req.Amount, outcome.Message,
	)
	return err
}

func (l *DecisionLog) Get(ctx context.Context, id tx.ID) (DecisionRecord, error) {
	var rec DecisionRecord
	var txID string
	err := l.db.QueryRowContext(ctx,
		`SELECT tx_id, state, from_account, to_account, amount, message, created_at
		 FROM transaction_decisions WHERE tx_id = $1`, string(id),
	).Scan(&txID, &rec.State, &rec.FromAccount, &rec.ToAccount, &rec.Amount, &rec.Message, &rec.CreatedAt)
	if err == sql.ErrNoRows {
		return DecisionRecord{}, ErrDecisionNotFound
	}
	if err != nil {
		return DecisionRecord{}, err
	}
	rec.TxID = tx.ID(txID)
	return rec, nil
}
