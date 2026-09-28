package participant

import (
	"context"
	"database/sql"
	"sync"
	"time"

	"twophasecommit/internal/tx"
)

// CommitHook runs extra work inside the commit transaction (used by ledger shard).
type CommitHook func(ctx context.Context, txDB *sql.Tx, id tx.ID, accountID string, delta int64, req tx.TransferRequest) error

// SQLDB is a Postgres-backed participant.
type SQLDB struct {
	shardName  string
	db         *sql.DB
	commitHook CommitHook
}

func NewSQL(name string, db *sql.DB, hook CommitHook) *SQLDB {
	return &SQLDB{shardName: name, db: db, commitHook: hook}
}

func (d *SQLDB) Name() string {
	return d.shardName
}

func (d *SQLDB) Balance(accountID string) (int64, error) {
	var balance int64
	err := d.db.QueryRow(`SELECT balance FROM accounts WHERE id = $1`, accountID).Scan(&balance)
	if err == sql.ErrNoRows {
		return 0, ErrAccountNotFound
	}
	return balance, err
}

func (d *SQLDB) Prepare(req tx.TransferRequest, accountID string, delta int64) (tx.Vote, error) {
	ctx := context.Background()
	txDB, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return tx.VoteNo, err
	}
	defer txDB.Rollback()

	var exists int
	err = txDB.QueryRow(`SELECT 1 FROM prepared_tx WHERE tx_id = $1`, string(req.ID)).Scan(&exists)
	if err == nil {
		return tx.VoteNo, ErrAlreadyPrepared
	}
	if err != sql.ErrNoRows {
		return tx.VoteNo, err
	}

	var balance int64
	err = txDB.QueryRow(`SELECT balance FROM accounts WHERE id = $1`, accountID).Scan(&balance)
	if err == sql.ErrNoRows {
		if _, err := txDB.Exec(`INSERT INTO wal (tx_id, phase, action, account_id, amount) VALUES ($1, 'prepare', 'reject_not_found', $2, $3)`,
			string(req.ID), accountID, delta); err != nil {
			return tx.VoteNo, err
		}
		if err := txDB.Commit(); err != nil {
			return tx.VoteNo, err
		}
		return tx.VoteNo, ErrAccountNotFound
	}
	if err != nil {
		return tx.VoteNo, err
	}

	if delta < 0 && balance+delta < 0 {
		if _, err := txDB.Exec(`INSERT INTO wal (tx_id, phase, action, account_id, amount) VALUES ($1, 'prepare', 'reject_insufficient', $2, $3)`,
			string(req.ID), accountID, delta); err != nil {
			return tx.VoteNo, err
		}
		if err := txDB.Commit(); err != nil {
			return tx.VoteNo, err
		}
		return tx.VoteNo, ErrInsufficientFunds
	}

	if _, err := txDB.Exec(`INSERT INTO wal (tx_id, phase, action, account_id, amount) VALUES ($1, 'prepare', 'yes', $2, $3)`,
		string(req.ID), accountID, delta); err != nil {
		return tx.VoteNo, err
	}
	if _, err := txDB.Exec(`INSERT INTO prepared_tx (tx_id, account_id, delta) VALUES ($1, $2, $3)`,
		string(req.ID), accountID, delta); err != nil {
		return tx.VoteNo, err
	}

	if err := txDB.Commit(); err != nil {
		return tx.VoteNo, err
	}
	return tx.VoteYes, nil
}

func (d *SQLDB) Commit(id tx.ID) error {
	return d.commitWithReq(id, tx.TransferRequest{})
}

func (d *SQLDB) commitWithReq(id tx.ID, req tx.TransferRequest) error {
	ctx := context.Background()
	txDB, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer txDB.Rollback()

	var accountID string
	var delta int64
	err = txDB.QueryRow(`SELECT account_id, delta FROM prepared_tx WHERE tx_id = $1`, string(id)).Scan(&accountID, &delta)
	if err == sql.ErrNoRows {
		return ErrNotPrepared
	}
	if err != nil {
		return err
	}

	if _, err := txDB.Exec(`UPDATE accounts SET balance = balance + $1 WHERE id = $2`, delta, accountID); err != nil {
		return err
	}
	if _, err := txDB.Exec(`INSERT INTO wal (tx_id, phase, action, account_id, amount) VALUES ($1, 'commit', 'apply', $2, $3)`,
		string(id), accountID, delta); err != nil {
		return err
	}
	if d.commitHook != nil {
		if err := d.commitHook(ctx, txDB, id, accountID, delta, req); err != nil {
			return err
		}
	}
	if _, err := txDB.Exec(`DELETE FROM prepared_tx WHERE tx_id = $1`, string(id)); err != nil {
		return err
	}

	return txDB.Commit()
}

func (d *SQLDB) Abort(id tx.ID) error {
	ctx := context.Background()
	txDB, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer txDB.Rollback()

	var accountID string
	var delta int64
	err = txDB.QueryRow(`SELECT account_id, delta FROM prepared_tx WHERE tx_id = $1`, string(id)).Scan(&accountID, &delta)
	if err == sql.ErrNoRows {
		return nil
	}
	if err != nil {
		return err
	}

	if _, err := txDB.Exec(`INSERT INTO wal (tx_id, phase, action, account_id, amount) VALUES ($1, 'abort', 'release', $2, $3)`,
		string(id), accountID, delta); err != nil {
		return err
	}
	if _, err := txDB.Exec(`DELETE FROM prepared_tx WHERE tx_id = $1`, string(id)); err != nil {
		return err
	}

	return txDB.Commit()
}

func (d *SQLDB) WAL() []tx.WALRecord {
	rows, err := d.db.Query(`SELECT tx_id, phase, action, account_id, amount, created_at FROM wal ORDER BY id`)
	if err != nil {
		return nil
	}
	defer rows.Close()
	return scanWALRows(rows)
}

func (d *SQLDB) WALForTx(id tx.ID) []tx.WALRecord {
	rows, err := d.db.Query(`SELECT tx_id, phase, action, account_id, amount, created_at FROM wal WHERE tx_id = $1 ORDER BY id`, string(id))
	if err != nil {
		return nil
	}
	defer rows.Close()
	return scanWALRows(rows)
}

func scanWALRows(rows *sql.Rows) []tx.WALRecord {
	var out []tx.WALRecord
	for rows.Next() {
		var rec tx.WALRecord
		var createdAt time.Time
		var txID string
		if err := rows.Scan(&txID, &rec.Phase, &rec.Action, &rec.Account, &rec.Amount, &createdAt); err != nil {
			continue
		}
		rec.TxID = tx.ID(txID)
		rec.Timestamp = createdAt
		out = append(out, rec)
	}
	return out
}

// LedgerParticipant records GL entries on commit.
type LedgerParticipant struct {
	*SQLDB
	mu      sync.Mutex
	pending map[tx.ID]tx.TransferRequest
}

func NewLedger(name string, db *sql.DB) *LedgerParticipant {
	return &LedgerParticipant{
		SQLDB:   NewSQL(name, db, nil),
		pending: make(map[tx.ID]tx.TransferRequest),
	}
}

func (l *LedgerParticipant) Prepare(req tx.TransferRequest, accountID string, delta int64) (tx.Vote, error) {
	vote, err := l.SQLDB.Prepare(req, accountID, delta)
	if err == nil && vote == tx.VoteYes {
		l.mu.Lock()
		l.pending[req.ID] = req
		l.mu.Unlock()
	}
	return vote, err
}

func (l *LedgerParticipant) Commit(id tx.ID) error {
	l.mu.Lock()
	req, ok := l.pending[id]
	delete(l.pending, id)
	l.mu.Unlock()
	if !ok {
		req = tx.TransferRequest{}
	}

	origHook := l.SQLDB.commitHook
	l.SQLDB.commitHook = func(ctx context.Context, txDB *sql.Tx, txID tx.ID, accountID string, delta int64, _ tx.TransferRequest) error {
		if req.FromAccount == "" {
			return nil
		}
		_, err := txDB.Exec(`INSERT INTO gl_entries (tx_id, from_account, to_account, amount) VALUES ($1, $2, $3, $4)`,
			string(txID), req.FromAccount, req.ToAccount, req.Amount)
		return err
	}
	err := l.SQLDB.commitWithReq(id, req)
	l.SQLDB.commitHook = origHook
	return err
}

func (l *LedgerParticipant) Abort(id tx.ID) error {
	l.mu.Lock()
	delete(l.pending, id)
	l.mu.Unlock()
	return l.SQLDB.Abort(id)
}
