package participant

import (
	"errors"
	"fmt"
	"sync"

	"twophasecommit/internal/tx"
)

var (
	ErrInsufficientFunds = errors.New("insufficient funds")
	ErrAccountNotFound   = errors.New("account not found")
	ErrAlreadyPrepared   = errors.New("transaction already prepared")
	ErrNotPrepared       = errors.New("transaction not prepared")
)

// Participant is a 2PC resource manager (one database shard).
type Participant interface {
	Name() string
	Balance(accountID string) (int64, error)
	Prepare(req tx.TransferRequest, accountID string, delta int64) (tx.Vote, error)
	Commit(id tx.ID) error
	Abort(id tx.ID) error
	WAL() []tx.WALRecord
	WALForTx(id tx.ID) []tx.WALRecord
}

type Account struct {
	ID      string
	Balance int64
}

type preparedEntry struct {
	account string
	delta   int64
}

// MemoryDB is an in-memory participant for demos and unit tests.
type MemoryDB struct {
	name     string
	mu       sync.Mutex
	accounts map[string]*Account
	wal      []tx.WALRecord
	prepared map[tx.ID]preparedEntry
}

func NewMemory(name string, accounts map[string]int64) *MemoryDB {
	db := &MemoryDB{
		name:     name,
		accounts: make(map[string]*Account),
		prepared: make(map[tx.ID]preparedEntry),
	}
	for id, balance := range accounts {
		db.accounts[id] = &Account{ID: id, Balance: balance}
	}
	return db
}

func (d *MemoryDB) Name() string {
	return d.name
}

func (d *MemoryDB) WAL() []tx.WALRecord {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]tx.WALRecord, len(d.wal))
	copy(out, d.wal)
	return out
}

func (d *MemoryDB) WALForTx(id tx.ID) []tx.WALRecord {
	d.mu.Lock()
	defer d.mu.Unlock()
	var out []tx.WALRecord
	for _, rec := range d.wal {
		if rec.TxID == id {
			out = append(out, rec)
		}
	}
	return out
}

func (d *MemoryDB) Balance(accountID string) (int64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	acct, ok := d.accounts[accountID]
	if !ok {
		return 0, ErrAccountNotFound
	}
	return acct.Balance, nil
}

func (d *MemoryDB) Prepare(req tx.TransferRequest, accountID string, delta int64) (tx.Vote, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if _, exists := d.prepared[req.ID]; exists {
		return tx.VoteNo, ErrAlreadyPrepared
	}

	acct, ok := d.accounts[accountID]
	if !ok {
		d.appendWAL(req.ID, "prepare", "reject_not_found", accountID, delta)
		return tx.VoteNo, ErrAccountNotFound
	}

	if delta < 0 && acct.Balance+delta < 0 {
		d.appendWAL(req.ID, "prepare", "reject_insufficient", accountID, delta)
		return tx.VoteNo, ErrInsufficientFunds
	}

	d.appendWAL(req.ID, "prepare", "yes", accountID, delta)
	d.prepared[req.ID] = preparedEntry{account: accountID, delta: delta}
	return tx.VoteYes, nil
}

func (d *MemoryDB) Commit(id tx.ID) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	entry, ok := d.prepared[id]
	if !ok {
		return ErrNotPrepared
	}

	acct := d.accounts[entry.account]
	acct.Balance += entry.delta
	d.appendWAL(id, "commit", "apply", entry.account, entry.delta)
	delete(d.prepared, id)
	return nil
}

func (d *MemoryDB) Abort(id tx.ID) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	entry, ok := d.prepared[id]
	if !ok {
		return nil
	}

	d.appendWAL(id, "abort", "release", entry.account, entry.delta)
	delete(d.prepared, id)
	return nil
}

func (d *MemoryDB) appendWAL(id tx.ID, phase, action, account string, amount int64) {
	d.wal = append(d.wal, tx.WALRecord{
		TxID:    id,
		Phase:   phase,
		Action:  action,
		Account: account,
		Amount:  amount,
	})
}

func (d *MemoryDB) String() string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return fmt.Sprintf("%s(%d accounts, %d prepared)", d.name, len(d.accounts), len(d.prepared))
}
