package registry

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

var ErrAccountNotFound = errors.New("account not found")

type Shard string

const (
	ShardChecking Shard = "checking"
	ShardSavings  Shard = "savings"
	ShardLedger   Shard = "ledger"
)

type Entry struct {
	AccountID string
	Shard     Shard
}

type Registry struct {
	entries map[string]Entry
}

func Load(ctx context.Context, checking, savings *sql.DB) (*Registry, error) {
	reg := &Registry{entries: make(map[string]Entry)}

	if err := reg.loadShard(ctx, checking, ShardChecking); err != nil {
		return nil, fmt.Errorf("load checking: %w", err)
	}
	if err := reg.loadShard(ctx, savings, ShardSavings); err != nil {
		return nil, fmt.Errorf("load savings: %w", err)
	}

	reg.entries["gl:transfers"] = Entry{AccountID: "gl:transfers", Shard: ShardLedger}
	return reg, nil
}

func (r *Registry) loadShard(ctx context.Context, db *sql.DB, shard Shard) error {
	rows, err := db.QueryContext(ctx, `SELECT id FROM accounts`)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return err
		}
		r.entries[id] = Entry{AccountID: id, Shard: shard}
	}
	return rows.Err()
}

func (r *Registry) Lookup(accountID string) (Entry, error) {
	entry, ok := r.entries[accountID]
	if !ok {
		return Entry{}, ErrAccountNotFound
	}
	return entry, nil
}
