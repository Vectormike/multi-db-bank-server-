package bank

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"

	"twophasecommit/internal/coordinator"
	"twophasecommit/internal/participant"
	"twophasecommit/internal/registry"
	"twophasecommit/internal/tx"
)

var (
	ErrInvalidAmount  = errors.New("amount must be positive")
	ErrSameAccount    = errors.New("from and to accounts must differ")
	ErrUnknownAccount = errors.New("unknown account")
)

const glAccount = "gl:transfers"

type Service struct {
	coord    *coordinator.Coordinator
	decisions *coordinator.DecisionLog
	reg      *registry.Registry
	shards   map[registry.Shard]participant.Participant
}

func NewService(
	coord *coordinator.Coordinator,
	decisions *coordinator.DecisionLog,
	reg *registry.Registry,
	checking, savings participant.Participant,
	ledger *participant.LedgerParticipant,
) *Service {
	return &Service{
		coord:     coord,
		decisions: decisions,
		reg:       reg,
		shards: map[registry.Shard]participant.Participant{
			registry.ShardChecking: checking,
			registry.ShardSavings:  savings,
			registry.ShardLedger:   ledger,
		},
	}
}

type TransferInput struct {
	From   string
	To     string
	Amount int64
}

func (s *Service) Transfer(ctx context.Context, in TransferInput) (coordinator.Outcome, error) {
	if in.Amount <= 0 {
		return coordinator.Outcome{}, ErrInvalidAmount
	}
	if in.From == in.To {
		return coordinator.Outcome{}, ErrSameAccount
	}

	fromEntry, err := s.reg.Lookup(in.From)
	if err != nil {
		return coordinator.Outcome{}, fmt.Errorf("%w: %s", ErrUnknownAccount, in.From)
	}
	toEntry, err := s.reg.Lookup(in.To)
	if err != nil {
		return coordinator.Outcome{}, fmt.Errorf("%w: %s", ErrUnknownAccount, in.To)
	}

	fromShard, ok := s.shards[fromEntry.Shard]
	if !ok {
		return coordinator.Outcome{}, fmt.Errorf("missing shard: %s", fromEntry.Shard)
	}
	toShard, ok := s.shards[toEntry.Shard]
	if !ok {
		return coordinator.Outcome{}, fmt.Errorf("missing shard: %s", toEntry.Shard)
	}
	ledgerShard, ok := s.shards[registry.ShardLedger]
	if !ok {
		return coordinator.Outcome{}, errors.New("missing ledger shard")
	}

	req := tx.TransferRequest{
		ID:          tx.ID("tx-" + uuid.NewString()),
		FromAccount: in.From,
		ToAccount:   in.To,
		Amount:      in.Amount,
	}

	legs := []coordinator.Leg{
		{DB: fromShard, AccountID: in.From, Delta: -in.Amount},
		{DB: toShard, AccountID: in.To, Delta: in.Amount},
		{DB: ledgerShard, AccountID: glAccount, Delta: in.Amount},
	}

	outcome := s.coord.ExecuteTransfer(ctx, req, legs...)
	return outcome, nil
}

func (s *Service) GetAccount(ctx context.Context, accountID string) (registry.Entry, int64, error) {
	entry, err := s.reg.Lookup(accountID)
	if err != nil {
		return registry.Entry{}, 0, err
	}
	shard, ok := s.shards[entry.Shard]
	if !ok {
		return registry.Entry{}, 0, fmt.Errorf("missing shard: %s", entry.Shard)
	}
	balance, err := shard.Balance(accountID)
	if err != nil {
		return registry.Entry{}, 0, err
	}
	return entry, balance, nil
}

func (s *Service) GetTransfer(ctx context.Context, id tx.ID) (coordinator.DecisionRecord, error) {
	return s.decisions.Get(ctx, id)
}

type WALSnapshot struct {
	Shard   string           `json:"shard"`
	Records []tx.WALRecord   `json:"records"`
}

func (s *Service) GetWAL(ctx context.Context, id tx.ID) []WALSnapshot {
	var out []WALSnapshot
	for shardName, p := range s.shards {
		out = append(out, WALSnapshot{
			Shard:   string(shardName),
			Records: p.WALForTx(id),
		})
	}
	return out
}

func (s *Service) Ping(ctx context.Context) error {
	for _, p := range s.shards {
		if _, err := p.Balance("___ping___"); err != nil && !errors.Is(err, participant.ErrAccountNotFound) {
			return err
		}
	}
	return nil
}
