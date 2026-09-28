package coordinator

import (
	"context"
	"fmt"
	"log"
	"sync"

	"twophasecommit/internal/participant"
	"twophasecommit/internal/tx"
)

type Leg struct {
	DB        participant.Participant
	AccountID string
	Delta     int64
}

type Outcome struct {
	TxID    tx.ID
	State   tx.State
	Message string
}

// Coordinator is the transaction manager inside the vendor stack.
type Coordinator struct {
	mu       sync.Mutex
	decision map[tx.ID]tx.State
	log      *DecisionLog
}

func New() *Coordinator {
	return &Coordinator{
		decision: make(map[tx.ID]tx.State),
	}
}

func NewWithLog(log *DecisionLog) *Coordinator {
	return &Coordinator{
		decision: make(map[tx.ID]tx.State),
		log:      log,
	}
}

func (c *Coordinator) ExecuteTransfer(ctx context.Context, req tx.TransferRequest, legs ...Leg) Outcome {
	if len(legs) == 0 {
		return c.finalize(ctx, req, Outcome{TxID: req.ID, State: tx.StateAborted, Message: "no participants"})
	}

	log.Printf("[coordinator] tx=%s phase=1 prepare from=%s to=%s amount=%d",
		req.ID, req.FromAccount, req.ToAccount, req.Amount)

	votes := make([]tx.Vote, 0, len(legs))
	for _, leg := range legs {
		vote, err := leg.DB.Prepare(req, leg.AccountID, leg.Delta)
		if err != nil {
			log.Printf("[coordinator] tx=%s participant=%s vote=NO err=%v", req.ID, leg.DB.Name(), err)
			votes = append(votes, tx.VoteNo)
			continue
		}
		log.Printf("[coordinator] tx=%s participant=%s vote=YES account=%s delta=%d",
			req.ID, leg.DB.Name(), leg.AccountID, leg.Delta)
		votes = append(votes, vote)
	}

	allYes := true
	for _, vote := range votes {
		if vote != tx.VoteYes {
			allYes = false
			break
		}
	}

	if allYes {
		return c.finalize(ctx, req, c.commit(req.ID, legs...))
	}
	return c.finalize(ctx, req, c.abort(req.ID, legs...))
}

func (c *Coordinator) commit(id tx.ID, legs ...Leg) Outcome {
	c.mu.Lock()
	c.decision[id] = tx.StateCommitted
	c.mu.Unlock()

	log.Printf("[coordinator] tx=%s phase=2 decision=COMMIT", id)
	for _, leg := range legs {
		if err := commitLeg(leg, id); err != nil {
			log.Printf("[coordinator] tx=%s participant=%s commit_err=%v", id, leg.DB.Name(), err)
		}
	}
	return Outcome{TxID: id, State: tx.StateCommitted, Message: "transfer committed across vendor stack"}
}

func commitLeg(leg Leg, id tx.ID) error {
	if lp, ok := leg.DB.(*participant.LedgerParticipant); ok {
		return lp.Commit(id)
	}
	return leg.DB.Commit(id)
}

func (c *Coordinator) abort(id tx.ID, legs ...Leg) Outcome {
	c.mu.Lock()
	c.decision[id] = tx.StateAborted
	c.mu.Unlock()

	log.Printf("[coordinator] tx=%s phase=2 decision=ABORT", id)
	for _, leg := range legs {
		if err := leg.DB.Abort(id); err != nil {
			log.Printf("[coordinator] tx=%s participant=%s abort_err=%v", id, leg.DB.Name(), err)
		}
	}
	return Outcome{TxID: id, State: tx.StateAborted, Message: "transfer aborted; no balances changed"}
}

func (c *Coordinator) finalize(ctx context.Context, req tx.TransferRequest, outcome Outcome) Outcome {
	if c.log != nil {
		if err := c.log.Save(ctx, outcome, req); err != nil {
			log.Printf("[coordinator] tx=%s decision_log_err=%v", req.ID, err)
		}
	}
	return outcome
}

func (c *Coordinator) Decision(id tx.ID) (tx.State, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	state, ok := c.decision[id]
	return state, ok
}

func FormatBalances(checking, savings participant.Participant, alice, bob string) string {
	a, _ := checking.Balance(alice)
	b, _ := savings.Balance(bob)
	return fmt.Sprintf("alice(checking)=%d bob(savings)=%d", a, b)
}
