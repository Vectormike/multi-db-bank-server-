package main

import (
	"context"
	"fmt"
	"log"

	"twophasecommit/internal/coordinator"
	"twophasecommit/internal/participant"
	"twophasecommit/internal/tx"
)

func main() {
	checkingDB := participant.NewMemory("checking-db", map[string]int64{
		"alice": 500_00,
	})
	savingsDB := participant.NewMemory("savings-db", map[string]int64{
		"bob": 100_00,
	})

	tm := coordinator.New()
	ctx := context.Background()

	fmt.Println("=== Vendor stack 2PC: internal transfer ===")
	fmt.Println("Before:", coordinator.FormatBalances(checkingDB, savingsDB, "alice", "bob"))

	success := tm.ExecuteTransfer(ctx, tx.TransferRequest{
		ID:          "tx-001",
		FromAccount: "alice",
		ToAccount:   "bob",
		Amount:      150_00,
	},
		coordinator.Leg{DB: checkingDB, AccountID: "alice", Delta: -150_00},
		coordinator.Leg{DB: savingsDB, AccountID: "bob", Delta: 150_00},
	)
	fmt.Println("Outcome:", success.State, "-", success.Message)
	fmt.Println("After:", coordinator.FormatBalances(checkingDB, savingsDB, "alice", "bob"))

	fmt.Println("\n=== Vendor stack 2PC: insufficient funds abort ===")
	fmt.Println("Before:", coordinator.FormatBalances(checkingDB, savingsDB, "alice", "bob"))

	fail := tm.ExecuteTransfer(ctx, tx.TransferRequest{
		ID:          "tx-002",
		FromAccount: "alice",
		ToAccount:   "bob",
		Amount:      400_00,
	},
		coordinator.Leg{DB: checkingDB, AccountID: "alice", Delta: -400_00},
		coordinator.Leg{DB: savingsDB, AccountID: "bob", Delta: 400_00},
	)
	fmt.Println("Outcome:", fail.State, "-", fail.Message)
	fmt.Println("After:", coordinator.FormatBalances(checkingDB, savingsDB, "alice", "bob"))

	printWAL("checking-db", checkingDB)
	printWAL("savings-db", savingsDB)
}

func printWAL(name string, db participant.Participant) {
	fmt.Printf("\nWAL snapshot (%s):\n", name)
	for _, rec := range db.WAL() {
		log.Printf("  tx=%s phase=%s action=%s account=%s amount=%d",
			rec.TxID, rec.Phase, rec.Action, rec.Account, rec.Amount)
	}
}
