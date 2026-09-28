package main

import (
	"context"
	"log"
	"net/http"
	"os"

	"twophasecommit/internal/bank"
	"twophasecommit/internal/coordinator"
	"twophasecommit/internal/participant"
	"twophasecommit/internal/registry"
	"twophasecommit/internal/store"
)

func main() {
	cfg := store.ConfigFromEnv()
	st, err := store.Open(cfg)
	if err != nil {
		log.Fatalf("store: %v", err)
	}
	defer st.Close()

	ctx := context.Background()
	reg, err := registry.Load(ctx, st.Checking, st.Savings)
	if err != nil {
		log.Fatalf("registry: %v", err)
	}

	checking := participant.NewSQL("checking-db", st.Checking, nil)
	savings := participant.NewSQL("savings-db", st.Savings, nil)
	ledger := participant.NewLedger("ledger-db", st.Ledger)

	decisionLog := coordinator.NewDecisionLog(st.Ledger)
	coord := coordinator.NewWithLog(decisionLog)

	svc := bank.NewService(coord, decisionLog, reg, checking, savings, ledger)
	srv := bank.NewServer(svc)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	log.Printf("bank server listening on :%s", port)
	if err := http.ListenAndServe(":"+port, srv.Handler()); err != nil {
		log.Fatal(err)
	}
}
