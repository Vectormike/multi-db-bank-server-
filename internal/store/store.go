package store

import (
	"database/sql"
	"fmt"
	"os"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

type Config struct {
	CheckingURL string
	SavingsURL  string
	LedgerURL   string
}

func ConfigFromEnv() Config {
	return Config{
		CheckingURL: envOr("CHECKING_DATABASE_URL", "postgres://bank:bank@localhost:5434/checking?sslmode=disable"),
		SavingsURL:  envOr("SAVINGS_DATABASE_URL", "postgres://bank:bank@localhost:5434/savings?sslmode=disable"),
		LedgerURL:   envOr("LEDGER_DATABASE_URL", "postgres://bank:bank@localhost:5434/ledger?sslmode=disable"),
	}
}

type Store struct {
	Checking *sql.DB
	Savings  *sql.DB
	Ledger   *sql.DB
}

func Open(cfg Config) (*Store, error) {
	checking, err := openDB(cfg.CheckingURL)
	if err != nil {
		return nil, fmt.Errorf("checking db: %w", err)
	}
	savings, err := openDB(cfg.SavingsURL)
	if err != nil {
		checking.Close()
		return nil, fmt.Errorf("savings db: %w", err)
	}
	ledger, err := openDB(cfg.LedgerURL)
	if err != nil {
		checking.Close()
		savings.Close()
		return nil, fmt.Errorf("ledger db: %w", err)
	}
	return &Store{Checking: checking, Savings: savings, Ledger: ledger}, nil
}

func (s *Store) Close() {
	s.Checking.Close()
	s.Savings.Close()
	s.Ledger.Close()
}

func (s *Store) Ping() error {
	for name, db := range map[string]*sql.DB{
		"checking": s.Checking,
		"savings":  s.Savings,
		"ledger":   s.Ledger,
	} {
		if err := db.Ping(); err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
	}
	return nil
}

func openDB(url string) (*sql.DB, error) {
	db, err := sql.Open("pgx", url)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(30 * time.Minute)

	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if err := db.Ping(); err == nil {
			return db, nil
		}
		time.Sleep(500 * time.Millisecond)
	}
	return db, db.Ping()
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
