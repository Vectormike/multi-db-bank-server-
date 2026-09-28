# multi-db-bank-server

A Go bank server demonstrating **two-phase commit (2PC)** across three PostgreSQL databases in a vendor-stack setup.

## Architecture

One HTTP server acts as the transaction manager. Three separate Postgres databases participate in every transfer:

| Database | Role |
|----------|------|
| `checking` | Demand deposit accounts |
| `savings` | Savings accounts |
| `ledger` | GL audit trail + transaction decision log |

Each transfer runs three 2PC legs: debit source, credit destination, post to ledger.

## Stack

- Go (stdlib HTTP)
- PostgreSQL 16
- Docker Compose

## Quick start

```bash
# Start Postgres (localhost:5434)
docker compose up -d

# Run the bank server
go run ./cmd/server

# Health check
curl localhost:8080/health

# Transfer $150 from alice to bob
curl -X POST localhost:8080/transfers \
  -H 'Content-Type: application/json' \
  -d '{"from":"alice","to":"bob","amount":15000}'

# Check balances
curl localhost:8080/accounts/alice
curl localhost:8080/accounts/bob
```

## API

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/health` | Server and DB health |
| `GET` | `/accounts/{id}` | Account balance and shard |
| `POST` | `/transfers` | Execute a 2PC transfer |
| `GET` | `/transfers/{id}` | Transaction outcome |
| `GET` | `/transfers/{id}/wal` | WAL records across all shards |

## Seed data

| Account | Shard | Balance |
|---------|-------|---------|
| `alice` | checking | $500.00 |
| `bob` | savings | $100.00 |

## In-memory demo

```bash
go run ./cmd/demo
```

## How 2PC works here

1. **Prepare** — each DB validates, writes WAL, reserves the change in `prepared_tx`
2. **Commit/Abort** — if all vote YES, all apply; if any votes NO, all roll back

## Configuration

| Variable | Default |
|----------|---------|
| `PORT` | `8080` |
| `CHECKING_DATABASE_URL` | `postgres://bank:bank@localhost:5434/checking?sslmode=disable` |
| `SAVINGS_DATABASE_URL` | `postgres://bank:bank@localhost:5434/savings?sslmode=disable` |
| `LEDGER_DATABASE_URL` | `postgres://bank:bank@localhost:5434/ledger?sslmode=disable` |
