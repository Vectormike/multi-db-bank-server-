\c ledger

CREATE TABLE accounts (
    id TEXT PRIMARY KEY,
    balance BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE prepared_tx (
    tx_id TEXT PRIMARY KEY,
    account_id TEXT NOT NULL,
    delta BIGINT NOT NULL
);

CREATE TABLE wal (
    id SERIAL PRIMARY KEY,
    tx_id TEXT NOT NULL,
    phase TEXT NOT NULL,
    action TEXT NOT NULL,
    account_id TEXT NOT NULL,
    amount BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE transaction_decisions (
    tx_id TEXT PRIMARY KEY,
    state TEXT NOT NULL,
    from_account TEXT NOT NULL,
    to_account TEXT NOT NULL,
    amount BIGINT NOT NULL,
    message TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE gl_entries (
    id SERIAL PRIMARY KEY,
    tx_id TEXT NOT NULL,
    from_account TEXT NOT NULL,
    to_account TEXT NOT NULL,
    amount BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO accounts (id, balance) VALUES ('gl:transfers', 0);
