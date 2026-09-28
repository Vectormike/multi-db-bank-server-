package bank

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"twophasecommit/internal/coordinator"
	"twophasecommit/internal/participant"
	"twophasecommit/internal/tx"
)

type Server struct {
	svc *Service
}

func NewServer(svc *Service) *Server {
	return &Server{svc: svc}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/accounts/", s.handleAccount)
	mux.HandleFunc("/transfers/", s.handleTransferByID)
	mux.HandleFunc("/transfers", s.handleTransfers)
	return mux
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w)
		return
	}
	if err := s.svc.Ping(r.Context()); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{
			"status": "unhealthy",
			"error":  err.Error(),
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleAccount(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w)
		return
	}
	id := strings.TrimPrefix(r.URL.Path, "/accounts/")
	if id == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "account id required"})
		return
	}

	entry, balance, err := s.svc.GetAccount(r.Context(), id)
	if errors.Is(err, ErrUnknownAccount) || errors.Is(err, participant.ErrAccountNotFound) {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "account not found"})
		return
	}
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"id":      entry.AccountID,
		"shard":   entry.Shard,
		"balance": balance,
	})
}

type transferRequest struct {
	From   string `json:"from"`
	To     string `json:"to"`
	Amount int64  `json:"amount"`
}

func (s *Server) handleTransfers(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/transfers" {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodPost {
		methodNotAllowed(w)
		return
	}

	var body transferRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json body"})
		return
	}

	outcome, err := s.svc.Transfer(r.Context(), TransferInput{
		From:   body.From,
		To:     body.To,
		Amount: body.Amount,
	})
	if errors.Is(err, ErrInvalidAmount) || errors.Is(err, ErrSameAccount) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if errors.Is(err, ErrUnknownAccount) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	status := http.StatusOK
	if outcome.State == tx.StateAborted {
		status = http.StatusConflict
	}
	writeJSON(w, status, map[string]any{
		"tx_id":   outcome.TxID,
		"state":   outcome.State,
		"message": outcome.Message,
	})
}

func (s *Server) handleTransferByID(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w)
		return
	}

	path := strings.TrimPrefix(r.URL.Path, "/transfers/")
	if path == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "transaction id required"})
		return
	}

	if strings.HasSuffix(path, "/wal") {
		id := tx.ID(strings.TrimSuffix(path, "/wal"))
		id = tx.ID(strings.TrimSuffix(string(id), "/"))
		snapshots := s.svc.GetWAL(r.Context(), id)
		writeJSON(w, http.StatusOK, map[string]any{"tx_id": id, "wal": snapshots})
		return
	}

	rec, err := s.svc.GetTransfer(r.Context(), tx.ID(path))
	if errors.Is(err, coordinator.ErrDecisionNotFound) {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "transaction not found"})
		return
	}
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"tx_id":   rec.TxID,
		"state":   rec.State,
		"from":    rec.FromAccount,
		"to":      rec.ToAccount,
		"amount":  rec.Amount,
		"message": rec.Message,
		"created_at": rec.CreatedAt,
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func methodNotAllowed(w http.ResponseWriter) {
	writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
}
