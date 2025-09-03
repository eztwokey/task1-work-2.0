package httpapi

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/jackc/pgx/v5/pgxpool"
	kafka "github.com/segmentio/kafka-go"
	"wb-order-service/internal/repo"
	"wb-order-service/internal/service"
)

type Router struct {
	r  *chi.Mux
	l  *slog.Logger
	s  *service.Service
	pg *pgxpool.Pool
	kb []string
	topic string
}

func New(s *service.Service, logger *slog.Logger, pg *pgxpool.Pool, kafkaBrokers []string, topic string) *Router {
	r := chi.NewRouter()
	r.Use(middleware.RequestID, middleware.RealIP, middleware.Recoverer, middleware.Timeout(15*time.Second))
	h := &Router{r: r, l: logger, s: s, pg: pg, kb: kafkaBrokers, topic: topic}
	r.Get("/healthz", h.healthz)
	r.Get("/readyz", h.readyz)
	r.Get("/order/{id}", h.getOrder)
	r.Handle("/", http.FileServer(http.Dir("./web")))
	return h
}

func (h *Router) ServeHTTP(w http.ResponseWriter, r *http.Request) { h.r.ServeHTTP(w, r) }

func (h *Router) healthz(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK); w.Write([]byte("ok"))
}

func (h *Router) readyz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second); defer cancel()
	if err := h.pg.Ping(ctx); err != nil { http.Error(w, "db not ready", http.StatusServiceUnavailable); return }
	// Try a quick kafka dial
	c, err := kafka.DialLeader(ctx, "tcp", h.kb[0], h.topic, 0)
	if err == nil { _ = c.Close() }
	// If kafka not ready, still return 200 but include hint
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"db":"ok", "kafka":"checked"})
}

func (h *Router) getOrder(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if id == "" || len(id) < 6 {
		http.Error(w, "bad order id", http.StatusBadRequest)
		return
	}
	o, err := h.s.GetOrder(r.Context(), id)
	if err != nil {
		if err == repo.ErrNotFound {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(o)
}
