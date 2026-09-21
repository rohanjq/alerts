package api

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/rohanjq/alerts/internal/model"
	"github.com/rohanjq/alerts/internal/service"
	signalsadapter "github.com/rohanjq/alerts/internal/signals"
)

type Options struct {
	Addr, APIToken, IngestToken string
	Insecure                    bool
}
type Server struct {
	options Options
	service *service.Service
	http    *http.Server
}

func New(options Options, svc *service.Service) (*Server, error) {
	if options.Addr == "" || svc == nil {
		return nil, errors.New("address and service are required")
	}
	if !options.Insecure && (options.APIToken == "" || options.IngestToken == "") {
		return nil, errors.New("API and ingest tokens are required")
	}
	s := &Server{options: options, service: svc}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { write(w, 200, map[string]string{"status": "ok"}) })
	mux.HandleFunc("GET /readyz", s.ready)
	mux.HandleFunc("GET /metrics", s.metrics)
	mux.Handle("GET /v1/rule-types", s.control(http.HandlerFunc(s.catalog)))
	mux.Handle("POST /v1/definitions", s.control(http.HandlerFunc(s.createDefinition)))
	mux.Handle("GET /v1/definitions", s.control(http.HandlerFunc(s.listDefinitions)))
	mux.Handle("PATCH /v1/definitions/{id}", s.control(http.HandlerFunc(s.updateDefinition)))
	mux.Handle("DELETE /v1/definitions/{id}", s.control(http.HandlerFunc(s.deleteDefinition)))
	mux.Handle("GET /v1/events", s.control(http.HandlerFunc(s.listAlerts)))
	mux.Handle("POST /v1/internal/observations", s.ingest(http.HandlerFunc(s.observation)))
	mux.Handle("POST /v1/internal/signal-events", s.ingest(http.HandlerFunc(s.signalEvent)))
	s.http = &http.Server{Addr: options.Addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 60 * time.Second}
	return s, nil
}

func (s *Server) Run() error                         { return s.http.ListenAndServe() }
func (s *Server) Shutdown(ctx context.Context) error { return s.http.Shutdown(ctx) }

func (s *Server) control(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !s.options.Insecure && !bearerOK(r, s.options.APIToken) {
			problem(w, 401, "unauthorized", "invalid bearer token")
			return
		}
		next.ServeHTTP(w, r)
	})
}
func (s *Server) ingest(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !s.options.Insecure && !bearerOK(r, s.options.IngestToken) {
			problem(w, 401, "unauthorized", "invalid ingest token")
			return
		}
		next.ServeHTTP(w, r)
	})
}
func bearerOK(r *http.Request, expected string) bool {
	got := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	return len(got) == len(expected) && subtle.ConstantTimeCompare([]byte(got), []byte(expected)) == 1
}

type createRequest struct {
	Name            string       `json:"name"`
	Series          model.Series `json:"series"`
	Rule            model.Rule   `json:"rule"`
	EvaluationMode  string       `json:"evaluation_mode,omitempty"`
	TriggerMode     string       `json:"trigger_mode,omitempty"`
	CooldownSeconds int          `json:"cooldown_seconds"`
}

func (s *Server) createDefinition(w http.ResponseWriter, r *http.Request) {
	var in createRequest
	if err := decode(r, &in); err != nil {
		problem(w, 400, "invalid_json", err.Error())
		return
	}
	definition, created, err := s.service.Create(r.Context(), in.Name, in.Series, in.Rule, in.EvaluationMode, in.TriggerMode, in.CooldownSeconds)
	if err != nil {
		problem(w, 422, "invalid_definition", err.Error())
		return
	}
	status := http.StatusCreated
	if !created {
		status = http.StatusOK
	}
	write(w, status, definition)
}
func (s *Server) listDefinitions(w http.ResponseWriter, r *http.Request) {
	definitions, err := s.service.ListDefinitions(r.Context())
	if err != nil {
		problem(w, 500, "store_error", "could not list alert definitions")
		return
	}
	write(w, 200, map[string]any{"data": definitions})
}
func (s *Server) updateDefinition(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Enabled *bool `json:"enabled"`
	}
	if err := decode(r, &in); err != nil {
		problem(w, 400, "invalid_json", err.Error())
		return
	}
	if in.Enabled == nil {
		problem(w, 422, "invalid_definition", "enabled is required")
		return
	}
	definition, err := s.service.SetEnabled(r.Context(), r.PathValue("id"), *in.Enabled)
	if service.IsNotFound(err) {
		problem(w, 404, "not_found", "alert definition not found")
		return
	}
	if err != nil {
		problem(w, 500, "store_error", "could not update alert definition")
		return
	}
	write(w, 200, definition)
}
func (s *Server) deleteDefinition(w http.ResponseWriter, r *http.Request) {
	if err := s.service.Delete(r.Context(), r.PathValue("id")); service.IsNotFound(err) {
		problem(w, 404, "not_found", "alert definition not found")
		return
	} else if err != nil {
		problem(w, 500, "store_error", "could not archive alert definition")
		return
	}
	w.WriteHeader(204)
}
func (s *Server) listAlerts(w http.ResponseWriter, r *http.Request) {
	after, err := parseInt64(r.URL.Query().Get("after"))
	if err != nil {
		problem(w, 400, "invalid_cursor", "after must be a non-negative integer")
		return
	}
	limit, err := parseInt(r.URL.Query().Get("limit"))
	if err != nil {
		problem(w, 400, "invalid_limit", "limit must be an integer")
		return
	}
	alerts, err := s.service.ListAlerts(r.Context(), after, limit)
	if err != nil {
		problem(w, 500, "store_error", "could not list alert events")
		return
	}
	write(w, 200, map[string]any{"data": alerts})
}
func (s *Server) observation(w http.ResponseWriter, r *http.Request) {
	var o model.Observation
	if err := decode(r, &o); err != nil {
		problem(w, 400, "invalid_json", err.Error())
		return
	}
	alerts, err := s.service.Ingest(r.Context(), o)
	if err != nil {
		problem(w, 422, "invalid_observation", err.Error())
		return
	}
	write(w, 202, map[string]any{"accepted": true, "alerts": alerts})
}
func (s *Server) signalEvent(w http.ResponseWriter, r *http.Request) {
	var event signalsadapter.Event
	if err := decodePermissive(r, &event); err != nil {
		problem(w, 400, "invalid_json", err.Error())
		return
	}
	o, err := signalsadapter.Observation(event)
	if err != nil {
		problem(w, 422, "invalid_signal_event", err.Error())
		return
	}
	alerts, err := s.service.Ingest(r.Context(), o)
	if err != nil {
		problem(w, 422, "invalid_observation", err.Error())
		return
	}
	write(w, 202, map[string]any{"accepted": true, "alerts": alerts})
}
func decodePermissive(r *http.Request, out any) error {
	defer r.Body.Close()
	decoder := json.NewDecoder(io.LimitReader(r.Body, 1<<20))
	if err := decoder.Decode(out); err != nil {
		return err
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		return errors.New("request must contain one JSON value")
	}
	return nil
}
func (s *Server) ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if !s.service.Ready(ctx) {
		write(w, 503, map[string]string{"status": "not ready"})
		return
	}
	write(w, 200, map[string]string{"status": "ready"})
}
func (s *Server) metrics(w http.ResponseWriter, r *http.Request) {
	m := s.service.Metrics(r.Context())
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	_, _ = fmt.Fprintf(w, "alertsd_observations_total %d\nalertsd_alerts_generated_total %d\nalertsd_alert_events_published_total %d\nalertsd_alert_event_publish_failures_total %d\nalertsd_evaluation_batches_total %d\nalertsd_evaluation_failures_total %d\nalertsd_evaluation_duration_seconds_sum %.9f\nalertsd_rule_evaluations_total %d\nalertsd_stale_events_total %d\nalertsd_reset_events_total %d\nalertsd_definition_refresh_failures_total %d\nalertsd_definition_cache_age_seconds %.3f\nalertsd_outbox_pending %d\nalertsd_outbox_oldest_age_seconds %.3f\nalertsd_retention_inbox_deleted_total %d\nalertsd_retention_alerts_deleted_total %d\nalertsd_retention_failures_total %d\n", m.Observations, m.Alerts, m.Published, m.PublishFailures, m.Batches, m.IngestFailures, float64(m.EvaluationNanoseconds)/float64(time.Second), m.RuleEvaluations, m.StaleEvents, m.ResetEvents, m.DefinitionRefreshFailures, m.DefinitionCacheAgeSeconds, m.PendingOutbox, m.OldestOutboxAgeSeconds, m.InboxDeleted, m.AlertsDeleted, m.MaintenanceFailures)
	for index, bound := range service.EvaluationBucketBounds() {
		_, _ = fmt.Fprintf(w, "alertsd_evaluation_duration_seconds_bucket{le=\"%.3f\"} %d\n", bound.Seconds(), m.EvaluationBuckets[index])
	}
	_, _ = fmt.Fprintf(w, "alertsd_evaluation_duration_seconds_bucket{le=\"+Inf\"} %d\nalertsd_evaluation_duration_seconds_count %d\n", m.Batches, m.Batches)
}
func (s *Server) catalog(w http.ResponseWriter, _ *http.Request) {
	write(w, 200, map[string]any{"schema": "alerts.catalog.v1", "rule_types": []map[string]any{{"type": "price_hits_zone", "kinds": []string{"fvg", "order_block"}, "transitions": []string{"touched", "mitigated"}}, {"type": "zone_transition", "kinds": []string{"fvg", "order_block"}, "transitions": []string{"created", "touched", "mitigated", "invalidated", "expired"}}, {"type": "price_near_zone", "kinds": []string{"fvg", "order_block"}, "distance_unit": "basis_points"}, {"type": "price_crosses_indicator", "directions": []string{"above", "below", "either"}}, {"type": "candle_color", "directions": []string{"red", "green"}}, {"type": "candle_streak", "directions": []string{"red", "green"}}, {"type": "candle_color_flip", "directions": []string{"either", "red_to_green", "green_to_red"}, "threshold": "min_elapsed_percent", "evaluation_mode": "intrabar"}, {"type": "pattern"}, {"type": "all"}, {"type": "any"}, {"type": "not"}}})
}

func decode(r *http.Request, out any) error {
	defer r.Body.Close()
	decoder := json.NewDecoder(io.LimitReader(r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(out); err != nil {
		return err
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		return errors.New("request must contain one JSON value")
	}
	return nil
}
func write(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
func problem(w http.ResponseWriter, status int, code, message string) {
	write(w, status, map[string]any{"error": map[string]any{"code": code, "message": message}})
}
func parseInt64(value string) (int64, error) {
	if value == "" {
		return 0, nil
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil || parsed < 0 {
		return 0, errors.New("invalid integer")
	}
	return parsed, nil
}
func parseInt(value string) (int, error) {
	if value == "" {
		return 100, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed < 1 {
		return 0, errors.New("invalid integer")
	}
	return parsed, nil
}
