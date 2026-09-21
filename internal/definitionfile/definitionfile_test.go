package definitionfile

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/rohanjq/alerts/internal/model"
)

func TestDecodeRejectsUnknownFieldsAndInvalidSchema(t *testing.T) {
	tests := []string{
		`{"schema":"alerts.definitions.v2","definitions":[]}`,
		`{"schema":"alerts.definitions.v1","definitions":[],"extra":true}`,
	}
	for _, input := range tests {
		if _, err := Decode(strings.NewReader(input)); err == nil {
			t.Fatalf("Decode(%s) unexpectedly succeeded", input)
		}
	}
}

func TestEncodeDecodePreservesDisabledState(t *testing.T) {
	disabled := false
	manifest := Manifest{Schema: Schema, Definitions: []Definition{{
		Name: "Disabled red candle", Enabled: &disabled,
		Series:         model.Series{Dataset: "live", Symbol: "BTCUSDT", Timeframe: "1m"},
		Rule:           model.Rule{Type: "candle_color", Direction: "red"},
		EvaluationMode: "confirmed_close", TriggerMode: "once_per_bar",
	}}}
	var encoded bytes.Buffer
	if err := Encode(&encoded, manifest); err != nil {
		t.Fatal(err)
	}
	decoded, err := Decode(&encoded)
	if err != nil {
		t.Fatal(err)
	}
	if len(decoded.Definitions) != 1 || decoded.Definitions[0].Enabled == nil || *decoded.Definitions[0].Enabled {
		t.Fatalf("decoded enabled state = %#v", decoded.Definitions)
	}
}

func TestClientExportProducesPortableSortedManifest(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/v1/definitions" {
			t.Errorf("request = %s %s", r.Method, r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer control-token" {
			t.Errorf("Authorization = %q", got)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"data": []model.AlertDefinition{
			{ID: "database-id-2", DefinitionHash: "hash-2", Name: "Z red", Enabled: false, Series: model.Series{Dataset: "live", Symbol: "ETHUSDT", Timeframe: "1m"}, Rule: model.Rule{Type: "candle_color", Direction: "red"}, EvaluationMode: "confirmed_close", TriggerMode: "once_per_bar", CreatedAt: time.Now()},
			{ID: "database-id-1", DefinitionHash: "hash-1", Name: "A green", Enabled: true, Series: model.Series{Dataset: "live", Symbol: "BTCUSDT", Timeframe: "1m"}, Rule: model.Rule{Type: "candle_color", Direction: "green"}, EvaluationMode: "confirmed_close", TriggerMode: "once_per_bar", CreatedAt: time.Now()},
		}})
	}))
	defer server.Close()

	manifest, err := (Client{BaseURL: server.URL, Token: "control-token"}).Export(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(manifest.Definitions) != 2 || manifest.Definitions[0].Name != "A green" || manifest.Definitions[1].Name != "Z red" {
		t.Fatalf("definition order = %#v", manifest.Definitions)
	}
	encoded, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"database-id", "definition_hash", "created_at"} {
		if strings.Contains(string(encoded), forbidden) {
			t.Errorf("portable manifest contains %q: %s", forbidden, encoded)
		}
	}
}

func TestClientImportIsIdempotentAndAppliesDisabledState(t *testing.T) {
	disabled := false
	manifest := Manifest{Schema: Schema, Definitions: []Definition{
		{Name: "New disabled", Enabled: &disabled, Series: model.Series{Dataset: "live", Symbol: "BTCUSDT", Timeframe: "1m"}, Rule: model.Rule{Type: "candle_color", Direction: "red"}, EvaluationMode: "confirmed_close", TriggerMode: "once_per_bar"},
		{Name: "Already present", Series: model.Series{Dataset: "live", Symbol: "ETHUSDT", Timeframe: "1m"}, Rule: model.Rule{Type: "candle_color", Direction: "green"}, EvaluationMode: "confirmed_close", TriggerMode: "once_per_bar"},
	}}
	postCount := 0
	patchCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer control-token" {
			t.Errorf("Authorization = %q", got)
		}
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/definitions":
			postCount++
			var raw map[string]any
			if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
				t.Errorf("decode POST: %v", err)
			}
			if _, exists := raw["enabled"]; exists {
				t.Error("POST included unsupported enabled field")
			}
			name, _ := raw["name"].(string)
			status := http.StatusOK
			id := "existing-id"
			if name == "New disabled" {
				status = http.StatusCreated
				id = "new-id"
			}
			w.WriteHeader(status)
			_ = json.NewEncoder(w).Encode(model.AlertDefinition{ID: id, Name: name, Enabled: true})
		case r.Method == http.MethodPatch && r.URL.Path == "/v1/definitions/new-id":
			patchCount++
			var patch struct {
				Enabled bool `json:"enabled"`
			}
			if err := json.NewDecoder(r.Body).Decode(&patch); err != nil {
				t.Errorf("decode PATCH: %v", err)
			}
			if patch.Enabled {
				t.Error("PATCH enabled = true, want false")
			}
			_ = json.NewEncoder(w).Encode(model.AlertDefinition{ID: "new-id", Name: "New disabled", Enabled: false})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	result, err := (Client{BaseURL: server.URL, Token: "control-token"}).Import(context.Background(), manifest)
	if err != nil {
		t.Fatal(err)
	}
	if postCount != 2 || patchCount != 1 {
		t.Fatalf("POST count = %d, PATCH count = %d", postCount, patchCount)
	}
	if result.Created != 1 || result.Existing != 1 || result.StateUpdated != 1 {
		t.Fatalf("result = %#v", result)
	}
}
