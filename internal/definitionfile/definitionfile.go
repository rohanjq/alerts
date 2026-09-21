// Package definitionfile imports and exports portable alert-definition manifests.
package definitionfile

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/rohanjq/alerts/internal/model"
	"github.com/rohanjq/alerts/internal/rules"
)

const Schema = "alerts.definitions.v1"

// Definition contains only the portable, user-authored part of an alert
// definition. Database IDs, hashes, versions, and timestamps are regenerated
// by the destination service.
type Definition struct {
	Name            string       `json:"name"`
	Enabled         *bool        `json:"enabled,omitempty"`
	Series          model.Series `json:"series"`
	Rule            model.Rule   `json:"rule"`
	EvaluationMode  string       `json:"evaluation_mode,omitempty"`
	TriggerMode     string       `json:"trigger_mode,omitempty"`
	CooldownSeconds int          `json:"cooldown_seconds"`
}

func (d Definition) enabled() bool {
	return d.Enabled == nil || *d.Enabled
}

type Manifest struct {
	Schema      string       `json:"schema"`
	Definitions []Definition `json:"definitions"`
}

func New(definitions []model.AlertDefinition) Manifest {
	portable := make([]Definition, 0, len(definitions))
	for _, definition := range definitions {
		enabled := definition.Enabled
		portable = append(portable, Definition{
			Name:            definition.Name,
			Enabled:         &enabled,
			Series:          definition.Series,
			Rule:            definition.Rule,
			EvaluationMode:  definition.EvaluationMode,
			TriggerMode:     definition.TriggerMode,
			CooldownSeconds: definition.CooldownSeconds,
		})
	}
	sort.SliceStable(portable, func(i, j int) bool {
		return definitionSortKey(portable[i]) < definitionSortKey(portable[j])
	})
	return Manifest{Schema: Schema, Definitions: portable}
}

func definitionSortKey(definition Definition) string {
	rule, _ := json.Marshal(definition.Rule)
	return strings.Join([]string{
		definition.Series.Dataset,
		definition.Series.Symbol,
		definition.Series.Timeframe,
		definition.Name,
		string(rule),
	}, "\x00")
}

func Decode(reader io.Reader) (Manifest, error) {
	decoder := json.NewDecoder(reader)
	decoder.DisallowUnknownFields()
	var manifest Manifest
	if err := decoder.Decode(&manifest); err != nil {
		return Manifest{}, fmt.Errorf("decode definition manifest: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			err = errors.New("multiple JSON values")
		}
		return Manifest{}, fmt.Errorf("decode definition manifest: %w", err)
	}
	if err := manifest.Validate(); err != nil {
		return Manifest{}, err
	}
	return manifest, nil
}

func Encode(writer io.Writer, manifest Manifest) error {
	if err := manifest.Validate(); err != nil {
		return err
	}
	encoder := json.NewEncoder(writer)
	encoder.SetIndent("", "  ")
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(manifest); err != nil {
		return fmt.Errorf("encode definition manifest: %w", err)
	}
	return nil
}

func (m Manifest) Validate() error {
	if m.Schema != Schema {
		return fmt.Errorf("definition manifest schema must be %q", Schema)
	}
	for index, definition := range m.Definitions {
		if definition.Name == "" || definition.Series.Symbol == "" || definition.Series.Timeframe == "" {
			return fmt.Errorf("definition %d: name, symbol, and timeframe are required", index+1)
		}
		if definition.CooldownSeconds < 0 {
			return fmt.Errorf("definition %d (%q): cooldown_seconds cannot be negative", index+1, definition.Name)
		}
		if definition.EvaluationMode != "" && definition.EvaluationMode != "confirmed_close" && definition.EvaluationMode != "intrabar" && definition.EvaluationMode != "both" {
			return fmt.Errorf("definition %d (%q): invalid evaluation_mode", index+1, definition.Name)
		}
		if definition.TriggerMode != "" && definition.TriggerMode != "on_occurrence" && definition.TriggerMode != "once_per_bar" && definition.TriggerMode != "every_match" {
			return fmt.Errorf("definition %d (%q): invalid trigger_mode", index+1, definition.Name)
		}
		if err := rules.Validate(definition.Rule); err != nil {
			return fmt.Errorf("definition %d (%q): %w", index+1, definition.Name, err)
		}
		if rules.RequiresIntrabar(definition.Rule) && (definition.EvaluationMode == "" || definition.EvaluationMode == "confirmed_close") {
			return fmt.Errorf("definition %d (%q): candle_color_flip requires intrabar or both evaluation_mode", index+1, definition.Name)
		}
	}
	return nil
}

type Client struct {
	BaseURL    string
	Token      string
	HTTPClient *http.Client
}

type ImportResult struct {
	Created        int
	Existing       int
	StateUpdated   int
	NameMismatches []string
}

func (c Client) Export(ctx context.Context) (Manifest, error) {
	var response struct {
		Data []model.AlertDefinition `json:"data"`
	}
	if _, err := c.do(ctx, http.MethodGet, "/v1/definitions", nil, &response); err != nil {
		return Manifest{}, err
	}
	return New(response.Data), nil
}

func (c Client) Import(ctx context.Context, manifest Manifest) (ImportResult, error) {
	if err := manifest.Validate(); err != nil {
		return ImportResult{}, err
	}
	result := ImportResult{}
	for index, definition := range manifest.Definitions {
		request := struct {
			Name            string       `json:"name"`
			Series          model.Series `json:"series"`
			Rule            model.Rule   `json:"rule"`
			EvaluationMode  string       `json:"evaluation_mode,omitempty"`
			TriggerMode     string       `json:"trigger_mode,omitempty"`
			CooldownSeconds int          `json:"cooldown_seconds"`
		}{definition.Name, definition.Series, definition.Rule, definition.EvaluationMode, definition.TriggerMode, definition.CooldownSeconds}
		var stored model.AlertDefinition
		status, err := c.do(ctx, http.MethodPost, "/v1/definitions", request, &stored)
		if err != nil {
			return result, fmt.Errorf("import definition %d (%q): %w", index+1, definition.Name, err)
		}
		if status == http.StatusCreated {
			result.Created++
		} else {
			result.Existing++
		}
		if stored.Name != definition.Name {
			result.NameMismatches = append(result.NameMismatches, fmt.Sprintf("%q retained existing name %q", definition.Name, stored.Name))
		}
		if stored.Enabled == definition.enabled() {
			continue
		}
		patch := struct {
			Enabled bool `json:"enabled"`
		}{definition.enabled()}
		if _, err := c.do(ctx, http.MethodPatch, "/v1/definitions/"+url.PathEscape(stored.ID), patch, &stored); err != nil {
			return result, fmt.Errorf("set enabled state for definition %d (%q): %w", index+1, definition.Name, err)
		}
		result.StateUpdated++
	}
	return result, nil
}

func (c Client) do(ctx context.Context, method, path string, input, output any) (int, error) {
	base, err := url.Parse(strings.TrimRight(c.BaseURL, "/"))
	if err != nil || (base.Scheme != "http" && base.Scheme != "https") || base.Host == "" {
		return 0, fmt.Errorf("invalid Alerts API URL %q", c.BaseURL)
	}
	var body io.Reader
	if input != nil {
		encoded, marshalErr := json.Marshal(input)
		if marshalErr != nil {
			return 0, fmt.Errorf("encode request: %w", marshalErr)
		}
		body = bytes.NewReader(encoded)
	}
	request, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(c.BaseURL, "/")+path, body)
	if err != nil {
		return 0, fmt.Errorf("create request: %w", err)
	}
	request.Header.Set("Accept", "application/json")
	if input != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	if c.Token != "" {
		request.Header.Set("Authorization", "Bearer "+c.Token)
	}
	client := c.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	response, err := client.Do(request)
	if err != nil {
		return 0, fmt.Errorf("%s %s: %w", method, path, err)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		message, _ := io.ReadAll(io.LimitReader(response.Body, 64<<10))
		return response.StatusCode, fmt.Errorf("%s %s: HTTP %s: %s", method, path, response.Status, strings.TrimSpace(string(message)))
	}
	if output != nil {
		if err := json.NewDecoder(response.Body).Decode(output); err != nil {
			return response.StatusCode, fmt.Errorf("decode %s %s response: %w", method, path, err)
		}
	}
	return response.StatusCode, nil
}
