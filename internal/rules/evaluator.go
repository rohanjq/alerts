package rules

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/rohanjq/alerts/internal/model"
)

const maxDepth = 8

func Validate(rule model.Rule) error { return validate(rule, 0) }

func RequiresIntrabar(rule model.Rule) bool {
	if rule.Type == "candle_color_flip" {
		return true
	}
	if rule.Type == "not" && rule.Rule != nil {
		return RequiresIntrabar(*rule.Rule)
	}
	for _, child := range rule.Rules {
		if RequiresIntrabar(child) {
			return true
		}
	}
	return false
}

func validate(rule model.Rule, depth int) error {
	if depth > maxDepth {
		return fmt.Errorf("rule nesting exceeds %d", maxDepth)
	}
	switch rule.Type {
	case "all", "any":
		if len(rule.Rules) < 2 || len(rule.Rules) > 20 {
			return fmt.Errorf("%s requires 2 to 20 child rules", rule.Type)
		}
		for _, child := range rule.Rules {
			if err := validate(child, depth+1); err != nil {
				return err
			}
		}
	case "not":
		if rule.Rule == nil {
			return fmt.Errorf("not requires rule")
		}
		return validate(*rule.Rule, depth+1)
	case "price_hits_zone", "zone_transition":
		if rule.Kind != "fvg" && rule.Kind != "order_block" {
			return fmt.Errorf("%s kind must be fvg or order_block", rule.Type)
		}
		if rule.Type == "zone_transition" && rule.Transition == "" {
			return fmt.Errorf("zone_transition requires transition")
		}
		if rule.Transition != "" && rule.Transition != "created" && rule.Transition != "touched" && rule.Transition != "mitigated" && rule.Transition != "invalidated" && rule.Transition != "expired" {
			return fmt.Errorf("unsupported zone transition")
		}
	case "price_near_zone":
		if rule.Kind != "fvg" && rule.Kind != "order_block" {
			return fmt.Errorf("price_near_zone kind must be fvg or order_block")
		}
		if rule.DistanceBps <= 0 || rule.DistanceBps > 10000 {
			return fmt.Errorf("distance_bps must be greater than 0 and at most 10000")
		}
	case "price_crosses_indicator":
		if rule.Name == "" || rule.Period < 1 {
			return fmt.Errorf("indicator name and positive period are required")
		}
		if rule.Direction != "above" && rule.Direction != "below" && rule.Direction != "either" {
			return fmt.Errorf("cross direction must be above, below, or either")
		}
	case "candle_color":
		if rule.Direction != "red" && rule.Direction != "green" {
			return fmt.Errorf("candle color direction must be red or green")
		}
	case "candle_streak":
		if rule.Direction != "red" && rule.Direction != "green" {
			return fmt.Errorf("candle streak direction must be red or green")
		}
		if rule.Count < 2 || rule.Count > 100 {
			return fmt.Errorf("candle streak count must be 2 to 100")
		}
	case "candle_color_flip":
		if rule.Direction != "either" && rule.Direction != "red_to_green" && rule.Direction != "green_to_red" {
			return fmt.Errorf("candle color flip direction must be either, red_to_green, or green_to_red")
		}
		if rule.MinElapsedPercent <= 0 || rule.MinElapsedPercent > 100 {
			return fmt.Errorf("min_elapsed_percent must be greater than 0 and at most 100")
		}
	case "pattern":
		if strings.TrimSpace(rule.Kind) == "" {
			return fmt.Errorf("pattern kind is required")
		}
	default:
		return fmt.Errorf("unsupported rule type %q", rule.Type)
	}
	return nil
}

type result struct {
	matched, known bool
	reasons        []string
	facts          []model.FactReference
}

type Dependency uint64

const (
	DependencyCandle Dependency = 1 << iota
	DependencyIndicator
	DependencyZone
	DependencyPattern
	DependencyHistory
)

type Compiled struct {
	Rule         model.Rule
	Dependencies Dependency
}

func Compile(rule model.Rule) (Compiled, error) {
	if err := Validate(rule); err != nil {
		return Compiled{}, err
	}
	return Compiled{Rule: rule, Dependencies: dependencies(rule)}, nil
}

func dependencies(rule model.Rule) Dependency {
	var result Dependency
	switch rule.Type {
	case "candle_color", "candle_color_flip":
		result = DependencyCandle
	case "candle_streak":
		result = DependencyCandle | DependencyHistory
	case "price_crosses_indicator":
		result = DependencyCandle | DependencyIndicator | DependencyHistory
	case "price_hits_zone", "zone_transition":
		result = DependencyZone
	case "price_near_zone":
		result = DependencyCandle | DependencyZone
	case "pattern":
		result = DependencyPattern
	case "not":
		if rule.Rule != nil {
			result = dependencies(*rule.Rule)
		}
	default:
		for _, child := range rule.Rules {
			result |= dependencies(child)
		}
	}
	return result
}

func (compiled Compiled) Evaluate(observation model.Observation, state model.FeatureState) (bool, []string) {
	return Evaluate(compiled.Rule, observation, state)
}

func (compiled Compiled) EvaluateDetailed(observation model.Observation, state model.FeatureState) (bool, []string, []model.FactReference) {
	r := evaluate(compiled.Rule, observation, state)
	return r.known && r.matched, r.reasons, r.facts
}

func Evaluate(rule model.Rule, observation model.Observation, state model.FeatureState) (bool, []string) {
	r := evaluate(rule, observation, state)
	return r.known && r.matched, r.reasons
}

// Pulse reports whether truth represents an occurrence on this observation,
// rather than a state that should trigger only on its false-to-true edge.
func Pulse(rule model.Rule) bool {
	switch rule.Type {
	case "price_hits_zone", "zone_transition", "price_crosses_indicator", "pattern", "candle_color_flip":
		return true
	case "all", "any":
		for _, child := range rule.Rules {
			if Pulse(child) {
				return true
			}
		}
	}
	return false
}

func evaluate(rule model.Rule, o model.Observation, state model.FeatureState) result {
	switch rule.Type {
	case "all":
		out := result{matched: true, known: true}
		for _, child := range rule.Rules {
			r := evaluate(child, o, state)
			out.known = out.known && r.known
			out.matched = out.matched && r.matched
			if r.matched {
				out.reasons = append(out.reasons, r.reasons...)
				out.facts = append(out.facts, r.facts...)
			}
		}
		return out
	case "any":
		out := result{}
		for _, child := range rule.Rules {
			r := evaluate(child, o, state)
			out.known = out.known || r.known
			out.matched = out.matched || (r.known && r.matched)
			if r.matched {
				out.reasons = append(out.reasons, r.reasons...)
				out.facts = append(out.facts, r.facts...)
			}
		}
		return out
	case "not":
		r := evaluate(*rule.Rule, o, state)
		r.matched = !r.matched
		if r.matched {
			r.facts = nil
		}
		return r
	case "price_hits_zone", "zone_transition":
		transition := rule.Transition
		if transition == "" && rule.Type == "price_hits_zone" {
			transition = "touched"
		}
		for _, z := range o.ZoneTransitions {
			if z.Kind == rule.Kind && z.Transition == transition && (rule.Side == "" || z.Side == rule.Side) {
				return result{matched: true, known: true, reasons: []string{fmt.Sprintf("zone %s %s %s", transition, z.Side, z.Kind)}, facts: []model.FactReference{{Kind: z.Kind, ID: z.ID, Side: z.Side, Value: (z.Upper + z.Lower) / 2}}}
			}
		}
		return result{matched: false, known: true}
	case "price_near_zone":
		price := o.Candle.Close
		for _, z := range o.ActiveZones {
			if z.Kind != rule.Kind || (rule.Side != "" && z.Side != rule.Side) || z.State != "active" {
				continue
			}
			distance := 0.0
			if price < z.Lower {
				distance = (z.Lower - price) / price * 10000
			} else if price > z.Upper {
				distance = (price - z.Upper) / price * 10000
			}
			if distance > 0 && distance <= rule.DistanceBps {
				return result{matched: true, known: true, reasons: []string{fmt.Sprintf("price is %.2f bps from %s", distance, z.Kind)}, facts: []model.FactReference{{Kind: z.Kind, ID: z.ID, Side: z.Side, Value: (z.Upper + z.Lower) / 2}}}
			}
		}
		return result{matched: false, known: true}
	case "pattern":
		for _, p := range o.Patterns {
			if p.Kind == rule.Kind && (rule.Side == "" || p.Side == rule.Side) {
				return result{matched: true, known: true, reasons: []string{"pattern " + p.Kind}, facts: []model.FactReference{{Kind: "pattern", Name: p.Kind, Side: p.Side}}}
			}
		}
		return result{matched: false, known: true}
	case "candle_color":
		color := candleColor(o.Candle)
		return result{matched: color == rule.Direction, known: true, reasons: []string{fmt.Sprintf("%s candle closed", color)}}
	case "candle_streak":
		color := candleColor(o.Candle)
		colors := append(append([]string(nil), state.CandleColors...), color)
		if len(colors) < rule.Count {
			return result{matched: false, known: true}
		}
		for _, got := range colors[len(colors)-rule.Count:] {
			if got != rule.Direction {
				return result{matched: false, known: true}
			}
		}
		return result{matched: true, known: true, reasons: []string{fmt.Sprintf("%d consecutive %s candles", rule.Count, rule.Direction)}}
	case "candle_color_flip":
		if o.Status != "provisional" || !state.FormingBar.Equal(o.Candle.OpenTime) {
			return result{matched: false, known: true}
		}
		from, to := state.FormingColor, candleColor(o.Candle)
		if from == "" || from == "doji" || to == "doji" || from == to {
			return result{matched: false, known: true}
		}
		transition := from + "_to_" + to
		if rule.Direction != "either" && rule.Direction != transition {
			return result{matched: false, known: true}
		}
		duration, err := timeframeDuration(o.Series.Timeframe)
		if err != nil {
			return result{matched: false, known: false}
		}
		elapsed := o.OccurredAt.Sub(o.Candle.OpenTime)
		percent := elapsed.Seconds() / duration.Seconds() * 100
		if percent < rule.MinElapsedPercent {
			return result{matched: false, known: true}
		}
		return result{matched: true, known: true, reasons: []string{fmt.Sprintf("candle flipped %s to %s at %.1f%% elapsed", from, to, percent)}}
	case "price_crosses_indicator":
		if state.LastClose == nil {
			return result{matched: false, known: false}
		}
		key := indicatorKey(rule.Name, rule.Period)
		previousIndicator, ok := state.Indicators[key]
		if !ok {
			return result{matched: false, known: false}
		}
		var current *float64
		for _, indicator := range o.Indicators {
			if indicator.Ready && indicatorKey(indicator.Name, indicator.Period) == key {
				value := indicator.Value
				current = &value
				break
			}
		}
		if current == nil {
			return result{matched: false, known: false}
		}
		above := *state.LastClose <= previousIndicator && o.Candle.Close > *current
		below := *state.LastClose >= previousIndicator && o.Candle.Close < *current
		matched := (rule.Direction == "above" && above) || (rule.Direction == "below" && below) || (rule.Direction == "either" && (above || below))
		return result{matched: matched, known: true, reasons: []string{fmt.Sprintf("price crossed %s %s(%d)", rule.Direction, rule.Name, rule.Period)}, facts: []model.FactReference{{Kind: "indicator", ID: key, Name: rule.Name, Value: *current}}}
	}
	return result{}
}

func Advance(state model.FeatureState, o model.Observation) model.FeatureState {
	closeValue := o.Candle.Close
	state.LastClose = &closeValue
	if state.Indicators == nil {
		state.Indicators = map[string]float64{}
	}
	for _, indicator := range o.Indicators {
		if indicator.Ready {
			state.Indicators[indicatorKey(indicator.Name, indicator.Period)] = indicator.Value
		}
	}
	// A forming candle may be evaluated many times. Persist its colour only
	// when it closes, otherwise one red candle could look like a red streak.
	if o.Status == "confirmed" {
		state.Candles = append(state.Candles, o.Candle)
		if len(state.Candles) > 500 {
			state.Candles = append([]model.Candle(nil), state.Candles[len(state.Candles)-500:]...)
		}
		state.CandleColors = append(state.CandleColors, candleColor(o.Candle))
		if len(state.CandleColors) > 100 {
			state.CandleColors = append([]string(nil), state.CandleColors[len(state.CandleColors)-100:]...)
		}
		state.FormingBar = time.Time{}
		state.FormingColor = ""
	} else {
		if !state.FormingBar.Equal(o.Candle.OpenTime) {
			state.FormingBar = o.Candle.OpenTime.UTC()
			state.FormingColor = ""
		}
		// Preserve the last real colour through a momentary doji so a transition
		// from one side of the open to the other is still detected.
		if color := candleColor(o.Candle); color != "doji" {
			state.FormingColor = color
		}
	}
	return state
}

func indicatorKey(name string, period int) string { return fmt.Sprintf("%s:%d", name, period) }
func candleColor(c model.Candle) string {
	if c.Close < c.Open {
		return "red"
	}
	if c.Close > c.Open {
		return "green"
	}
	return "doji"
}

func timeframeDuration(value string) (time.Duration, error) {
	if len(value) > 1 && (strings.HasSuffix(value, "d") || strings.HasSuffix(value, "w")) {
		quantity, err := strconv.Atoi(value[:len(value)-1])
		if err != nil || quantity <= 0 {
			return 0, fmt.Errorf("invalid timeframe %q", value)
		}
		unit := 24 * time.Hour
		if strings.HasSuffix(value, "w") {
			unit *= 7
		}
		return time.Duration(quantity) * unit, nil
	}
	duration, err := time.ParseDuration(value)
	if err != nil || duration <= 0 {
		return 0, fmt.Errorf("invalid timeframe %q", value)
	}
	return duration, nil
}
