// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

// Copyright 2018 Prometheus Team
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package metadata

import (
	"encoding/json"
	"regexp"
	"strings"
	"unicode/utf8"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"gopkg.in/yaml.v3"
)

var (
	// '=~' has to come before '=' because otherwise only the '='
	// will be consumed, and the '~' will be part of the 3rd token.
	re      = regexp.MustCompile(`^\s*([a-zA-Z_:][a-zA-Z0-9_:]*)\s*(=~|=|!=|!~)\s*((?s).*?)\s*$`)
	typeMap = map[string]labels.MatchType{
		"=":  labels.MatchEqual,
		"!=": labels.MatchNotEqual,
		"=~": labels.MatchRegexp,
		"!~": labels.MatchNotRegexp,
	}
)

type Matchers []*labels.Matcher

func (m *Matchers) UnmarshalYAML(value *yaml.Node) (err error) {
	*m, err = parser.ParseMetricSelector(value.Value)
	if err != nil {
		return errors.Wrapf(err, "parse metric selector %v", value.Value)
	}
	return nil
}

func (m *Matchers) MarshalJSON() ([]byte, error) {
	if len(*m) == 0 {
		return []byte("[]"), nil
	}
	result := make([]string, len(*m))
	for i, matcher := range *m {
		result[i] = matcher.String()
	}
	return json.Marshal(result)
}

func (m *Matchers) UnmarshalJSON(data []byte) (err error) {
	var lines []string
	if err := json.Unmarshal(data, &lines); err != nil {
		return err
	}
	var pm []*labels.Matcher
	for _, line := range lines {
		pm, err = ParseMatchers(line)
		if err != nil {
			return errors.Wrapf(err, "parse matchers %v", line)
		}
		*m = append(*m, pm...)
	}
	return nil
}

// ParseMatchers parses label matchers from string.
// Copied from https://github.com/prometheus/alertmanager/blob/v0.24.0/pkg/labels/parse.go#L55.
func ParseMatchers(s string) ([]*labels.Matcher, error) {
	matchers := []*labels.Matcher{}
	s = strings.TrimPrefix(s, "{")
	s = strings.TrimSuffix(s, "}")

	var (
		insideQuotes bool
		escaped      bool
		token        strings.Builder
		tokens       []string
	)
	for _, r := range s {
		switch r {
		case ',':
			if !insideQuotes {
				tokens = append(tokens, token.String())
				token.Reset()
				continue
			}
		case '"':
			if !escaped {
				insideQuotes = !insideQuotes
			} else {
				escaped = false
			}
		case '\\':
			escaped = !escaped
		default:
			escaped = false
		}
		token.WriteRune(r)
	}
	if s := strings.TrimSpace(token.String()); s != "" {
		tokens = append(tokens, s)
	}
	for _, token := range tokens {
		m, err := ParseMatcher(token)
		if err != nil {
			return nil, err
		}
		matchers = append(matchers, m)
	}

	return matchers, nil
}

// ParseMatcher parses a label matcher from string.
// Copied from https://github.com/prometheus/alertmanager/blob/v0.24.0/pkg/labels/parse.go#L117.
func ParseMatcher(s string) (*labels.Matcher, error) {
	ms := re.FindStringSubmatch(s)
	if len(ms) == 0 {
		return nil, errors.Errorf("bad matcher format: %s", s)
	}

	var (
		rawValue            = ms[3]
		value               strings.Builder
		escaped             bool
		expectTrailingQuote bool
	)

	if rawValue[0] == '"' {
		rawValue = strings.TrimPrefix(rawValue, "\"")
		expectTrailingQuote = true
	}

	if !utf8.ValidString(rawValue) {
		return nil, errors.Errorf("matcher value not valid UTF-8: %s", ms[3])
	}

	// Unescape the rawValue:
	for i, r := range rawValue {
		if escaped {
			escaped = false
			switch r {
			case 'n':
				value.WriteByte('\n')
			case '"', '\\':
				value.WriteRune(r)
			default:
				// This was a spurious escape, so treat the '\' as literal.
				value.WriteByte('\\')
				value.WriteRune(r)
			}
			continue
		}
		switch r {
		case '\\':
			if i < len(rawValue)-1 {
				escaped = true
				continue
			}
			// '\' encountered as last byte. Treat it as literal.
			value.WriteByte('\\')
		case '"':
			if !expectTrailingQuote || i < len(rawValue)-1 {
				return nil, errors.Errorf("matcher value contains unescaped double quote: %s", ms[3])
			}
			expectTrailingQuote = false
		default:
			value.WriteRune(r)
		}
	}

	if expectTrailingQuote {
		return nil, errors.Errorf("matcher value contains unescaped double quote: %s", ms[3])
	}

	return labels.NewMatcher(typeMap[ms[2]], ms[1], value.String())
}
