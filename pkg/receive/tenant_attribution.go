// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/model/labels"
	"gopkg.in/yaml.v2"
)

// TenantRuleConfig represents a single tenant rule in the configuration file.
type TenantRuleConfig struct {
	Filter string `yaml:"filter"`
	Tenant string `yaml:"tenant"`
}

// TenantRule is a parsed tenant rule with a compiled filter.
type TenantRule struct {
	Filter TagsFilter
	Tenant string
}

// TenantAttributor handles tenant attribution based on time series labels.
type TenantAttributor struct {
	rules         []TenantRule
	defaultTenant string
	logger        log.Logger

	// Metrics (only registered in verify mode)
	verifyMode            bool
	attributionMatches    prometheus.Counter
	attributionMismatches prometheus.Counter
}

// NewTenantAttributor creates a new TenantAttributor from a config file.
func NewTenantAttributor(
	configPath string,
	defaultTenant string,
	verifyMode bool,
	reg prometheus.Registerer,
	logger log.Logger,
) (*TenantAttributor, error) {
	ta := &TenantAttributor{
		defaultTenant: defaultTenant,
		logger:        logger,
		verifyMode:    verifyMode,
	}

	// Register metrics only in verify mode
	if verifyMode && reg != nil {
		ta.attributionMatches = promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Namespace: "thanos",
			Subsystem: "receive",
			Name:      "tenant_attribution_matches_total",
			Help:      "Total number of time series where attributed tenant matches HTTP header tenant.",
		})
		ta.attributionMismatches = promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Namespace: "thanos",
			Subsystem: "receive",
			Name:      "tenant_attribution_mismatches_total",
			Help:      "Total number of time series where attributed tenant differs from HTTP header tenant.",
		})
	}

	// Load and parse config file
	if configPath == "" {
		return nil, errors.New("tenant rules config path is required")
	}

	configData, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("reading tenant rules config: %w", err)
	}

	var ruleConfigs []TenantRuleConfig
	if err := yaml.UnmarshalStrict(configData, &ruleConfigs); err != nil {
		return nil, fmt.Errorf("parsing tenant rules config: %w", err)
	}

	// Parse and compile each rule
	for i, rc := range ruleConfigs {
		if rc.Filter == "" {
			return nil, fmt.Errorf("rule %d: filter is required", i)
		}
		if rc.Tenant == "" {
			return nil, fmt.Errorf("rule %d: tenant is required", i)
		}

		filterValues, err := ParseTagFilterValueMap(rc.Filter)
		if err != nil {
			return nil, fmt.Errorf("rule %d: parsing filter %q: %w", i, rc.Filter, err)
		}

		filter, err := NewTagsFilter(filterValues, Conjunction, TagsFilterOptions{})
		if err != nil {
			return nil, fmt.Errorf("rule %d: creating filter %q: %w", i, rc.Filter, err)
		}

		ta.rules = append(ta.rules, TenantRule{
			Filter: filter,
			Tenant: rc.Tenant,
		})

		level.Info(logger).Log("msg", "loaded tenant rule", "index", i, "filter", rc.Filter, "tenant", rc.Tenant)
	}

	level.Info(logger).Log("msg", "tenant attributor initialized", "rules", len(ta.rules), "default_tenant", defaultTenant, "verify_mode", verifyMode)
	return ta, nil
}

// GetTenantFromLabels returns the tenant for the given labels based on rules.
// First matching rule wins. Returns defaultTenant if no rule matches.
func (ta *TenantAttributor) GetTenantFromLabels(lbls labels.Labels) string {
	for _, rule := range ta.rules {
		if rule.Filter.MatchLabels(lbls) {
			return rule.Tenant
		}
	}
	return ta.defaultTenant
}

// RecordVerification records match/mismatch metrics for verification mode.
// httpTenant is the tenant from HTTP header (or default if no header).
func (ta *TenantAttributor) RecordVerification(attributedTenant, httpTenant string) {
	if !ta.verifyMode {
		return
	}
	if attributedTenant == httpTenant {
		if ta.attributionMatches != nil {
			ta.attributionMatches.Inc()
		}
	} else {
		if ta.attributionMismatches != nil {
			ta.attributionMismatches.Inc()
		}
	}
}

// IsVerifyMode returns true if the attributor is in verification mode.
func (ta *TenantAttributor) IsVerifyMode() bool {
	return ta.verifyMode
}

// Filter ported over from M3DB.

var (
	errInvalidFilterPattern = errors.New("invalid filter pattern defined")
)

// LogicalOp is a logical operator.
type LogicalOp string

// chainSegment is the part of the pattern that the chain represents.
type chainSegment int

const (
	// Conjunction is logical AND.
	Conjunction LogicalOp = "&&"
	// Disjunction is logical OR.
	Disjunction LogicalOp = "||"

	middle chainSegment = iota
	start
	end

	wildcardChar         = '*'
	negationChar         = '!'
	singleAnyChar        = '?'
	singleRangeStartChar = '['
	singleRangeEndChar   = ']'
	rangeChar            = '-'
	multiRangeStartChar  = '{'
	multiRangeEndChar    = '}'
	invalidNestedChars   = "?[{"

	// tagFilterListSeparator splits key:value pairs in a tag filter list.
	tagFilterListSeparator = " "
)

var (
	multiRangeSplit = []byte(",")

	// defaultFilterSeparator represents the default filter separator with no negation.
	defaultFilterSeparator = tagFilterSeparator{str: ":", negate: false}
	// databricksFilterSeparator is an alternative separator.
	databricksFilterSeparator = tagFilterSeparator{str: "#", negate: false}

	// validFilterSeparators represent a list of valid filter separators.
	// NB: the separators here are tried in order during parsing.
	validFilterSeparators = []tagFilterSeparator{
		databricksFilterSeparator,
		defaultFilterSeparator,
	}
)

type tagFilterSeparator struct {
	str    string
	negate bool
}

// FilterValue contains the filter pattern and a boolean flag indicating
// whether the filter should be negated.
type FilterValue struct {
	Pattern string
	Negate  bool
}

// TagFilterValueMapKey represents a key in the tag filter value map.
type TagFilterValueMapKey struct {
	Name    string
	Exclude bool
}

// TagFilterValueMap is a map containing mappings from tag names to filter values.
type TagFilterValueMap map[TagFilterValueMapKey]FilterValue

// ParseTagFilterValueMap parses the input string and creates a tag filter value map.
func ParseTagFilterValueMap(str string) (TagFilterValueMap, error) {
	trimmed := strings.TrimSpace(str)
	tagPairs := strings.Split(trimmed, tagFilterListSeparator)
	res := make(map[TagFilterValueMapKey]FilterValue, len(tagPairs))
	for _, p := range tagPairs {
		sanitized := strings.TrimSpace(p)
		if sanitized == "" {
			continue
		}
		parts, separator, err := parseTagFilter(sanitized)
		if err != nil {
			return nil, err
		}
		// NB: we do not allow duplicate tags at the moment.
		exclude := parts[0][0] == negationChar
		name := parts[0]
		if exclude {
			name = parts[0][1:]
		}
		key := TagFilterValueMapKey{Name: name, Exclude: exclude}
		_, exists := res[key]
		if exists {
			return nil, fmt.Errorf("invalid filter %s: duplicate tag %s found", str, parts[0])
		}
		if key.Exclude {
			// For exclude rules, it doesn't make sense to have a value filter pattern since we are simply checking for the absence of the tag.
			// To avoid confusion, we enforce that the value filter pattern is a wildcard.
			if parts[1] != string(wildcardChar) {
				return nil, fmt.Errorf("invalid filter %s: negation only supported for wildcard patterns", str)
			}
		}
		res[key] = FilterValue{Pattern: parts[1], Negate: separator.negate}
	}
	return res, nil
}

func parseTagFilter(str string) ([]string, tagFilterSeparator, error) {
	unknownSeparator := tagFilterSeparator{}
	parseByOneSeparator := func(separator tagFilterSeparator) ([]string, tagFilterSeparator, error) {
		items := strings.Split(str, separator.str)
		if len(items) == 2 {
			if items[0] == "" {
				return nil, unknownSeparator, fmt.Errorf("invalid filter %s: empty tag name", str)
			}
			if items[1] == "" {
				return nil, unknownSeparator, fmt.Errorf("invalid filter %s: empty filter pattern", str)
			}
			return items, separator, nil
		}
		return nil, unknownSeparator, fmt.Errorf("invalid filter %s: expecting tag pattern pairs", str)
	}
	var returnedErr error
	for _, separator := range validFilterSeparators {
		parts, _, err := parseByOneSeparator(separator)
		if err == nil {
			return parts, separator, nil
		}
		returnedErr = err
	}
	return nil, unknownSeparator, returnedErr
}

// Filter matches a string against certain conditions.
type Filter interface {
	fmt.Stringer
	Matches(val []byte) bool
}

// TagsFilter matches labels against certain conditions.
type TagsFilter interface {
	fmt.Stringer
	MatchLabels(lbls labels.Labels) bool
}

// TagsFilterOptions provide a set of tag filter options.
type TagsFilterOptions struct {
	// NameTagKey is the name of the name tag (not used in this simplified version).
	NameTagKey []byte
}

// tagFilter is a filter associated with a given tag.
type tagFilter struct {
	name        []byte
	valueFilter Filter
	exclude     bool
}

func (f *tagFilter) String() string {
	excludePrefix := ""
	if f.exclude {
		excludePrefix = string(negationChar)
	}
	return fmt.Sprintf("%s%s:%s", excludePrefix, string(f.name), f.valueFilter.String())
}

type tagFiltersByNameAsc []tagFilter

func (tn tagFiltersByNameAsc) Len() int           { return len(tn) }
func (tn tagFiltersByNameAsc) Swap(i, j int)      { tn[i], tn[j] = tn[j], tn[i] }
func (tn tagFiltersByNameAsc) Less(i, j int) bool { return bytes.Compare(tn[i].name, tn[j].name) < 0 }

// tagsFilter contains a list of tag filters.
type tagsFilter struct {
	tagFilters []tagFilter
	op         LogicalOp
	opts       TagsFilterOptions
}

// NewTagsFilter creates a new tags filter.
func NewTagsFilter(
	filters TagFilterValueMap,
	op LogicalOp,
	opts TagsFilterOptions,
) (TagsFilter, error) {
	tagFilters := make([]tagFilter, 0, len(filters))
	for entry, value := range filters {
		// We disallow OR support for exclude rules for simplicity.
		if entry.Exclude && op == Disjunction {
			return nil, fmt.Errorf("invalid filter %s: exclude not supported for disjunction", entry.Name)
		}
		valFilter, err := NewFilterFromFilterValue(value)
		if err != nil {
			return nil, err
		}
		tagFilters = append(tagFilters, tagFilter{
			name:        []byte(entry.Name),
			valueFilter: valFilter,
			exclude:     entry.Exclude,
		})
	}
	sort.Sort(tagFiltersByNameAsc(tagFilters))
	return &tagsFilter{
		tagFilters: tagFilters,
		op:         op,
		opts:       opts,
	}, nil
}

func (f *tagsFilter) String() string {
	separator := " " + string(f.op) + " "
	var buf bytes.Buffer
	numTagFilters := len(f.tagFilters)
	for i := 0; i < numTagFilters; i++ {
		buf.WriteString(f.tagFilters[i].String())
		if i < numTagFilters-1 {
			buf.WriteString(separator)
		}
	}
	return buf.String()
}

// MatchLabels checks if the labels match the filter.
func (f *tagsFilter) MatchLabels(lbls labels.Labels) bool {
	return f.matchLabelsSimple(lbls)
}

func (f *tagsFilter) matchLabelsSimple(lbls labels.Labels) bool {
	if len(f.tagFilters) == 0 {
		return true
	}

	for _, tf := range f.tagFilters {
		labelValue := lbls.Get(string(tf.name))
		hasLabel := labelValue != ""

		if tf.exclude {
			// For exclude rules, we want the tag to NOT exist
			if hasLabel {
				return false
			}
			continue
		}

		if !hasLabel {
			// Tag doesn't exist
			if f.op == Conjunction {
				return false
			}
			continue
		}

		match := tf.valueFilter.Matches([]byte(labelValue))
		if match && f.op == Disjunction {
			return true
		}
		if !match && f.op == Conjunction {
			return false
		}
	}

	return f.op != Disjunction
}

// NewFilterFromFilterValue creates a filter from the given filter value.
func NewFilterFromFilterValue(fv FilterValue) (Filter, error) {
	f, err := NewFilter([]byte(fv.Pattern))
	if err != nil {
		return nil, err
	}
	if !fv.Negate {
		return f, nil
	}
	return &negationFilter{filter: f}, nil
}

// NewFilter supports startsWith, endsWith, contains and a single wildcard
// along with negation and glob matching support.
func NewFilter(pattern []byte) (Filter, error) {
	if len(pattern) == 0 {
		return &equalityFilter{pattern: pattern}, nil
	}

	if pattern[0] != negationChar {
		return newWildcardFilter(pattern)
	}

	if len(pattern) == 1 {
		// Only negation symbol.
		return nil, errInvalidFilterPattern
	}

	filter, err := newWildcardFilter(pattern[1:])
	if err != nil {
		return nil, err
	}

	return &negationFilter{filter: filter}, nil
}

// newWildcardFilter creates a filter that segments the pattern based
// on wildcards, creating a rangeFilter for each segment.
func newWildcardFilter(pattern []byte) (Filter, error) {
	wIdx := bytes.IndexRune(pattern, wildcardChar)

	if wIdx == -1 {
		// No wildcards.
		return newRangeFilter(pattern, false, middle)
	}

	if len(pattern) == 1 {
		// Whole thing is wildcard.
		return &allowFilter{}, nil
	}

	if wIdx == len(pattern)-1 {
		// Single wildcard at end.
		return newRangeFilter(pattern[:len(pattern)-1], false, start)
	}

	secondWIdx := bytes.IndexRune(pattern[wIdx+1:], wildcardChar)
	if secondWIdx == -1 {
		if wIdx == 0 {
			// Single wildcard at start.
			return newRangeFilter(pattern[1:], true, end)
		}

		// Single wildcard in the middle.
		first, err := newRangeFilter(pattern[:wIdx], false, start)
		if err != nil {
			return nil, err
		}

		second, err := newRangeFilter(pattern[wIdx+1:], true, end)
		if err != nil {
			return nil, err
		}

		return &multiFilter{filters: []Filter{first, second}, op: Conjunction}, nil
	}

	if wIdx == 0 && secondWIdx == len(pattern)-2 && len(pattern) > 2 {
		// Wildcard at beginning and end.
		return newContainsFilter(pattern[1 : len(pattern)-1])
	}

	return nil, errInvalidFilterPattern
}

// newRangeFilter creates a filter that checks for ranges (? or [] or {}) and segments
// the pattern into a multiple chain filters based on ranges found.
func newRangeFilter(pattern []byte, backwards bool, seg chainSegment) (Filter, error) {
	var filters []chainFilter
	eqIdx := -1
	for i := 0; i < len(pattern); i++ {
		if pattern[i] == singleRangeStartChar {
			// Found '[', create an equality filter for the chars before this one if any
			if eqIdx != -1 {
				filters = append(filters, &equalityChainFilter{pattern: pattern[eqIdx:i], backwards: backwards})
				eqIdx = -1
			}

			endIdx := bytes.IndexRune(pattern[i+1:], singleRangeEndChar)
			if endIdx == -1 {
				return nil, errInvalidFilterPattern
			}

			f, err := newSingleRangeFilter(pattern[i+1:i+1+endIdx], backwards)
			if err != nil {
				return nil, errInvalidFilterPattern
			}

			filters = append(filters, f)
			i += endIdx + 1
		} else if pattern[i] == multiRangeStartChar {
			// Found '{', create equality filter for chars before this if any
			if eqIdx != -1 {
				filters = append(filters, &equalityChainFilter{pattern: pattern[eqIdx:i], backwards: backwards})
				eqIdx = -1
			}

			endIdx := bytes.IndexRune(pattern[i+1:], multiRangeEndChar)
			if endIdx == -1 {
				return nil, errInvalidFilterPattern
			}

			f, err := newMultiCharSequenceFilter(pattern[i+1:i+1+endIdx], backwards)
			if err != nil {
				return nil, errInvalidFilterPattern
			}

			filters = append(filters, f)
			i += endIdx + 1
		} else if pattern[i] == singleAnyChar {
			// Found '?', create equality filter for chars before this one if any
			if eqIdx != -1 {
				filters = append(filters, &equalityChainFilter{pattern: pattern[eqIdx:i], backwards: backwards})
				eqIdx = -1
			}

			filters = append(filters, &singleAnyCharFilter{backwards: backwards})
		} else if eqIdx == -1 {
			// Normal char, need to mark index to start next equality filter.
			eqIdx = i
		}
	}

	if eqIdx != -1 {
		filters = append(filters, &equalityChainFilter{pattern: pattern[eqIdx:], backwards: backwards})
	}

	return &multiChainFilter{filters: filters, seg: seg, backwards: backwards}, nil
}

// allowFilter is a filter that allows all.
type allowFilter struct{}

func (f *allowFilter) String() string          { return "All" }
func (f *allowFilter) Matches(val []byte) bool { return true }

// equalityFilter is a filter that matches exact values.
type equalityFilter struct {
	pattern []byte
}

func (f *equalityFilter) String() string {
	return "Equals(\"" + string(f.pattern) + "\")"
}

func (f *equalityFilter) Matches(val []byte) bool {
	return bytes.Equal(f.pattern, val)
}

// containsFilter is a filter that performs contains matches.
type containsFilter struct {
	pattern []byte
}

func newContainsFilter(pattern []byte) (Filter, error) {
	if bytes.ContainsAny(pattern, invalidNestedChars) {
		return nil, errInvalidFilterPattern
	}
	return &containsFilter{pattern: pattern}, nil
}

func (f *containsFilter) String() string {
	return "Contains(\"" + string(f.pattern) + "\")"
}

func (f *containsFilter) Matches(val []byte) bool {
	return bytes.Contains(val, f.pattern)
}

// negationFilter is a filter that matches the opposite of the provided filter.
type negationFilter struct {
	filter Filter
}

func (f *negationFilter) String() string {
	return "Not(" + f.filter.String() + ")"
}

func (f *negationFilter) Matches(val []byte) bool {
	return !f.filter.Matches(val)
}

// multiFilter chains multiple filters together with a logicalOp.
type multiFilter struct {
	filters []Filter
	op      LogicalOp
}

func (f *multiFilter) String() string {
	separator := " " + string(f.op) + " "
	var buf bytes.Buffer
	numFilters := len(f.filters)
	for i := 0; i < numFilters; i++ {
		buf.WriteString(f.filters[i].String())
		if i < numFilters-1 {
			buf.WriteString(separator)
		}
	}
	return buf.String()
}

func (f *multiFilter) Matches(val []byte) bool {
	if len(f.filters) == 0 {
		return true
	}

	for _, filter := range f.filters {
		match := filter.Matches(val)
		if f.op == Conjunction && !match {
			return false
		}

		if f.op == Disjunction && match {
			return true
		}
	}

	return f.op == Conjunction
}

// chainFilter matches an input string against certain conditions
// while returning the unmatched part of the input if there is a match.
type chainFilter interface {
	fmt.Stringer
	matches(val []byte) ([]byte, bool)
}

// equalityChainFilter is a filter that performs equality string matches
// from either the front or back of the string.
type equalityChainFilter struct {
	pattern   []byte
	backwards bool
}

func (f *equalityChainFilter) String() string {
	return "Equals(\"" + string(f.pattern) + "\")"
}

func (f *equalityChainFilter) matches(val []byte) ([]byte, bool) {
	if f.backwards && bytes.HasSuffix(val, f.pattern) {
		return val[:len(val)-len(f.pattern)], true
	}

	if !f.backwards && bytes.HasPrefix(val, f.pattern) {
		return val[len(f.pattern):], true
	}

	return nil, false
}

// singleAnyCharFilter is a filter that allows any one char.
type singleAnyCharFilter struct {
	backwards bool
}

func (f *singleAnyCharFilter) String() string { return "AnyChar" }

func (f *singleAnyCharFilter) matches(val []byte) ([]byte, bool) {
	if len(val) == 0 {
		return nil, false
	}

	if f.backwards {
		return val[:len(val)-1], true
	}

	return val[1:], true
}

// newSingleRangeFilter creates a filter that performs range matching
// on a single char.
func newSingleRangeFilter(pattern []byte, backwards bool) (chainFilter, error) {
	if len(pattern) == 0 {
		return nil, errInvalidFilterPattern
	}

	negate := false
	if pattern[0] == negationChar {
		negate = true
		pattern = pattern[1:]
	}

	if len(pattern) > 1 && pattern[1] == rangeChar {
		// If there is a '-' char at position 2, look for repeated instances of a-z.
		if len(pattern)%3 != 0 {
			return nil, errInvalidFilterPattern
		}

		patterns := make([][]byte, 0, len(pattern)%3)
		for i := 0; i < len(pattern); i += 3 {
			if pattern[i+1] != rangeChar || pattern[i] > pattern[i+2] {
				return nil, errInvalidFilterPattern
			}
			patterns = append(patterns, pattern[i:i+3])
		}

		return &singleRangeFilter{patterns: patterns, backwards: backwards, negate: negate}, nil
	}

	return &singleCharSetFilter{pattern: pattern, backwards: backwards, negate: negate}, nil
}

// singleRangeFilter is a filter that performs a single character match against
// a range of chars given in a range format eg. [a-z].
type singleRangeFilter struct {
	patterns  [][]byte
	backwards bool
	negate    bool
}

func (f *singleRangeFilter) String() string {
	var negatePrefix, negateSuffix string
	if f.negate {
		negatePrefix = "Not("
		negateSuffix = ")"
	}
	return negatePrefix + "Range(\"" + string(bytes.Join(f.patterns, []byte(fmt.Sprintf(" %s ", Disjunction)))) + "\")" + negateSuffix
}

func (f *singleRangeFilter) matches(val []byte) ([]byte, bool) {
	if len(val) == 0 {
		return nil, false
	}

	match := false
	idx := 0
	remainder := val[1:]
	if f.backwards {
		idx = len(val) - 1
		remainder = val[:idx]
	}

	for _, pattern := range f.patterns {
		if val[idx] >= pattern[0] && val[idx] <= pattern[2] {
			match = true
			break
		}
	}

	if f.negate {
		match = !match
	}

	return remainder, match
}

// singleCharSetFilter is a filter that performs a single character match against
// a set of chars given explicitly eg. [abcdefg].
type singleCharSetFilter struct {
	pattern   []byte
	backwards bool
	negate    bool
}

func (f *singleCharSetFilter) String() string {
	var negatePrefix, negateSuffix string
	if f.negate {
		negatePrefix = "Not("
		negateSuffix = ")"
	}
	return negatePrefix + "Range(\"" + string(f.pattern) + "\")" + negateSuffix
}

func (f *singleCharSetFilter) matches(val []byte) ([]byte, bool) {
	if len(val) == 0 {
		return nil, false
	}

	match := false
	for i := 0; i < len(f.pattern); i++ {
		if f.backwards && val[len(val)-1] == f.pattern[i] {
			match = true
			break
		}

		if !f.backwards && val[0] == f.pattern[i] {
			match = true
			break
		}
	}

	if f.negate {
		match = !match
	}

	if f.backwards {
		return val[:len(val)-1], match
	}

	return val[1:], match
}

// multiCharSequenceFilter is a filter that performs matches against multiple sets of chars
// eg. {abc,defg}.
type multiCharSequenceFilter struct {
	patterns  [][]byte
	backwards bool
}

func newMultiCharSequenceFilter(patterns []byte, backwards bool) (chainFilter, error) {
	if len(patterns) == 0 {
		return nil, errInvalidFilterPattern
	}

	return &multiCharSequenceFilter{
		patterns:  bytes.Split(patterns, multiRangeSplit),
		backwards: backwards,
	}, nil
}

func (f *multiCharSequenceFilter) String() string {
	return "Range(\"" + string(bytes.Join(f.patterns, multiRangeSplit)) + "\")"
}

func (f *multiCharSequenceFilter) matches(val []byte) ([]byte, bool) {
	if len(val) == 0 {
		return nil, false
	}

	for _, pattern := range f.patterns {
		if f.backwards && bytes.HasSuffix(val, pattern) {
			return val[:len(val)-len(pattern)], true
		}

		if !f.backwards && bytes.HasPrefix(val, pattern) {
			return val[len(pattern):], true
		}
	}

	return nil, false
}

// multiChainFilter chains multiple chainFilters together with &&.
type multiChainFilter struct {
	filters   []chainFilter
	seg       chainSegment
	backwards bool
}

func (f *multiChainFilter) String() string {
	separator := " then "
	var buf bytes.Buffer
	switch f.seg {
	case start:
		buf.WriteString("StartsWith(")
	case end:
		buf.WriteString("EndsWith(")
	}

	numFilters := len(f.filters)
	for i := 0; i < numFilters; i++ {
		buf.WriteString(f.filters[i].String())
		if i < numFilters-1 {
			buf.WriteString(separator)
		}
	}

	switch f.seg {
	case start, end:
		buf.WriteString(")")
	}

	return buf.String()
}

func (f *multiChainFilter) Matches(val []byte) bool {
	if len(f.filters) == 0 {
		return true
	}

	var match bool

	if f.backwards {
		for i := len(f.filters) - 1; i >= 0; i-- {
			val, match = f.filters[i].matches(val)
			if !match {
				return false
			}
		}
	} else {
		for i := 0; i < len(f.filters); i++ {
			val, match = f.filters[i].matches(val)
			if !match {
				return false
			}
		}
	}

	if f.seg == middle && len(val) != 0 {
		// chain was middle segment and some value was left over at end of chain.
		return false
	}

	return true
}
