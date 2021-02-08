package runutil

import (
	"context"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"gopkg.in/yaml.v2"
)

type BackoffType string

const (
	EXPONENTIAL BackoffType = "EXPONENTIAL"
	SEQUENCE    BackoffType = "SEQUENCE"
)

type Config struct {
	Type   BackoffType `yaml:"type"`
	Config interface{} `yaml:"config"`
}

func NewBackoff(ctx context.Context, logger log.Logger, confContentYaml []byte) (Backoff, error) {
	level.Info(logger).Log("msg", "loading backoff configuration")
	backoffConf := &Config{}
	if err := yaml.UnmarshalStrict(confContentYaml, backoffConf); err != nil {
		return nil, errors.Wrap(err, "parsing config YAML file")
	}

	config, err := yaml.Marshal(backoffConf.Config)
	if err != nil {
		return nil, errors.Wrap(err, "marshal content of backoff configuration")
	}

	var backoff Backoff
	switch strings.ToUpper(string(backoffConf.Type)) {
	case string(EXPONENTIAL):
		backoff, err = NewExponentialBackoffFromYAML(ctx, config)
	case string(SEQUENCE):
		backoff, err = NewSequenceBackoffFromYAML(ctx, config)
	default:
		return nil, errors.Errorf("backoff with type %s is not supported", backoffConf.Type)
	}
	if err != nil {
		return nil, errors.Wrapf(err, "create %s backoff", backoffConf.Type)
	}
	return backoff, nil
}

type ExponentialBackoffConfig struct {
	TimeUnit     string        `yaml:"time_unit"`
	MaxAttempts  int           `yaml:"max_attempts"`
	Jitter       float64       `yaml:"jitter"`
	MaxTimeTotal time.Duration `yaml:"max_time_total"`
}

func (cfg *ExponentialBackoffConfig) Defaults() {
	cfg.TimeUnit = "seconds"
	cfg.MaxAttempts = 10
	cfg.Jitter = 0.0
}

type SequenceBackoffConfig struct {
	TimeUnit     string        `yaml:"time_unit"`
	Elements     []int         `yaml:"elements"`
	MaxTimeTotal time.Duration `yaml:"max_time_total"`
}

func (cfg *SequenceBackoffConfig) Defaults() {
	cfg.TimeUnit = "seconds"
	cfg.Elements = []int{0, 1, 3, 10, 30, 60, 300}
}

type Backoff interface {
	Duration(n int) time.Duration
	Max() int
	Done() <-chan struct{}
}

type backoff struct {
	timeUnit time.Duration
	random   *rand.Rand
	done     chan struct{}
}

type randSource struct {
	mtx sync.Mutex
	src rand.Source
}

func (r *randSource) Int63() int64 {
	r.mtx.Lock()
	i := r.src.Int63()
	r.mtx.Unlock()
	return i
}

func (r *randSource) Seed(seed int64) {
	r.mtx.Lock()
	r.src.Seed(seed)
	r.mtx.Unlock()
}

type sequenceBackoff struct {
	Backoff
	*backoff
	elements []int
}

func newBackoff(ctx context.Context, timeUnit, maxTimeTotal time.Duration) *backoff {
	b := &backoff{
		timeUnit: timeUnit,
		random:   rand.New(&randSource{src: rand.NewSource(time.Now().UTC().UnixNano())}),
	}
	if maxTimeTotal > 0 {
		b.done = make(chan struct{})
		go func() {
			t := time.NewTimer(maxTimeTotal)
			defer func() {
				t.Stop()
				close(b.done)
			}()
			select {
			case <-ctx.Done():
			case <-t.C:
			}
		}()
	}

	return b
}

func NewSequenceBackoff(ctx context.Context, timeUnit string, maxTimeTotal time.Duration, elements ...int) (Backoff, error) {
	d, err := unitToDuration(timeUnit)
	if err != nil {
		return nil, err
	}
	pb := &sequenceBackoff{
		backoff:  newBackoff(ctx, d, maxTimeTotal),
		elements: elements,
	}

	return pb, nil
}

func NewSequenceBackoffWithConfig(ctx context.Context, conf SequenceBackoffConfig) (Backoff, error) {
	return NewSequenceBackoff(ctx, conf.TimeUnit, conf.MaxTimeTotal, conf.Elements...)
}

func NewSequenceBackoffFromYAML(ctx context.Context, yamlConf []byte) (Backoff, error) {
	config := &SequenceBackoffConfig{}
	config.Defaults()

	if err := yaml.UnmarshalStrict(yamlConf, config); err != nil {
		return nil, errors.Wrap(err, "parsing config YAML file")
	}

	return NewSequenceBackoffWithConfig(ctx, *config)
}

// Duration returns the time duration of the nth wait cycle in a
// backoff policy. This is p.elements[n], randomized to avoid thundering
// herds.
func (b *sequenceBackoff) Duration(n int) time.Duration {
	if len(b.elements) == 0 {
		return 0
	}
	if n >= len(b.elements) {
		n = len(b.elements) - 1
	}
	if n < 0 {
		n = 0
	}

	return time.Duration(b.jitter(b.elements[n])) * b.timeUnit
}

func (b *sequenceBackoff) Max() int {
	return len(b.elements) - 1
}

func (b *sequenceBackoff) Done() <-chan struct{} {
	return b.done
}

// jitter returns a random integer uniformly distributed in the range
// [0.5 * n .. 1.5 * n]
func (b *sequenceBackoff) jitter(n int) int {
	if n == 0 {
		return 0
	}

	return n/2 + b.random.Intn(n)
}

type exponentialBackoff struct {
	Backoff
	*backoff
	maxAttempts int
	jitter      float64
}

func NewExponentialBackoff(ctx context.Context, timeUnit string, maxTimeTotal time.Duration, maxAttempts int) (Backoff, error) {
	d, err := unitToDuration(timeUnit)
	if err != nil {
		return nil, err
	}
	return &exponentialBackoff{
		backoff:     newBackoff(ctx, d, maxTimeTotal),
		maxAttempts: maxAttempts,
	}, nil
}

func NewExponentialBackoffWithConfig(ctx context.Context, conf ExponentialBackoffConfig) (Backoff, error) {
	return NewExponentialBackoff(ctx, conf.TimeUnit, conf.MaxTimeTotal, conf.MaxAttempts)
}

func NewExponentialBackoffFromYAML(ctx context.Context, yamlConf []byte) (Backoff, error) {
	config := &ExponentialBackoffConfig{}
	config.Defaults()

	if err := yaml.UnmarshalStrict(yamlConf, config); err != nil {
		return nil, errors.Wrap(err, "parsing config YAML file")
	}

	return NewExponentialBackoffWithConfig(ctx, *config)
}

func (b *exponentialBackoff) Duration(n int) time.Duration {
	d := b.timeUnit * time.Duration(1<<uint(n))
	d -= time.Duration(b.random.Float64() * float64(d) * b.jitter)

	return d
}

func (b *exponentialBackoff) Max() int {
	return b.maxAttempts
}

func (b *exponentialBackoff) Done() <-chan struct{} {
	return b.done
}

func unitToDuration(unit string) (time.Duration, error) {
	var d time.Duration
	u := strings.ToLower(unit)
	switch u {
	case "ns", "nano", "nanosecond", "nanoseconds":
		d = time.Nanosecond
		break
	case "us", "µs", "micro", "microsecond", "microseconds":
		d = time.Microsecond
		break
	case "ms", "milli", "millisecond", "milliseconds":
		d = time.Millisecond
		break
	case "", "s", "sec", "second", "seconds":
		d = time.Second
		break
	case "m", "min", "minute", "minutes":
		d = time.Minute
		break
	case "h", "hour", "hours":
		d = time.Hour
		break
	default:
		return 0, errors.New("invalid unit")
	}

	return d, nil
}
