// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package migration

import (
	"fmt"

	"go.opentelemetry.io/otel/attribute"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
)

// ForceTracingAttributeKey is used to signalize a span should be traced.
const ForceTracingAttributeKey = "thanos.force_tracing"

type samplerWithOverride struct {
	baseSampler tracesdk.Sampler
	overrideKey attribute.Key
}

// SamplerWithOverride creates a new sampler with the capability to override
// the sampling decision, if the span includes an attribute with the specified key.
// Otherwise the sampler delegates the decision to the wrapped base sampler. This
// is primarily used to enable forced tracing in Thanos components.
// Implements go.opentelemetry.io/otel/sdk/trace.Sampler interface.
func SamplerWithOverride(baseSampler tracesdk.Sampler, overrideKey attribute.Key) tracesdk.Sampler {
	return samplerWithOverride{
		baseSampler,
		overrideKey,
	}
}

func (s samplerWithOverride) ShouldSample(p tracesdk.SamplingParameters) tracesdk.SamplingResult {
	for _, attr := range p.Attributes {
		if attr.Key == s.overrideKey {
			return tracesdk.SamplingResult{
				Decision: tracesdk.RecordAndSample,
			}
		}
	}

	return s.baseSampler.ShouldSample(p)
}

func (s samplerWithOverride) Description() string {
	return fmt.Sprintf("SamplerWithOverride{%s}", string(s.overrideKey))
}
