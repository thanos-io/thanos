// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package component

// Component is a generic component interface.
type Component interface {
	String() string
}

// StoreAPI is a component that implements Thanos' gRPC StoreAPI.
type StoreAPI interface {
	implementsStoreAPI()
	String() string
}

// Source is a Thanos component that produce blocks of metrics.
type Source interface {
	producesBlocks()
	String() string
}

// SourceStoreAPI is a component that implements Thanos' gRPC StoreAPI
// and produce blocks of metrics.
type SourceStoreAPI interface {
	implementsStoreAPI()
	producesBlocks()
	String() string
}

type component struct {
	name string
}

func (c component) String() string { return c.name }

type storeAPI struct {
	component
}

func (storeAPI) implementsStoreAPI() {}

type source struct {
	component
}

func (source) producesBlocks() {}

type sourceStoreAPI struct {
	component
	source
	storeAPI
}

func FromString(storeType string) StoreAPI {
	switch storeType {
	case "query":
		return Query
	case "rule":
		return Rule
	case "sidecar":
		return Sidecar
	case "store":
		return Store
	case "receive":
		return Receive
	case "debug":
		return Debug
	default:
		return UnknownStoreAPI
	}
}

var (
	Bucket          = source{component: component{name: "bucket"}}
	Cleanup         = source{component: component{name: "cleanup"}}
	Mark            = source{component: component{name: "mark"}}
	Upload          = source{component: component{name: "upload"}}
	Rewrite         = source{component: component{name: "rewrite"}}
	Retention       = source{component: component{name: "retention"}}
	Compact         = source{component: component{name: "compact"}}
	Downsample      = source{component: component{name: "downsample"}}
	Replicate       = source{component: component{name: "replicate"}}
	QueryFrontend   = source{component: component{name: "query-frontend"}}
	Debug           = sourceStoreAPI{component: component{name: "debug"}}
	Receive         = sourceStoreAPI{component: component{name: "receive"}}
	Rule            = sourceStoreAPI{component: component{name: "rule"}}
	Sidecar         = sourceStoreAPI{component: component{name: "sidecar"}}
	Store           = storeAPI{component: component{name: "store"}}
	UnknownStoreAPI = storeAPI{component: component{name: "unknown-store-api"}}
	Query           = storeAPI{component: component{name: "query"}}

	All = []Component{
		Bucket,
		Cleanup,
		Mark,
		Upload,
		Rewrite,
		Retention,
		Compact,
		Downsample,
		Replicate,
		QueryFrontend,
		Debug,
		Receive,
		Rule,
		Sidecar,
		Store,
		UnknownStoreAPI,
		Query,
	}
)
