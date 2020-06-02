module github.com/thanos-io/thanos

require (
	cloud.google.com/go v0.56.0
	cloud.google.com/go/storage v1.6.0
	github.com/Azure/azure-storage-blob-go v0.8.0
	github.com/NYTimes/gziphandler v1.1.1
	github.com/alecthomas/units v0.0.0-20190924025748-f65c72e2690d
	github.com/aliyun/aliyun-oss-go-sdk v2.0.4+incompatible
	github.com/armon/go-metrics v0.3.3
	github.com/bradfitz/gomemcache v0.0.0-20190913173617-a41fca850d0b
	github.com/cespare/xxhash v1.1.0
	github.com/cortexproject/cortex v0.6.1-0.20200228110116-92ab6cbe0995
	github.com/davecgh/go-spew v1.1.1
	github.com/facette/natsort v0.0.0-20181210072756-2cd4dd1e2dcb
	github.com/fatih/structtag v1.1.0
	github.com/fortytw2/leaktest v1.3.0
	github.com/fsnotify/fsnotify v1.4.7
	github.com/go-kit/kit v0.10.0
	github.com/go-openapi/strfmt v0.19.5
	github.com/gogo/protobuf v1.3.1
	github.com/gogo/status v1.0.3
	github.com/golang/groupcache v0.0.0-20200121045136-8c9f03a8e57e
	github.com/golang/snappy v0.0.1
	github.com/googleapis/gax-go v2.0.2+incompatible
	github.com/gophercloud/gophercloud v0.10.0
	github.com/grpc-ecosystem/go-grpc-middleware v1.1.0
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0
	github.com/hashicorp/golang-lru v0.5.4
	github.com/leanovate/gopter v0.2.4
	github.com/lightstep/lightstep-tracer-go v0.18.1
	github.com/lovoo/gcloud-opentracing v0.3.0
	github.com/miekg/dns v1.1.29
	github.com/minio/minio-go/v6 v6.0.56
	github.com/mozillazg/go-cos v0.13.0
	github.com/mwitkow/go-conntrack v0.0.0-20190716064945-2f068394615f
	github.com/oklog/run v1.1.0
	github.com/oklog/ulid v1.3.1
	github.com/olekukonko/tablewriter v0.0.2
	github.com/opentracing/basictracer-go v1.0.0
	github.com/opentracing/opentracing-go v1.1.1-0.20200124165624-2876d2018785
	github.com/pkg/errors v0.9.1
	github.com/pmezard/go-difflib v1.0.0
	github.com/prometheus/alertmanager v0.20.0
	github.com/prometheus/client_golang v1.6.0
	github.com/prometheus/client_model v0.2.0
	github.com/prometheus/common v0.10.0
	github.com/prometheus/prometheus v1.8.2-0.20200522113006-f4dd45609a05
	github.com/sercand/kuberesolver v2.4.0+incompatible // indirect
	github.com/uber/jaeger-client-go v2.23.0+incompatible
	github.com/uber/jaeger-lib v2.2.0+incompatible
	go.elastic.co/apm v1.5.0
	go.elastic.co/apm/module/apmot v1.5.0
	go.uber.org/automaxprocs v1.2.0
	golang.org/x/crypto v0.0.0-20200422194213-44a606286825
	golang.org/x/oauth2 v0.0.0-20200107190931-bf48bf16ab8d
	golang.org/x/sync v0.0.0-20200317015054-43a5402ce75a
	golang.org/x/text v0.3.2
	google.golang.org/api v0.22.0
	google.golang.org/genproto v0.0.0-20200420144010-e5e8543f8aeb
	google.golang.org/grpc v1.29.0
	gopkg.in/alecthomas/kingpin.v2 v2.2.6
	gopkg.in/fsnotify.v1 v1.4.7
	gopkg.in/yaml.v2 v2.2.8
	gopkg.in/yaml.v3 v3.0.0-20200504163728-5308cda29e3d
)

// We want to replace the client-go version with a specific commit hash,
// so that we don't get errors about being incompatible with the Go proxies.
// See https://github.com/thanos-io/thanos/issues/1415
replace (
	// Mitigation for: https://github.com/Azure/go-autorest/issues/414
	github.com/Azure/go-autorest => github.com/Azure/go-autorest v12.3.0+incompatible
	// Make sure Cortex is not forcing us to some other Prometheus version.
	github.com/prometheus/prometheus => github.com/prometheus/prometheus v1.8.2-0.20200522113006-f4dd45609a05 // @f4dd45609a05e8f582cdcd8ef369004d1f9e3c02 (current master after v2.18.0).
	k8s.io/api => k8s.io/api v0.17.5
	k8s.io/apimachinery => k8s.io/apimachinery v0.17.5
	k8s.io/client-go => k8s.io/client-go v0.17.5
	k8s.io/klog => k8s.io/klog v0.3.1
	k8s.io/kube-openapi => k8s.io/kube-openapi v0.0.0-20190228160746-b3a7cee44a30
)

go 1.13
