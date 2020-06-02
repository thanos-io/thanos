module github.com/thanos-io/thanos

require (
	cloud.google.com/go v0.49.0
	cloud.google.com/go/storage v1.3.0
	github.com/Azure/azure-storage-blob-go v0.8.0
	github.com/NYTimes/gziphandler v1.1.1
	github.com/alecthomas/units v0.0.0-20190924025748-f65c72e2690d
	github.com/aliyun/aliyun-oss-go-sdk v2.0.4+incompatible
	github.com/armon/go-metrics v0.3.0
	github.com/bradfitz/gomemcache v0.0.0-20190913173617-a41fca850d0b
	github.com/cespare/xxhash v1.1.0
	github.com/cortexproject/cortex v0.6.1-0.20200228110116-92ab6cbe0995
	github.com/davecgh/go-spew v1.1.1
	github.com/facette/natsort v0.0.0-20181210072756-2cd4dd1e2dcb
	github.com/fatih/structtag v1.1.0
	github.com/fortytw2/leaktest v1.3.0
	github.com/fsnotify/fsnotify v1.4.7
	github.com/go-kit/kit v0.10.0
	github.com/go-openapi/strfmt v0.19.4
	github.com/gogo/protobuf v1.3.1
	github.com/golang/groupcache v0.0.0-20191027212112-611e8accdfc9
	github.com/golang/snappy v0.0.1
	github.com/googleapis/gax-go v2.0.2+incompatible
	github.com/gophercloud/gophercloud v0.8.0
	github.com/grpc-ecosystem/go-grpc-middleware v1.1.0
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0
	github.com/hashicorp/golang-lru v0.5.3
	github.com/leanovate/gopter v0.2.4
	github.com/lightstep/lightstep-tracer-go v0.18.1
	github.com/lovoo/gcloud-opentracing v0.3.0
	github.com/miekg/dns v1.1.27
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
	github.com/prometheus/client_golang v1.5.1
	github.com/prometheus/client_model v0.2.0
	github.com/prometheus/common v0.9.1
	github.com/prometheus/prometheus v1.8.2-0.20200407102557-cd73b3d33e06
	github.com/sercand/kuberesolver v2.4.0+incompatible // indirect
	github.com/uber/jaeger-client-go v2.20.1+incompatible
	github.com/uber/jaeger-lib v2.2.0+incompatible
	go.elastic.co/apm v1.5.0
	go.elastic.co/apm/module/apmot v1.5.0
	go.uber.org/automaxprocs v1.2.0
	golang.org/x/crypto v0.0.0-20191206172530-e9b2fee46413
	golang.org/x/oauth2 v0.0.0-20200107190931-bf48bf16ab8d
	golang.org/x/sync v0.0.0-20190911185100-cd5d95a43a6e
	golang.org/x/text v0.3.2
	golang.org/x/tools v0.0.0-20200306191617-51e69f71924f // indirect
	google.golang.org/api v0.20.0
	google.golang.org/genproto v0.0.0-20200305110556-506484158171
	google.golang.org/grpc v1.27.1
	gopkg.in/alecthomas/kingpin.v2 v2.2.6
	gopkg.in/fsnotify.v1 v1.4.7
	gopkg.in/yaml.v2 v2.2.8
	gopkg.in/yaml.v3 v3.0.0-20200121175148-a6ecf24a6d71
)

// We want to replace the client-go version with a specific commit hash,
// so that we don't get errors about being incompatible with the Go proxies.
// See https://github.com/thanos-io/thanos/issues/1415
replace (
	// Mitigation for: https://github.com/Azure/go-autorest/issues/414
	github.com/Azure/go-autorest => github.com/Azure/go-autorest v12.3.0+incompatible
	// Make sure Cortex is not forcing us to some other Prometheus version.
	github.com/prometheus/prometheus => github.com/prometheus/prometheus v1.8.2-0.20200420120812-fc3fb3265a0f // @fc3fb3265a0f0061d07d0bf5fb4103e2144565b2 (master around v2.17.2).
	k8s.io/klog => github.com/simonpasquier/klog-gokit v0.1.0
)

go 1.13
