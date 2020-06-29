module github.com/thanos-io/thanos

require (
	cloud.google.com/go v0.59.0
	cloud.google.com/go/storage v1.10.0
	github.com/Azure/azure-sdk-for-go v43.3.0+incompatible // indirect
	github.com/Azure/azure-storage-blob-go v0.10.0
	github.com/Azure/go-autorest/autorest v0.11.0 // indirect
	github.com/Azure/go-autorest/autorest/to v0.4.0 // indirect
	github.com/Azure/go-autorest/autorest/validation v0.3.0 // indirect
	github.com/NYTimes/gziphandler v1.1.1
	github.com/alecthomas/units v0.0.0-20190924025748-f65c72e2690d
	github.com/aliyun/aliyun-oss-go-sdk v2.1.2+incompatible
	github.com/armon/go-metrics v0.3.3
	github.com/aws/aws-sdk-go v1.32.11 // indirect
	github.com/blang/semver v3.5.1+incompatible // indirect
	github.com/bradfitz/gomemcache v0.0.0-20190913173617-a41fca850d0b
	github.com/cespare/xxhash v1.1.0
	github.com/chromedp/cdproto v0.0.0-20200424080200-0de008e41fa0
	github.com/chromedp/chromedp v0.5.3
	github.com/cortexproject/cortex v1.1.0
	github.com/davecgh/go-spew v1.1.1
	github.com/elastic/go-sysinfo v1.3.0 // indirect
	github.com/facette/natsort v0.0.0-20181210072756-2cd4dd1e2dcb
	github.com/fatih/structtag v1.2.0
	github.com/fortytw2/leaktest v1.3.0
	github.com/fsnotify/fsnotify v1.4.9
	github.com/go-kit/kit v0.10.0
	github.com/go-openapi/runtime v0.19.19 // indirect
	github.com/go-openapi/strfmt v0.19.5
	github.com/gogo/googleapis v1.4.0 // indirect
	github.com/gogo/protobuf v1.3.1
	github.com/gogo/status v1.1.0
	github.com/golang/groupcache v0.0.0-20200121045136-8c9f03a8e57e
	github.com/golang/snappy v0.0.1
	github.com/googleapis/gax-go v2.0.2+incompatible
	github.com/gophercloud/gophercloud v0.12.0
	github.com/gorilla/mux v1.7.4 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.2.0
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0
	github.com/hashicorp/consul/api v1.5.0 // indirect
	github.com/hashicorp/go-hclog v0.14.1 // indirect
	github.com/hashicorp/golang-lru v0.5.4
	github.com/hashicorp/serf v0.9.2 // indirect
	github.com/klauspost/cpuid v1.3.0 // indirect
	github.com/leanovate/gopter v0.2.4
	github.com/lightstep/lightstep-tracer-common/golang/gogo v0.0.0-20200310182322-adf4263e074b // indirect
	github.com/lightstep/lightstep-tracer-go v0.20.0
	github.com/lovoo/gcloud-opentracing v0.3.0
	github.com/mattn/go-colorable v0.1.7 // indirect
	github.com/mattn/go-ieproxy v0.0.1 // indirect
	github.com/mattn/go-runewidth v0.0.9 // indirect
	github.com/miekg/dns v1.1.29
	github.com/minio/minio-go/v6 v6.0.57
	github.com/mozillazg/go-cos v0.13.0
	github.com/mwitkow/go-conntrack v0.0.0-20190716064945-2f068394615f
	github.com/oklog/run v1.1.0
	github.com/oklog/ulid v1.3.1
	github.com/olekukonko/tablewriter v0.0.4
	github.com/opentracing-contrib/go-grpc v0.0.0-20191001143057-db30781987df // indirect
	github.com/opentracing-contrib/go-stdlib v1.0.0 // indirect
	github.com/opentracing/basictracer-go v1.1.0
	github.com/opentracing/opentracing-go v1.1.1-0.20200124165624-2876d2018785
	github.com/pkg/errors v0.9.1
	github.com/pmezard/go-difflib v1.0.0
	github.com/prometheus/alertmanager v0.21.0
	github.com/prometheus/client_golang v1.7.1
	github.com/prometheus/client_model v0.2.0
	github.com/prometheus/common v0.10.0
	github.com/prometheus/node_exporter v1.0.1 // indirect
	github.com/prometheus/prometheus v1.8.2-0.20200629082805-315564210816
	github.com/sercand/kuberesolver v2.4.0+incompatible // indirect
	github.com/shirou/gopsutil v2.20.5+incompatible // indirect
	github.com/sirupsen/logrus v1.6.0 // indirect
	github.com/uber/jaeger-client-go v2.24.0+incompatible
	github.com/uber/jaeger-lib v2.2.0+incompatible
	github.com/weaveworks/common v0.0.0-20200625145055-4b1847531bc9 // indirect
	go.elastic.co/apm v1.8.0
	go.elastic.co/apm/module/apmot v1.8.0
	go.elastic.co/fastjson v1.1.0 // indirect
	go.opencensus.io v0.22.4 // indirect
	go.uber.org/automaxprocs v1.3.0
	golang.org/x/crypto v0.0.0-20200622213623-75b288015ac9
	golang.org/x/net v0.0.0-20200625001655-4c5254603344 // indirect
	golang.org/x/oauth2 v0.0.0-20200107190931-bf48bf16ab8d
	golang.org/x/sync v0.0.0-20200625203802-6e8e738ad208
	golang.org/x/sys v0.0.0-20200625212154-ddb9806d33ae // indirect
	golang.org/x/text v0.3.3
	golang.org/x/tools v0.0.0-20200626171337-aa94e735be7f // indirect
	google.golang.org/api v0.28.0
	google.golang.org/genproto v0.0.0-20200626011028-ee7919e894b5
	google.golang.org/grpc v1.30.0
	google.golang.org/protobuf v1.25.0 // indirect
	gopkg.in/alecthomas/kingpin.v2 v2.2.6
	gopkg.in/fsnotify.v1 v1.4.7
	gopkg.in/ini.v1 v1.57.0 // indirect
	gopkg.in/yaml.v2 v2.3.0
	gopkg.in/yaml.v3 v3.0.0-20200615113413-eeeca48fe776
	howett.net/plist v0.0.0-20200419221736-3b63eb3a43b5 // indirect
	k8s.io/client-go v0.18.5 // indirect
	k8s.io/klog/v2 v2.2.0 // indirect
	k8s.io/utils v0.0.0-20200619165400-6e3d28b6ed19 // indirect
)

replace (
	// googleapis/gnostic messed up and released a patch release with module names which have been lowercased.
	github.com/googleapis/gnostic v0.4.2 => github.com/googleapis/gnostic v0.4.0
	// Make sure Cortex is not forcing us to some other Prometheus version.
	github.com/prometheus/prometheus => github.com/prometheus/prometheus v1.8.2-0.20200629082805-315564210816
	// We need to stay on 0.x.y according to https://github.com/kubernetes/client-go/#client-go
	k8s.io/client-go => k8s.io/client-go v0.18.5
)

go 1.14
