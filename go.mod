module github.com/thanos-io/thanos

go 1.18

require (
	cloud.google.com/go/storage v1.27.0 // indirect
	cloud.google.com/go/trace v1.4.0
	github.com/GoogleCloudPlatform/opentelemetry-operations-go/exporter/trace v1.8.3
	github.com/NYTimes/gziphandler v1.1.1
	github.com/alecthomas/units v0.0.0-20211218093645-b94a6e3cc137
	github.com/alicebob/miniredis/v2 v2.22.0
	github.com/armon/go-metrics v0.4.0 // indirect
	github.com/blang/semver/v4 v4.0.0
	github.com/bradfitz/gomemcache v0.0.0-20190913173617-a41fca850d0b
	github.com/cespare/xxhash v1.1.0
	github.com/cespare/xxhash/v2 v2.2.0
	github.com/chromedp/cdproto v0.0.0-20220629234738-4cfc9cdeeb92
	github.com/chromedp/chromedp v0.8.2
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/dustin/go-humanize v1.0.0
	github.com/efficientgo/e2e v0.14.1-0.20230119090947-fa7ceb0197c5
	github.com/efficientgo/tools/extkingpin v0.0.0-20220817170617-6c25e3b627dd
	github.com/facette/natsort v0.0.0-20181210072756-2cd4dd1e2dcb
	github.com/fatih/structtag v1.2.0
	github.com/felixge/fgprof v0.9.2
	github.com/fortytw2/leaktest v1.3.0
	github.com/fsnotify/fsnotify v1.6.0
	github.com/go-kit/log v0.2.1
	github.com/go-openapi/strfmt v0.21.3
	github.com/go-redis/redis/v8 v8.11.5
	github.com/gogo/protobuf v1.3.2
	github.com/gogo/status v1.1.1
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da
	github.com/golang/protobuf v1.5.2
	github.com/golang/snappy v0.0.4
	github.com/googleapis/gax-go v2.0.2+incompatible
	github.com/gorilla/mux v1.8.0 // indirect
	github.com/grafana/regexp v0.0.0-20221122212121-6b5c0a4cb7fd // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.3.0 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware/providers/kit/v2 v2.0.0-20201002093600-73cf2ae9d891
	github.com/grpc-ecosystem/go-grpc-middleware/v2 v2.0.0-rc.2.0.20201207153454-9f6bf00c00a7
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0
	github.com/hashicorp/golang-lru v0.6.0
	github.com/jpillora/backoff v1.0.0
	github.com/json-iterator/go v1.1.12
	github.com/klauspost/compress v1.15.9
	github.com/leanovate/gopter v0.2.9
	github.com/lightstep/lightstep-tracer-go v0.25.0
	github.com/lovoo/gcloud-opentracing v0.3.0
	github.com/miekg/dns v1.1.50
	github.com/minio/minio-go/v7 v7.0.45 // indirect
	github.com/mwitkow/go-conntrack v0.0.0-20190716064945-2f068394615f
	github.com/oklog/run v1.1.0
	github.com/oklog/ulid v1.3.1
	github.com/olekukonko/tablewriter v0.0.5
	github.com/opentracing-contrib/go-grpc v0.0.0-20210225150812-73cb765af46e // indirect
	github.com/opentracing-contrib/go-stdlib v1.0.0 // indirect
	github.com/opentracing/basictracer-go v1.0.0
	github.com/opentracing/opentracing-go v1.2.0
	github.com/pkg/errors v0.9.1
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/prometheus/alertmanager v0.25.0
	github.com/prometheus/client_golang v1.14.0
	github.com/prometheus/client_model v0.3.0
	github.com/prometheus/common v0.39.1-0.20230110141620-846591a16635
	github.com/prometheus/exporter-toolkit v0.8.2
	// Prometheus maps version 2.x.y to tags v0.x.y.
	github.com/prometheus/prometheus v0.41.0
	github.com/sony/gobreaker v0.5.0
	github.com/stretchr/testify v1.8.1
	github.com/thanos-community/promql-engine v0.0.0-20230124070417-9e293186b7e4
	github.com/thanos-io/objstore v0.0.0-20221205132204-5aafc0079f06
	github.com/uber/jaeger-client-go v2.30.0+incompatible
	github.com/uber/jaeger-lib v2.4.1+incompatible // indirect
	github.com/vimeo/galaxycache v0.0.0-20210323154928-b7e5d71c067a
	github.com/weaveworks/common v0.0.0-20221201103051-7c2720a9024d
	go.elastic.co/apm v1.11.0
	go.elastic.co/apm/module/apmot v1.11.0
	go.opentelemetry.io/contrib/propagators/ot v1.13.0 // indirect
	go.opentelemetry.io/otel v1.12.0
	go.opentelemetry.io/otel/bridge/opentracing v1.12.0
	go.opentelemetry.io/otel/sdk v1.12.0
	go.opentelemetry.io/otel/trace v1.12.0
	go.uber.org/atomic v1.10.0
	go.uber.org/automaxprocs v1.5.1
	go.uber.org/goleak v1.2.0
	golang.org/x/crypto v0.1.0
	golang.org/x/net v0.5.0
	golang.org/x/sync v0.1.0
	golang.org/x/text v0.6.0
	golang.org/x/time v0.3.0
	google.golang.org/api v0.104.0 // indirect
	google.golang.org/genproto v0.0.0-20230131230820-1c016267d619 // indirect
	google.golang.org/grpc v1.52.0
	google.golang.org/grpc/examples v0.0.0-20211119005141-f45e61797429
	gopkg.in/alecthomas/kingpin.v2 v2.2.6
	gopkg.in/fsnotify.v1 v1.4.7
	gopkg.in/yaml.v2 v2.4.0
	gopkg.in/yaml.v3 v3.0.1
)

require (
	github.com/efficientgo/core v1.0.0-rc.2
	github.com/minio/sha256-simd v1.0.0
)

require (
	cloud.google.com/go v0.105.0 // indirect
	cloud.google.com/go/compute v1.13.0 // indirect
	cloud.google.com/go/iam v0.8.0 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/azcore v1.1.0 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/azidentity v1.1.0 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/internal v1.0.0 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/storage/azblob v0.4.1 // indirect
	github.com/AzureAD/microsoft-authentication-library-for-go v0.5.1 // indirect
	go.opentelemetry.io/contrib/samplers/jaegerremote v0.7.0
	go.opentelemetry.io/otel/exporters/jaeger v1.12.0
	go.opentelemetry.io/otel/exporters/otlp/otlptrace v1.12.0
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc v1.12.0
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp v1.12.0
)

require (
	go.opentelemetry.io/contrib/propagators/autoprop v0.38.0
	go4.org/intern v0.0.0-20220617035311-6925f38cc365
	golang.org/x/exp v0.0.0-20221212164502-fae10dda9338
)

require go4.org/unsafe/assume-no-moving-gc v0.0.0-20220617031537-928513b29760 // indirect

require (
	cloud.google.com/go/compute/metadata v0.2.2 // indirect
	github.com/GoogleCloudPlatform/opentelemetry-operations-go/internal/resourcemapping v0.32.3 // indirect
	github.com/OneOfOne/xxhash v1.2.6 // indirect
	github.com/alecthomas/template v0.0.0-20190718012654-fb15b899a751 // indirect
	github.com/alicebob/gopher-json v0.0.0-20200520072559-a9ecdc9d1d3a // indirect
	github.com/aliyun/aliyun-oss-go-sdk v2.2.2+incompatible // indirect
	github.com/armon/go-radix v1.0.0 // indirect
	github.com/asaskevich/govalidator v0.0.0-20210307081110-f21760c49a8d // indirect
	github.com/aws/aws-sdk-go v1.44.159 // indirect
	github.com/aws/aws-sdk-go-v2 v1.16.0 // indirect
	github.com/aws/aws-sdk-go-v2/config v1.15.1 // indirect
	github.com/aws/aws-sdk-go-v2/credentials v1.11.0 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.12.1 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.1.7 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.4.1 // indirect
	github.com/aws/aws-sdk-go-v2/internal/ini v1.3.8 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.9.1 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.11.1 // indirect
	github.com/aws/aws-sdk-go-v2/service/sts v1.16.1 // indirect
	github.com/aws/smithy-go v1.11.1 // indirect
	github.com/baidubce/bce-sdk-go v0.9.111 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cenkalti/backoff/v4 v4.2.0 // indirect
	github.com/chromedp/sysutil v1.0.0 // indirect
	github.com/clbanning/mxj v1.8.4 // indirect
	github.com/coreos/go-systemd/v22 v22.4.0 // indirect
	github.com/dennwc/varint v1.0.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/dnaeon/go-vcr v1.2.0 // indirect
	github.com/edsrzf/mmap-go v1.1.0 // indirect
	github.com/elastic/go-sysinfo v1.8.1 // indirect
	github.com/elastic/go-windows v1.0.1 // indirect
	github.com/felixge/httpsnoop v1.0.3 // indirect
	github.com/go-kit/kit v0.12.0 // indirect
	github.com/go-logfmt/logfmt v0.5.1 // indirect
	github.com/go-logr/logr v1.2.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-ole/go-ole v1.2.6 // indirect
	github.com/go-openapi/analysis v0.21.4 // indirect
	github.com/go-openapi/errors v0.20.3 // indirect
	github.com/go-openapi/jsonpointer v0.19.5 // indirect
	github.com/go-openapi/jsonreference v0.20.0 // indirect
	github.com/go-openapi/loads v0.21.2 // indirect
	github.com/go-openapi/spec v0.20.7 // indirect
	github.com/go-openapi/swag v0.22.3 // indirect
	github.com/go-openapi/validate v0.22.0 // indirect
	github.com/gobwas/httphead v0.1.0 // indirect
	github.com/gobwas/pool v0.2.1 // indirect
	github.com/gobwas/ws v1.1.0 // indirect
	github.com/gofrs/flock v0.8.1 // indirect
	github.com/gogo/googleapis v1.4.0 // indirect
	github.com/golang-jwt/jwt v3.2.1+incompatible // indirect
	github.com/golang-jwt/jwt/v4 v4.4.1 // indirect
	github.com/google/go-cmp v0.5.9 // indirect
	github.com/google/go-querystring v1.1.0 // indirect
	github.com/google/pprof v0.0.0-20221212185716-aee1124e3a93 // indirect
	github.com/google/uuid v1.3.0 // indirect
	github.com/googleapis/enterprise-certificate-proxy v0.2.0 // indirect
	github.com/googleapis/gax-go/v2 v2.7.0 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.11.1 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/joeshaw/multierror v0.0.0-20140124173710-69b34d4ec901 // indirect
	github.com/josharian/intern v1.0.0 // indirect
	github.com/julienschmidt/httprouter v1.3.0 // indirect
	github.com/klauspost/cpuid/v2 v2.1.0 // indirect
	github.com/kr/pretty v0.3.0 // indirect
	github.com/kylelemons/godebug v1.1.0 // indirect
	github.com/lightstep/lightstep-tracer-common/golang/gogo v0.0.0-20210210170715-a8dfcb80d3a7 // indirect
	github.com/lufia/plan9stats v0.0.0-20211012122336-39d0f177ccd0 // indirect
	github.com/mailru/easyjson v0.7.7 // indirect
	github.com/mattn/go-runewidth v0.0.13 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.4 // indirect
	github.com/minio/md5-simd v1.1.2 // indirect
	github.com/mitchellh/mapstructure v1.5.0 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/mozillazg/go-httpheader v0.2.1 // indirect
	github.com/ncw/swift v1.0.53 // indirect
	github.com/oracle/oci-go-sdk/v65 v65.13.0 // indirect
	github.com/pkg/browser v0.0.0-20210115035449-ce105d075bb4 // indirect
	github.com/power-devops/perfstat v0.0.0-20210106213030-5aafc221ea8c // indirect
	github.com/prometheus/common/sigv4 v0.1.0 // indirect
	github.com/prometheus/procfs v0.8.0 // indirect
	github.com/rivo/uniseg v0.2.0 // indirect
	github.com/rs/xid v1.4.0 // indirect
	github.com/rueian/rueidis v0.0.93
	github.com/santhosh-tekuri/jsonschema v1.2.4 // indirect
	github.com/sercand/kuberesolver v2.4.0+incompatible // indirect
	github.com/shirou/gopsutil/v3 v3.22.9 // indirect
	github.com/sirupsen/logrus v1.9.0 // indirect
	github.com/spaolacci/murmur3 v1.1.0 // indirect
	github.com/stretchr/objx v0.5.0 // indirect
	github.com/tencentyun/cos-go-sdk-v5 v0.7.34 // indirect
	github.com/tidwall/pretty v1.2.0 // indirect
	github.com/tklauser/go-sysconf v0.3.10 // indirect
	github.com/tklauser/numcpus v0.4.0 // indirect
	github.com/weaveworks/promrus v1.2.0 // indirect
	github.com/yuin/gopher-lua v0.0.0-20210529063254-f4c35e4016d9 // indirect
	github.com/yusufpapurcu/wmi v1.2.2 // indirect
	go.elastic.co/apm/module/apmhttp v1.11.0 // indirect
	go.elastic.co/fastjson v1.1.0 // indirect
	go.mongodb.org/mongo-driver v1.11.0 // indirect
	go.opencensus.io v0.24.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.38.0 // indirect
	go.opentelemetry.io/contrib/propagators/aws v1.13.0 // indirect
	go.opentelemetry.io/contrib/propagators/b3 v1.13.0 // indirect
	go.opentelemetry.io/contrib/propagators/jaeger v1.13.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/internal/retry v1.12.0 // indirect
	go.opentelemetry.io/otel/metric v0.35.0 // indirect
	go.opentelemetry.io/proto/otlp v0.19.0 // indirect
	go.uber.org/multierr v1.9.0 // indirect
	golang.org/x/mod v0.7.0 // indirect
	golang.org/x/oauth2 v0.3.0 // indirect
	golang.org/x/sys v0.4.0 // indirect
	golang.org/x/tools v0.4.0 // indirect
	golang.org/x/xerrors v0.0.0-20220907171357-04be3eba64a2 // indirect
	gonum.org/v1/gonum v0.12.0 // indirect
	google.golang.org/appengine v1.6.7 // indirect
	google.golang.org/protobuf v1.28.1 // indirect
	gopkg.in/ini.v1 v1.66.6 // indirect
	howett.net/plist v0.0.0-20181124034731-591f970eefbb // indirect
)

replace (
	// Using a 3rd-party branch for custom dialer - see https://github.com/bradfitz/gomemcache/pull/86.
	// Required by Cortex https://github.com/cortexproject/cortex/pull/3051.
	github.com/bradfitz/gomemcache => github.com/themihai/gomemcache v0.0.0-20180902122335-24332e2d58ab

	github.com/vimeo/galaxycache => github.com/thanos-community/galaxycache v0.0.0-20211122094458-3a32041a1f1e

	// Override due to https://github.com/weaveworks/common/issues/239
	google.golang.org/grpc => google.golang.org/grpc v1.45.0

	// Overriding to use latest commit.
	gopkg.in/alecthomas/kingpin.v2 => github.com/alecthomas/kingpin v1.3.8-0.20210301060133-17f40c25f497

	// From Prometheus.
	k8s.io/klog => github.com/simonpasquier/klog-gokit v0.3.0
	k8s.io/klog/v2 => github.com/simonpasquier/klog-gokit/v3 v3.0.0
)
