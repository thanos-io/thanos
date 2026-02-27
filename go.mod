module github.com/thanos-io/thanos

go 1.25.0

require (
	capnproto.org/go/capnp/v3 v3.1.0-alpha.1
	cloud.google.com/go/trace v1.11.6
	github.com/GoogleCloudPlatform/opentelemetry-operations-go/exporter/trace v1.27.0
	github.com/KimMachineGun/automemlimit v0.7.5
	github.com/alecthomas/units v0.0.0-20240927000941-0f3dac36c52b
	github.com/alicebob/miniredis/v2 v2.35.0
	github.com/blang/semver/v4 v4.0.0
	github.com/bradfitz/gomemcache v0.0.0-20250403215159-8d39553ac7cf
	github.com/caio/go-tdigest v3.1.0+incompatible
	github.com/cespare/xxhash/v2 v2.3.0
	github.com/chromedp/cdproto v0.0.0-20230802225258-3cf4e6d46a89
	github.com/chromedp/chromedp v0.9.2
	github.com/coreos/go-systemd/v22 v22.6.0
	github.com/cortexproject/promqlsmith v0.0.0-20250407233056-90db95b1a4e4
	github.com/cristalhq/hedgedhttp v0.9.1
	github.com/dustin/go-humanize v1.0.1
	github.com/efficientgo/core v1.0.0-rc.3
	github.com/efficientgo/e2e v0.14.1-0.20260204162810-8c75b1e33ef9
	github.com/efficientgo/tools/extkingpin v0.0.0-20230505153745-6b7392939a60
	github.com/facette/natsort v0.0.0-20181210072756-2cd4dd1e2dcb
	github.com/fatih/structtag v1.2.0
	github.com/felixge/fgprof v0.9.5
	github.com/fortytw2/leaktest v1.3.0
	github.com/fsnotify/fsnotify v1.9.0
	github.com/go-kit/log v0.2.1
	github.com/go-openapi/strfmt v0.25.0
	github.com/gogo/protobuf v1.3.2
	github.com/gogo/status v1.1.1
	github.com/golang/groupcache v0.0.0-20241129210726-2c02b8208cf8
	github.com/golang/protobuf v1.5.4
	github.com/golang/snappy v1.0.0
	github.com/google/go-cmp v0.7.0
	github.com/google/uuid v1.6.0
	github.com/googleapis/gax-go v2.0.2+incompatible
	github.com/grpc-ecosystem/go-grpc-middleware/providers/prometheus v1.0.1
	github.com/grpc-ecosystem/go-grpc-middleware/v2 v2.3.2
	github.com/hashicorp/golang-lru/v2 v2.0.7
	github.com/jpillora/backoff v1.0.0
	github.com/json-iterator/go v1.1.12
	github.com/klauspost/compress v1.18.2
	github.com/leanovate/gopter v0.2.9
	github.com/lightstep/lightstep-tracer-go v0.26.0
	github.com/lovoo/gcloud-opentracing v0.3.0
	github.com/miekg/dns v1.1.69
	github.com/minio/sha256-simd v1.0.1
	github.com/mitchellh/go-ps v1.0.0
	github.com/mwitkow/go-conntrack v0.0.0-20190716064945-2f068394615f
	github.com/oklog/run v1.2.0
	github.com/oklog/ulid v1.3.1 // indirect
	github.com/olekukonko/tablewriter v0.0.5
	github.com/onsi/gomega v1.36.2
	github.com/opentracing/basictracer-go v1.1.0
	github.com/opentracing/opentracing-go v1.2.0
	github.com/pkg/errors v0.9.1
	github.com/prometheus-community/prom-label-proxy v0.11.1
	github.com/prometheus/alertmanager v0.30.0
	github.com/prometheus/client_golang v1.23.2
	github.com/prometheus/client_model v0.6.2
	github.com/prometheus/common v0.67.5
	github.com/prometheus/exporter-toolkit v0.15.0
	// Prometheus maps version 3.x.y to tags v0.30x.y.
	github.com/prometheus/prometheus v0.309.1
	github.com/redis/rueidis v1.0.61
	github.com/seiflotfy/cuckoofilter v0.0.0-20240715131351-a2f2c23f1771
	github.com/sony/gobreaker v1.0.0
	github.com/stretchr/testify v1.11.1
	github.com/thanos-io/objstore v0.0.0-20250804093838-71d60dfee488
	github.com/thanos-io/promql-engine v0.0.0-20260119085929-dd5223783674
	github.com/uber/jaeger-client-go v2.30.0+incompatible
	github.com/vimeo/galaxycache v1.3.1
	github.com/weaveworks/common v0.0.0-20230728070032-dd9e68f319d5
	go.elastic.co/apm v1.15.0
	go.elastic.co/apm/module/apmot v1.15.0
	go.opentelemetry.io/contrib/propagators/autoprop v0.61.0
	go.opentelemetry.io/contrib/samplers/jaegerremote v0.30.0
	go.opentelemetry.io/otel v1.39.0
	go.opentelemetry.io/otel/bridge/opentracing v1.36.0
	go.opentelemetry.io/otel/exporters/jaeger v1.17.0
	go.opentelemetry.io/otel/exporters/otlp/otlptrace v1.39.0
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc v1.39.0
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp v1.39.0
	go.opentelemetry.io/otel/sdk v1.39.0
	go.opentelemetry.io/otel/trace v1.39.0
	go.uber.org/atomic v1.11.0
	go.uber.org/automaxprocs v1.6.0
	go.uber.org/goleak v1.3.0
	go4.org/intern v0.0.0-20230525184215-6c62f75575cb
	golang.org/x/crypto v0.47.0
	golang.org/x/net v0.49.0
	golang.org/x/sync v0.19.0
	golang.org/x/text v0.33.0
	golang.org/x/time v0.14.0
	google.golang.org/grpc v1.77.0
	google.golang.org/grpc/examples v0.0.0-20250407062114-b368379ef8f6
	google.golang.org/protobuf v1.36.11
	gopkg.in/yaml.v2 v2.4.0
	gopkg.in/yaml.v3 v3.0.1
)

require (
	cloud.google.com/go v0.120.0 // indirect
	cloud.google.com/go/auth v0.17.0 // indirect
	cloud.google.com/go/auth/oauth2adapt v0.2.8 // indirect
	cloud.google.com/go/compute/metadata v0.9.0 // indirect
	cloud.google.com/go/iam v1.5.2 // indirect
	cloud.google.com/go/storage v1.50.0 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/azcore v1.20.0 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/azidentity v1.13.1 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/internal v1.11.2 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/storage/azblob v1.6.1 // indirect
	github.com/AzureAD/microsoft-authentication-library-for-go v1.6.0 // indirect
)

require (
	github.com/VictoriaMetrics/easyproto v1.1.3
	github.com/alecthomas/kingpin/v2 v2.4.0
	github.com/oklog/ulid/v2 v2.1.1
	github.com/prometheus/otlptranslator v1.0.0
	github.com/tjhop/slog-gokit v0.1.4
	go.opentelemetry.io/collector/pdata v1.48.0
	go.opentelemetry.io/collector/semconv v0.128.0
)

require github.com/dgryski/go-metro v0.0.0-20250106013310-edb8663e5e33 // indirect

require (
	github.com/HdrHistogram/hdrhistogram-go v1.1.2 // indirect
	github.com/bboreham/go-loser v0.0.0-20230920113527-fcc2c21820a3 // indirect
	github.com/elastic/go-licenser v0.4.2 // indirect
	github.com/go-ini/ini v1.67.0 // indirect
	github.com/go-openapi/runtime v0.29.2 // indirect
	github.com/goccy/go-json v0.10.5 // indirect
	github.com/golang-jwt/jwt/v5 v5.3.0 // indirect
	github.com/google/s2a-go v0.1.9 // indirect
	github.com/huaweicloud/huaweicloud-sdk-go-obs v3.25.4+incompatible // indirect
	github.com/jcchavezs/porto v0.7.0 // indirect
	github.com/leesper/go_rng v0.0.0-20190531154944-a612b043e353 // indirect
	github.com/mdlayher/socket v0.5.1 // indirect
	github.com/mdlayher/vsock v1.2.1 // indirect
	github.com/metalmatze/signal v0.0.0-20210307161603-1c9aa721a97a // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/onsi/ginkgo v1.16.5 // indirect
	github.com/pbnjay/memory v0.0.0-20210728143218-7b4eea64cf58 // indirect
	github.com/sercand/kuberesolver/v4 v4.0.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.61.0 // indirect
	go.opentelemetry.io/contrib/propagators/ot v1.36.0 // indirect
	go4.org/unsafe/assume-no-moving-gc v0.0.0-20231121144256-b99613f794b6 // indirect
	golang.org/x/lint v0.0.0-20241112194109-818c5a804067 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20251213004720-97cd9d5aeac2 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260114163908-3f89685c29c3 // indirect
	k8s.io/apimachinery v0.34.3 // indirect
	k8s.io/client-go v0.34.3 // indirect
	k8s.io/klog/v2 v2.130.1 // indirect
	k8s.io/utils v0.0.0-20250604170112-4c0f3b243397 // indirect
)

require (
	cel.dev/expr v0.24.0 // indirect
	cloud.google.com/go/monitoring v1.24.2 // indirect
	github.com/GoogleCloudPlatform/opentelemetry-operations-go/detectors/gcp v1.30.0 // indirect
	github.com/GoogleCloudPlatform/opentelemetry-operations-go/exporter/metric v0.50.0 // indirect
	github.com/GoogleCloudPlatform/opentelemetry-operations-go/internal/resourcemapping v0.52.0 // indirect
	github.com/aliyun/aliyun-oss-go-sdk v3.0.2+incompatible // indirect
	github.com/armon/go-radix v1.0.0 // indirect
	github.com/aws/aws-sdk-go-v2 v1.41.0 // indirect
	github.com/aws/aws-sdk-go-v2/config v1.32.6 // indirect
	github.com/aws/aws-sdk-go-v2/credentials v1.19.6 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.18.16 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.4.16 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.7.16 // indirect
	github.com/aws/aws-sdk-go-v2/internal/ini v1.8.4 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.13.4 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.13.16 // indirect
	github.com/aws/aws-sdk-go-v2/service/signin v1.0.4 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.30.8 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.35.12 // indirect
	github.com/aws/aws-sdk-go-v2/service/sts v1.41.5 // indirect
	github.com/aws/smithy-go v1.24.0 // indirect
	github.com/baidubce/bce-sdk-go v0.9.230 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cenkalti/backoff/v5 v5.0.3 // indirect
	github.com/chromedp/sysutil v1.0.0 // indirect
	github.com/clbanning/mxj v1.8.4 // indirect
	github.com/cncf/xds/go v0.0.0-20251022180443-0feb69152e9f // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/dennwc/varint v1.0.0 // indirect
	github.com/edsrzf/mmap-go v1.2.0 // indirect
	github.com/elastic/go-sysinfo v1.15.3 // indirect
	github.com/elastic/go-windows v1.0.2 // indirect
	github.com/envoyproxy/go-control-plane/envoy v1.36.0 // indirect
	github.com/envoyproxy/protoc-gen-validate v1.3.0 // indirect
	github.com/fatih/color v1.18.0 // indirect
	github.com/felixge/httpsnoop v1.0.4 // indirect
	github.com/go-jose/go-jose/v4 v4.1.3 // indirect
	github.com/go-logfmt/logfmt v0.6.0 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-openapi/analysis v0.24.1 // indirect
	github.com/go-openapi/errors v0.22.4 // indirect
	github.com/go-openapi/jsonpointer v0.22.1 // indirect
	github.com/go-openapi/jsonreference v0.21.3 // indirect
	github.com/go-openapi/loads v0.23.2 // indirect
	github.com/go-openapi/spec v0.22.1 // indirect
	github.com/go-openapi/swag v0.25.4 // indirect
	github.com/go-openapi/swag/cmdutils v0.25.4 // indirect
	github.com/go-openapi/swag/conv v0.25.4 // indirect
	github.com/go-openapi/swag/fileutils v0.25.4 // indirect
	github.com/go-openapi/swag/jsonname v0.25.4 // indirect
	github.com/go-openapi/swag/jsonutils v0.25.4 // indirect
	github.com/go-openapi/swag/loading v0.25.4 // indirect
	github.com/go-openapi/swag/mangling v0.25.4 // indirect
	github.com/go-openapi/swag/netutils v0.25.4 // indirect
	github.com/go-openapi/swag/stringutils v0.25.4 // indirect
	github.com/go-openapi/swag/typeutils v0.25.4 // indirect
	github.com/go-openapi/swag/yamlutils v0.25.4 // indirect
	github.com/go-openapi/validate v0.25.1 // indirect
	github.com/go-viper/mapstructure/v2 v2.4.0 // indirect
	github.com/gobwas/glob v0.2.3 // indirect
	github.com/gobwas/httphead v0.1.0 // indirect
	github.com/gobwas/pool v0.2.1 // indirect
	github.com/gobwas/ws v1.2.1 // indirect
	github.com/gofrs/flock v0.12.1 // indirect
	github.com/gogo/googleapis v1.4.1 // indirect
	github.com/google/go-querystring v1.1.0 // indirect
	github.com/google/pprof v0.0.0-20251213031049-b05bdaca462f // indirect
	github.com/googleapis/enterprise-certificate-proxy v0.3.7 // indirect
	github.com/googleapis/gax-go/v2 v2.15.0 // indirect
	github.com/gorilla/mux v1.8.1 // indirect
	github.com/grafana/regexp v0.0.0-20250905093917-f7b3be9d1853 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.27.3 // indirect
	github.com/hashicorp/go-version v1.8.0 // indirect
	github.com/jaegertracing/jaeger-idl v0.6.0 // indirect
	github.com/josharian/intern v1.0.0 // indirect
	github.com/julienschmidt/httprouter v1.3.0 // indirect
	github.com/klauspost/cpuid/v2 v2.2.10 // indirect
	github.com/knadh/koanf/maps v0.1.2 // indirect
	github.com/knadh/koanf/providers/confmap v1.0.0 // indirect
	github.com/knadh/koanf/v2 v2.3.0 // indirect
	github.com/kylelemons/godebug v1.1.0 // indirect
	github.com/lightstep/lightstep-tracer-common/golang/gogo v0.0.0-20210210170715-a8dfcb80d3a7 // indirect
	github.com/mailru/easyjson v0.9.0 // indirect
	github.com/mattn/go-colorable v0.1.14 // indirect
	github.com/mattn/go-runewidth v0.0.16 // indirect
	github.com/minio/crc64nvme v1.0.1 // indirect
	github.com/minio/md5-simd v1.1.2 // indirect
	github.com/minio/minio-go/v7 v7.0.93 // indirect
	github.com/mitchellh/copystructure v1.2.0 // indirect
	github.com/mitchellh/mapstructure v1.5.0 // indirect
	github.com/mitchellh/reflectwalk v1.0.2 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.3-0.20250322232337-35a7c28c31ee // indirect
	github.com/mozillazg/go-httpheader v0.4.0 // indirect
	github.com/ncw/swift v1.0.53 // indirect
	github.com/open-telemetry/opentelemetry-collector-contrib/internal/exp/metrics v0.142.0 // indirect
	github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatautil v0.142.0 // indirect
	github.com/open-telemetry/opentelemetry-collector-contrib/processor/deltatocumulativeprocessor v0.142.0 // indirect
	github.com/opentracing-contrib/go-grpc v0.1.2 // indirect
	github.com/opentracing-contrib/go-stdlib v1.1.0 // indirect
	github.com/oracle/oci-go-sdk/v65 v65.93.1 // indirect
	github.com/philhofer/fwd v1.1.3-0.20240916144458-20a13a1f6b7c // indirect
	github.com/pkg/browser v0.0.0-20240102092130-5ac0b6a4141c // indirect
	github.com/planetscale/vtprotobuf v0.6.1-0.20240319094008-0393e58bdf10 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/prometheus/client_golang/exp v0.0.0-20251212205219-7ba246a648ca // indirect
	github.com/prometheus/procfs v0.16.1 // indirect
	github.com/prometheus/sigv4 v0.3.0 // indirect
	github.com/puzpuzpuz/xsync/v3 v3.5.1 // indirect
	github.com/rivo/uniseg v0.4.7 // indirect
	github.com/rs/xid v1.6.0 // indirect
	github.com/santhosh-tekuri/jsonschema v1.2.4 // indirect
	github.com/sirupsen/logrus v1.9.3 // indirect
	github.com/spiffe/go-spiffe/v2 v2.6.0 // indirect
	github.com/stretchr/objx v0.5.2 // indirect
	github.com/tencentyun/cos-go-sdk-v5 v0.7.66 // indirect
	github.com/tinylib/msgp v1.3.0 // indirect
	github.com/uber/jaeger-lib v2.4.1+incompatible // indirect
	github.com/weaveworks/promrus v1.2.0 // indirect
	github.com/xhit/go-str2duration/v2 v2.1.0 // indirect
	github.com/youmark/pkcs8 v0.0.0-20240726163527-a2c0da244d78 // indirect
	github.com/yuin/gopher-lua v1.1.1 // indirect
	go.elastic.co/apm/module/apmhttp v1.15.0 // indirect
	go.elastic.co/fastjson v1.5.1 // indirect
	go.mongodb.org/mongo-driver v1.17.6 // indirect
	go.opencensus.io v0.24.0 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/collector/component v1.48.0 // indirect
	go.opentelemetry.io/collector/confmap v1.48.0 // indirect
	go.opentelemetry.io/collector/confmap/xconfmap v0.142.0 // indirect
	go.opentelemetry.io/collector/consumer v1.48.0 // indirect
	go.opentelemetry.io/collector/featuregate v1.48.0 // indirect
	go.opentelemetry.io/collector/pipeline v1.48.0 // indirect
	go.opentelemetry.io/collector/processor v1.48.0 // indirect
	go.opentelemetry.io/contrib/detectors/gcp v1.38.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/net/http/httptrace/otelhttptrace v0.64.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.64.0 // indirect
	go.opentelemetry.io/contrib/propagators/aws v1.36.0 // indirect
	go.opentelemetry.io/contrib/propagators/b3 v1.36.0 // indirect
	go.opentelemetry.io/contrib/propagators/jaeger v1.36.0 // indirect
	go.opentelemetry.io/otel/metric v1.39.0 // indirect
	go.opentelemetry.io/otel/sdk/metric v1.39.0 // indirect
	go.opentelemetry.io/proto/otlp v1.9.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.uber.org/zap v1.27.1 // indirect
	go.yaml.in/yaml/v2 v2.4.3 // indirect
	go.yaml.in/yaml/v3 v3.0.4 // indirect
	golang.org/x/exp v0.0.0-20250808145144-a408d31f581a // indirect
	golang.org/x/mod v0.31.0 // indirect
	golang.org/x/oauth2 v0.34.0 // indirect
	golang.org/x/sys v0.40.0 // indirect
	golang.org/x/telemetry v0.0.0-20251203150158-8fff8a5912fc // indirect
	golang.org/x/tools v0.40.0 // indirect
	gonum.org/v1/gonum v0.16.0 // indirect
	google.golang.org/api v0.257.0 // indirect
	google.golang.org/genproto v0.0.0-20250603155806-513f23925822 // indirect
	howett.net/plist v1.0.1 // indirect
	zenhack.net/go/util v0.0.0-20230414204917-531d38494cf5 // indirect
)

replace (
	// Pinnning capnp due to https://github.com/thanos-io/thanos/issues/7944
	capnproto.org/go/capnp/v3 => capnproto.org/go/capnp/v3 v3.0.0-alpha.30

	// Using a 3rd-party branch for custom dialer - see https://github.com/bradfitz/gomemcache/pull/86.
	// Required by Cortex https://github.com/cortexproject/cortex/pull/3051.
	github.com/bradfitz/gomemcache => github.com/themihai/gomemcache v0.0.0-20180902122335-24332e2d58ab

	// Pin kuberesolver/v5 to support new grpc version. Need to upgrade kuberesolver version on weaveworks/common.
	github.com/sercand/kuberesolver/v4 => github.com/sercand/kuberesolver/v5 v5.1.1

	github.com/vimeo/galaxycache => github.com/thanos-community/galaxycache v0.0.0-20211122094458-3a32041a1f1e

	google.golang.org/grpc => github.com/thanos-community/grpc-go v0.0.0-20251106112228-b9020406f781

	// Overriding to use latest commit.
	gopkg.in/alecthomas/kingpin.v2 => github.com/alecthomas/kingpin v1.3.8-0.20210301060133-17f40c25f497

	// The domain `zenhack.net` expired.
	zenhack.net/go/util => github.com/zenhack/go-util v0.0.0-20231005031245-66f5419c2aea
)
