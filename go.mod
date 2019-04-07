module github.com/improbable-eng/thanos

replace github.com/cortexproject/cortex => github.com/bwplotka/cortex v0.0.0-20190416102825-ae4098d14a9c

require (
	cloud.google.com/go v0.34.0
	github.com/Azure/azure-storage-blob-go v0.0.0-20181022225951-5152f14ace1c
	github.com/NYTimes/gziphandler v1.1.1
	github.com/armon/go-metrics v0.0.0-20180917152333-f0300d1749da
	github.com/blang/semver v3.5.1+incompatible // indirect
	github.com/bradfitz/gomemcache v0.0.0-20190329173943-551aad21a668 // indirect
	github.com/cortexproject/cortex v0.0.0
	github.com/fatih/structtag v1.0.0
	github.com/fortytw2/leaktest v1.2.0
	github.com/fsnotify/fsnotify v1.4.7
	github.com/go-kit/kit v0.8.0
	github.com/gogo/protobuf v1.2.0
	github.com/gogo/status v1.1.0 // indirect
	github.com/golang/snappy v0.0.0-20180518054509-2e65f85255db
	github.com/google/martian v2.1.0+incompatible // indirect
	github.com/googleapis/gax-go v2.0.2+incompatible // indirect
	github.com/gophercloud/gophercloud v0.0.0-20181206160319-9d88c34913a9
	github.com/gorilla/mux v1.7.1 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.0.0
	github.com/grpc-ecosystem/go-grpc-prometheus v0.0.0-20181025070259-68e3a13e4117
	github.com/hashicorp/go-sockaddr v0.0.0-20180320115054-6d291a969b86
	github.com/hashicorp/golang-lru v0.5.0
	github.com/hashicorp/memberlist v0.1.0
	github.com/julienschmidt/httprouter v1.1.0 // indirect
	github.com/lovoo/gcloud-opentracing v0.3.0
	github.com/miekg/dns v1.0.8
	github.com/minio/minio-go v0.0.0-20190131015406-c8a261de75c1
	github.com/mozillazg/go-cos v0.11.0
	github.com/mwitkow/go-conntrack v0.0.0-20161129095857-cc309e4a2223
	github.com/oklog/run v1.0.0
	github.com/oklog/ulid v1.3.1
	github.com/olekukonko/tablewriter v0.0.1
	github.com/opentracing-contrib/go-grpc v0.0.0-20180928155321-4b5a12d3ff02 // indirect
	github.com/opentracing-contrib/go-stdlib v0.0.0-20170113013457-1de4cc2120e7
	github.com/opentracing/basictracer-go v1.0.0
	github.com/opentracing/opentracing-go v1.1.0
	github.com/pkg/errors v0.8.1
	github.com/prometheus/client_golang v0.9.2
	github.com/prometheus/common v0.0.0-20181218105931-67670fe90761
	github.com/prometheus/prometheus v0.0.0-20190118110214-3bd41cc92c78
	github.com/prometheus/tsdb v0.4.0
	github.com/sercand/kuberesolver v2.1.0+incompatible // indirect
	github.com/sirupsen/logrus v1.4.1 // indirect
	github.com/uber-go/atomic v1.3.2 // indirect
	github.com/uber/jaeger-client-go v2.16.0+incompatible // indirect
	github.com/uber/jaeger-lib v2.0.0+incompatible // indirect
	github.com/weaveworks/common v0.0.0-20190403142338-51eb0fb475a6 // indirect
	github.com/weaveworks/promrus v1.2.0 // indirect
	go.opencensus.io v0.19.0 // indirect
	go.uber.org/atomic v1.3.2 // indirect
	golang.org/x/net v0.0.0-20190213061140-3a22650c66bd
	golang.org/x/oauth2 v0.0.0-20181203162652-d668ce993890
	golang.org/x/sync v0.0.0-20190227155943-e225da77a7e6
	golang.org/x/sys v0.0.0-20190222072716-a9d3bda3a223 // indirect
	golang.org/x/text v0.3.1-0.20180807135948-17ff2d5776d2
	google.golang.org/api v0.1.0
	google.golang.org/appengine v1.4.0 // indirect
	google.golang.org/grpc v1.17.0
	gopkg.in/alecthomas/kingpin.v2 v2.2.6
	gopkg.in/fsnotify/fsnotify.v1 v1.4.7 // indirect
	gopkg.in/yaml.v2 v2.2.2
)
