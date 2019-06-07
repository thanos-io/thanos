module github.com/improbable-eng/thanos

require (
	cloud.google.com/go v0.34.0
	github.com/Azure/azure-storage-blob-go v0.0.0-20181022225951-5152f14ace1c
	github.com/NYTimes/gziphandler v1.1.1
	github.com/aliyun/aliyun-oss-go-sdk v1.9.8
	github.com/armon/go-metrics v0.0.0-20180917152333-f0300d1749da
	github.com/baiyubin/aliyun-sts-go-sdk v0.0.0-20180326062324-cfa1a18b161f // indirect
	github.com/cespare/xxhash v1.1.0
	github.com/fatih/structtag v1.0.0
	github.com/fortytw2/leaktest v1.3.0
	github.com/fsnotify/fsnotify v1.4.7
	github.com/go-kit/kit v0.8.0
	github.com/gogo/protobuf v1.2.1
	github.com/golang/snappy v0.0.1
	github.com/google/martian v2.1.0+incompatible // indirect
	github.com/googleapis/gax-go v2.0.2+incompatible // indirect
	github.com/gophercloud/gophercloud v0.0.0-20190301152420-fca40860790e
	github.com/gopherjs/gopherjs v0.0.0-20181103185306-d547d1d9531e // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.0.0
	github.com/grpc-ecosystem/go-grpc-prometheus v0.0.0-20181025070259-68e3a13e4117
	github.com/hashicorp/golang-lru v0.5.1
	github.com/leanovate/gopter v0.2.4
	github.com/lovoo/gcloud-opentracing v0.3.0
	github.com/miekg/dns v1.1.8
	github.com/minio/minio-go/v6 v6.0.27-0.20190529152532-de69c0e465ed
	github.com/mozillazg/go-cos v0.12.0
	github.com/mwitkow/go-conntrack v0.0.0-20161129095857-cc309e4a2223
	github.com/oklog/run v1.0.0
	github.com/oklog/ulid v1.3.1
	github.com/olekukonko/tablewriter v0.0.1
	github.com/opentracing-contrib/go-stdlib v0.0.0-20170113013457-1de4cc2120e7
	github.com/opentracing/basictracer-go v1.0.0
	github.com/opentracing/opentracing-go v1.1.0
	github.com/pkg/errors v0.8.1
	github.com/prometheus/client_golang v0.9.3-0.20190127221311-3c4408c8b829
	github.com/prometheus/common v0.4.0
	github.com/prometheus/prometheus v0.0.0-20190424153033-d3245f150225 //+incompatible
	github.com/prometheus/tsdb v0.8.0
	github.com/uber-go/atomic v1.4.0 // indirect
	github.com/uber/jaeger-client-go v2.16.0+incompatible
	github.com/uber/jaeger-lib v2.0.0+incompatible
	go.uber.org/atomic v1.4.0 // indirect
	golang.org/x/net v0.0.0-20190522155817-f3200d17e092
	golang.org/x/oauth2 v0.0.0-20190226205417-e64efc72b421
	golang.org/x/sync v0.0.0-20190423024810-112230192c58
	golang.org/x/text v0.3.1-0.20181227161524-e6919f6577db
	google.golang.org/api v0.3.2
	google.golang.org/grpc v1.19.1
	gopkg.in/alecthomas/kingpin.v2 v2.2.6
	gopkg.in/fsnotify/fsnotify.v1 v1.4.7 // indirect
	gopkg.in/yaml.v2 v2.2.2
)

replace (
	cloud.google.com/go => github.com/googleapis/google-cloud-go v0.34.0
	golang.org/x/crypto => github.com/golang/crypto v0.0.0-20190510104115-cbcb75029529
	golang.org/x/exp => github.com/golang/exp v0.0.0-20190510132918-efd6b22b2522
	golang.org/x/image => github.com/golang/image v0.0.0-20190507092727-e4e5bf290fec
	golang.org/x/lint => github.com/golang/lint v0.0.0-20190409202823-959b441ac422
	golang.org/x/mobile => github.com/golang/mobile v0.0.0-20190509164839-32b2708ab171
	golang.org/x/net => github.com/golang/net v0.0.0-20190509222800-a4d6f7feada5
	golang.org/x/oauth2 => github.com/golang/oauth2 v0.0.0-20190402181905-9f3314589c9a
	golang.org/x/sync => github.com/golang/sync v0.0.0-20190423024810-112230192c58
	golang.org/x/sys => github.com/golang/sys v0.0.0-20190509141414-a5b02f93d862
	golang.org/x/text => github.com/golang/text v0.3.2
	golang.org/x/time => github.com/golang/time v0.0.0-20190308202827-9d24e82272b4
	golang.org/x/tools => github.com/golang/tools v0.0.0-20190511041617-99f201b6807e
	google.golang.org/api => github.com/google/google-api-go-client v0.5.0
	google.golang.org/appengine => github.com/golang/appengine v1.5.0
	google.golang.org/genproto => github.com/google/go-genproto v0.0.0-20190307195333-5fe7a883aa19
	google.golang.org/grpc => github.com/grpc/grpc-go v1.20.1
	gopkg.in/yaml.v2 => github.com/go-yaml/yaml v2.1.0+incompatible
	k8s.io/api => github.com/kubernetes/api v0.0.0-20190512063542-eae0ddcf85ba
	k8s.io/apimachinery => github.com/kubernetes/apimachinery v0.0.0-20190511063452-5b67e417bf61
	k8s.io/client-go => github.com/kubernetes/client-go v11.0.0+incompatible
	k8s.io/gengo => github.com/kubernetes/gengo v0.0.0-20190327210449-e17681d19d3a
	k8s.io/klog => github.com/kubernetes/klog v0.3.0
	k8s.io/kube-openapi => github.com/kubernetes/kube-openapi v0.0.0-20190510232812-a01b7d5d6c22
	labix.org/v2/mgo => gopkg.in/mgo.v2 v2.0.0-20180705113604-9856a29383ce
	launchpad.net/gocheck => github.com/adjust/gocheck v0.0.0-20131111155431-fbc315b36e0e
	sigs.k8s.io/structured-merge-diff => github.com/kubernetes-sigs/structured-merge-diff v0.0.0-20190426204423-ea680f03cc65
	sigs.k8s.io/yaml => github.com/kubernetes-sigs/yaml v1.1.0
)
