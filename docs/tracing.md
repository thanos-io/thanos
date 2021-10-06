# Tracing

Thanos supports different tracing backends that implements `opentracing.Tracer` interface.

All clients are configured using `--tracing.config-file` to reference to the configuration file or `--tracing.config` to put yaml config directly.

## How to use `config` flags?

You can either pass YAML file defined below in `--tracing.config-file` or pass the YAML content directly using `--tracing.config`. We recommend the latter as it gives an explicit static view of configuration for each component. It also saves you the fuss of creating and managing additional file.

Don't be afraid of multiline flags!

In Kubernetes it is as easy as (using Thanos sidecar example):

```yaml
      - args:
        - sidecar
        - |
          --objstore.config=type: GCS
          config:
            bucket: <bucket>
        - --prometheus.url=http://localhost:9090
        - |
          --tracing.config=type: STACKDRIVER
          config:
            service_name: ""
            project_id: <project>
            sample_factor: 16
        - --tsdb.path=/prometheus-data
```

## How to add a new client?

1. Create new directory under `pkg/tracing/<provider>`
2. Implement `opentracing.Tracer` interface
3. Add client implementation to the factory in [factory](../pkg/tracing/client/factory.go) code. (Using as small amount of flags as possible in every command)
4. Add client struct config to [cfggen](../scripts/cfggen/main.go) to allow config auto generation.

At that point, anyone can use your provider by spec.

See [this issue](https://github.com/thanos-io/thanos/issues/1972) to check our progress on moving to OpenTelemetry Go client library.

## Usage

Once tracing is enabled and sampling per backend is configured, Thanos will generate traces for all gRPC and HTTP APIs thanks to generic "middlewares". Some more interesting to observe APIs like `query` or `query_range` have more low-level spans with focused metadata showing latency for important functionalities. For example, Jaeger view of HTTP query_range API call might look as follows:

![view](img/tracing2.png)

As you can see it contains both HTTP request and spans around gRPC request, since [Querier](components/query.md) calls gRPC services to get fetch series data.

Each Thanos component generates spans related to its work and sends them to central place e.g Jaeger or OpenTelemetry collector. Such place is then responsible to tie all spans to a single trace, showing a request execution path.

### Obtaining Trace ID

Single trace is tied to a single, unique request to the system and is composed of many spans from different components. Trace is identifiable using `Trace ID`, which is a unique hash e.g `131da78f02aa3525`. This information can be also referred as `request id` and `operation id` in other systems. In order to use trace data you want to find trace IDs that explains the requests you are interested in e.g request with interesting error, or longer latency, or just debug call you just made.

When using tracing with Thanos, you can obtain trace ID in multiple ways:

* Search by labels/attributes/tags/time/component/latency e.g. using Jaeger indexing.
* [Exemplars](https://www.bwplotka.dev/2021/correlations-exemplars/)
* If request was sampled, response will have `X-Thanos-Trace-Id` response header with trace ID of this request as value.

![view](img/tracing.png)

### Forcing Sampling

Every request against any Thanos component's API with header `X-Thanos-Force-Tracing` will be sampled if tracing backend was configured.

## Configuration

Currently supported tracing backends:

### Jaeger

Client for https://github.com/jaegertracing/jaeger tracing. For details, please see [client config](https://github.com/jaegertracing/jaeger-client-go/blob/master/config/config.go).

```yaml mdox-exec="go run scripts/cfggen/main.go --name=jaeger.Config"
type: JAEGER
config:
  # Disabled makes the config return opentracing.NoopTracer.
  disabled: false
  # ServiceName specifies the service name to use on the tracer.
  service_name: ""
  # Endpoint instructs reporter to send spans to jaeger-collector at this URL.
  # If not specified, it joins to AgentHost (localhost) with AgentPort (6831).
  endpoint: ""
  # AgentHost instructs reporter to send spans to jaeger-agent at this host.
  agent_host: ""
  # AgentPort instructs reporter to send spans to jaeger-agent at this port.
  agent_port: 0
  # User instructs reporter to include a user for basic http authentication when sending spans to jaeger-collector.
  user: ""
  # Password instructs reporter to include a password for basic http authentication when sending spans to jaeger-collector.
  password: ""
  # RPCMetrics enables generations of RPC metrics (requires metrics factory to be provided).
  rpc_metrics: false
  # Tags specifies comma separated list of key=value.
  tags: ""
  # SamplerType specifies the type of the sampler: const, probabilistic, rateLimiting, or remote.
  sampler_type: ""
  # SamplerParam is a value passed to the sampler.
	# Valid values for Param field are:
	# - for "const" sampler, 0 or 1 for always false/true respectively
	# - for "probabilistic" sampler, a probability between 0 and 1
	# - for "rateLimiting" sampler, the number of spans per second
	# - for "remote" sampler, param is the same as for "probabilistic"
	#   and indicates the initial sampling rate before the actual one
	#   is received from the mothership.
  sampler_param: 0
  # SamplingServerURL is the URL of sampling manager that can provide
  # sampling strategy to this service.
  sampler_manager_host_port: ""
  # MaxOperations is the maximum number of operations that the PerOperationSampler
  # will keep track of. If an operation is not tracked, a default probabilistic
  # sampler will be used rather than the per operation specific sampler.
  sampler_max_operations: 0
  # SamplingRefreshInterval controls how often the remotely controlled sampler will poll
  # sampling manager for the appropriate sampling strategy.
  sampler_refresh_interval: 0s
  # ReporterMaxQueueSize controls how many spans the reporter can keep in memory before it starts dropping
  # new spans. The queue is continuously drained by a background go-routine, as fast as spans
  # can be sent out of process.
  reporter_max_queue_size: 0
  # ReporterFlushInterval controls how often the buffer is force-flushed, even if it's not full.
  # It is generally not useful, as it only matters for very low traffic services.
  reporter_flush_interval: 0s
  # ReporterLogSpans, when true, enables LoggingReporter that runs in parallel with the main reporter
  # and logs all submitted spans. Main Configuration.Logger must be initialized in the code
  # for this option to have any effect.
  reporter_log_spans: false
```

### Stackdriver

Client for https://cloud.google.com/trace/ tracing. 
For details, please see [client config](https://github.com/lovoo/gcloud-opentracing/blob/master/recorder.go).
The current library that we use in the [client](https://github.com/thanos-io/thanos/blob/0618ac3974fccdcbd4ac92625a4a0785bf746376/pkg/tracing/stackdriver/tracer.go#L16), [lovoo/gcloud-opentracing](https://github.com/lovoo/gcloud-opentracing), is outdated.

```yaml mdox-exec="go run scripts/cfggen/main.go --name=stackdriver.Config"
type: STACKDRIVER
config:
  # ServiceName specifies the service name to use on the tracer.
  service_name: ""
  # The project ID is a unique identifier for a GCP project. 
  project_id: ""
  # How often to send traces. One in every x requests.
  # (1/sample_factor)
  sample_factor: 0
```

### Elastic APM

Client for https://www.elastic.co/products/apm tracing. For details, please see [client config](https://github.com/elastic/apm-agent-go/blob/master/tracer.go).

```yaml mdox-exec="go run scripts/cfggen/main.go --name=elasticapm.Config"
type: ELASTIC_APM
config:
  # ServiceName holds the service name.
  service_name: ""
  # ServiceVersion holds the service version.
  service_version: ""
  # ServiceEnvironment holds the service environment.
  service_environment: ""
  # SampleRate holds the sample rate in effect at the
  # time of the sampling decision. This is used for
  # propagating the value downstream, and for inclusion
  # in events sent to APM Server.
  sample_rate: 0
```

### Lightstep

Client for [Lightstep](https://lightstep.com). For details, please see [client config](https://github.com/lightstep/lightstep-tracer-go/blob/master/options.go).

In order to configure Thanos to interact with Lightstep you need to provide at least an [access token](https://docs.lightstep.com/docs/create-and-use-access-tokens) in the configuration file. The `collector` key is optional and used when you have on-premise satellites.

```yaml mdox-exec="go run scripts/cfggen/main.go --name=lightstep.Config"
type: LIGHTSTEP
config:
  # AccessToken is the unique API key for your LightStep project.  It is
  # available on your account page at https://app.lightstep.com/account
  access_token: ""
  # Collector is the host, port, and plaintext option to use
  # for the collector.
  collector:
    # Scheme to use for the endpoint, defaults to appropriate one if no custom one is required
    scheme: ""
    # Host on which the endpoint is running
    host: ""
    # Port on which the endpoint is listening
    port: 0
    # Whether or not to encrypt data send to the endpoint
    plaintext: false
    # Path to a custom CA cert file, defaults to system defined certs if omitted
    custom_ca_cert_file: ""
  # Tags are arbitrary key-value pairs that apply to all spans generated by
  # this Tracer.
  tags: ""
```
