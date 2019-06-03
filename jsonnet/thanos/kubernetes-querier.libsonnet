local k = import 'ksonnet/ksonnet.beta.4/k.libsonnet';

{
  _config+:: {
    querier+: {
      name: 'thanos-querier',
      labels: { app: $._config.querier.name },
      ports: {
        grpc: 10901,
        http: 10902,
      },
    },
  },

  thanos+:: {
    querier+: {
      service:
        local service = k.core.v1.service;
        local ports = service.mixin.spec.portsType;

        service.new(
          $._config.querier.name,
          $._config.querier.labels,
          [
            ports.newNamed('grpc', $._config.querier.ports.grpc, $._config.querier.ports.grpc),
            ports.newNamed('http', 9090, $._config.querier.ports.http),
          ]
        ) +
        service.mixin.metadata.withNamespace($._config.namespace) +
        service.mixin.metadata.withLabels($._config.querier.labels),

      deployment:
        local deployment = k.apps.v1.deployment;
        local container = deployment.mixin.spec.template.spec.containersType;

        local args = [
          'query',
          '--query.replica-label=replica',
        ] + [
          '--store=%s-%d.%s.svc.cluster.local:%d' % [
            $.thanos.store.service.metadata.name,
            i,
            $._config.namespace,
            $._config.store.ports.grpc,
          ]
          for i in std.range(0, $._config.store.replicas - 1)
        ];


        local c =
          container.new($._config.querier.name, $._config.images.thanos) +
          container.withArgs(args);

        deployment.new($._config.querier.name, 1, c, $._config.querier.labels) +
        deployment.mixin.metadata.withNamespace($._config.namespace) +
        deployment.mixin.spec.selector.withMatchLabels($._config.querier.labels),
    },
  },
}
