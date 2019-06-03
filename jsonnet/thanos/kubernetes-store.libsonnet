local k = import 'ksonnet/ksonnet.beta.4/k.libsonnet';

{
  _config+:: {
    store+: {
      name: 'thanos-store',
      replicas: 1,
      labels: { app: $._config.store.name },
      ports: {
        grpc: 10901,
      },
    },
  },

  thanos+:: {
    store: {
      service:
        local service = k.core.v1.service;
        local ports = service.mixin.spec.portsType;

        service.new(
          $._config.store.name,
          $._config.store.labels,
          [
            ports.newNamed('grpc', $._config.store.ports.grpc, $._config.store.ports.grpc),
          ]
        ) +
        service.mixin.metadata.withNamespace($._config.namespace) +
        service.mixin.metadata.withLabels($._config.store.labels) +
        service.mixin.spec.withClusterIp('None'),

      statefulSet:
        local sts = k.apps.v1.statefulSet;
        local volume = sts.mixin.spec.template.spec.volumesType;
        local container = sts.mixin.spec.template.spec.containersType;
        local containerEnv = container.envType;
        local containerVolumeMount = container.volumeMountsType;


        local c =
          container.new($._config.store.name, $._config.images.thanos) +
          container.withArgs([
            'store',
            '--data-dir=/var/thanos/store',
            '--objstore.config=$(OBJSTORE_CONFIG)',
          ]) +
          container.withEnv([
            containerEnv.fromSecretRef(
              'OBJSTORE_CONFIG',
              $._config.thanos.objectStorageConfig.name,
              $._config.thanos.objectStorageConfig.key,
            ),
          ]) +
          container.withPorts([
            { name: 'grpc', containerPort: 10901 },
            { name: 'http', containerPort: 10902 },
          ]) +
          container.withVolumeMounts([
            containerVolumeMount.new('data', '/var/thanos/store', false),
          ]);

        sts.new($._config.store.name, $._config.store.replicas, c, [], $._config.store.labels) +
        sts.mixin.metadata.withNamespace($._config.namespace) +
        sts.mixin.metadata.withLabels($._config.store.labels) +
        sts.mixin.spec.withServiceName($.thanos.store.service.metadata.name) +
        sts.mixin.spec.selector.withMatchLabels($._config.store.labels) +
        sts.mixin.spec.template.spec.withVolumes([
          volume.fromEmptyDir('data'),
        ]),
    },
  },
}
