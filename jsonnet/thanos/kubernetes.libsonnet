local k = import 'ksonnet/ksonnet.beta.4/k.libsonnet';

local thanos =
  (import 'kubernetes-querier.libsonnet') +
  (import 'kubernetes-store.libsonnet') +
  (import 'kubernetes-pvc.libsonnet') +
  {
    _config+:: {
      namespace: 'thanos',

      images+: {
        thanos: 'improbable/thanos:v0.5.0-rc.0',
      },

      thanos+: {
        // MAKE SURE TO CREATE THE SECRET FIRST
        objectStorageConfig+: {
          name: 'thanos-objectstorage',
          key: 'thanos.yaml',
        },
      },

      store+:{
        replicas: 3,
      },
    },
  };

k.core.v1.list.new([
  thanos.thanos.querier.service,
  thanos.thanos.querier.deployment,
  thanos.thanos.store.service,
  thanos.thanos.store.statefulSet,
])
