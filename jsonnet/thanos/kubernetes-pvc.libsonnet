local k = import 'ksonnet/ksonnet.beta.4/k.libsonnet';

{
  _config+:: {
    store+: {
      pvc+: {
        class: 'standard',
        size: '10Gi',
      },
    },
  },

  thanos+:: {
    store+: {
      statefulSet+:
        local sts = k.apps.v1.statefulSet;
        local pvc = sts.mixin.spec.volumeClaimTemplatesType;

        {
          spec+: {
            template+: {
              spec+: {
                volumes: [],
              },
            },
            volumeClaimTemplates: [
              {
                metadata: {
                  name: 'data',
                },
                spec: {
                  accessModes: [
                    'ReadWriteOnce',
                  ],
                  storageClassName: $._config.store.pvc.class,
                  resources: {
                    requests: {
                      storage: $._config.store.pvc.size,
                    },
                  },
                },
              },
            ],
          },
        },
    },
  },
}
