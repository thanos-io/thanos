{
  local thanos = self,
  replicator+:: {
    selector: error 'must provide selector for Thanos Replicate dashboard',
  },
  prometheusRules+:: {
    groups+: [
      {
        name: 'thanos-replicate.rules',
        rules: [
        ],
      },
    ],
  },
}
