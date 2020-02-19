{
  local thanos = self,
  replicate+:: {
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
