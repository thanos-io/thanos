{
  local thanos = self,
  bucket_replicate+:: {
    selector: error 'must provide selector for Thanos Bucket Replicate dashboard',
  },
  prometheusRules+:: {
    groups+: [
      {
        name: 'thanos-bucket-replicate.rules',
        rules: [
        ],
      },
    ],
  },
}
