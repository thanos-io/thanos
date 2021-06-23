{
  local thanos = self,
  bucket_replicate+:: {
    selector: error 'must provide selector for Thanos Bucket Replicate dashboard',
  },
  prometheusRules+:: {
    groups+: if thanos.bucket_replicate == null then [] else [
      {
        name: 'thanos-bucket-replicate.rules',
        rules: [],
      },
    ],
  },
}
