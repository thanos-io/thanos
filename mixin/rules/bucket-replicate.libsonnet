{
  local thanos = self,
  bucketReplicate+:: {
    selector: error 'must provide selector for Thanos Bucket Replicate dashboard',
  },
  prometheusRules+:: {
    groups+: if thanos.bucketReplicate == null then [] else [
      {
        name: 'thanos-bucket-replicate.rules',
        rules: [],
      },
    ],
  },
}
