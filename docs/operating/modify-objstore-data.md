# Modify series in the object storage via bucket rewrite tool

For operational purposes, there are some use cases to manipulate data in the object storage. For example, delete some high cardinality metrics or relabel metrics if needed. This is already possible via the bucket rewrite tool.

## Delete series

```shell
thanos tools bucket rewrite --rewrite.to-delete-config-file config.yaml --objstore.config-file objstore.yaml --id <block ID>
```

This is the example command to delete some data in the specified TSDB block from your object store bucket. For example, if `k8s_app_metric37` is the metric you want to delete, then the config file `config.yaml` would be:

```yaml
- matchers: '{__name__="k8s_app_metric37"}'
```

Example output from my mac looks like below. Dry run mode is enabled by default to prevent unexpected series from being deleted.

A changelog file is generated so that you can check the expected modification of the provided deletion request.

```shell
thanos tools bucket rewrite  --rewrite.to-delete-config-file config.yaml --objstore.config-file ~/local-bucket-config.yaml --id 01FET1EK9BC3E0QD4886RQCM8K

level=info ts=2021-09-25T05:47:14.87316Z caller=factory.go:49 msg="loading bucket configuration"
level=info ts=2021-09-25T05:47:14.875365Z caller=tools_bucket.go:1078 msg="downloading block" source=01FET1EK9BC3E0QD4886RQCM8K
level=info ts=2021-09-25T05:47:14.887816Z caller=tools_bucket.go:1115 msg="changelog will be available" file=/var/folders/ny/yy113mqs6szcpjy2qrnhq9rh0000gq/T/thanos-rewrite/01FGDQWKJ7H29B3V4HCQ691WN9/change.log
level=info ts=2021-09-25T05:47:14.912544Z caller=tools_bucket.go:1130 msg="starting rewrite for block" source=01FET1EK9BC3E0QD4886RQCM8K new=01FGDQWKJ7H29B3V4HCQ691WN9 toDelete="- matchers: '{__name__=\"k8s_app_metric37\"}'\n" toRelabel=
level=info ts=2021-09-25T05:47:14.919438Z caller=compactor.go:41 msg="processed 10.00% of 15000 series"
level=info ts=2021-09-25T05:47:14.925442Z caller=compactor.go:41 msg="processed 20.00% of 15000 series"
level=info ts=2021-09-25T05:47:14.930263Z caller=compactor.go:41 msg="processed 30.00% of 15000 series"
level=info ts=2021-09-25T05:47:14.934325Z caller=compactor.go:41 msg="processed 40.00% of 15000 series"
level=info ts=2021-09-25T05:47:14.939466Z caller=compactor.go:41 msg="processed 50.00% of 15000 series"
level=info ts=2021-09-25T05:47:14.944513Z caller=compactor.go:41 msg="processed 60.00% of 15000 series"
level=info ts=2021-09-25T05:47:14.950254Z caller=compactor.go:41 msg="processed 70.00% of 15000 series"
level=info ts=2021-09-25T05:47:14.955336Z caller=compactor.go:41 msg="processed 80.00% of 15000 series"
level=info ts=2021-09-25T05:47:14.960193Z caller=compactor.go:41 msg="processed 90.00% of 15000 series"
level=info ts=2021-09-25T05:47:14.964705Z caller=compactor.go:41 msg="processed 100.00% of 15000 series"
level=info ts=2021-09-25T05:47:14.964768Z caller=tools_bucket.go:1136 msg="dry run finished. Changes should be printed to stderr"
level=info ts=2021-09-25T05:47:14.965101Z caller=main.go:160 msg=exiting
```

Below is an example output of the changelog. All the series that match the given deletion config will be deleted. The last column `[{1630713615001 1630715400001}]` represents the start and end time of the series.

```shell
cat /var/folders/ny/yy113mqs6szcpjy2qrnhq9rh0000gq/T/thanos-rewrite/01FGDQWKJ7H29B3V4HCQ691WN9/change.log

Deleted {__blockgen_target__="1", __name__="k8s_app_metric37", next_rollout_time="2021-09-03 23:30:00 +0000 UTC"} [{1630713615001 1630715400001}]
Deleted {__blockgen_target__="1", __name__="k8s_app_metric37", next_rollout_time="2021-09-04 00:30:00 +0000 UTC"} [{1630715415000 1630719015000}]
Deleted {__blockgen_target__="1", __name__="k8s_app_metric37", next_rollout_time="2021-09-04 01:30:00 +0000 UTC"} [{1630719015000 1630720815000}]
Deleted {__blockgen_target__="10", __name__="k8s_app_metric37", next_rollout_time="2021-09-03 23:30:00 +0000 UTC"} [{1630713615001 1630715400001}]
Deleted {__blockgen_target__="10", __name__="k8s_app_metric37", next_rollout_time="2021-09-04 00:30:00 +0000 UTC"} [{1630715415000 1630719015000}]
Deleted {__blockgen_target__="10", __name__="k8s_app_metric37", next_rollout_time="2021-09-04 01:30:00 +0000 UTC"} [{1630719015000 1630720815000}]
Deleted {__blockgen_target__="100", __name__="k8s_app_metric37", next_rollout_time="2021-09-03 23:30:00 +0000 UTC"} [{1630713615001 1630715400001}]
Deleted {__blockgen_target__="100", __name__="k8s_app_metric37", next_rollout_time="2021-09-04 00:30:00 +0000 UTC"} [{1630715415000 1630719015000}]
Deleted {__blockgen_target__="100", __name__="k8s_app_metric37", next_rollout_time="2021-09-04 01:30:00 +0000 UTC"} [{1630719015000 1630720815000}]
Deleted {__blockgen_target__="11", __name__="k8s_app_metric37", next_rollout_time="2021-09-03 23:30:00 +0000 UTC"} [{1630713615001 1630715400001}]
Deleted {__blockgen_target__="11", __name__="k8s_app_metric37", next_rollout_time="2021-09-04 00:30:00 +0000 UTC"} [{1630715415000 1630719015000}]
Deleted {__blockgen_target__="11", __name__="k8s_app_metric37", next_rollout_time="2021-09-04 01:30:00 +0000 UTC"} [{1630719015000 1630720815000}]
Deleted {__blockgen_target__="12", __name__="k8s_app_metric37", next_rollout_time="2021-09-03 23:30:00 +0000 UTC"} [{1630713615001 1630715400001}]
Deleted {__blockgen_target__="12", __name__="k8s_app_metric37", next_rollout_time="2021-09-04 00:30:00 +0000 UTC"} [{1630715415000 1630719015000}]
Deleted {__blockgen_target__="12", __name__="k8s_app_metric37", next_rollout_time="2021-09-04 01:30:00 +0000 UTC"} [{1630719015000 1630720815000}]
Deleted {__blockgen_target__="13", __name__="k8s_app_metric37", next_rollout_time="2021-09-03 23:30:00 +0000 UTC"} [{1630713615001 1630715400001}]
Deleted {__blockgen_target__="13", __name__="k8s_app_metric37", next_rollout_time="2021-09-04 00:30:00 +0000 UTC"} [{1630715415000 1630719015000}]
Deleted {__blockgen_target__="13", __name__="k8s_app_metric37", next_rollout_time="2021-09-04 01:30:00 +0000 UTC"} [{1630719015000 1630720815000}]
...
```

If the changelog output is expected, then we can use the same command in the first step, but with `--no-dry-run` flag to actually delete the data we want.

```shell
thanos tools bucket rewrite --no-dry-run --rewrite.to-delete-config-file config.yaml --objstore.config-file objstore.yaml --id <block ID>
```

The output is listed below.

```shell
thanos tools bucket rewrite --no-dry-run --rewrite.to-delete-config-file config.yaml --objstore.config-file ~/local-bucket-config.yaml --id 01FET1EK9BC3E0QD4886RQCM8K

level=info ts=2021-09-25T05:59:18.05232Z caller=factory.go:49 msg="loading bucket configuration"
level=info ts=2021-09-25T05:59:18.059056Z caller=tools_bucket.go:1078 msg="downloading block" source=01FET1EK9BC3E0QD4886RQCM8K
level=info ts=2021-09-25T05:59:18.074761Z caller=tools_bucket.go:1115 msg="changelog will be available" file=/var/folders/ny/yy113mqs6szcpjy2qrnhq9rh0000gq/T/thanos-rewrite/01FGDRJNST2EYDY2RKWFZJPGWJ/change.log
level=info ts=2021-09-25T05:59:18.108293Z caller=tools_bucket.go:1130 msg="starting rewrite for block" source=01FET1EK9BC3E0QD4886RQCM8K new=01FGDRJNST2EYDY2RKWFZJPGWJ toDelete="- matchers: '{__name__=\"k8s_app_metric37\"}'\n" toRelabel=
level=info ts=2021-09-25T05:59:18.395253Z caller=compactor.go:41 msg="processed 10.00% of 15000 series"
level=info ts=2021-09-25T05:59:18.406416Z caller=compactor.go:41 msg="processed 20.00% of 15000 series"
level=info ts=2021-09-25T05:59:18.419826Z caller=compactor.go:41 msg="processed 30.00% of 15000 series"
level=info ts=2021-09-25T05:59:18.428238Z caller=compactor.go:41 msg="processed 40.00% of 15000 series"
level=info ts=2021-09-25T05:59:18.436017Z caller=compactor.go:41 msg="processed 50.00% of 15000 series"
level=info ts=2021-09-25T05:59:18.444738Z caller=compactor.go:41 msg="processed 60.00% of 15000 series"
level=info ts=2021-09-25T05:59:18.452328Z caller=compactor.go:41 msg="processed 70.00% of 15000 series"
level=info ts=2021-09-25T05:59:18.465218Z caller=compactor.go:41 msg="processed 80.00% of 15000 series"
level=info ts=2021-09-25T05:59:18.477385Z caller=compactor.go:41 msg="processed 90.00% of 15000 series"
level=info ts=2021-09-25T05:59:18.485254Z caller=compactor.go:41 msg="processed 100.00% of 15000 series"
level=info ts=2021-09-25T05:59:18.485296Z caller=tools_bucket.go:1140 msg="wrote new block after modifications; flushing" source=01FET1EK9BC3E0QD4886RQCM8K new=01FGDRJNST2EYDY2RKWFZJPGWJ
level=info ts=2021-09-25T05:59:18.662059Z caller=tools_bucket.go:1149 msg="uploading new block" source=01FET1EK9BC3E0QD4886RQCM8K new=01FGDRJNST2EYDY2RKWFZJPGWJ
level=info ts=2021-09-25T05:59:18.667883Z caller=tools_bucket.go:1159 msg=uploaded source=01FET1EK9BC3E0QD4886RQCM8K new=01FGDRJNST2EYDY2RKWFZJPGWJ
level=info ts=2021-09-25T05:59:18.667921Z caller=tools_bucket.go:1167 msg="rewrite done" IDs=01FET1EK9BC3E0QD4886RQCM8K
level=info ts=2021-09-25T05:59:18.668136Z caller=main.go:160 msg=exiting
```

After rewriting, a new block `01FGDRJNST2EYDY2RKWFZJPGWJ` will be uploaded to your object store bucket.

However, the old block will not be deleted by default for the reason of safety. You can add `--delete-blocks` flag so that the source block will be marked as deletion after rewrite is done and will be deleted automatically if you have a compactor running against that bucket.

### Advanced deletion config

Multiple matchers can be added in the deletion config.

For example, the config file below specifies deletion for all series that match:

1. metric name `k8s_app_metric1`
2. metric name `k8s_app_metric37` and label `__blockgen_target__` that regexp matched `7.*`

```yaml
- matchers: '{__name__="k8s_app_metric37", __blockgen_target__=~"7.*"}'
- matchers: '{__name__="k8s_app_metric1"}'
```

It is also possible to specify a time range in case you do not want to delete the matching series from the entire block. The interval is specified via millisecond timestamps `mint` and `maxt`.

```yaml
- matchers: '{__name__="k8s_app_metric1"}'
  intervals:
    - mint: 1663279200000
      maxt: 1663292160000
```

For reference, you can convert the dates via GNU `date(1)`:

```shell
$ TZ=UTC date --date='2022-09-15 22:00:00' '+%s'
1663279200

$ TZ=UTC date --date='2022-09-16 01:36:00' '+%s'
1663292160

$ TZ=UTC date -d @1663279200 --iso=seconds
2022-09-15T22:00:00+00:00

$ TZ=UTC date -d @1663292160 --iso=seconds
2022-09-16T01:36:00+00:00
```

## Relabel series

```shell
thanos tools bucket rewrite --rewrite.to-relabel-config-file config.yaml --objstore.config-file objstore.yaml --id <block ID>
```

Series relabeling is needed when you want to rename your metrics or drop some high cardinality labels. The command is similar to rewrite deletion, but with `--rewrite.to-relabel-config-file` flag. The configuration is the same as [Prometheus relabel_config](https://prometheus.io/docs/prometheus/latest/configuration/configuration/#relabel_config). For example, the relabel config file does:

1. delete all series that match `{__name__="k8s_app_metric37"}`

2. rename `k8s_app_metric38` to `old_metric`

```yaml
- action: drop
  regex: k8s_app_metric37
  source_labels: [__name__]
- action: replace
  source_labels: [__name__]
  regex: k8s_app_metric38
  target_label: __name__
  replacement: old_metric
```

Example output of the changelog:

```shell
Deleted {__blockgen_target__="1", __name__="k8s_app_metric37", next_rollout_time="2021-09-03 23:30:00 +0000 UTC"} [{1630713615001 1630715400001}]
Deleted {__blockgen_target__="1", __name__="k8s_app_metric37", next_rollout_time="2021-09-04 00:30:00 +0000 UTC"} [{1630715415000 1630719015000}]
Deleted {__blockgen_target__="1", __name__="k8s_app_metric37", next_rollout_time="2021-09-04 01:30:00 +0000 UTC"} [{1630719015000 1630720815000}]
Relabelled {__blockgen_target__="1", __name__="k8s_app_metric38", next_rollout_time="2021-09-03 23:30:00 +0000 UTC"} {__blockgen_target__="1", __name__="old_metric", next_rollout_time="2021-09-03 23:30:00 +0000 UTC"}
Relabelled {__blockgen_target__="1", __name__="k8s_app_metric38", next_rollout_time="2021-09-04 00:30:00 +0000 UTC"} {__blockgen_target__="1", __name__="old_metric", next_rollout_time="2021-09-04 00:30:00 +0000 UTC"}
Relabelled {__blockgen_target__="1", __name__="k8s_app_metric38", next_rollout_time="2021-09-04 01:30:00 +0000 UTC"} {__blockgen_target__="1", __name__="old_metric", next_rollout_time="2021-09-04 01:30:00 +0000 UTC"}
Deleted {__blockgen_target__="10", __name__="k8s_app_metric37", next_rollout_time="2021-09-03 23:30:00 +0000 UTC"} [{1630713615001 1630715400001}]
Deleted {__blockgen_target__="10", __name__="k8s_app_metric37", next_rollout_time="2021-09-04 00:30:00 +0000 UTC"} [{1630715415000 1630719015000}]
Deleted {__blockgen_target__="10", __name__="k8s_app_metric37", next_rollout_time="2021-09-04 01:30:00 +0000 UTC"} [{1630719015000 1630720815000}]
Relabelled {__blockgen_target__="10", __name__="k8s_app_metric38", next_rollout_time="2021-09-03 23:30:00 +0000 UTC"} {__blockgen_target__="10", __name__="old_metric", next_rollout_time="2021-09-03 23:30:00 +0000 UTC"}
Relabelled {__blockgen_target__="10", __name__="k8s_app_metric38", next_rollout_time="2021-09-04 00:30:00 +0000 UTC"} {__blockgen_target__="10", __name__="old_metric", next_rollout_time="2021-09-04 00:30:00 +0000 UTC"}
Relabelled {__blockgen_target__="10", __name__="k8s_app_metric38", next_rollout_time="2021-09-04 01:30:00 +0000 UTC"} {__blockgen_target__="10", __name__="old_metric", next_rollout_time="2021-09-04 01:30:00 +0000 UTC"}
Deleted {__blockgen_target__="100", __name__="k8s_app_metric37", next_rollout_time="2021-09-03 23:30:00 +0000 UTC"} [{1630713615001 1630715400001}]
Deleted {__blockgen_target__="100", __name__="k8s_app_metric37", next_rollout_time="2021-09-04 00:30:00 +0000 UTC"} [{1630715415000 1630719015000}]
Deleted {__blockgen_target__="100", __name__="k8s_app_metric37", next_rollout_time="2021-09-04 01:30:00 +0000 UTC"} [{1630719015000 1630720815000}]
Relabelled {__blockgen_target__="100", __name__="k8s_app_metric38", next_rollout_time="2021-09-03 23:30:00 +0000 UTC"} {__blockgen_target__="100", __name__="old_metric", next_rollout_time="2021-09-03 23:30:00 +0000 UTC"}
Relabelled {__blockgen_target__="100", __name__="k8s_app_metric38", next_rollout_time="2021-09-04 00:30:00 +0000 UTC"} {__blockgen_target__="100", __name__="old_metric", next_rollout_time="2021-09-04 00:30:00 +0000 UTC"}
Relabelled {__blockgen_target__="100", __name__="k8s_app_metric38", next_rollout_time="2021-09-04 01:30:00 +0000 UTC"} {__blockgen_target__="100", __name__="old_metric", next_rollout_time="2021-09-04 01:30:00 +0000 UTC"}
...
```

If the output is expected, then you can add `--no-dry-run` flag to rewrite blocks.

## Rewrite Prometheus TSDB blocks

Thanos object storage supports `local filesystem`, which used local filesystem as bucket. If you want to delete/rewrite Prometheus TSDB, you can use the command below:

```shell
thanos tools bucket rewrite --prom-blocks --rewrite.to-relabel-config-file config.yaml --objstore.config-file local-bucket.yaml --id <block ID>
```

`--prom-blocks` disables external labels check when adding new blocks. For the local bucket config file, please refer to [this](https://thanos.io/tip/thanos/storage.md/#filesystem).
