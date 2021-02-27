# Alerts

## Rule Groups

* [thanos-bucket-replicate.rules](#thanos-bucket-replicate.rules)
* [thanos-compact.rules](#thanos-compact.rules)
* [thanos-component-absent.rules](#thanos-component-absent.rules)
* [thanos-query.rules](#thanos-query.rules)
* [thanos-receive.rules](#thanos-receive.rules)
* [thanos-rule.rules](#thanos-rule.rules)
* [thanos-sidecar.rules](#thanos-sidecar.rules)
* [thanos-store.rules](#thanos-store.rules)

## thanos-bucket-replicate.rules

|Name|Summary|Description|Severity|Runbook|
|---|---|---|---|---|
|ThanosBucketReplicateIsDown|Thanos Replicate has disappeared from Prometheus target discovery.|Thanos Replicate has disappeared from Prometheus target discovery.|critical|[https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosbucketreplicateisdown](https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosbucketreplicateisdown)|
|ThanosBucketReplicateErrorRate|Thanose Replicate is failing to run.|Thanos Replicate failing to run, {{ $value  humanize }}% of attempts failed.|critical|[https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosbucketreplicateerrorrate](https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosbucketreplicateerrorrate)|
|ThanosBucketReplicateRunLatency|Thanos Replicate has a high latency for replicate operations.|Thanos Replicate {{$labels.job}} has a 99th percentile latency of {{ $value }} seconds for the replicate operations.|critical|[https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosbucketreplicaterunlatency](https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosbucketreplicaterunlatency)|

## thanos-compact.rules

|Name|Summary|Description|Severity|Runbook|
|---|---|---|---|---|
|ThanosCompactMultipleRunning|Thanos Compact has multiple instances running.|No more than one Thanos Compact instance should be running at once. There are {{ $value }}|warning|[https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanoscompactmultiplerunning](https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanoscompactmultiplerunning)|
|ThanosCompactHalted|Thanos Compact has failed to run ans is now halted.|Thanos Compact {{$labels.job}} has failed to run and now is halted.|warning|[https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanoscompacthalted](https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanoscompacthalted)|
|ThanosCompactHighCompactionFailures|Thanos Compact is failing to execute compactions.|Thanos Compact {{$labels.job}} is failing to execute {{ $value  humanize }}% of compactions.|warning|[https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanoscompacthighcompactionfailures](https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanoscompacthighcompactionfailures)|
|ThanosCompactBucketHighOperationFailures|Thanos Compact Bucket is having a high number of operation failures.|Thanos Compact {{$labels.job}} Bucket is failing to execute {{ $value  humanize }}% of operations.|warning|[https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanoscompactbuckethighoperationfailures](https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanoscompactbuckethighoperationfailures)|
|ThanosCompactHasNotRun|Thanos Compact has not uploaded anything for last 24 hours.|Thanos Compact {{$labels.job}} has not uploaded anything for 24 hours.|warning|[https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanoscompacthasnotrun](https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanoscompacthasnotrun)|

## thanos-component-absent.rules

|Name|Summary|Description|Severity|Runbook|
|---|---|---|---|---|
|ThanosCompactIsDown|thanos component has disappeared from Prometheus target discovery.|ThanosCompact has disappeared from Prometheus target discovery.|critical|[https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanoscompactisdown](https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanoscompactisdown)|
|ThanosQueryIsDown|thanos component has disappeared from Prometheus target discovery.|ThanosQuery has disappeared from Prometheus target discovery.|critical|[https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosqueryisdown](https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosqueryisdown)|
|ThanosReceiveIsDown|thanos component has disappeared from Prometheus target discovery.|ThanosReceive has disappeared from Prometheus target discovery.|critical|[https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosreceiveisdown](https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosreceiveisdown)|
|ThanosRuleIsDown|thanos component has disappeared from Prometheus target discovery.|ThanosRule has disappeared from Prometheus target discovery.|critical|[https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosruleisdown](https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosruleisdown)|
|ThanosSidecarIsDown|thanos component has disappeared from Prometheus target discovery.|ThanosSidecar has disappeared from Prometheus target discovery.|critical|[https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanossidecarisdown](https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanossidecarisdown)|
|ThanosStoreIsDown|thanos component has disappeared from Prometheus target discovery.|ThanosStore has disappeared from Prometheus target discovery.|critical|[https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosstoreisdown](https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosstoreisdown)|

## thanos-query.rules

|Name|Summary|Description|Severity|Runbook|
|---|---|---|---|---|
|ThanosQueryHttpRequestQueryErrorRateHigh|Thanos Query is failing to handle requests.|Thanos Query {{$labels.job}} is failing to handle {{ $value  humanize }}% of "query" requests.|critical|[https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosqueryhttprequestqueryerrorratehigh](https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosqueryhttprequestqueryerrorratehigh)|
|ThanosQueryHttpRequestQueryRangeErrorRateHigh|Thanos Query is failing to handle requests.|Thanos Query {{$labels.job}} is failing to handle {{ $value  humanize }}% of "query_range" requests.|critical|[https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosqueryhttprequestqueryrangeerrorratehigh](https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosqueryhttprequestqueryrangeerrorratehigh)|
|ThanosQueryGrpcServerErrorRate|Thanos Query is failing to handle requests.|Thanos Query {{$labels.job}} is failing to handle {{ $value  humanize }}% of requests.|warning|[https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosquerygrpcservererrorrate](https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosquerygrpcservererrorrate)|
|ThanosQueryGrpcClientErrorRate|Thanos Query is failing to send requests.|Thanos Query {{$labels.job}} is failing to send {{ $value  humanize }}% of requests.|warning|[https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosquerygrpcclienterrorrate](https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosquerygrpcclienterrorrate)|
|ThanosQueryHighDNSFailures|Thanos Query is having high number of DNS failures.|Thanos Query {{$labels.job}} have {{ $value  humanize }}% of failing DNS queries for store endpoints.|warning|[https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosqueryhighdnsfailures](https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosqueryhighdnsfailures)|
|ThanosQueryInstantLatencyHigh|Thanos Query has high latency for queries.|Thanos Query {{$labels.job}} has a 99th percentile latency of {{ $value }} seconds for instant queries.|critical|[https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosqueryinstantlatencyhigh](https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosqueryinstantlatencyhigh)|
|ThanosQueryRangeLatencyHigh|Thanos Query has high latency for queries.|Thanos Query {{$labels.job}} has a 99th percentile latency of {{ $value }} seconds for range queries.|critical|[https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosqueryrangelatencyhigh](https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosqueryrangelatencyhigh)|

## thanos-receive.rules

|Name|Summary|Description|Severity|Runbook|
|---|---|---|---|---|
|ThanosReceiveHttpRequestErrorRateHigh|Thanos Receive is failing to handle requests.|Thanos Receive {{$labels.job}} is failing to handle {{ $value  humanize }}% of requests.|critical|[https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosreceivehttprequesterrorratehigh](https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosreceivehttprequesterrorratehigh)|
|ThanosReceiveHttpRequestLatencyHigh|Thanos Receive has high HTTP requests latency.|Thanos Receive {{$labels.job}} has a 99th percentile latency of {{ $value }} seconds for requests.|critical|[https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosreceivehttprequestlatencyhigh](https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosreceivehttprequestlatencyhigh)|
|ThanosReceiveHighReplicationFailures|Thanos Receive is having high number of replication failures.|Thanos Receive {{$labels.job}} is failing to replicate {{ $value  humanize }}% of requests.|warning|[https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosreceivehighreplicationfailures](https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosreceivehighreplicationfailures)|
|ThanosReceiveHighForwardRequestFailures|Thanos Receive is failing to forward requests.|Thanos Receive {{$labels.job}} is failing to forward {{ $value  humanize }}% of requests.|warning|[https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosreceivehighforwardrequestfailures](https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosreceivehighforwardrequestfailures)|
|ThanosReceiveHighHashringFileRefreshFailures|Thanos Receive is failing to refresh hasring file.|Thanos Receive {{$labels.job}} is failing to refresh hashring file, {{ $value  humanize }} of attempts failed.|warning|[https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosreceivehighhashringfilerefreshfailures](https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosreceivehighhashringfilerefreshfailures)|
|ThanosReceiveConfigReloadFailure|Thanos Receive has not been able to reload configuration.|Thanos Receive {{$labels.job}} has not been able to reload hashring configurations.|warning|[https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosreceiveconfigreloadfailure](https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosreceiveconfigreloadfailure)|
|ThanosReceiveNoUpload|Thanos Receive has not uploaded latest data to object storage.|Thanos Receive {{ $labels.instance }} of {{$labels.job}} has not uploaded latest data to object storage.|critical|[https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosreceivenoupload](https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosreceivenoupload)|

## thanos-rule.rules

|Name|Summary|Description|Severity|Runbook|
|---|---|---|---|---|
|ThanosRuleQueueIsDroppingAlerts|Thanos Rule is failing to queue alerts.|Thanos Rule {{$labels.job}} {{$labels.pod}} is failing to queue alerts.|critical|[https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosrulequeueisdroppingalerts](https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosrulequeueisdroppingalerts)|
|ThanosRuleSenderIsFailingAlerts|Thanos Rule is failing to send alerts to alertmanager.|Thanos Rule {{$labels.job}} {{$labels.pod}} is failing to send alerts to alertmanager.|critical|[https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosrulesenderisfailingalerts](https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosrulesenderisfailingalerts)|
|ThanosRuleHighRuleEvaluationFailures|Thanos Rule is failing to evaluate rules.|Thanos Rule {{$labels.job}} {{$labels.pod}} is failing to evaluate rules.|critical|[https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosrulehighruleevaluationfailures](https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosrulehighruleevaluationfailures)|
|ThanosRuleHighRuleEvaluationWarnings|Thanos Rule has high number of evaluation warnings.|Thanos Rule {{$labels.job}} {{$labels.pod}} has high number of evaluation warnings.|info|[https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosrulehighruleevaluationwarnings](https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosrulehighruleevaluationwarnings)|
|ThanosRuleRuleEvaluationLatencyHigh|Thanos Rule has high rule evaluation latency.|Thanos Rule {{$labels.job}}/{{$labels.pod}} has higher evaluation latency than interval for {{$labels.rule_group}}.|warning|[https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosruleruleevaluationlatencyhigh](https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosruleruleevaluationlatencyhigh)|
|ThanosRuleGrpcErrorRate|Thanos Rule is failing to handle grpc requests.|Thanos Rule {{$labels.job}} is failing to handle {{ $value  humanize }}% of requests.|warning|[https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosrulegrpcerrorrate](https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosrulegrpcerrorrate)|
|ThanosRuleConfigReloadFailure|Thanos Rule has not been able to reload configuration.|Thanos Rule {{$labels.job}} has not been able to reload its configuration.|info|[https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosruleconfigreloadfailure](https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosruleconfigreloadfailure)|
|ThanosRuleQueryHighDNSFailures|Thanos Rule is having high number of DNS failures.|Thanos Rule {{$labels.job}} has {{ $value  humanize }}% of failing DNS queries for query endpoints.|warning|[https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosrulequeryhighdnsfailures](https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosrulequeryhighdnsfailures)|
|ThanosRuleAlertmanagerHighDNSFailures|Thanos Rule is having high number of DNS failures.|Thanos Rule {{$labels.job}} has {{ $value  humanize }}% of failing DNS queries for Alertmanager endpoints.|warning|[https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosrulealertmanagerhighdnsfailures](https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosrulealertmanagerhighdnsfailures)|
|ThanosRuleNoEvaluationFor10Intervals|Thanos Rule has rule groups that did not evaluate for 10 intervals.|Thanos Rule {{$labels.job}} has {{ $value  humanize }}% rule groups that did not evaluate for at least 10x of their expected interval.|info|[https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosrulenoevaluationfor10intervals](https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosrulenoevaluationfor10intervals)|
|ThanosNoRuleEvaluations|Thanos Rule did not perform any rule evaluations.|Thanos Rule {{$labels.job}} did not perform any rule evaluations in the past 2 minutes.|critical|[https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosnoruleevaluations](https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosnoruleevaluations)|

## thanos-sidecar.rules

|Name|Summary|Description|Severity|Runbook|
|---|---|---|---|---|
|ThanosSidecarPrometheusDown|Thanos Sidecar cannot connect to Prometheus|Thanos Sidecar {{$labels.job}} {{$labels.pod}} cannot connect to Prometheus.|critical|[https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanossidecarprometheusdown](https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanossidecarprometheusdown)|
|ThanosSidecarUnhealthy|Thanos Sidecar is unhealthy.|Thanos Sidecar {{$labels.job}} {{$labels.pod}} is unhealthy for {{ $value }} seconds.|critical|[https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanossidecarunhealthy](https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanossidecarunhealthy)|

## thanos-store.rules

|Name|Summary|Description|Severity|Runbook|
|---|---|---|---|---|
|ThanosStoreGrpcErrorRate|Thanos Store is failing to handle qrpcd requests.|Thanos Store {{$labels.job}} is failing to handle {{ $value  humanize }}% of requests.|warning|[https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosstoregrpcerrorrate](https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosstoregrpcerrorrate)|
|ThanosStoreSeriesGateLatencyHigh|Thanos Store has high latency for store series gate requests.|Thanos Store {{$labels.job}} has a 99th percentile latency of {{ $value }} seconds for store series gate requests.|warning|[https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosstoreseriesgatelatencyhigh](https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosstoreseriesgatelatencyhigh)|
|ThanosStoreBucketHighOperationFailures|Thanos Store Bucket is failing to execute operations.|Thanos Store {{$labels.job}} Bucket is failing to execute {{ $value  humanize }}% of operations.|warning|[https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosstorebuckethighoperationfailures](https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosstorebuckethighoperationfailures)|
|ThanosStoreObjstoreOperationLatencyHigh|Thanos Store is having high latency for bucket operations.|Thanos Store {{$labels.job}} Bucket has a 99th percentile latency of {{ $value }} seconds for the bucket operations.|warning|[https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosstoreobjstoreoperationlatencyhigh](https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosstoreobjstoreoperationlatencyhigh)|
