# Alerts

## Rule Groups

* [thanos-bucket-replicate](#thanos-bucket-replicate)
* [thanos-compact](#thanos-compact)
* [thanos-component-absent](#thanos-component-absent)
* [thanos-query](#thanos-query)
* [thanos-receive](#thanos-receive)
* [thanos-rule](#thanos-rule)
* [thanos-sidecar](#thanos-sidecar)
* [thanos-store](#thanos-store)

## thanos-bucket-replicate

|Name|Summary|Description|Severity|Runbook|
|---|---|---|---|---|
|ThanosBucketReplicateErrorRate|Thanos Replicate is failing to run.|Thanos Replicate is failing to run, {{$value  humanize}}% of attempts failed.|critical|[https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosbucketreplicateerrorrate](https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosbucketreplicateerrorrate)|
|ThanosBucketReplicateRunLatency|Thanos Replicate has a high latency for replicate operations.|Thanos Replicate {{$labels.job}} has a 99th percentile latency of {{$value}} seconds for the replicate operations.|critical|[https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosbucketreplicaterunlatency](https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosbucketreplicaterunlatency)|

## thanos-compact

|Name|Summary|Description|Severity|Runbook|
|---|---|---|---|---|
|ThanosCompactMultipleRunning|Thanos Compact has multiple instances running.|No more than one Thanos Compact instance should be running at once. There are {{$value}} instances running.|warning|[https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanoscompactmultiplerunning](https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanoscompactmultiplerunning)|
|ThanosCompactHalted|Thanos Compact has failed to run and is now halted.|Thanos Compact {{$labels.job}} has failed to run and now is halted.|warning|[https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanoscompacthalted](https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanoscompacthalted)|
|ThanosCompactHighCompactionFailures|Thanos Compact is failing to execute compactions.|Thanos Compact {{$labels.job}} is failing to execute {{$value  humanize}}% of compactions.|warning|[https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanoscompacthighcompactionfailures](https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanoscompacthighcompactionfailures)|
|ThanosCompactBucketHighOperationFailures|Thanos Compact Bucket is having a high number of operation failures.|Thanos Compact {{$labels.job}} Bucket is failing to execute {{$value  humanize}}% of operations.|warning|[https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanoscompactbuckethighoperationfailures](https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanoscompactbuckethighoperationfailures)|
|ThanosCompactHasNotRun|Thanos Compact has not uploaded anything for last 24 hours.|Thanos Compact {{$labels.job}} has not uploaded anything for 24 hours.|warning|[https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanoscompacthasnotrun](https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanoscompacthasnotrun)|

## thanos-component-absent

|Name|Summary|Description|Severity|Runbook|
|---|---|---|---|---|
|ThanosCompactIsDown|Thanos component has disappeared.|ThanosCompact has disappeared. Prometheus target for the component cannot be discovered.|critical|[https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanoscompactisdown](https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanoscompactisdown)|
|ThanosQueryIsDown|Thanos component has disappeared.|ThanosQuery has disappeared. Prometheus target for the component cannot be discovered.|critical|[https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosqueryisdown](https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosqueryisdown)|
|ThanosReceiveIsDown|Thanos component has disappeared.|ThanosReceive has disappeared. Prometheus target for the component cannot be discovered.|critical|[https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosreceiveisdown](https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosreceiveisdown)|
|ThanosRuleIsDown|Thanos component has disappeared.|ThanosRule has disappeared. Prometheus target for the component cannot be discovered.|critical|[https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosruleisdown](https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosruleisdown)|
|ThanosSidecarIsDown|Thanos component has disappeared.|ThanosSidecar has disappeared. Prometheus target for the component cannot be discovered.|critical|[https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanossidecarisdown](https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanossidecarisdown)|
|ThanosStoreIsDown|Thanos component has disappeared.|ThanosStore has disappeared. Prometheus target for the component cannot be discovered.|critical|[https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosstoreisdown](https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosstoreisdown)|

## thanos-query

|Name|Summary|Description|Severity|Runbook|
|---|---|---|---|---|
|ThanosQueryHttpRequestQueryErrorRateHigh|Thanos Query is failing to handle requests.|Thanos Query {{$labels.job}} is failing to handle {{$value  humanize}}% of "query" requests.|critical|[https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosqueryhttprequestqueryerrorratehigh](https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosqueryhttprequestqueryerrorratehigh)|
|ThanosQueryHttpRequestQueryRangeErrorRateHigh|Thanos Query is failing to handle requests.|Thanos Query {{$labels.job}} is failing to handle {{$value  humanize}}% of "query_range" requests.|critical|[https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosqueryhttprequestqueryrangeerrorratehigh](https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosqueryhttprequestqueryrangeerrorratehigh)|
|ThanosQueryGrpcServerErrorRate|Thanos Query is failing to handle requests.|Thanos Query {{$labels.job}} is failing to handle {{$value  humanize}}% of requests.|warning|[https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosquerygrpcservererrorrate](https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosquerygrpcservererrorrate)|
|ThanosQueryGrpcClientErrorRate|Thanos Query is failing to send requests.|Thanos Query {{$labels.job}} is failing to send {{$value  humanize}}% of requests.|warning|[https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosquerygrpcclienterrorrate](https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosquerygrpcclienterrorrate)|
|ThanosQueryHighDNSFailures|Thanos Query is having high number of DNS failures.|Thanos Query {{$labels.job}} have {{$value  humanize}}% of failing DNS queries for store endpoints.|warning|[https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosqueryhighdnsfailures](https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosqueryhighdnsfailures)|
|ThanosQueryInstantLatencyHigh|Thanos Query has high latency for queries.|Thanos Query {{$labels.job}} has a 99th percentile latency of {{$value}} seconds for instant queries.|critical|[https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosqueryinstantlatencyhigh](https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosqueryinstantlatencyhigh)|
|ThanosQueryRangeLatencyHigh|Thanos Query has high latency for queries.|Thanos Query {{$labels.job}} has a 99th percentile latency of {{$value}} seconds for range queries.|critical|[https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosqueryrangelatencyhigh](https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosqueryrangelatencyhigh)|
|ThanosQueryOverload|Thanos query reaches its maximum capacity serving concurrent requests.|Thanos Query {{$labels.job}} has been overloaded for more than 15 minutes. This may be a symptom of excessive simultanous complex requests, low performance of the Prometheus API, or failures within these components. Assess the health of the Thanos query instances, the connnected Prometheus instances, look for potential senders of these requests and then contact support.|warning|[https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosqueryoverload](https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosqueryoverload)|

## thanos-receive

|Name|Summary|Description|Severity|Runbook|
|---|---|---|---|---|
|ThanosReceiveHttpRequestErrorRateHigh|Thanos Receive is failing to handle requests.|Thanos Receive {{$labels.job}} is failing to handle {{$value  humanize}}% of requests.|critical|[https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosreceivehttprequesterrorratehigh](https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosreceivehttprequesterrorratehigh)|
|ThanosReceiveHttpRequestLatencyHigh|Thanos Receive has high HTTP requests latency.|Thanos Receive {{$labels.job}} has a 99th percentile latency of {{ $value }} seconds for requests.|critical|[https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosreceivehttprequestlatencyhigh](https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosreceivehttprequestlatencyhigh)|
|ThanosReceiveHighReplicationFailures|Thanos Receive is having high number of replication failures.|Thanos Receive {{$labels.job}} is failing to replicate {{$value  humanize}}% of requests.|warning|[https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosreceivehighreplicationfailures](https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosreceivehighreplicationfailures)|
|ThanosReceiveHighForwardRequestFailures|Thanos Receive is failing to forward requests.|Thanos Receive {{$labels.job}} is failing to forward {{$value  humanize}}% of requests.|info|[https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosreceivehighforwardrequestfailures](https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosreceivehighforwardrequestfailures)|
|ThanosReceiveHighHashringFileRefreshFailures|Thanos Receive is failing to refresh hasring file.|Thanos Receive {{$labels.job}} is failing to refresh hashring file, {{$value  humanize}} of attempts failed.|warning|[https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosreceivehighhashringfilerefreshfailures](https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosreceivehighhashringfilerefreshfailures)|
|ThanosReceiveConfigReloadFailure|Thanos Receive has not been able to reload configuration.|Thanos Receive {{$labels.job}} has not been able to reload hashring configurations.|warning|[https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosreceiveconfigreloadfailure](https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosreceiveconfigreloadfailure)|
|ThanosReceiveNoUpload|Thanos Receive has not uploaded latest data to object storage.|Thanos Receive {{$labels.instance}} has not uploaded latest data to object storage.|critical|[https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosreceivenoupload](https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosreceivenoupload)|

## thanos-rule

|Name|Summary|Description|Severity|Runbook|
|---|---|---|---|---|
|ThanosRuleQueueIsDroppingAlerts|Thanos Rule is failing to queue alerts.|Thanos Rule {{$labels.instance}} is failing to queue alerts.|critical|[https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosrulequeueisdroppingalerts](https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosrulequeueisdroppingalerts)|
|ThanosRuleSenderIsFailingAlerts|Thanos Rule is failing to send alerts to alertmanager.|Thanos Rule {{$labels.instance}} is failing to send alerts to alertmanager.|critical|[https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosrulesenderisfailingalerts](https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosrulesenderisfailingalerts)|
|ThanosRuleHighRuleEvaluationFailures|Thanos Rule is failing to evaluate rules.|Thanos Rule {{$labels.instance}} is failing to evaluate rules.|critical|[https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosrulehighruleevaluationfailures](https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosrulehighruleevaluationfailures)|
|ThanosRuleHighRuleEvaluationWarnings|Thanos Rule has high number of evaluation warnings.|Thanos Rule {{$labels.instance}} has high number of evaluation warnings.|info|[https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosrulehighruleevaluationwarnings](https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosrulehighruleevaluationwarnings)|
|ThanosRuleRuleEvaluationLatencyHigh|Thanos Rule has high rule evaluation latency.|Thanos Rule {{$labels.instance}} has higher evaluation latency than interval for {{$labels.rule_group}}.|warning|[https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosruleruleevaluationlatencyhigh](https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosruleruleevaluationlatencyhigh)|
|ThanosRuleGrpcErrorRate|Thanos Rule is failing to handle grpc requests.|Thanos Rule {{$labels.job}} is failing to handle {{$value  humanize}}% of requests.|warning|[https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosrulegrpcerrorrate](https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosrulegrpcerrorrate)|
|ThanosRuleConfigReloadFailure|Thanos Rule has not been able to reload configuration.|Thanos Rule {{$labels.job}} has not been able to reload its configuration.|info|[https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosruleconfigreloadfailure](https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosruleconfigreloadfailure)|
|ThanosRuleQueryHighDNSFailures|Thanos Rule is having high number of DNS failures.|Thanos Rule {{$labels.job}} has {{$value  humanize}}% of failing DNS queries for query endpoints.|warning|[https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosrulequeryhighdnsfailures](https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosrulequeryhighdnsfailures)|
|ThanosRuleAlertmanagerHighDNSFailures|Thanos Rule is having high number of DNS failures.|Thanos Rule {{$labels.instance}} has {{$value  humanize}}% of failing DNS queries for Alertmanager endpoints.|warning|[https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosrulealertmanagerhighdnsfailures](https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosrulealertmanagerhighdnsfailures)|
|ThanosRuleNoEvaluationFor10Intervals|Thanos Rule has rule groups that did not evaluate for 10 intervals.|Thanos Rule {{$labels.job}} has rule groups that did not evaluate for at least 10x of their expected interval.|info|[https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosrulenoevaluationfor10intervals](https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosrulenoevaluationfor10intervals)|
|ThanosNoRuleEvaluations|Thanos Rule did not perform any rule evaluations.|Thanos Rule {{$labels.instance}} did not perform any rule evaluations in the past 10 minutes.|critical|[https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosnoruleevaluations](https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosnoruleevaluations)|

## thanos-sidecar

|Name|Summary|Description|Severity|Runbook|
|---|---|---|---|---|
|ThanosSidecarBucketOperationsFailed|Thanos Sidecar bucket operations are failing|Thanos Sidecar {{$labels.instance}} bucket operations are failing|critical|[https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanossidecarbucketoperationsfailed](https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanossidecarbucketoperationsfailed)|
|ThanosSidecarNoConnectionToStartedPrometheus|Thanos Sidecar cannot access Prometheus, even though Prometheus seems healthy and has reloaded WAL.|Thanos Sidecar {{$labels.instance}} is unhealthy.|critical|[https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanossidecarnoconnectiontostartedprometheus](https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanossidecarnoconnectiontostartedprometheus)|

## thanos-store

|Name|Summary|Description|Severity|Runbook|
|---|---|---|---|---|
|ThanosStoreGrpcErrorRate|Thanos Store is failing to handle qrpcd requests.|Thanos Store {{$labels.job}} is failing to handle {{$value  humanize}}% of requests.|warning|[https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosstoregrpcerrorrate](https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosstoregrpcerrorrate)|
|ThanosStoreSeriesGateLatencyHigh|Thanos Store has high latency for store series gate requests.|Thanos Store {{$labels.job}} has a 99th percentile latency of {{$value}} seconds for store series gate requests.|warning|[https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosstoreseriesgatelatencyhigh](https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosstoreseriesgatelatencyhigh)|
|ThanosStoreBucketHighOperationFailures|Thanos Store Bucket is failing to execute operations.|Thanos Store {{$labels.job}} Bucket is failing to execute {{$value  humanize}}% of operations.|warning|[https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosstorebuckethighoperationfailures](https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosstorebuckethighoperationfailures)|
|ThanosStoreObjstoreOperationLatencyHigh|Thanos Store is having high latency for bucket operations.|Thanos Store {{$labels.job}} Bucket has a 99th percentile latency of {{$value}} seconds for the bucket operations.|warning|[https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosstoreobjstoreoperationlatencyhigh](https://github.com/thanos-io/thanos/tree/main/mixin/runbook.md#alert-name-thanosstoreobjstoreoperationlatencyhigh)|
