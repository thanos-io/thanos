# PromLTS kubernetes test setup.

This directory contains scripts and k8s resource definitions for PromLTS. This allows to test is locally, 
as well as on external clusters.

## Local mini-kube

To run minikube with Prometheus:

`bash ./kube/run-local.sh -i -d none` for linux or `bash ./kube/run-local.sh -i -d <vm-driver>` with some vm driver for MacOS (e.g virtualbox). 
What it does:
  - run minikube
  - setup kubectl and local custom kube/config
  - deploy local Prometheus which will be exposed on 10.0.0.88:9090 accessible from you local machine.
  
To use cluster from your terminal do:
`source ./kube/envs.sh`

From now on you can use `kubectl` as well as `minikube` command, including `minikube stop` to stop the whole cluster.

## Example setup.

This directory covers are required k8s manifest to start example setup that will include:
- Thanos headless service for discovery purposes.
- Prometheus + Thanos sidecar.
- Thanos query node

This setup will have GCS upload disabled, but will show how we can proxy requests from Prometheus.

This example can be easily extended to show the HA Prometheus use case. (TODO)

To run example setup:
1. `bash kube/apply-example.sh`

You will be now able to reach Prometheus on http://prometheus.default.svc.cluster.local:9090/graph
And Thanos Query UI on http://thanos-query.default.svc.cluster.local:19099/graph

Thanos Query UI should show exactly the same data as Prometheus.

To tear down example setup:
1. `bash kube/delete-example.sh`

## Long term storage setup

This example is running setup that is supposed to upload blocks to GCS for long term storage. This setup includes:
- Thanos headless service for discovery purposes.
- Prometheus + Thanos sidecar with GCS shipper configured
- Thanos query node
- Thanos store node.

To run example setup:
1. Create GCS bucket in your GCP project. Either name it "thanos-test" or put its name into
  * manifest/prometheus-gcs/deployment.yaml inside `"--gcs.bucket` flag.
  * manifest/thanos-query/deployment.yaml inside `"--gcs.bucket` flag.
2. Create service account that have permission to this bucket
3. Download JSON credentials for service account and run: `kubectl create secret generic gcs-credentials --from-file=<your-json-file>`
4. Run `bash kube/apply-lts.sh`

You will be now able to reach Prometheus on http://prometheus-gcs.default.svc.cluster.local:9090/graph
And Thanos Query UI on http://thanos-query.default.svc.cluster.local:19099/graph

Thanos Query UI should show exactly the same data as Prometheus, but also older data if it's running longer that 12h.

After 3h sidecar should upload first block to GCS. You can make that quicker by changing prometheus `storage.tsdb.{min,max}-block-duration` to smaller value (e.g 20m)

To tear down example setup:
1. `bash kube/delete-lts.sh`