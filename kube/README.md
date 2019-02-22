# PromLTS kubernetes test setup.

This directory contains example, runnable scripts and k8s resource definitions for Thanos.

## Local mini-kube

To run minikube with Prometheus:

`bash ./kube/run-local.sh -i -d none` for linux or `bash ./kube/run-local.sh -i -d <vm-driver>` with some vm driver for MacOS (e.g virtualbox). 
What it does:
  - run minikube
  - setup kubectl and local custom kube/config

To use cluster from your terminal do:
`source ./kube/envs.sh`

From now on you can use `kubectl` as well as `minikube` command, including `minikube stop` to stop the whole cluster.

## Example setup

This section covers are required k8s manifest to start example setup that will include:
- Thanos headless service for discovery purposes.
- Prometheus + Thanos sidecar.
- Thanos query node

This setup will have GCS upload disabled, but will show how we can proxy requests from Prometheus.

This example can be easily extended to show the HA Prometheus use case.

To run example setup:
1. `source ./kube/envs.sh`
2. `kubectl apply -f kube/manifests/prometheus.yaml`
3. `kubectl apply -f kube/manifests/thanos-query.yaml`

You will be now able to reach Prometheus on http://prometheus.default.svc.cluster.local:9090/graph
And Thanos Query UI on http://thanos-query.default.svc.cluster.local:9090/graph

If you cannot access these items from browser ensure that you have `10.0.0.10` address in your resolv.conf.
Alternatively you can look for service address using `kubectl get svc` and go to proper IP address.

Thanos Query UI should show exactly the same data as Prometheus.

To tear down example setup:
1. `source ./kube/envs.sh`
2. `kubectl delete -f kube/manifests/prometheus.yaml`
3. `kubectl delete -f kube/manifests/thanos-query.yaml`

## Long term storage setup

This example is running setup that is supposed to upload blocks to GCS for long term storage. This setup includes:
- Thanos headless service for discovery purposes.
- Prometheus + Thanos sidecar with GCS shipper configured
- Thanos query node
- Thanos store gateway.

To run example setup:
1. Create GCS bucket in your GCP project. Either name it "thanos-test" or put its name into
  * manifest/prometheus-gcs.yaml inside `"--gcs.bucket` flag.
  * manifest/thanos-store.yaml inside `"--gcs.bucket` flag.
2. Create service account that have permission to this bucket
3. Download JSON credentials for service account and run: `kubectl create secret generic gcs-credentials --from-file=<your-json-file>`
4. `source ./kube/envs.sh`
5. `kubectl apply -f kube/manifests/prometheus-gcs.yaml`
6. `kubectl apply -f kube/manifests/thanos-query.yaml`
7. `kubectl apply -f kube/manifests/thanos-store.yaml`

You will be now able to reach Prometheus on http://prometheus-gcs.default.svc.cluster.local:9090/graph
And Thanos Query UI on http://thanos-query.default.svc.cluster.local:9090/graph

Thanos Query UI should show exactly the same data as Prometheus, but also older data if it's running longer that 24h.

After 3h (default `storage.tsdb.{min,max}-block-duration` flag value) sidecar should upload first block to GCS.
You can make that quicker by changing prometheus `storage.tsdb.{min,max}-block-duration` to smaller value (e.g 20m)

To tear down example setup:
1. `source ./kube/envs.sh`
2. `kubectl delete -f kube/manifests/prometheus-gcs.yaml`
3. `kubectl delete -f kube/manifests/thanos-query.yaml`
4. `kubectl delete -f kube/manifests/thanos-store.yaml`