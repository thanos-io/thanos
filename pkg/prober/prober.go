// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package prober

// Prober represents health and readiness status of given component.
//
// From Kubernetes documentation https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-probes/ :
//
//   liveness: Many applications running for long periods of time eventually transition to broken states,
//   (healthy) and cannot recover except by being restarted.
//             Kubernetes provides liveness probes to detect and remedy such situations.
//
//   readiness: Sometimes, applications are temporarily unable to serve traffic.
//   (ready)    For example, an application might need to load large data or configuration files during startup,
//              or depend on external services after startup. In such cases, you don’t want to kill the application,
//              but you don’t want to send it requests either. Kubernetes provides readiness probes to detect
//              and mitigate these situations. A pod with containers reporting that they are not ready
//              does not receive traffic through Kubernetes Services.
type Probe interface {
	Healthy()
	NotHealthy(err error)
	Ready()
	NotReady(err error)
}
