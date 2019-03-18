package main

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

const (
	scrapeInterval      = time.Second
	numMetricsProducers = 50
	resultsPollInterval = 30 * time.Second
)

// This test will attempt to quantify the latency added by a thanos installation compared to vanilla prometheus:
// 1. Start a prometheus instance & thanos sidecar configured to scrape some metrics producers.
// 2. Start a thanos query layer targeting the sidecar on this prometheus.
// 3. Run various queries against both the query layer, and the direct prometheus endpoint.
// 4. Measure response times for both endpoints to find latency added by thanos.

func testLatency(logger log.Logger, opts *opts) error {
	// Create k8s client.
	k8sConfig, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		&clientcmd.ClientConfigLoadingRules{ExplicitPath: *opts.kubeConfig},
		&clientcmd.ConfigOverrides{CurrentContext: *opts.cluster},
	).ClientConfig()
	if err != nil {
		return errors.Wrap(err, "failed to create client config for cluster")
	}
	k8sClient, err := kubernetes.NewForConfig(k8sConfig)
	if err != nil {
		return errors.Wrap(err, "failed to create client set")
	}

	// Remove any resources in the cluster.
	if err := cleanCluster(logger, k8sClient); err != nil {
		return err
	}

	// Get the cluster ready.
	if err := bootstrapLatencyCluster(logger, opts, k8sClient); err != nil {
		return err
	}

	// Collect metrics for some time.
	level.Info(logger).Log("msg", "Deployment completed, pausing to collect metrics", "time", opts.gatherTime)
	time.Sleep(*opts.gatherTime)

	// Remove metric producers.
	level.Info(logger).Log("msg", "Deleting metrics producers")
	if err := k8sClient.AppsV1().ReplicaSets(loadgenNamespace).Delete("loadgen", nil); err != nil {
		return errors.Wrap(err, "failed to delete loadgen replicaset")
	}

	promResults, err := getQueryTimes(logger, opts, k8sClient, "mon-0", promNamespace, "querier-prom", 9090)
	if err != nil {
		return err
	}

	thanosResults, err := getQueryTimes(logger, opts, k8sClient, "thanos-query", thanosNamespace, "querier-thanos", thanosHTTPPort)
	if err != nil {
		return err
	}

	level.Info(logger).Log("component", "thanos", "results", string(thanosResults))
	level.Info(logger).Log("component", "prometheus", "results", string(promResults))
	return nil
}

func getQueryTimes(logger log.Logger, opts *opts, k8sClient *kubernetes.Clientset, podName string, podNamespace string, querierName string, port int32) ([]byte, error) {
	// Get query endpoint.
	queryPod, err := k8sClient.CoreV1().Pods(podNamespace).Get(podName, metav1.GetOptions{})
	if err != nil {
		return []byte{}, errors.Wrapf(err, "failed to get pod (%s)", podName)
	}

	// Deploy querier.
	level.Info(logger).Log("msg", "Running querier", "name", querierName)
	pod, service := createPrometheusQuerier(opts, querierName, fmt.Sprintf("%s:%d", queryPod.Status.PodIP, port), strings.Join(*opts.queries, ";"))
	if _, err := k8sClient.CoreV1().Pods(thanosNamespace).Create(pod); err != nil {
		return []byte{}, errors.Wrap(err, "failed to create querier pod")
	}
	if _, err := k8sClient.CoreV1().Services(thanosNamespace).Create(service); err != nil {
		return []byte{}, errors.Wrap(err, "failed to create querier service")
	}

	// Wait for queries to finish.
	var waitTime time.Duration
	for {
		svc, err := k8sClient.CoreV1().Services(thanosNamespace).Get(querierName, metav1.GetOptions{})
		if err != nil || len(svc.Status.LoadBalancer.Ingress) == 0 {
			time.Sleep(resultsPollInterval)
			waitTime += resultsPollInterval
			level.Info(logger).Log("msg", "Waiting for test results", "time", waitTime)
			continue
		}

		resp, err := http.Get(fmt.Sprintf("http://%s:%d/results", svc.Status.LoadBalancer.Ingress[0].IP, prometheusQuerierPort))
		// Retry on failure.
		if err != nil || resp.StatusCode != http.StatusOK {
			time.Sleep(resultsPollInterval)
			waitTime += resultsPollInterval
			level.Info(logger).Log("msg", "Waiting for test result", "time", waitTime)
			continue
		}

		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return []byte{}, errors.Wrap(err, "failed to read response body")
		}

		return body, nil
	}
}

func bootstrapLatencyCluster(logger log.Logger, opts *opts, k8sClient *kubernetes.Clientset) error {
	// Create namespaces.
	if err := createNamespaces(logger, k8sClient); err != nil {
		return errors.Wrap(err, "failed to create namespaces")
	}

	// Create admin role for prometheus.
	crb, sa := createAdminRole()
	if _, err := k8sClient.CoreV1().ServiceAccounts(promNamespace).Create(sa); err != nil {
		return errors.Wrap(err, "failed to create monitoring service account")
	}
	if _, err := k8sClient.RbacV1().ClusterRoleBindings().Create(crb); err != nil {
		return errors.Wrap(err, "failed to create clusterrolebinding")
	}

	cfg, err := createPrometheusConfig("^loadgen-.*$")
	if err != nil {
		return err
	}

	if _, err := k8sClient.CoreV1().ConfigMaps(promNamespace).Create(cfg); err != nil {
		return errors.Wrap(err, "failed to create prometheus configmap")
	}

	// Create headless services for thanos gossip members.
	if _, err := k8sClient.CoreV1().Services(thanosNamespace).Create(createThanosGossipService(thanosNamespace)); err != nil {
		return errors.Wrap(err, "failed to create headless service for thanos gossip")
	}
	if _, err := k8sClient.CoreV1().Services(promNamespace).Create(createThanosGossipService(promNamespace)); err != nil {
		return errors.Wrap(err, "failed to create headless service for thanos gossip")
	}

	// Create prometheus instance.
	level.Info(logger).Log("msg", "Creating prometheus statefulset")
	if _, err := k8sClient.AppsV1().StatefulSets(promNamespace).Create(createPrometheus(opts, "mon", "")); err != nil {
		return errors.Wrap(err, "failed to create prometheus statefulset")
	}

	level.Info(logger).Log("msg", "Creating metrics producers")
	if _, err := k8sClient.AppsV1().ReplicaSets(loadgenNamespace).Create(createLoadgen("loadgen", numMetricsProducers)); err != nil {
		return errors.Wrap(err, "failed to create metrics producer replicaset")
	}

	// Create thanos query layer.
	level.Info(logger).Log("msg", "Creating thanos query layer")
	svc, pod := createThanosQuery(opts)
	if _, err := k8sClient.CoreV1().Services(thanosNamespace).Create(svc); err != nil {
		return errors.Wrap(err, "failed to create thanos query service")
	}
	if _, err := k8sClient.CoreV1().Pods(thanosNamespace).Create(pod); err != nil {
		return errors.Wrap(err, "failed to create thanos query pod")
	}

	return nil
}
