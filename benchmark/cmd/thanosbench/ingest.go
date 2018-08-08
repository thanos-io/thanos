package main

import (
	"fmt"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

// This test will:
// 1. Spin up a number of prometheus+thanos sidecars.
// 2. Spin up a number of metrics producers for each scraper.
// 3. Run a thanos-query in front of all the scrapers & expose it.

func testIngest(logger log.Logger, opts *opts) error {
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

	// Create resources.
	if err := bootstrapIngestCluster(logger, opts, k8sClient); err != nil {
		return err
	}

	return nil
}

func bootstrapIngestCluster(logger log.Logger, opts *opts, k8sClient *kubernetes.Clientset) error {
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

	// Create headless services for thanos gossip members.
	if _, err := k8sClient.CoreV1().Services(thanosNamespace).Create(createThanosGossipService(thanosNamespace)); err != nil {
		return errors.Wrap(err, "failed to create headless service for thanos gossip")
	}
	if _, err := k8sClient.CoreV1().Services(promNamespace).Create(createThanosGossipService(promNamespace)); err != nil {
		return errors.Wrap(err, "failed to create headless service for thanos gossip")
	}

	cfg, err := createPrometheusConfig("^loadgen-$(MON_ID)-.*$")
	if err != nil {
		return err
	}

	if _, err := k8sClient.CoreV1().ConfigMaps(promNamespace).Create(cfg); err != nil {
		return errors.Wrap(err, "failed to create prometheus configmap")
	}

	for i := 0; i < *opts.numPrometheus; i++ {
		name := fmt.Sprintf("mon-%d", i)

		loadgenName := fmt.Sprintf("loadgen-%s", name)
		level.Info(logger).Log("msg", "Creating loadgen", "name", loadgenName)
		if _, err := k8sClient.AppsV1().ReplicaSets(loadgenNamespace).Create(createLoadgen(loadgenName, int32(*opts.numLoadgen))); err != nil {
			return errors.Wrapf(err, "failed to create loadgen (%s)", loadgenName)
		}

		level.Info(logger).Log("msg", "Creating prometheus", "name", name)
		if _, err := k8sClient.AppsV1().StatefulSets(promNamespace).Create(createPrometheus(opts, name, "")); err != nil {
			return errors.Wrapf(err, "failed to create prometheus (%s)", name)
		}
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
