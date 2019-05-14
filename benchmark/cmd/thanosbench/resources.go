package main

import (
	"fmt"
	"path/filepath"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	prom "github.com/prometheus/prometheus/config"
	"gopkg.in/yaml.v2"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
	_ "k8s.io/client-go/plugin/pkg/client/auth/gcp"
)

const (
	promNamespace       = "mon"
	thanosNamespace     = "thanos"
	loadgenNamespace    = "gen"
	cleanupPollInterval = 2 * time.Second

	adminSAName = "admin"

	scraperConfigTemplatePath = "/opt/prometheus/config.yaml.tmpl"
	scraperConfigPath         = "/etc/prometheus/config.yaml"
	scraperDataPath           = "/var/prometheus"
	scraperDataVolumeSize     = "5Gi"

	thanosGossipServiceName = "thanos-gossip"
	thanosGossipPort        = int32(10900)
	thanosHTTPPort          = int32(10902)

	prometheusQuerierPort = int32(8080)
)

var (
	thanosGossipTmpl = fmt.Sprintf("%s.%%s.svc.cluster.local:%d", thanosGossipServiceName, thanosGossipPort)
)

// Deletes all deployed resources from the cluster.
func cleanCluster(logger log.Logger, k8sClient *kubernetes.Clientset) error {
	level.Info(logger).Log("msg", "Cleaning cluster")
	listOpts := metav1.ListOptions{
		LabelSelector: "group=loadtest",
	}

	// Delete namespaces (and everything in them).
	nss, err := k8sClient.CoreV1().Namespaces().List(listOpts)
	if err != nil {
		return errors.Wrap(err, "failed to list namespaces")
	}

	for _, ns := range nss.Items {
		level.Info(logger).Log("msg", "Deleting namespace", "name", ns.Name)
		if err := k8sClient.CoreV1().Namespaces().Delete(ns.Name, nil); err != nil {
			return errors.Wrapf(err, "failed to delete namespace (%s)", ns.Name)
		}
	}

	// Delete clusterrolebindings as they don't live in a namespace.
	crbs, err := k8sClient.RbacV1().ClusterRoleBindings().List(listOpts)
	if err != nil {
		return errors.Wrap(err, "failed to list clusterrolebindings")
	}
	for _, crb := range crbs.Items {
		level.Info(logger).Log("msg", "Deleting CRB", "name", crb.Name)
		if err := k8sClient.RbacV1().ClusterRoleBindings().Delete(crb.Name, nil); err != nil {
			return errors.Wrapf(err, "failed to delete crb (%s)", crb.Name)
		}
	}

	// Block until namespaces have finished deleting.
	level.Info(logger).Log("msg", "Waiting for namespaces to delete")
	var totalTime time.Duration
	for {
		nss, err := k8sClient.CoreV1().Namespaces().List(listOpts)
		if err != nil {
			return errors.Wrap(err, "failed to list namespaces")
		}

		// Return if all namespaces are active.
		allActive := true
		for _, ns := range nss.Items {
			allActive = allActive && ns.Status.Phase == v1.NamespaceActive
		}

		if allActive {
			return nil
		}

		time.Sleep(cleanupPollInterval)
		totalTime += cleanupPollInterval
		level.Info(logger).Log("msg", "Still deleting", "time", totalTime)
	}
}

// Creates a namespace with a given name.
func createNamespace(name string) *v1.Namespace {
	return &v1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Labels: map[string]string{
				"group": "loadtest",
			},
		},
	}
}

// Creates a serviceacconut & rolebinding to for a cluster-admin role.
func createAdminRole() (*rbacv1.ClusterRoleBinding, *v1.ServiceAccount) {
	om := metav1.ObjectMeta{
		Name:      adminSAName,
		Namespace: promNamespace,
		Labels: map[string]string{
			"group": "loadtest",
		},
	}

	return &rbacv1.ClusterRoleBinding{
			TypeMeta: metav1.TypeMeta{
				Kind:       "ClusterRoleBinding",
				APIVersion: "rbac.authorization.k8s.io/v1",
			},
			ObjectMeta: om,
			Subjects: []rbacv1.Subject{{
				Kind:      "ServiceAccount",
				Name:      adminSAName,
				Namespace: promNamespace,
			}},
			RoleRef: rbacv1.RoleRef{
				Kind: "ClusterRole",
				Name: "cluster-admin",
			},
		}, &v1.ServiceAccount{
			TypeMeta: metav1.TypeMeta{
				Kind:       "ServiceAccount",
				APIVersion: "v1",
			},
			ObjectMeta: om,
		}
}

// Returns a configmap holding prometheus config.
func createPrometheusConfig(podNameSelectorRegex string) (*v1.ConfigMap, error) {
	// Prometheus config.
	cfg := prom.Config{
		GlobalConfig: prom.GlobalConfig{
			ScrapeInterval: model.Duration(scrapeInterval),
			ScrapeTimeout:  model.Duration(scrapeInterval),

			ExternalLabels: model.LabelSet{
				"id": "$(MON_ID)",
			},
		},
		ScrapeConfigs: []*prom.ScrapeConfig{{
			JobName:     "loadgen",
			MetricsPath: "/metrics",
			ServiceDiscoveryConfig: prom.ServiceDiscoveryConfig{
				KubernetesSDConfigs: []*prom.KubernetesSDConfig{{
					Role: prom.KubernetesRolePod,
				}},
			},
			RelabelConfigs: []*prom.RelabelConfig{
				{ // Only scrape pods that are assigned to this prometheus.
					SourceLabels: model.LabelNames{"__meta_kubernetes_pod_name"},
					Regex:        prom.MustNewRegexp(podNameSelectorRegex),
					Action:       prom.RelabelKeep,
				},
				{
					SourceLabels: model.LabelNames{"__address__"},
					Replacement:  "${1}:8080",
					TargetLabel:  "__address__",
				},
			},
		}},
	}
	cfgBytes, err := yaml.Marshal(cfg)
	if err != nil {
		return nil, errors.Wrap(err, "failed to marshal prometheus config")
	}

	return &v1.ConfigMap{
		TypeMeta: metav1.TypeMeta{
			Kind:       "ConfigMap",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "prom",
			Namespace: promNamespace,
		},
		Data: map[string]string{
			filepath.Base(scraperConfigTemplatePath): string(cfgBytes),
		},
	}, nil
}

// Returns a statefulset representation containing a prometheus pod & thanos sidecar.
func createPrometheus(opts *opts, name string, bucket string) *appsv1.StatefulSet {
	om := metav1.ObjectMeta{
		Name:      name,
		Namespace: promNamespace,
		Labels: map[string]string{
			"app":                  name,
			"thanos-gossip-member": "true",
		},
	}

	fsGroup := int64(2000)
	runAsNonRoot := true
	runAsUser := int64(1000)
	ps := v1.PodSpec{
		ServiceAccountName: adminSAName,
		Containers: []v1.Container{
			{ // Prometheus.
				Name:  "prom",
				Image: "eu.gcr.io/io-crafty-shelter/prometheus:v2.2.1",
				Args: []string{
					"--config.file=" + scraperConfigPath,
					"--storage.tsdb.path=" + scraperDataPath,
					"--storage.tsdb.no-lockfile",
					"--web.enable-lifecycle",
					"--storage.tsdb.min-block-duration=2h",
					"--storage.tsdb.max-block-duration=2h",
					"--storage.tsdb.retention=2h",
					"--query.timeout=10m",
				},
				VolumeMounts: []v1.VolumeMount{
					{
						Name:      name,
						MountPath: scraperDataPath,
					},
					{
						Name:      "prom-config",
						MountPath: filepath.Dir(scraperConfigPath),
					},
				},
			},
			{ // Thanos.
				Name:  "thanos",
				Image: *opts.thanosImage,
				Args: []string{
					"sidecar",
					"--prometheus.url=http://localhost:9090",
					"--tsdb.path=" + scraperDataPath,
					"--gcs.bucket=" + bucket,
					"--reloader.config-file=" + scraperConfigTemplatePath,
					"--reloader.config-envsubst-file=" + scraperConfigPath,
					"--cluster.peers=" + fmt.Sprintf(thanosGossipTmpl, thanosNamespace),
					"--cluster.peers=" + fmt.Sprintf(thanosGossipTmpl, promNamespace),
				},
				Env: []v1.EnvVar{
					{Name: "MON_ID", Value: name},
				},
				VolumeMounts: []v1.VolumeMount{
					{
						Name:      name,
						MountPath: scraperDataPath,
					},
					{
						Name:      "prom-config",
						MountPath: filepath.Dir(scraperConfigPath),
					},
					{
						Name:      "prom-config-tmpl",
						MountPath: filepath.Dir(scraperConfigTemplatePath),
					},
				},
			},
		},
		SecurityContext: &v1.PodSecurityContext{
			FSGroup:      &fsGroup,
			RunAsNonRoot: &runAsNonRoot,
			RunAsUser:    &runAsUser,
		},
		Volumes: []v1.Volume{
			{
				Name: "prom-config-tmpl",
				VolumeSource: v1.VolumeSource{
					ConfigMap: &v1.ConfigMapVolumeSource{
						LocalObjectReference: v1.LocalObjectReference{
							Name: "prom",
						},
					},
				},
			},
			{
				Name: "prom-config",
				VolumeSource: v1.VolumeSource{
					EmptyDir: &v1.EmptyDirVolumeSource{},
				},
			},
		},
	}

	vc := []v1.PersistentVolumeClaim{{
		TypeMeta: metav1.TypeMeta{
			Kind:       "VolumeClaim",
			APIVersion: "v1",
		},
		ObjectMeta: om,
		Spec: v1.PersistentVolumeClaimSpec{
			AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce},
			Resources: v1.ResourceRequirements{
				Requests: map[v1.ResourceName]resource.Quantity{
					v1.ResourceStorage: resource.MustParse(scraperDataVolumeSize),
				},
			},
		},
	}}

	replicas := int32(1)
	return &appsv1.StatefulSet{
		TypeMeta: metav1.TypeMeta{
			Kind:       "StatefulSet",
			APIVersion: "apps/v1",
		},
		ObjectMeta: om,
		Spec: appsv1.StatefulSetSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"app": name,
				},
			},
			Template: v1.PodTemplateSpec{
				ObjectMeta: om,
				Spec:       ps,
			},
			VolumeClaimTemplates: vc,
		},
	}
}

// Returns a headless service for a namespace, to allow thanos to discover cluster peers. Peers should contain the
// "thanos-gossip-member=true" label to be selected for these services.
func createThanosGossipService(namespace string) *v1.Service {
	return &v1.Service{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Service",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      thanosGossipServiceName,
			Namespace: namespace,
		},
		Spec: v1.ServiceSpec{
			Selector: map[string]string{
				"thanos-gossip-member": "true",
			},
			Ports: []v1.ServicePort{{
				Name: "gossip",
				Port: thanosGossipPort,
			}},
			Type:      v1.ServiceTypeClusterIP,
			ClusterIP: v1.ClusterIPNone,
		},
	}
}

// The global query layer for thanos.
func createThanosQuery(opts *opts) (*v1.Service, *v1.Pod) {
	om := metav1.ObjectMeta{
		Name:      "thanos-query",
		Namespace: thanosNamespace,
		Labels: map[string]string{
			"app":                  "thanos-query",
			"thanos-gossip-member": "true",
		},
	}

	return &v1.Service{
			TypeMeta: metav1.TypeMeta{
				Kind:       "Service",
				APIVersion: "v1",
			},
			ObjectMeta: om,
			Spec: v1.ServiceSpec{
				Ports: []v1.ServicePort{
					{Name: "http", Port: int32(80), TargetPort: intstr.FromInt(10902)},
				},
				// NOTE(adam) this does not need to be external, is only enabled for debugging.
				Type: v1.ServiceTypeLoadBalancer,
				Selector: map[string]string{
					"app": "thanos-query",
				},
			},
		}, &v1.Pod{
			TypeMeta: metav1.TypeMeta{
				Kind:       "Pod",
				APIVersion: "v1",
			},
			ObjectMeta: om,
			Spec: v1.PodSpec{
				Containers: []v1.Container{{
					Name:  "thanos",
					Image: *opts.thanosImage,
					Args: []string{
						"query",
						"--log.level=debug",
						"--cluster.peers=" + fmt.Sprintf(thanosGossipTmpl, thanosNamespace),
						"--cluster.peers=" + fmt.Sprintf(thanosGossipTmpl, promNamespace),
					},
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							v1.ResourceCPU:    resource.MustParse("1"),
							v1.ResourceMemory: resource.MustParse("4000Mi"),
						},
					},
				}},
			},
		}
}

// The service allowing us to query the backend.
func createThanosStore(opts *opts, bucket string) *v1.Pod {
	return &v1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "thanos-store",
			Namespace: thanosNamespace,
			Labels: map[string]string{
				"thanos-gossip-member": "true",
			},
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{{
				Name:  "thanos",
				Image: *opts.thanosImage,
				Args: []string{
					"store",
					"--log.level=debug",
					"--cluster.peers=" + fmt.Sprintf(thanosGossipTmpl, thanosNamespace),
					"--cluster.peers=" + fmt.Sprintf(thanosGossipTmpl, promNamespace),
					"--gcs.bucket=" + bucket,
					"--index-cache-size=250MB",
					"--chunk-pool-size=2GB",
				},
			}},
		},
	}
}

// Creates a replicaset containing some number of metrics producers.
func createLoadgen(name string, numProducers int32) *appsv1.ReplicaSet {
	om := metav1.ObjectMeta{
		Name:      name,
		Namespace: loadgenNamespace,
		Labels: map[string]string{
			"app": name,
		},
	}

	return &appsv1.ReplicaSet{
		TypeMeta: metav1.TypeMeta{
			Kind:       "ReplicaSet",
			APIVersion: "apps/v1",
		},
		ObjectMeta: om,
		Spec: appsv1.ReplicaSetSpec{
			Replicas: &numProducers,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"app": name,
				},
			},
			Template: v1.PodTemplateSpec{
				ObjectMeta: om,
				Spec: v1.PodSpec{
					Containers: []v1.Container{{
						Name:  "loadgen",
						Image: "eu.gcr.io/io-crafty-shelter/thanos-loadgen:latest",
						Resources: v1.ResourceRequirements{
							Limits: v1.ResourceList{
								v1.ResourceCPU:    resource.MustParse("50m"),
								v1.ResourceMemory: resource.MustParse("10Mi"),
							},
						},
					}},
				},
			},
		},
	}
}

// Creates a pod with a querier targeting a prometheus endpoint.
func createPrometheusQuerier(opts *opts, name string, endpoint string, queries string) (*v1.Pod, *v1.Service) {
	om := metav1.ObjectMeta{
		Name:      name,
		Namespace: thanosNamespace,
		Labels: map[string]string{
			"app": name,
		},
	}

	return &v1.Pod{
			TypeMeta: metav1.TypeMeta{
				Kind:       "Pod",
				APIVersion: "v1",
			},
			ObjectMeta: om,
			Spec: v1.PodSpec{
				Containers: []v1.Container{{
					Name: "querier",
					//TODO(domgreen): move this to the same repository as Thanos
					Image: "eu.gcr.io/io-crafty-shelter/thanos-querier:latest",
					Args: []string{
						"--host=" + endpoint,
						"--queries=" + queries,
						"--range-offset-start=" + opts.queryRangeOffsetStart.String(),
						"--range-offset-end=" + opts.queryRangeOffsetEnd.String(),
						"--query-time=" + opts.queryTime.String(),
						"--server=true",
					},
				}},
			},
		}, &v1.Service{
			TypeMeta: metav1.TypeMeta{
				Kind:       "Service",
				APIVersion: "v1",
			},
			ObjectMeta: om,
			Spec: v1.ServiceSpec{
				Ports: []v1.ServicePort{{
					Name: "http",
					Port: prometheusQuerierPort,
				}},
				Type: v1.ServiceTypeLoadBalancer,
				Selector: map[string]string{
					"app": name,
				},
			},
		}
}

func createNamespaces(logger log.Logger, k8sClient *kubernetes.Clientset) error {
	if promNamespace == "default" {
		return errors.New("prometheus namespace cannot be default")
	}
	level.Info(logger).Log("msg", "Creating namespace for prometheus", "namespace", promNamespace)
	if _, err := k8sClient.CoreV1().Namespaces().Create(createNamespace(promNamespace)); err != nil {
		return err
	}

	if loadgenNamespace == "default" {
		return errors.New("loadgen namespace cannot be default")
	}
	level.Info(logger).Log("msg", "Creating namespace for loadgen", "namespace", loadgenNamespace)
	if _, err := k8sClient.CoreV1().Namespaces().Create(createNamespace(loadgenNamespace)); err != nil {
		return err
	}

	if thanosNamespace == "default" {
		return errors.New("thanos namespace cannot be default")
	}
	level.Info(logger).Log("msg", "Creating namespace for thanos", "namespace", thanosNamespace)
	if _, err := k8sClient.CoreV1().Namespaces().Create(createNamespace(thanosNamespace)); err != nil {
		return err
	}
	return nil
}
