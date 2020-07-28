At Red Hat we help to maintain many open source projects.

One of such project is the https://github.com/coreos/prometheus-operator, first project that
started leveraging CRDs to operate stateful resources like Prometheus. 

Thanos, while mostly built from stateless components, is well integrated with Prometheus Operator.

This quick demo will show you quickly how to deploy Prometheuses with Thanos with seamless HA support. 

## Apply Prometheus-Operator Custom Resource Definitions.

```
ls /root/mainfests/monitoring.coreos.com_*
```{{execute}}


```
kubectl apply -f /root/mainfests/monitoring.coreos.com_*
```{{execute}}
