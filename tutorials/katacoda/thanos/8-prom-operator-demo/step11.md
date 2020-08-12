## Ok, Let's get back to our goal: Maintain SLA across "clusters" (namespaces in our demo case)

Let's use following tool to create our SLO based on alert: https://promtools.matthiasloibl.com/

Let's generate sample SLO:

![slo](/root/promtools.png)

We can paste the output here:

`manifests/rulerslo/thanos-ruler-slo-rule.yaml`{{open}}

And apply!

```
kubectl apply -f /root/manifests/rulerslo/
```{{execute}}

Let's now see the UI of Ruler: [Thanos Ruler UI](https://[[HOST_SUBDOMAIN]]-30094-[[KATACODA_HOST]].environments.katacoda.com)
