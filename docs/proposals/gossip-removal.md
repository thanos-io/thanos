# Deprecated gossip clustering in favor of File SD

Status: draft | **in-review** | rejected | accepted | complete

Implementation Owner: [@bplotka](https://github.com/Bplotka)

Ticket: https://github.com/improbable-eng/thanos/issues/484

## Summary

It is becoming clear that we need to remove gossip protocol as our main way of communication between Thanos Querier and
other components. Static configuration seems to be well enough for our simple use cases. To give users more flexibility 
(similar to gossip auto-join logic), we already wanted to introduce a [File SD](https://github.com/improbable-eng/thanos/issues/492) 
that allows changing `StoreAPI`s on-the-fly.

## Motivation

[Gossip](https://en.wikipedia.org/wiki/Gossip_protocol) protocol (with the [membership](https://github.com/hashicorp/memberlist) implementation) 
was built into Thanos from the very beginning. The main advantages over other solution to connect components were:
* Auto-join and auto-drop of components based on health checks.
* Propagation of tiny metadata.

After a couple of month of maintaining Thanos project and various discussions with different users, we realized that those advantages
are not outstanding anymore and are not worth keeping, compared to the issues gossip causes. There are numerous reasons why we should 
deprecate gossip:
* Gossip has been proven to be extremely confusing for the new users. Peer logic and really confusing `cluster.advertise-address` that
was sometimes able to magically deduce private IP address (and sometimes not!) were leading to lots of questions and issues. 
Something that was made for quick ramp-up into Thanos ("just click the button and it auto-joins everything") created lots of confusion
and made it harder to experiment.
* Auto-join logic has its price. All peers connected were aware of each other. All were included in metadata propagation. 
Membership project community made an awesome job to optimize N^N communication (all-to-all), but nevertheless, it is something
that is totally unnecessary to Thanos. And it led to wrong assumptions that e.g sidecar needs to be aware of another sidecar etc.
*Thanos Querier is the only component that requires to be aware of other `StoreAPI`s*. It is clear that `all-to-all` communication is
neither necessary nor educative.
* Unless specifically configured (which requires advanced knowledge) Gossip uses mix of TCP and UPD underneath its 
custom app level protocol. This is a no-go if you use L7 loadbalancers and proxies. 
* Global gossip is really difficult to achieve. BTW, If you know how to setup this, please write a blog about it! (: 
* With the addition of the simplest possible solution to give Thanos Querier knowledge where are `StoreAPI`s (static `--store` flag),
 we needed to implement health check and metadata propagation anyway. In fact, `StoreAPI.Info` was already there all the time.
* Gossip operates per `peer` level and there is no way you can abstract multiple peers behind loadbalancer. This hides easy
solutions from our eyes, e.g how to make Store Gateway HA. Without gossip, you can just use Kubernetes HA Service or any other loadbalancer. 
To support Store Gateway HA for gossip we would end up implementing LB logic in Thanos Querier (like proposed [here](https://github.com/improbable-eng/thanos/pull/404))
* At some point we want to be flexible and allow other discovery mechanisms. Gossip does not work for everyone and static flags
are too.. static. (: We need [File SD](https://github.com/improbable-eng/thanos/issues/492) for flexibility anyway.
* One thing less to maintain.

## Goals

* Remove gossip support from code (Decision after initial feedback)
  * We are still RC, so technically there are no API guarantees yet.
* Leave --store flags
* Make sure [File SD](https://github.com/improbable-eng/thanos/issues/492) is in place and documented before removal.

## Proposal: Steps

* Add File Service Discovery (SD): https://github.com/improbable-eng/thanos/issues/492
* Remove gossip from the documentation, be clear what talks with what (!)
* Deprecate gossip in code.
* Remove gossip code and flags AFTER some time (month?)

### Backwards compatibility

**We will break backward compatibility**, as gossip was the main way to connect components. As it is not very nice, it will
ensure stability and better user experience and flexibility in the future. As we are in 0.1.0 RC process, we will not break any guarantee.

As help for migration we might want to add sidecar that uses FileSD but support gossip clustering.

## Alternatives considered

1. Just improve documentation of gossip

* It will not help in efficiency/unnecessary traffic problem
* We will have 2 ways of communication as it is now - hard to maintain and think about.

## Related Future Work

1. Add example sidecars that will provide more sophisticated discovery and integrate with our File SD (e.g k8s SD)

