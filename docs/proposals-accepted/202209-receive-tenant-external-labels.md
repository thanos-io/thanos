---
type: proposal
title: Allow specifying tenant-specific external labels in Receivers
status: accepted
owner: haanhvu
menu: proposals-accepted
---

## 1 Related links/tickets

* https://github.com/thanos-io/thanos/issues/5434

## 2 What problem(s) are we trying to solve?

We want to add arbitrary external labels to each tenant in Thanos Receiver.

## 3 How is it going to be helpful?

This new functionality is helpful in many use cases. Two typical ones are:
* Compact tenant blocks based on tenants’ external labels
* Query data from tenants which share the same external labels

## 4 Pitfalls of the current solution

Currently, we can only add external labels to Receivers. Each tenant in a Receiver only gets one external label, which is the tenant ID. This is not flexible enough to do complex compact and query works at the tenant level.

## 5 What are the possible solution approaches?

We’re seeing two solution approaches:
* Specify the tenants for each external label in Receiver: Since we already have the functionality of adding external labels to Receiver, is there a way to specify which tenants each external label goes to? Because many tenants may share the same external labels, this may be a straightforward way for users.
* Add external labels to each tenant directly: This way, we treat each tenant independently, which seems to be a more straightforward way in implementation. If goinf this way, we can start with the tenant configuration file.

## 6 Which solution approach is the most favorable?

After discussing internally in the issue team (@fpetkovski, @saswatamcode, @haanhvu), we agree the second approach may be the better one to start with since it’s more straightforward in implementation.

## 7 Potential challenges of the favorable approach?

We’re seeing two main challenges:
* Support external labels for decoupled routers and ingesters: Receive routers and ingestors are decoupled functionally, but currently they are not decoupled in code. Issue [#5643](https://github.com/thanos-io/thanos/issues/5643) has already been raised for this. With routers and ingestors intermingled in code, we need to find a way to support external labels for RouterOnly and IngestorOnly. One apparent way is to fix [#5643](https://github.com/thanos-io/thanos/issues/5643) at least partially enough before tackling this challenge.
* Handling changes for external labels

## 8 Implementation plan?

* RouterIngestor:
  - Add a new `external_labels` field in `tenant` field in hasring config
  - Map tenant's `external_labels` to tenant's `tenant_id`
  - Wire tenant’s `external_labels` to the tenant’s TSDB instance
* RouterOnly
* IngestorOnly

### 9 Test plan?
