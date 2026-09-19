---
type: proposal
title: Allow statically specifying tenant-specific external labels in Receivers
status: accepted
owner: haanhvu
menu: proposals-accepted
---

## 1 Related links/tickets

https://github.com/thanos-io/thanos/issues/5434

## 2 Why

We would like to do cross-tenant activities like grouping tenants' blocks or querying tenants that share the same attributes. Tenant's external labels can help us do those.

## 3 Pitfalls of the current solution

Currently, we can only add external labels to Receiver itself, not to each tenant in the Receiver. So we can't do cross-tenant activities like grouping tenants' blocks or querying tenants that share the same attributes.

## 4 Goals

* Allow users to statically add arbitrary tenants’ external labels in the easiest way possible
* Allow users to statically change arbitrary tenants’ external labels in the easiest way possible
* Changes in tenants’ external labels are handled correctly
* Backward compatibility (e.g., with Receiver’s external labels) is assured
* Tenants’ external labels are handled separately in RouterIngestor, RouterOnly, and IngestorOnly modes

## 5 Non-goals

* Logically split RouterOnly and IngestorOnly modes (Issue [#5643](https://github.com/thanos-io/thanos/issues/5643)): If RouterOnly and IngestorOnly modes are logically split, implementing tenants’ external labels in RouterOnly and IngestorOnly modes would be less challenging. However, fixing this issue will not be a goal of this proposal, because it's not directly related to tenants' external labels. Regardless, fixing this issue before implementing tenants’ external labels in RouterOnly and IngestorOnly modes would be the best-case scenario.
* Dynamically extract tenants' external labels from time series' data: This proposal only covers statically specifying tenants' external labels. Dynamically receiving and extracting tenants' external labels from time series' data will be added as a follow-up to this proposal.

## 6 Audience

Users who are admin personas and need to perform admin operations on Thanos for multiple tenants

## 7 How

In the hashring config, there will be new field `external_labels`. Something like this:

```
    [
        {
            "hashring": "tenant-a-b",
            "endpoints": ["127.0.0.1:10901"],
            "tenants": ["tenant-a, tenant-b"]
            "external_labels": ["key1=value1", "key2=value2", "key3=value3"]
        },
    ]
```

In Receivers' MultiTSDB, external labels will be extended to each corresponding tenant's label set when the tenant's TSDB is started.

Next thing we have to do is handling changes for tenants' external labels. That is, whenever users make any changes to tenants' external labels, Receivers' MultiTSDB will update those changes in each corresponding tenant's label set.

We will handle the cases of hard tenancy first. Once tenants' external labels can be handled in those cases, we will move to soft tenancy cases.

Tenants’ external labels will be first implemented in RouterIngestor, since this is the most commonly used mode.

After that, we can implement tenants’ external labels in RouterOnly and IngestorOnly modes. As stated above, the best-case scenario would be logically splitting RouterOnly and IngestorOnl (Issue [#5643](https://github.com/thanos-io/thanos/issues/5643)) before implement tenants’ external labels in each.

For the tests, the foremost ones are testing defining one or multiple tenants’ external labels correctly, handling changes in tenants’ external labels correctly, backward compatibility with Receiver’s external labels, and shipper detecting and uploading tenants’ external labels correctly to block storage. We may add more tests in the future but currently these are the most important ones to do first.

## 8 Implementation plan

* Add a new `external_labels` field in the hashring config
* Allow MultiTSDB to extend external labels to each corresponding tenant's label set
* Allow MultiTSDB to update each tenant's label set whenever its external labels change
* Handle external labels in soft tenancy cases
* Implement tenants’ external labels in RouterOnly
* Implement tenants’ external labels in IngestorOnly

### 9 Test plan

* Defining one or multiple tenants’ external labels correctly
* Handling changes in tenants’ external labels correctly
* Backward compatibility with Receiver’s external labels
* Shipper detecting and uploading tenants’ external labels correctly to block storage

### 10 Follow-up

* Dynamically extract tenants' external labels from time series' data: Once statically specifying tenants' external labels have been implemented and tested successfully and completely, we can think of implementing dynamically receiving and extracting tenants' external labels from time series' data.
* Automatically making use of tenants' external labels: We can think of the most useful use cases with tenants' external labels and whether we should automate any of those use cases. One typical case is automatically grouping new blocks based on tenants' external labels.

(Both of these are Ben's ideas expressed [here](https://github.com/thanos-io/thanos/pull/5720#pullrequestreview-1167923565).)
