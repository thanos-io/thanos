---
type: proposal
title: Allow specifying tenant-specific external labels in Receivers
status: accepted
owner: haanhvu
menu: proposals-accepted
---

## 1 Related links/tickets

https://github.com/thanos-io/thanos/issues/5434

## 2 Why

We would like to select and query tenant blocks from Receivers in an arbitrary manner.

## 3 Pitfalls of the current solution

Currently, we can only add external labels to Receiver itself, not to each tenant in the Receiver. There’s currently no way to select and query tenant blocks in an arbitrary manner.

## 4 Goals

* Allow users to add arbitrary tenants’ external labels in the easiest way possible
* Allow users to change arbitrary tenants’ external labels in the easiest way possible
* Changes in tenants’ external labels are handled correctly
* Backward compatibility (e.g., with Receiver’s external labels) is assured
* Tenants’ external labels are handled separately in RouterIngestor, RouterOnly, and IngestorOnly modes

## 5 Non-goals

* Logically split RouterOnly and IngestorOnly modes (Issue [#5643](https://github.com/thanos-io/thanos/issues/5643)): If RouterOnly and IngestorOnly modes are logically split, implementing tenants’ external labels in RouterOnly and IngestorOnly modes would be less challenging. However, the split could be tricky and time-consuming, which might distract us from implementing tenants’ external labels. So we will not do it now.

## 6 Audience

Users who need to have cross-tenant information and do cross-tenant activities

## 7 How

We’ll add a new `external_labels` field in the hasring config. Something like this:
 ```
 [
    {
        "hashring": "tenant-a",
        "endpoints": ["tenant-a-1.metrics.local:19291/api/v1/receive", "tenant-a-2.metrics.local:19291/api/v1/receive"],
        "tenants": ["tenant-a"]
        "external_labels": ["label-1", "label-2", "label-3"]
    },
 ]
 ```
In [MultiTSDB](https://github.com/thanos-io/thanos/blob/4ce3fe19ebb39a308769fb2a9492295b1f113701/pkg/receive/multitsdb.go#L46), each tenant’s external labels will be mapped to its tenant ID.
Each tenant’s external labels will also be extended to the [list of labels](https://github.com/thanos-io/thanos/blob/4ce3fe19ebb39a308769fb2a9492295b1f113701/pkg/store/labelpb/label.go#L282) when the tenant’s TSDB instance is created.
Tenants’ external labels will be first implemented in RouterIngestor, since this is the main mode.
Once tenants’ external labels are implemented in RouterIngestor, we’ll handle changes in tenants’ external labels. So that when users change the values of `external_labels` field in the hasring config, the changes will appear on users’ query.
Finally, we will implement tenants’ external labels in RouterOnly and IngestorOnly modes.
For the tests, the foremost ones are testing backward compatibility (e.g., with Receiver’s external labels), defining one or multiple tenants’ external labels correctly, handling changes in tenants’ external labels correctly, and shipper detecting and uploading tenants’ external labels correctly to block storage. We may add more tests in the future but currently these are the most important ones to do first.

## 8 Implementation plan

* Add a new `external_labels` field in the hasring config
* Map tenant ID to tenant’s external labels
* Extend tenant’s external labels to the list of labels when the tenant’s TSDB instance is created
* Implement tenants’ external labels in RouterIngestor
* Handle changes for tenant’s external labels
* Implement tenants’ external labels in RouterOnly
*Implement tenants’ external labels in IngestorOnly

### 9 Test plan

* Backward compatibility (e.g., with Receiver’s external labels)
* Define one or multiple tenants’ external labels correctly
* Handle changes in tenants’ external labels correctly
* Shipper detects and uploads tenants’ external labels correctly to block storage
