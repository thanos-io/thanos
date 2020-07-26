---
title: Delete series for object storage
type: proposal
menu: proposals
status: approved
owner: bwplotka, Harshitha1234, metalmatze
---

### Ticket: https://github.com/thanos-io/thanos/issues/1598
## Summary

This design document proposes deletion of series for object storage in Thanos. This feature mainly causes changes in the Compactor and Store components and the object storage itself where the changes are expected to be reflected.

## Motivation

The main motivation for considering deletions in the object storage are the following use cases

*   **Accidental insertion of confidential data:** This is a scenario where a user accidentally inserts confidential data and wants to delete it immediately. In this case, the user expects their request to accelerate an immediate deletion of the series pertaining to the blocks concerned with that specific data to be deleted.
*   **GDPR:** Masking data and eventual deletion is expected.
*   **Deletions to sustain user requirements:** Let’s assume the user has some data which leads to some unexpected results or causes performance degradation (due to high cardinality) and the user wants to restore the previous data set-up for obtaining the desired results. In this scenario, the user would want to send a request to mask the data for the time being as there isn’t a high priority requirement to delete the data but eventually during the compaction the user can expect the data to be deleted by the compactor not leading to any major performance issues.
*   **Helps achieving series based retention (e.g rule aggregation) [#903](https://github.com/thanos-io/thanos/issues/903).**

## Goals

*   Unblock users and allow series deletion in the object storage using tombstones.
*   Deal with compaction and downsampling of blocks with tombstones.
*   Performing deletions at admin level.

## Proposed Approach

*   We propose to implement deletions using the tombstones approach.
*   To start off we use the Prometheus tombstone file format.
*   A user is expected to enter the following details for performing deletions:
    *   **label matchers**
    *   **start timestamp**
    *   **end timestamp** (start and end timestamps of the series data the user expects to be deleted)
*   The details are entered via a deletions API and they are processed by the compactor to create a tombstone file, if there’s a match for the details entered. Afterwards the tombstone file is uploaded to object storage making it accessible to other components.
*   **Why Compactor**? : The compactor is one of the very few components (next to Sidecar, Receiver, Ruler) that has write capabilities to object storage. At the same time it is only ever running once per object storage, so that we have a single writer to the deletion tombstones.
*   If the data with the requested details couldn’t be found in the storage an error message is returned back as a response to the request.
*   Store Gateway masks the series on processing the tombstones from the object storage.
*   **Perspectives to deal with Compaction of blocks having tombstones:**
    *   **Block with tombstones** Have a threshold to perform deletions on the compacted blocks ([In Prometheus](https://github.com/prometheus/prometheus/blob/f0a439bfc5d1f49cec113ee9202993be4b002b1b/tsdb/compact.go#L213), the blocks with big enough time range, that have >5% tombstones, are considered for compaction.) We solve the tombstones, if the tombstones are greater than than the threshold and then perform compaction. If not we attach the tombstone file to the new block. If multiple blocks are being compacted, we merge the tombstone files of the blocks whose threshold is not met.
    *   **Block with deletion-mark.json i.e., entire block marked for deletion:** Returns an error message as the entire block is going to be deleted.
*   **Perspectives to deal with Downsampling of blocks:**
    *   **Block with tombstones:** If the tombstones are less than the threshold we copy the tombstone file and attach it to the new downsampled block else we solve the tombstones and downsample the block. And the downsampled block with tombstones during its next compaction would again have the same cases as with the compaction of a block with tombstones.
    *   **Blocks without tombstones:** Downsampling happens...

##### Problems during implementations and possible ideas to solve :
During the implementation phase of this proposal one of the first problems we came across was that we had to pull the index of all the blocks for creating or appending a tombstone.

##### Proposed Alternative Approaches:
(1) To have a separate store like component with index cache.

(2) To have a different format for the tombstones. Instead of having <seriesRef, min, max> maybe we can have <matcher, min, max>
* **Idea 1:** Having a CLI tool for dealing with deletions.
* **Idea 2:** To have all tombstones at some single place and they are used while performing compaction or during query time.
* **Idea 3:** If we want to have an API for deletions, one way is to have the Store API configured in such a way that the compactor can use it to check for a match.
*Edge case:* Do we rebuild all the rows of a tombstone? Because we need to be aware that the tombstone is being included(or not) in the given Store API call.

*We're starting with (2) Idea 2*

#### Considerations :

*   Tombstones should be append only, so that we can solve tombstones and rewrite the blocks by performing changes. The old block and the tombstones are deleted during compaction.
*   We don’t want to add this feature to the sidecar. The sidecar is expected to be kept lightweight.

## Alternatives

1. A new component with write permission to the object storage, which creates the tombstones and exposes the deletions API. And the actual deletions are still performed by the compactor. To ensure object storage synchronization, a mutex lock system could be introduced. In this locking mechanism, when the new component or the compactor are operating on the object storage they start by first introducing a mutual exclusive lock to the block. This helps deal with the situation when the compactor and the new component are about to make changes to the same block.

**Advantages:** As the component has the write permission, it can perform immediate deletions.

## Action Plan

*   Add the deletion API (probably compactor) that only creates tombstones
*   Store Gateway should be able to mask based on the tombstones from object storage
*   Compactor solves the tombstones as per the proposed approach

## Future Work

*   Have a max waiting duration feature for performing deletions where a default value is considered if explicitly not specified by the user. (only after this time passes the deletions are performed by the compactor in the next compaction cycle)
*   Have the undoing deletions feature and there are two proposed ways
    *   API to undelete a time series - maybe delete the whole tombstones file?
    *   “Imaginary” deletion that can delete other tombstones