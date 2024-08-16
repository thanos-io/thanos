---
type: proposal
title: Delete series for object storage
status: approved
owner: bwplotka, Harshitha1234, metalmatze
menu: proposals-accepted
---

### Ticket: https://github.com/thanos-io/thanos/issues/1598

## Summary

This design document proposes deletion of series for object storage in Thanos. This feature mainly causes changes in the Store component and masks the series from objstore corresponding to the user deletion requests and does not trigger actual deletions in the object storage which is proposed to be the future work extension of the current proposal.

## Motivation

The main motivation for considering deletions in the object storage are the following use cases

* **Accidental insertion of confidential data:** This is a scenario where a user accidentally inserts confidential data and wants to delete it immediately. In this case, the user expects their request to accelerate an immediate deletion of the series pertaining to the blocks concerned with that specific data to be deleted.
* **GDPR:** Masking data and eventual deletion is expected.
* **Deletions to sustain user requirements:** Let’s assume the user has some data which leads to some unexpected results or causes performance degradation (due to high cardinality) and the user wants to restore the previous data set-up for obtaining the desired results. In this scenario, the user would want to send a request to mask the data for the time being as there isn’t a high priority requirement to delete the data but eventually during the compaction the user can expect the data to be deleted by the compactor not leading to any major performance issues.
* **Helps achieving series based retention (e.g rule aggregation) [#903](https://github.com/thanos-io/thanos/issues/903).**

## Goals

* Unblock users and allow series deletion in the object storage using tombstones.
* Performing deletions at admin level.

## Proposed Approach

* We propose to implement deletions via the tombstones approach using a CLI tool.
* A tombstone is proposed to be a **Custom format global - single file per request**. Example of a tombstone file is as follows

```
{
		"matchers":     "up{source=\"prometheus\"}",
		"minTime":      -62167219200000,
		"maxTime":      253402300799000,
		"creationTime": 1598367375935,
		"author":       "John Gabriel",
		"reason":       "not specified",
}
```

* **Why Custom format global - single file per request**? :
  * As global tombstones are concerned, they would be immutable and deletion requests concurrency wouldn't be an issue (because each request creates a new object) and the compactor would load multiple tombstones upfront (before compacting), performs compaction and makes necessary changes to the block and then deletes the tombstones once done. If new tombstones are created in the meanwhile, it wouldn't cause a problem as the next compaction run will take them in account.
  * We can easily have multiple writers
  * No need to have index data of the blocks
* A user is expected to enter the following details for performing deletions:
  * **label matchers**
  * **start timestamp**
  * **end timestamp** (start and end timestamps of the series data the user expects to be deleted)
  * **creation timestamp**
  * **author name**
  * **reason for deletion**
* The entered details are processed by the CLI tool to create a tombstone file (unique for a request and irrespective of the presence of series), and the file is uploaded to the object storage making it accessible to all components.
* **Filename optimization**: The filename is created from the hash of matchers, minTime and maxTime. This helps re-write an existing tombstone, whenever a same request is made in the future hence avoiding duplication of the same request. (NOTE: Requests which entail common deletions still creates different tombstones.)
* Store Gateway masks the series on processing the global tombstone files from the object storage. At chunk level, whenever there's a match with the data corresponding to at least one of the tombstones, we skip the chunk, potentially resulting in the masking of chunk.

## Considerations

* When any of the timestamps i.e., start timestamp or end timestamp or both remain unspecified we go with default values.
* We don’t want to add this feature to the sidecar. The sidecar is expected to be kept lightweight.

## Alternatives

#### Where the deletion API sits

1. **Compactor:**
* Pros: Easiest as it's single writer in Thanos (but not for Cortex)
* Cons: Does not have quick access to querying or block indexes
1. **Store:**
* Pros: Does have quick access to querying or block indexes
* Cons: There would be many writers, and store never was about to write things, it only needed read access

#### How we store the tombstone and in what format

1. Prometheus Format per block
2. Prometheus Format global
3. Custom format per block
4. Custom format global - appendable file

Reasons for not choosing 1 & 2 tombstone formats i.e., **Prometheus format**:
* In this format, for creation of a tombstone, we will need the series ID. To find the series ID we need to pull the index of all the blocks which will never scale.
* This is a mutable file in Prometheus and which is not the case in Thanos Object Store.

Reasons for not choosing 3 i.e., **Custom format per block**:
* This becomes the function of number of blocks which hinders scalability and adds complexity.

Reasons for not choosing 4 i.e., **Custom format global - appendable file**:
* Read-after-write consistency cannot be guaranteed.

## Action Plan

* Add a CLI tool which creates tombstones
* Store Gateway should be able to mask based on the tombstones from object storage

## Challenges

* How to "compact" / "remove" **Custom format global - single file per request** files when applied.
* Do we want those deletion to be applied only during compaction or also for already compacted blocks. If yes how? 5%? And how to tell that when is 5%? What component would do that?
* Any rate limiting, what if there are too many files? Would that scale?

## Future Work

* Performing actual deletions in the object storage and rewriting the blocks based on tombstones by the compactor.
* Have a max waiting duration feature for performing deletions where a default value is considered if explicitly not specified by the user. (only after this time passes the deletions are performed by the compactor in the next compaction cycle)
* Have the undoing deletions feature and there are two proposed ways
  * API to undelete a time series - maybe delete the whole tombstones file?
  * “Imaginary” deletion that can delete other tombstones
