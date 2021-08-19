---
type: proposal
title: Proposal Process
status: complete
owner: bwplotka
menu: proposals-done
---

# 2021-06: Proposal Process

* **Owners:**:
  * [`@bwplotka`](https://github.com/bwplotka)

* **Other docs:**
  * [KEP Process](https://github.com/kubernetes/enhancements/blob/master/keps/README.md)
  * [RHOBS Handbook Proposal Process](https://github.com/rhobs/handbook/pull/15)

> TL;DR: We would like to propose an improved, official proposal process for Thanos that clearly states when, where and how to create proposal/enhancement/design documents. We propose to store done, accepted and rejected proposals in markdown in the main Thanos repo.

## Why

More extensive architectural, process, or feature decisions are hard to explain, understand and discuss. It takes a lot of time to describe the idea, to motivate interested parties to review it, give feedback and approve. That's why it is essential to streamline the proposal process.

Given that as open-source community we work in highly distributed teams and work with multiple communities, we need to allow asynchronous discussions. This means it's essential to structure the talks into shared documents. Persisting those decisions, once approved or rejected, is equally important, allowing us to understand previous motivations.

There is a common saying [`"I've just been around long enough to know where the bodies are buried"`](https://twitter.com/AlexJonesax/status/1400103567822835714). We want to ensure the team related knowledge is accessible to everyone, every day, no matter if the team member is new or part of the team for ten years.

### Pitfalls of the current solution

Thanos process was actually following proposed process. We were just missing written rationales and official template that will make Thanos community easier to propose new ideas and motivate them!

## Goals

Goals and use cases for the solution as proposed in [How](#how):

* Allow easy collaboration and decision making on design ideas.
* Have a consistent design style that is readable and understandable.
* Ensure design docs are discoverable for better awareness and knowledge sharing about past decisions.
* Define a clear review and approval process.

## Non-Goals

* Define process for other type of documents.

## How

We want to propose an improved, official proposal process for Thanos Community that clearly states *when, where and how* to create proposal/enhancement/design documents.

See [the process and template defined here](../contributing/proposal-process.md)
