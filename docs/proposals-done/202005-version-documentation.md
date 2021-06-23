---
type: proposal
title: Building A Versioning Plugin For Thanos
status: complete
owner: thisisobate
menu: proposals-done
Date: May 2020
---

## Problem

The Thanos website contains the docs area which is built by rendering the markdown files from the tip of master of the Thanos repository. This frequently causes confusion for users, as different Thanos versions have different features.

This proposal aims to solve this by:

1. building a version picker that serves as a tool which will aid easy access to other versions of the documentation. This picker will include links to change version (the version must be in the URL).
2. providing a documentation structure.
3. designing a workflow for managing docs that integrates with Thanos's Git workflow, i.e. updating corresponding docs on pull requests, cherry-picks, etc.

## Motivation

Many users (mostly developers) often want to look through the docs of previous releases, but searching for them manually by looking through the entire GitHub repository is time-consuming and ineffective. The site is built by rendering the markdown files from the tip of master of the Thanos repository, which causes confusion for users, as breaking changes are often merged into master. The versioning plugin allows the user to access the docs of both latest and previous releases on the Thanos documentation page, and it also allows the developer to fetch, write, and fix the docs of both latest and previous releases.

## Requirements

#### User Story (Latest)

* As a Thanos developer, I want to be able to just write docs for my current `master` version.
* As a Thanos developer, I want to build the website with versioned docs with a single action.
* As a Thanos developer, I don't want to store duplicated docs in a single repository version.

#### User Story (Previous release)

* As a Thanos developer, I want to be able to fix docs for an individual previous release.
* As a Thanos user, I want to be able to read docs for older versions of Thanos.

#### Use Case

##### Users

Thanos developers, Hugo developers, and general users.

##### Precondition

The user visits the Thanos documentation page.

##### Basic Course Of Events

1. The user indicates that the site is to show a list of docs (both latest and previous releases) by clicking the version picker (dropdown).
2. The site responds by displaying a list of versions allowing the user to make a selection.
3. The user makes a selection and the required docs are rendered on the page/site.

##### Postcondition

The site now has a version picker containing a list of versioned docs consisting of both the latest and older versions.

## Proposed Solution

#### 1. Version Picker

Currently, the documentation resides under the docs/ folder of the Thanos repository. It is built by Hugo. It will have a proper drop-down menu just like the Prometheus website's [`drop-down menu`](https://prometheus.io/docs/introduction/overview/), which will enable proper versioning. This user-facing tool will be built using HTML and CSS.

#### 2. Documentation Structure

We want to propose a method called "Directory Sub Branching". Directory Sub branching means creating different sub branches in the `versioned` folder of the Thanos repository. Since the current architecture of the Thanos website is this:

```|- website
    |- archetypes
    |- layout
    |- data
    |- static
    |- resources
    |- tmp
        |- public
        |- docs-pre-processed
```

We want to add an additional `versioned` folder within the website's `tmp` directory. For example:

```|- website
    |- archetypes
    |- layout
    |- data
    |- static
    |- resources
    |- tmp
        |- public
        |- docs-pre-processed
            |- versioned
                |- master
                |- version 0.13.0
                |- version 0.12.2
                |- other-folder-for-other-releases
```

*NOTE: `tmp` directory is not committed, just temporarily built. The current version of docs lives in the `master` folder*

#### 3. Building a versioning plugin

Creating a plugin that can automate these processes would save us a lot of development time and stress. This approach promises to be useful when it comes to versioning different release in the Thanos website.

##### Workflow

1. The developer makes all the necessary edits in `/docs` on master.
2. The developer proceeds by committing a new release (i.e Release 0.x).
3. CI run some `make web` or `make web-serve` command.
4. Before anything, CI generates the docs and places them in a `versioned` tmp folder.
5. the rest of the web command is executed.

*NOTE: generated docs are not committed, just temporarily built.*

## FAQ

This section consists of some important questions and answers that are frequently asked by users.

##### How do you specify what part of the docs you want to version without cloning every file? docs.yaml

We hope to have a single, flexible configuration file (`docs.yaml`) that will help the developer in specifying what part of the docs they need without cloning all the docs. We could have this as a single file on master. The config file will look like this:

```versioned:
   default:
   - docs/components/*.md
   - docs/storage.md

  overrides:
     release-0.12:
     - docs/storage2221241.md
```

##### Alternative path

We could have this file on master, so the current `make web` will

1. Parse `docs.yaml` from master and use this as a default.
2. Check out each release (e.g. `release-0.10`)
3. Is there docs.yaml?
   * Yes - parse the file and use it
   * No - use docs.yaml from master

The design of docs.yaml will look like this:

```versioned:
   - docs/components/*.md
   - docs/storage2221241.md
```

##### What would the plugin look like?

CLI (`make generate-versioned-docs`)

##### What is the plan in terms of plugin placement?

We hope to start in the Thanos project and then move the project outside to a separate repository.

##### How we do fixes to release docs for older release, e.g. v0.12, work?

We will edit the particular release (release-0.12) branch and add a commit. The then tool fetches the latest commit on this branch and uses it to generate the docs. We expect to fetch docs for minor releases without breaking them with patches. We encourage immutability across all our release tags in Thanos.

##### How does the tool discover the releases for fixes?

With a regular expression. Instead of the developer manually checking out the individual release branches, the tool handles it for them by using a regex to select and clone valid release branches. The config file for the tool will have a `release-branch-regex` field. For Thanos, the regular expression would be something like `release-(.*)`.

## Summary

We understand [Cortex](https://github.com/cortexproject/cortex/pull/2349) is working on this as well so we are learning from their approach and knowledge.

## Future Work

We hope to rewrite the versioning plugin using Golang. We care a lot about code maintainability and because we use Golang as our primary programming language, it will be easier for developers to contribute to this plugin and make it even better.
