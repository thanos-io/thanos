---
title: Release Process
type: docs
menu: thanos
slug: /release-process.md
---

This page describes the release cadence and process for Thanos project.

NOTE: As [Semantic Versioning](http://semver.org/spec/v2.0.0.html) states all 0.y.z releases can contain breaking changes in API (flags, grpc API, any backward compatibility)

## Cadence

We aim for regular and strict one release per 6 weeks. 6 weeks is counter from first release candidate to another. 
This means that there is no *code freeze* or anything like that. We plan to stick to the exact 6 weeks, so there is no rush
into being within release (except bug fixes).

No feature should block release.

Additionally we (obviously) aim for `master` branch being stable.

## For maintainers: Cutting individual release

We will choose a release shepherd for each minor release. 

Release shepherd responsibilities:
* Perform releases (from first RC to actual release).
* Announce all releases on all communication channels.

Process of releasing a *minor* Thanos version:
1. Release `v<major>.<minor+1>.0-rc.0`
1. If after 3 work days there is no major bug, release `v<major>.<minor>.0`
1. If within 3 work days there is major bug, let's triage it to fix it and then release `v<major>.<minor>.0-rc.++` Go to step 2.
1. Do patch release if needed for any bugs afterwards. Use same `release-xxx` branch.

### How to release "a version"

1. Add PR on branch `release-<major>.<minor>` that will start minor release branch and prepare changes to cut release.
    
  For release candidate just reuse same branch and rebase it on every candidate until the actual release happens.
        
1. Update [CHANGELOG file](/CHANGELOG.md)

  Note that `CHANGELOG.md` should only document changes relevant to users of Thanos, including external API changes, performance improvements, and new features. Do not document changes of internal interfaces, code refactorings and clean-ups, changes to the build process, etc. People interested in these are asked to refer to the git history.
  Format is described in `CHANGELOG.md`.
  
  The whole release from release candidate `rc.0` to actual release should have exactly the same section. We don't separate
  what have changed between release candidates.

1. Double check backward compatibility:
    1. *In case of version after `v1+.y.z`*, double check if none of the changes break API compatibility. This should be done in PR review process, but double check is good to have.
    1. In case of `v0.y.z`, document all incompatibilities in changelog.

1. After review, merge the PR and immediately after this tag a version:

    ```bash
    $ tag=x.y.z
    $ git tag -s "v${tag}" -m "v${tag}"
    $ git push origin "v${tag}"
    ```

    Signing a tag with a GPG key is appreciated, but in case you can't add a GPG key to your Github account using the following [procedure](https://help.github.com/articles/generating-a-gpg-key/), you can replace the `-s` flag by `-a` flag of the `git tag` command to only annotate the tag without signing.

 1. Once a tag is created, the release process through CircleCI will be triggered for this tag.

 1. You must create a Github Release using the UI for this tag, as otherwise CircleCI will not be able to upload tarballs for this tag. Also, you must create the Github Release using a Github user that has granted access rights to CircleCI. List of maintainers is available [here](/MAINTAINERS.md)

 1. Go to the releases page of the project, click on the `Draft a new release` button and select the tag you just pushed. Describe release and post relevant entry from changelog. Click `Save draft` rather than `Publish release` at this time. (This will prevent the release being visible before it has got the binaries attached to it.)

 1. Once tarballs are published on release page, you can click `Publish` and release is complete.

 1. Announce `#thanos` slack channel.

## Branch management and versioning strategy

We use [Semantic Versioning](http://semver.org/).

NOTE: We have a separate branch for each minor release, named `release-<major>.<minor>`, e.g. `release-0.1`, `release-0.2`. but they are
*NOT* maintained as we don't have major version yet.

## Pre-releases (release candidates)

The following changes to the above procedures apply:

* In line with [Semantic Versioning](http://semver.org/), append something like `-rc.0` to the version (with the corresponding changes to the tag name, the release name etc.).
* Tick the `This is a pre-release` box when drafting the release in the Github UI.
* Still update `CHANGELOG.md`, but when you cut the final release later, merge all the changes from the pre-releases into the one final update.
