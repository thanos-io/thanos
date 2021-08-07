---
type: docs
title: Release Process
menu: thanos
---

# Release Process

This page describes the release cadence and process for Thanos project.

We use [Semantic Versioning](http://semver.org/).

NOTE: As [Semantic Versioning](http://semver.org/spec/v2.0.0.html) states all 0.y.z releases can contain breaking changes in API (flags, grpc API, any backward compatibility)

## Cadence

We aim for regular and strict one release per *6 weeks*. 6 weeks is counter from first release candidate to another. This means that there is no *code freeze* or anything like that. We plan to stick to the exact 6 weeks, so there is no rush into being within release (except bug fixes).

No feature should block release.

Additionally we (obviously) aim for `master` branch being stable.

We are assigning a release shepherd for each minor release.

Release shepherd responsibilities:

* Perform releases (from first RC to actual release).
* Announce all releases on all communication channels.

| Release | Time of first RC     | Shepherd (GitHub handle) |
|---------|----------------------|--------------------------|
| v0.17.0 | (planned) 2020.11.04 | `@metalmatze`            |
| v0.16.0 | (planned) 2020.09.23 | `@bwplotka`              |
| v0.15.0 | 2020.08.12           | `@kakkoyun`              |
| v0.14.0 | 2020.07.10           | `@kakkoyun`              |
| v0.13.0 | 2020.05.13           | `@bwplotka`              |
| v0.12.0 | 2020.04.15           | `@squat`                 |
| v0.11.0 | 2020.02.19           | `@metalmatze`            |
| v0.10.0 | 2020.01.08           | `@GiedriusS`             |
| v0.9.0  | 2019.11.26           | `@bwplotka`              |
| v0.8.0  | 2019.10.09           | `@bwplotka`              |
| v0.7.0  | 2019.08.28           | `@domgreen`              |
| v0.6.0  | 2019.07.12           | `@GiedriusS`             |
| v0.5.0  | 2019.06.31           | `@bwplotka`              |

# For maintainers: Cutting individual release

Process of releasing a *minor* Thanos version:
1. Release `v<major>.<minor+1>.0-rc.0`
2. If after 3 work days there is no major bug, release `v<major>.<minor>.0`
3. If within 3 work days there is major bug, let's triage it to fix it and then release `v<major>.<minor>.0-rc.++` Go to step 2.
4. Do patch release if needed for any bugs afterwards. Use same `release-xxx` branch and migrate fixes to master.

## How to release a version

Release is happening on separate `release-<major>.<minor>` branch.

1. Prepare PR to branch `release-<major>.<minor>` that will start minor release branch and prepare changes to cut release.

   Push the created branch to origin (Thanos repository) to be able to make your PR with the CHANGELOG.md changes against this branch later.

   ```bash
   $ git push origin release-<major>.<minor>
   ```

For release candidate just reuse same branch and rebase it on every candidate until the actual release happens.

1. Create small PR to master (!) to cut CHANGELOG. This helps to maintain new changelog on master. For example: https://github.com/thanos-io/thanos/pull/2627

2. Update [CHANGELOG file](../CHANGELOG.md)

Note that `CHANGELOG.md` should only document changes relevant to users of Thanos, including external API changes, performance improvements, and new features. Do not document changes of internal interfaces, code refactorings and clean-ups, changes to the build process, etc. People interested in these are asked to refer to the git history. Format is described in `CHANGELOG.md`.

The whole release from release candidate `rc.0` to actual release should have exactly the same section. We don't separate what have changed between release candidates.

1. Double check backward compatibility:

   1. *In case of version after `v1+.y.z`*, double check if none of the changes break API compatibility. This should be done in PR review process, but double check is good to have.
   2. In case of `v0.y.z`, document all incompatibilities in changelog.

2. Double check metric changes:

   1. Note any changes in the changelog
   2. If there were any changes then update the relevant alerting rules and/or dashboards since `thanos-mixin` is part of the repository now

3. Update tutorials:

   1. Update the Thanos version used in the [tutorials](../tutorials) manifests.
   2. In case of any breaking changes or necessary updates adjust the manifests so the tutorial stays up to date.
   3. Update the [scripts/quickstart.sh](../scripts/quickstart.sh) script if needed.

4. After review, merge the PR and immediately after this tag a version:

   ```bash
   tag=$(cat VERSION)
   git tag -s "v${tag}" -m "v${tag}"
   git push origin "v${tag}"
   ```

   Signing a tag with a GPG key is appreciated, but in case you can't add a GPG key to your Github account using the following [procedure](https://help.github.com/articles/generating-a-gpg-key/), you can replace the `-s` flag by `-a` flag of the `git tag` command to only annotate the tag without signing.

   Please make sure that you are tagging the merge commit because otherwise GitHub's UI will show that there were more commits after your release.

5. Once a tag is created, the release process through CircleCI will be triggered for this tag.

6. You must create a Github Release using the UI for this tag, as otherwise CircleCI will not be able to upload tarballs for this tag. Also, you must create the Github Release using a Github user that has granted access rights to CircleCI. List of maintainers is available [here](../MAINTAINERS.md)

7. Go to the releases page of the project, click on the `Draft a new release` button and select the tag you just pushed. Describe release and post relevant entry from changelog. Click `Save draft` rather than `Publish release` at this time. (This will prevent the release being visible before it has got the binaries attached to it.)

8. Once tarballs are published on release page, you can click `Publish` and release is complete.

9. Announce `#thanos` slack channel.

10. Pull commits from release branch to master branch for non `rc` releases.

## Pre-releases (release candidates)

The following changes to the above procedures apply:

* In line with [Semantic Versioning](http://semver.org/), append something like `-rc.0` to the version (with the corresponding changes to the tag name, the release name etc.).
* Tick the `This is a pre-release` box when drafting the release in the Github UI.
* Still update `CHANGELOG.md`, but when you cut the final release later, merge all the changes from the pre-releases into the one final update.
