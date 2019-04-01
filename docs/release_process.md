# Releases

This page describes the release process for Thanos project.

NOTE: As [Semantic Versioning](http://semver.org/spec/v2.0.0.html) states all 0.y.z releases can contain breaking changes in API (flags, grpc API, any backward compatibility)

## Cadence

We aim for *at least* 1 release per 6 weeks. However, no strict dates are planned.

No release candidates are required until major version.

Additionally we aim for `master` branch being stable.

## Cutting individual release

Process of cutting a new *minor* Thanos release:

1. Add PR on branch `release-<major>.<minor>` that will start minor release branch and prepare changes to cut release.
1. Bump [VERSION file](/VERSION)
1. Update [CHANGELOG file](/CHANGELOG.md)

  Note that `CHANGELOG.md` should only document changes relevant to users of Thanos, including external API changes, performance improvements, and new features. Do not document changes of internal interfaces, code refactorings and clean-ups, changes to the build process, etc. People interested in these are asked to refer to the git history.
  Format is described in `CHANGELOG.md`.

1. Double check backward compatibility:
    1. *In case of version after `v1+.y.z`*, double check if none of the changes break API compatibility. This should be done in PR review process, but double check is good to have.
    1. In case of `v0.y.z`, document all incompatibilities in changelog.

1. After review, merge the PR and immediately after this tag a version:

    ```bash
    $ tag=$(< VERSION)
    $ git tag -s "v${tag}" -m "v${tag}"
    $ git push origin "v${tag}"
    ```

    Signing a tag with a GPG key is appreciated, but in case you can't add a GPG key to your Github account using the following [procedure](https://help.github.com/articles/generating-a-gpg-key/), you can replace the `-s` flag by `-a` flag of the `git tag` command to only annotate the tag without signing.

 1. Once a tag is created, the release process through CircleCI will be triggered for this tag.

 1. You must create a Github Release using the UI for this tag, as otherwise CircleCI will not be able to upload tarballs for this tag. Also, you must create the Github Release using a Github user that has granted access rights to CircleCI. List of maintainers is available [here](/MAINTAINERS.md)

 1. Go to the releases page of the project, click on the `Draft a new release` button and select the tag you just pushed. Describe release and post relevant entry from changelog. Click `Save draft` rather than `Publish release` at this time. (This will prevent the release being visible before it has got the binaries attached to it.)

 1. Once tarballs are published on release page, you can click `Publish` and release is complete.

 1. Announce `#thanos` slack channel.

1. After release create a second PR adding `-master` [VERSION file](/VERSION) suffix to the end of version. This will ensure master built images will have different version then released one.

## Branch management and versioning strategy

We use [Semantic Versioning](http://semver.org/).

NOTE: We have a separate branch for each minor release, named `release-<major>.<minor>`, e.g. `release-0.1`, `release-0.2`. but they are
*NOT* maintained as we don't have major version yet.

## Pre-releases (release candidates)

The following changes to the above procedures apply:

* In line with [Semantic Versioning](http://semver.org/), append something like `-rc.0` to the version (with the corresponding changes to the tag name, the release name etc.).
* Tick the `This is a pre-release` box when drafting the release in the Github UI.
* Still update `CHANGELOG.md`, but when you cut the final release later, merge all the changes from the pre-releases into the one final update.
