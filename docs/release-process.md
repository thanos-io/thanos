# Release Process

This page describes the release cadence and process for Thanos project.

We use [Semantic Versioning](http://semver.org/).

NOTE: As [Semantic Versioning](http://semver.org/spec/v2.0.0.html) states all 0.y.z releases can contain breaking changes in API (flags, grpc API, any backward compatibility)

## Cadence

We aim for regular and strict one release per *6 weeks*. 6 weeks is counter from first release candidate to another. This means that there is no *code freeze* or anything like that. We plan to stick to the exact 6 weeks, so there is no rush into being within release (except bug fixes).

No feature should block release.

Additionally, we (obviously) aim for `main` branch being stable.

We are assigning a release shepherd for each minor release.

Release shepherd responsibilities:

* Perform releases (from first RC to actual release).
* Announce all releases on all communication channels.

| Release | Time of first RC     | Shepherd (GitHub handle)      |
|---------|----------------------|-------------------------------|
| v0.30.0 | (planned) 2022.11.21 | No one ATM                    |
| v0.29.0 | 2022.10.21           | `@GiedriusS`                  |
| v0.28.0 | 2022.08.22           | `@yeya24`                     |
| v0.27.0 | 2022.06.21           | `@wiardvanrij` and `@matej-g` |
| v0.26.0 | 2022.04.29           | `@wiardvanrij`                |
| v0.25.0 | 2022.03.01           | `@bwplotka` and `@matej-g`    |
| v0.24.0 | 2021.12.22           | `@squat`                      |
| v0.23.0 | 2021.09.02           | `@bwplotka`                   |
| v0.22.0 | 2021.07.06           | `@GiedriusS`                  |
| v0.21.0 | 2021.05.28           | `@metalmatze` and `@onprem`   |
| v0.20.0 | 2021.04.23           | `@kakkoyun`                   |
| v0.19.0 | 2021.03.02           | `@bwplotka`                   |
| v0.18.0 | 2021.01.06           | `@squat`                      |
| v0.17.0 | 2020.11.18           | `@metalmatze`                 |
| v0.16.0 | 2020.10.26           | `@bwplotka`                   |
| v0.15.0 | 2020.08.12           | `@kakkoyun`                   |
| v0.14.0 | 2020.07.10           | `@kakkoyun`                   |
| v0.13.0 | 2020.05.13           | `@bwplotka`                   |
| v0.12.0 | 2020.04.15           | `@squat`                      |
| v0.11.0 | 2020.02.19           | `@metalmatze`                 |
| v0.10.0 | 2020.01.08           | `@GiedriusS`                  |
| v0.9.0  | 2019.11.26           | `@bwplotka`                   |
| v0.8.0  | 2019.10.09           | `@bwplotka`                   |
| v0.7.0  | 2019.08.28           | `@domgreen`                   |
| v0.6.0  | 2019.07.12           | `@GiedriusS`                  |
| v0.5.0  | 2019.06.31           | `@bwplotka`                   |

# For maintainers: Cutting individual release

Process of releasing a *minor* Thanos version:

1. Release `v<major>.<minor+1>.0-rc.0`
2. If after 3 work days there is no major bug, release `v<major>.<minor>.0`
3. If within 3 work days there is major bug, let's triage it to fix it and then release `v<major>.<minor>.0-rc.++` Go to step 2.
4. Do patch release if needed for any bugs afterwards. Use same `release-xxx` branch and migrate fixes to main.

## How to release a version

Release is happening on separate `release-<major>.<minor>` branch.

### Prepare the release branch

Prepare branch `release-<major>.<minor>` that will start minor release branch and prepare changes to cut release.

Push the created branch to origin (Thanos repository) to be able to make your PR with the CHANGELOG.md changes against this branch later.

```bash
$ git push origin release-<major>.<minor>
```

For release candidate, reuse the same branch and rebase it on every candidate until the actual release happens.

### Indicate that a release is in progress

1. Create small PR to `main` (!) to cut CHANGELOG. This helps to maintain new changelog on main. Add entry to CHANGELOG indicating release in progress. This reduces risk for the new PRs to add changelog entries to already released release.

2. Update `VERSION` file to version one minor version higher than the released one and `dev` suffix. This allows CI to build Thanos binary with the version indicating potential next minor release, showing that someone uses non-released binary (which is fine, just better to know this!).

Feel free to mimic following PR: https://github.com/thanos-io/thanos/pull/3861

### Prepare the release

1. Create a branch based on the release branch. You will use this branch to include any changes that need to happen as a part of 'cutting' the release. Follow the steps below and commit and resulting changes to this branch.

2. Double check and update [CHANGELOG file](../CHANGELOG.md). Note that `CHANGELOG.md` should only document changes relevant to users of Thanos, including external API changes, bug fixes, performance improvements, and new features. Do not document changes of internal interfaces, code refactorings and clean-ups, changes to the build process, etc. People interested in these are asked to refer to the git history. Format is described in `CHANGELOG.md`.
   - The whole release from release candidate `rc.0` to actual release should have exactly the same section. We don't separate what have changed between release candidates.

3. Double check backward compatibility:

   1. *In case of version after `v1+.y.z`*, double check if none of the changes break API compatibility. This should be done in PR review process, but double check is good to have.
   2. In case of `v0.y.z`, document all incompatibilities in changelog.

4. Double check metric changes:

   1. Note any changes in the changelog
   2. If there were any changes then update the relevant alerting rules and/or dashboards since `thanos-mixin` is part of the repository now

5. *(Applies only to minor, non-`rc` release)* Update website's [hugo.yaml](https://github.com/thanos-io/thanos/blob/main/website/hugo.yaml) to have correct links for new release ( add `0.y.z: "/:sections/:filename.md"`).

6. *(Applies only to minor, non-`rc` release)* Update tutorials:

   1. Update the Thanos version used in the [tutorials](https://github.com/thanos-io/tutorials) manifests to use the latest version.
   2. In case of any breaking changes or necessary updates adjust the manifests so the tutorial stays up to date.
   3. Update the [scripts/quickstart.sh](https://github.com/thanos-io/thanos/blob/main/scripts/quickstart.sh) script if needed.

7. Set the version in `VERSION` to reflect the tag you are going to make.
   - If you are going to tag `v<major>.<minor>.0-rc.0`, this exact tag should be in `VERSION`. Example: [v0.25.2-rc.0/VERSION](https://github.com/thanos-io/thanos/blob/v0.25.2-rc.0/VERSION)

8. Open a PR with any changes resulting from the previous steps against the release branch and ask the maintainers to review it.

### Tag and publish the release

1. After review and obtaining (an) approval(s), merge the PR and after this tag a version:

   ```bash
   tag=$(cat VERSION)
   git tag -s "v${tag}" -m "v${tag}"
   git push origin "v${tag}"
   ```

   Signing a tag with a GPG key is appreciated, but in case you can't add a GPG key to your GitHub account using the following [procedure](https://docs.github.com/en/authentication/managing-commit-signature-verification/generating-a-new-gpg-key), you can replace the `-s` flag by `-a` flag of the `git tag` command to only annotate the tag without signing.

   Please make sure that you are tagging the merge commit because otherwise GitHub's UI will show that there were more commits after your release.

2. Once a tag is created and pushed, **immediately** create a GitHub Release using the UI for this tag, as otherwise CircleCI will not be able to upload tarballs for this tag. Go to the releases page of the project, click on the `Draft a new release` button and select the tag you just pushed. Describe release and post relevant entry from changelog. Click `Save draft` **rather** than `Publish release` at this time. (This will prevent the release being visible before it has got the binaries attached to it.) *In case you did not manage to create the draft release before CircleCI run is finished (it will fail on the artifacts upload step in this case), you can re-trigger the run manually from the CircleCI dashboard *after* you created the draft release.*

3. You are also encouraged to include a list of (first time) contributors to the release. You can do this by clicking on `Auto-generate release notes`, which will generate this section for you (edit the notes as required to remove unnecessary parts).

4. Once tarballs are published on release page, you can click `Publish` and release is complete.

### Completing the release

1. Announce the release on the `#thanos` Slack channel. You are also encouraged to announce the new release on any Thanos social media accounts, such as Twitter (the credentials are available via Thanos' [Keybase](https://keybase.io/) team which includes all maintainers).

2. Pull commits from release branch to main branch for non `rc` releases. Make sure to not modify `VERSION`, it should be still pointing to `version+1-dev` ([TODO to automate this](https://github.com/thanos-io/thanos/issues/4741))

3. After releasing a major version, please cut a release for `kube-thanos` as well. https://github.com/thanos-io/kube-thanos/releases Make sure all the flag changes are reflected in the manifests. Otherwise, the process is the same, except we don't have `rc` for the `kube-thanos`. We do this to make sure we have compatible manifests for each major versions.

4. Merge `release-<major>.<minor>` branch back to main. This is important for Go modules tooling to make release tags reachable from main branch.

   - Create `merge-release-<major>.<minor>-to-main` branch **from `release-<major>.<minor>` branch** locally
   - Merge upstream `main` branch into your `merge-release-<major>.<minor>-to-main` and resolve conflicts
   - Open a PR for merging your `merge-release-<major>.<minor>-to-main` branch against `main`
   - Once approved, merge the PR **by using "Merge" commit**.
     - This can either be done by temporarily enabling "Allow merge commits" option in "Settings > Options".
     - Alternatively, this can be done locally by merging `merge-release-<major>.<minor>-to-main` branch into `main`, and pushing resulting `main` to upstream repository. This doesn't break `main` branch protection, since PR has been approved already, and it also doesn't require removing the protection.

## Pre-releases (release candidates)

The following changes to the above procedures apply:

* In line with [Semantic Versioning](http://semver.org/), append something like `-rc.0` to the version (with the corresponding changes to the tag name, the release name etc.).
* Tick the `This is a pre-release` box when drafting the release in the GitHub UI.
* Still update `CHANGELOG.md`, but when you cut the final release later, merge all the changes from the pre-releases into the one final update.
