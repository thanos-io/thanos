`release` branch is used to build Docker image for PROD and `db_main` branch is used to build Docker image for DEV/STAGING.
# How to contribute
There are two types of contributions:

A. new features or non-critical fix 

B. critical fix that needs to go PROD immediately

Most contributions should be categorized as type A.
## Type A contribution SOP
1. Assuming you have a local thanos clone and `git remote -v` shows
```
origin	git@github.com:databricks/thanos.git (fetch)
origin	git@github.com:databricks/thanos.git (push)
```

2. Pull latest db_main branch `git checkout db_main && git fetch origin && git reset --hard origin/db_main`.
3. Checkout your dev branch `git checkout -b your_dev_branch`.
4. Commit your code changes and open a PR `git pp`. PR target should be databricks/thanos:db_main
5. After PR approval and all CI passes (some e2e tests are flaky it's fine if they fail), use "Squash and Merge" option to merge the PR. This is important to make sure we have a clean linear commit history.
## Type B contribution SOP (use with caution)
1. Assuming you have a local thanos clone and `git remote -v` shows
```
origin	git@github.com:databricks/thanos.git (fetch)
origin	git@github.com:databricks/thanos.git (push)
```

1. Pull latest release branch `git checkout release && git fetch origin && git reset --hard origin/release`
2. Checkout your dev branch `git checkout -b your_dev_branch`.
3. Commit your code changes and open a PR `git pp`. PR target should be databricks/thanos:release
4. After PR approval and all CI passes (some e2e tests are flaky it's fine if they fail), use "Squash and Merge" option to merge the PR.
5. Pull latest release branch `git checkout release && git fetch origin && git reset --hard origin/release`.
6. Pull latest db_main branch `git checkout db_main && git fetch origin && git reset --hard origin/db_main`.
7. Rebase db_main on release `git checkout db_main && git rebase release`.
8. Force push db_main to remote `git push --force origin db_main`.

# How to bump release branch
Assume at some point db_main looks like 1-2-3-6 and release looks like 1-2-3-4-5-6, where 1,2,3,4,5 are type-A commits, and 6 is a type-B commit. We want to bump release to include commit 4. We should do this
1. Pull latest release branch `git checkout release && git fetch origin && git reset --hard origin/release`.
2. Fast forward release branch to the commit cutoff `git merge --ff-only <hash-of-commit-4>`
3. Push to remote. Force push release branch should NEVER be used if we follow this SOP. `git push origin release`.

