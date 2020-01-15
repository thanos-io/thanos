---
title: Check
type: docs
menu: components
---

# Check

The check component contains tools for validation of Prometheus rules.

## Deployment
## Flags

[embedmd]:# (flags/check.txt $)
```$
usage: thanos check <command> [<args> ...]

Linting tools for Thanos

Flags:
  -h, --help               Show context-sensitive help (also try --help-long and
                           --help-man).
      --version            Show application version.
      --log.level=info     Log filtering level.
      --log.format=logfmt  Log format to use. Possible options: logfmt or json.
      --tracing.config-file=<file-path>
                           Path to YAML file with tracing configuration. See
                           format details:
                           https://thanos.io/tracing.md/#configuration
      --tracing.config=<content>
                           Alternative to 'tracing.config-file' flag (lower
                           priority). Content of YAML file with tracing
                           configuration. See format details:
                           https://thanos.io/tracing.md/#configuration

Subcommands:
  check rules <rule-files>...
    Check if the rule files are valid or not.


```


### Rules

`check rules` checks the Prometheus rules, used by the Thanos rule node, if they are valid.
The check should be equivalent for the `promtool check rules` but that cannot be used because
Thanos rule has extended rules file syntax, which includes `partial_response_strategy` field
which `promtool` does not allow.

If the check fails the command fails with exit code `1`, otherwise `0`.

Example:

```
$ ./thanos check rules cmd/thanos/testdata/rules-files/*.yaml
```

[embedmd]:# (flags/check_rules.txt)
```txt
usage: thanos check rules <rule-files>...

Check if the rule files are valid or not.

Flags:
  -h, --help               Show context-sensitive help (also try --help-long and
                           --help-man).
      --version            Show application version.
      --log.level=info     Log filtering level.
      --log.format=logfmt  Log format to use. Possible options: logfmt or json.
      --tracing.config-file=<file-path>
                           Path to YAML file with tracing configuration. See
                           format details:
                           https://thanos.io/tracing.md/#configuration
      --tracing.config=<content>
                           Alternative to 'tracing.config-file' flag (lower
                           priority). Content of YAML file with tracing
                           configuration. See format details:
                           https://thanos.io/tracing.md/#configuration

Args:
  <rule-files>  The rule files to check.

```
