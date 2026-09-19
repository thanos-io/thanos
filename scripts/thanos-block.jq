#!/usr/bin/env jq -f

{
  "ulid": .ulid,
  "minTime": (.minTime / 1000 | todateiso8601),
  "maxTime": (.maxTime / 1000 | todateiso8601),
  "stats": .stats,
  "thanos": .thanos,
  "compaction": {
    "level": .compaction.level,
    "sources": .compaction.sources,
    "sourcesCount": .compaction.sources | length,
    "parents": (if .compaction.parents? then [
      .compaction.parents[] | {
        "ulid": .ulid,
	"minTime": (.minTime / 1000 | todateiso8601),
	"maxTime": (.maxTime / 1000 | todateiso8601),
      }
    ] else null end)
  },
}

