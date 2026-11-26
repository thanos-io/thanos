{
  mapRuleGroups(f): {
    groups: [
      group {
        rules: [
          f(rule)
          for rule in super.rules
        ],
      }
      for group in super.groups
    ],
  },

  joinLabels(labels): std.join(', ', std.filter(function(x) std.length(std.stripChars(x, ' ')) > 0, labels)),

  firstCharUppercase(parts): std.join(
    '',
    [
      std.join(
        '',
        [std.asciiUpper(std.stringChars(part)[0]), std.substr(part, 1, std.length(part) - 1)]
      )
      for part in parts[1:std.length(parts)]
    ]
  ),

  toCamelCase(parts): std.join('', [parts[0], self.firstCharUppercase(parts)]),

  componentParts(name): std.split(name, '-'),

  sanitizeComponentName(name): if std.length(self.componentParts(name)) > 1 then self.toCamelCase(self.componentParts(name)) else name,

  // Query template variable that lists the values of a label from the $datasource.
  queryVariable(name, query, current=null): {
    allValue: null,
    current: if current == 'all' then { text: 'all', value: '$__all' } else {},
    datasource: '$datasource',
    hide: 0,
    includeAll: current == 'all',
    label: name,
    multi: false,
    name: name,
    options: [],
    query: query,
    refresh: 1,
    regex: '',
    sort: 2,
    tagValuesQuery: '',
    tags: [],
    tagsQuery: '',
    type: 'query',
    useTags: false,
  },

  // Interval template variable. Include 'auto' in query to enable the auto option.
  intervalVariable(name, query, current): {
    auto: std.count(std.split(query, ','), 'auto') > 0,
    auto_count: 300,
    auto_min: '10s',
    current: { text: current, value: current },
    hide: 0,
    label: name,
    name: name,
    query: std.join(',', std.filter(function(x) x != 'auto', std.split(query, ','))),
    refresh: 2,
    type: 'interval',
  },
}
