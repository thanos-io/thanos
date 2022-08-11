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

  location(targetGroups): if std.length(std.objectFields(targetGroups)) > 0 then
    ' in %s' % std.join('/', ['{{$labels.%s}}' % level for level in std.objectFields(targetGroups)])
  else
    '',

  // labelsTemplate returns a template based on grouping labels that can be inserted into an alert annotation.
  //
  // For example:
  // labelsTemplate('namespace, pod, region', {region: 'regionA'})
  // returns
  // 'namespace={{$labels.namespace}}/pod={{$labels.pod}}'.
  labelsTemplate(dimensions, targetGroups): std.join(
    '/',
    [
      '%s={{$labels.%s}}' % [label, label]
      for label in [
        std.stripChars(lbl, ' ')
        for lbl in std.split(dimensions, ',')
      ]
      if !std.objectHas(targetGroups, label)
    ]
  ),
}
