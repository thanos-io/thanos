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
}
