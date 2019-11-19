{
  dashboards(
    title,
    tags,
    asDropdown=true,
    includeVars=false,
    keepTime=false,
    icon='external link',
    url='',
    targetBlank=false,
    type='dashboards',
  )::
    {
      asDropdown: asDropdown,
      icon: icon,
      includeVars: includeVars,
      keepTime: keepTime,
      tags: tags,
      title: title,
      type: type,
      url: url,
      targetBlank: targetBlank,
    },
}
