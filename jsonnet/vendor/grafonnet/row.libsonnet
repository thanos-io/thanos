{
  new(
    title='Dashboard Row',
    height=null,
    collapse=false,
    repeat=null,
    showTitle=null,
    titleSize='h6'
  ):: {
    collapse: collapse,
    collapsed: collapse,
    [if height != null then 'height']: height,
    panels: [],
    repeat: repeat,
    repeatIteration: null,
    repeatRowId: null,
    showTitle:
      if showTitle != null then
        showTitle
      else
        title != 'Dashboard Row',
    title: title,
    type: 'row',
    titleSize: titleSize,
    addPanels(panels):: self {
      panels+: panels,
    },
    addPanel(panel, gridPos={}):: self {
      panels+: [panel { gridPos: gridPos }],
    },
  },
}
