{
  new(
    title='',
    span=null,
    mode='markdown',
    content='',
    transparent=null,
  )::
    {
      [if transparent != null then 'transparent']: transparent,
      title: title,
      [if span != null then 'span']: span,
      type: 'text',
      mode: mode,
      content: content,
    },
}
