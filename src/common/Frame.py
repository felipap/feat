import pandas as pd
import copy

class Frame(object):
  """
  (pivots, colName)
  """

  def __init__(self, colName, ctx, tableName, pivots):
    assert type(colName) == str
    assert type(pivots) != str # This mistake happens a lot.
    self.tableName = tableName

    self.pivots = set(pivots)
    self.name = colName
    self.df = None

  def __repr__(self):
    return 'Frame(%s.%s|%s)' % (self.tableName, self.name, self.pivots)

  def getPivots(self):
    return self.pivots

  def fillData(self, df, _force=False):
    if self.df is not None and not _force:
      raise Exception()

    assert type(df) == pd.DataFrame
    # NOTE: In the future, it might make sense to soften this restriction, for
    # instance, to allow {'month_block', 'item'} !< {'month_block', 'item.category'}.
    ourPivots = self.pivots # [*self.pivots,self.name]

    for col in self.pivots:
      assert col in df.columns, 'Pivot col %s not found in %s' % (col, df.columns)

    assert self.name in df.columns, '%s not in %s' % (self.name, df.columns)
    self.df = df.copy()

  def get_stripped(self):
    if self.df is None:
      raise Exception()
    # We only want the main column (self.name) and the pivots.
    wantedCols = list(set([self.name]) | set(self.pivots))
    return self.df[wantedCols]

  def rename_pivot(self, old, new):
    self.df.rename(
      columns={ old: new },
      inplace=True,
    )
    self.pivots = [(p if p != old else new) for p in self.pivots]

  def rename(self, new):
    old = self.name
    self.name = new
    self.df.rename(
      columns={ old: new },
      inplace=True
    )

  def translate_pivots_root(self, ctx, current, translation):
    """
      # TODO
      # If a pivot in result is 'order.user' and if 'user' (ie. 'Users.id')
      # is a pivot of Output, we have to rename 'order.user' somehow to make it
      # work.
    """

    if set(self.pivots).issubset(ctx.df.columns):
      return

    assert translation

    for (new, old) in translation:
      print("> translating pivot %s to %s" % (new, old))
      assert old in self.df.columns
      self.df.rename(columns={ old: new }, inplace=True)
      self.pivots = list(map(lambda x: x if x != old else new, self.pivots))

    # print("translation!", self.pivots, self.df.columns)

    # NOTE Code below is good, but it implements inferred translation. Code
    # above implements explicit translation (uses Table<a=b> syntax).
    # for col in self.pivots.copy():
    #   if col == 'CMONTH(date)':
    #     continue
    #
    #   # print(current, tableIn, colIn, ctx.pointers)
    #   info = ctx.getGraphLeafInformation(self.tableName, col)
    #   if not info:
    #     raise Exception('getGraphLeafInformation failed', self.tableName, col)
    #   tableIn, colIn = info
    #
    #   pointers = ctx.findGraphEdge(tableOut=current, tableIn=tableIn, colIn=colIn)
    #   if not pointers:
    #     raise Exception('Failed to translate %s to table %s' % (col, current))
    #
    #   # REVIEW: what are the cons of this??? what if we don't want to match
    #   # them>??? Does the colIn field have to be unique?
    #   if len(pointers) > 1:
    #     # BUG we don't want this exactly.
    #     # Child.father -> Parent and Child.mother -> Parent.
    #     # We should offer some pattern matching.
    #     raise Exception('Multiple edges found to %s' % tableIn)
    #
    #   # Rename pivot to col1 (ie. pointers[0][1])
    #
    #   translation = pointers[0][1]
    #   print("> Matching attribute found for {}: {}.{} points to {}.{}".format(col, *pointers[0]))
    #
    #   if translation == col:
    #     print("Already called that. Skip.")
    #     continue
    #   # elif translation in self.pivots:
    #   #   raise Exception('Should')
    #   self.pivots = list(map(lambda x: x if x != col else translation, self.pivots))
    #   self.df.rename(columns={col:translation}, inplace=True)
    #
    # print("New pivots:%s columns:%s" % (self.pivots, self.df.columns))
