import pandas as pd

class Frame(object):
  """
  (pivots, colName)
  """

  def __init__(self, colName, ctx, frameName, pivots=None):
    assert type(colName) == str
    assert type(pivots) != str # This mistake happens a lot.
    self.frameName = frameName

    if pivots is None:
      pivots = ctx.pivots[frameName]

    self.pivots = set(pivots)
    self.colName = colName
    self.df = None

  def __repr__(self):
    return 'Frame(%s.%s|%s)' % (self.frameName, self.colName, self.pivots)

  # def mergeChild(self, childResult):
  #   if self.df is not None:
  #     raise Exception()
  #   # assert isinstance(childResult, Frame) # Not working
  #
  #   assert type(df) == pd.DataFrame
  #   assert set(self.pivots).issubset(df.pivots)
  #   assert self.colName in df.columns

  def getPivots(self):
    return self.pivots

  def fillData(self, df, _force=False):
    if self.df is not None and not _force:
      raise Exception()

    assert type(df) == pd.DataFrame
    # NOTE: In the future, it might make sense to soften this restriction, for
    # instance, to allow {'month_block', 'item'} !<
    # {'month_block', 'item.category'}.
    assert set(self.pivots).issubset(df.columns), \
      "%s !< %s" % (self.pivots, df.columns)
    assert self.colName in df.columns
    self.df = df

  def getStripped(self):
    if self.df is None:
      raise Exception()
    wantedCols = list(set([self.colName]) | set(self.pivots))
    return self.df[wantedCols]

  # REVIEW perhaps rename?
  def getWithNamedRoot(self, rootName):
    """
    In something like `Output.MEAN(Sales.price|..)`, the result of evaluating
    price is a Frame in which everything (eg. pivots, frameName, colName etc) is
    named with respect to Sales. However, the result of evaluating Sales.price
    should be with respect to Output. This function exists to make this
    translation.
    """

    # FIXME: this should be a copy

    # TODO: this also necessary?
    # self.frameName = rootName

    assert self.df is not None

    col = self.getStripped().copy()

    columns = { self.colName: "%s.%s" % (rootName, self.colName), }
    self.colName = "%s.%s" % (rootName, self.colName)
    col.rename(
      columns=columns,
      inplace=True
    )

    self.fillData(col, True)

    return self

  # REVIEW perhaps rename?
  def getAsNested(self, ctx, frameOutName, keyOut):
    """
    In something like `Sales.item.category`, category belongs to the Items
    table. The result of evaluating the category field is a Frame in which
    everything (eg. pivots, frameName, colName etc) is named with respect to
    Items. However, the result of evaluating item.category should be a
    Frame with respect to Sales. This function exists to make
    this translation.
    """

    def nestedName(name):
      return '%s.%s' % (keyOut, name)

    # FIXME WARNING keyIn should be passed as an argument!
    keyIn = 'id'
    nested = Frame(nestedName(self.colName), ctx, frameOutName, [nestedName(keyIn)])

    # Merge into frameOutName the generated column from tableIn.
    col = self.getStripped().copy()
    col.rename(
      columns=dict((c, nestedName(c)) for c in col.columns),
      inplace=True
    )

    nested.fillData(col)

    return nested
