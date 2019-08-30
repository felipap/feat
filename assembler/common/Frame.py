import pandas as pd
import copy

from ..lib.workarounds import drop_hashable_duplicates

class Frame(object):
  """
  """

  def __init__(self, name, table_name, pivots, fillnan=None, dtype=None):
    """
    Note that pivots might include the own column name. If an Orders table has a
    compose key (buyer, product, time), a frame containing just the column
    product has all these three columns as pivot.
    """
    assert type(name) == str
    assert type(pivots) != str # This happens a lot.
    self.table_name = table_name

    self.pivots = set(pivots)
    self.name = name
    self.df = None
    self.fillnan = fillnan
    self.dtype = dtype


  def __repr__(self):
    return 'Frame(%s.%s|%s)' % (self.table_name, self.name, self.pivots)


  def get_pivots(self):
    return self.pivots


  def copy(self):
    frame = Frame(self.name, self.table_name, self.pivots)
    frame.fillnan = self.fillnan
    frame.dtype = self.dtype
    frame.df = self.df.copy()
    return frame


  def fill_data(self, df, fillnan=None, dtype=None):
    # print("self.name", self, self.df is not None and self.df.columns, df.columns)
    
    if self.df is not None:
      print("YOU ARE REFILLING DATA BRO")
    
    assert type(df) == pd.DataFrame
    for col in self.pivots:
      assert col in df.columns, 'Pivot col \'%s\' not found in %s' % (col, df.columns)
    assert self.name in df.columns, '%s not in %s' % (self.name, df.columns)

    assert df[~df[self.name].isna()].shape[0] > 0, "Empty dataset?"

    self.fillnan = fillnan
    self.dtype = dtype
    # if df.drop_duplicates().shape[0] != df.shape[0]:
    #   print("WARNING: frame %s filled with duplicate data" % self)
    # NOTE THIS IS NEW!!!!
    all_columns = list(set([*self.pivots, self.name]))
    
    self.df = drop_hashable_duplicates(df.copy()[all_columns])


  def get_stripped(self):
    if self.df is None:
      raise Exception()
    # We only want the main column (self.name) and the pivots.
    wanted_cols = list(set([self.name]) | set(self.pivots))
    if set(self.df.columns) != set(wanted_cols):
      print("THIS SHOULD NOT BE THE CASE.", self.df.columns, wanted_cols)
    return drop_hashable_duplicates(self.df[wanted_cols])


  def rename_pivot(self, old, new):
    self.df.rename(
      columns={ old: new },
      inplace=True,
    )
    self.pivots = [(p if p != old else new) for p in self.pivots]


  def rename(self, new):
    # print("RENAME: %s to %s", self.name, new)
    old = self.name
    self.name = new
    self.df.rename(
      columns={ old: new },
      inplace=True
    )
    
    # self.pivots = [p]


  def translate_pivots_root(self, ctx, current, translation):
    """
      # TODO
      # If a pivot in result is 'order.user' and if 'user' (ie. 'Users.id')
      # is a pivot of Output, we have to rename 'order.user' somehow to make it
      # work.
    """

    if set(self.pivots).issubset(ctx.df.columns):
      return

    assert translation, "A translation is required for column %s" % self.name

    for (new, old) in translation:
      print("> translating pivot %s to %s" % (new, old))
      assert old in self.df.columns, f'{old} in {self.df.columns}'
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
    #   info = ctx.getGraphLeafInformation(self.table_name, col)
    #   if not info:
    #     raise Exception('getGraphLeafInformation failed', self.table_name, col)
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
