import pandas as pd
import copy
from typing import Dict, List, Set

from ..lib.workarounds import drop_hashable_duplicates


class Frame(object):
  """
  """

  def __init__(self, name, table, pivots, fillnan=None, dtype=None):
    """
    Note that pivots might include the own column name. If an Orders table has a
    compose key (buyer, product, time), a frame containing just the column
    product has all these three columns as pivot.
    """

    if not table:
      raise Exception()

    assert type(name) == str
    assert type(pivots) != str # This happens a lot.
    self.table = table

    self.pivots: Set[str] = set(pivots)
    self.name = name
    self.df = None
    self.fillnan = fillnan
    self.dtype = dtype


  def __repr__(self):
    return 'Frame(%s.%s|%s)' % (self.table, self.name, self.pivots)


  def get_pivots(self):
    return self.pivots


  def get_date_col(self):
    if '__date__' in self.df.columns:
      return '__date__'
    elif 'DATE(created_at)' in self.df.columns:
      return 'DATE(created_at)'
    print(self.df.columns)
    raise Exception('WTF')


  def copy(self):
    frame = Frame(self.name, self.table, self.pivots)
    frame.fillnan = self.fillnan
    frame.dtype = self.dtype
    frame.df = self.df.copy()
    return frame


  def fill_empty(self, fillnan=None, dtype=None):
    """
    Shorthand for callign fill_data with a dataframe with columns [name, *pivots].
    """
    df = pd.DataFrame(columns=[self.name, *(self.pivots or [])])
    return self.fill_data(df, fillnan, dtype)


  def fill_data(self, df, fillnan=None, dtype=None):
    """
    Fill the current frame with the `df` dataframe. In `df` we expect to find
    one column for each pivot in this frame (ie. self.pivots) and one column
    for the main column in this frame (ie. self.name).
    """

    # TODO should we only allow it to be filled once?

    if self.df is not None:
      print("WARNING: You are refilling data, bro!")

    # Check that the dataframe has all the right columns etc.
    assert isinstance(df, pd.DataFrame)
    for col in self.pivots:
      if col not in df.columns:
        print(self.pivots, df.columns)
        raise Exception(f'Tried to fill frame {self.name} with a dataframe that lacks '
          f'a groupby column we expected: {col}')
    assert self.name in df.columns, '%s not in %s' % (self.name, df.columns)
    if df[~df[self.name].isna()].shape[0] == 0:
      print("WARNING: Empty dataset? " +self.name)

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
    old = self.name
    self.name = new
    self.df.rename(
      columns={ old: new },
      inplace=True
    )


  def translate_pivots_root(self, ctx, parent, translation):
    """
    Args:
      parent: the name of the parent node's table (ie. the upper-level's
        `current`)

    # TODO
    # is a pivot of Output, we have to rename 'order.user' somehow to make it
    # work.
    """

    need_translation = set(self.pivots) - set(ctx.df.columns)
    # NOTE this assumes that fields with the same name should be automatically
    # translated to each other.
    if not need_translation:
      return

    # Try first with user-provided translation instructions (Table{a=b} syntax).
    replace = {}
    if translation:
      for new, old in translation.items():
        # Translating pivot from `old` to `new`.
        assert old in self.df.columns, f'{old} in {self.df.columns}'
        replace[old] = new
        need_translation.remove(old)

    # Next, if needed, try to infer what should be translate to what. This
    # behavior might lead to unexpected results, so warn the user appropriately.
    if need_translation:
      for pivot in need_translation.copy():
        translation = self._infer_column_translation(ctx, parent, pivot)
        if translation:
          replace[pivot] = translation
          need_translation.remove(pivot)

    # Next, try to match a date field in the child to a date field in the
    # parent.
    if need_translation:
      for pivot in need_translation.copy():
        # TODO This behavior is either shaky or needs to be better documented.
        if pivot.startswith('DATE'):
          if parent == 'output':
            replace[pivot] = ctx.output.get_date_field()
            need_translation.remove(pivot)
          else:
            raise Exception('is date but dont know what to do')

    # Next, give up.
    if need_translation:
      raise Exception(need_translation)

    self.df.rename(columns=replace, inplace=True)
    self.pivots = { replace.get(e, e) for e in self.pivots }


  def _infer_column_translation(self, ctx, parent, column):
    info = ctx.graph.get_leaf_information(self.table.get_name(), column)
    if not info:
      # Nothing on this edge.
      return None
    table_in, col_in = info
    print(parent, table_in, col_in)

    pointers = ctx.graph.find_edge(tableOut=parent, tableIn=table_in, colIn=col_in)
    if not pointers:
      raise Exception('Failed to translate %s to table %s' % (column, parent))

    # REVIEW: what are the cons of this??? what if we don't want to match
    # them>??? Does the col_in field have to be unique?
    if len(pointers) > 1:
      # BUG we don't want this exactly.
      # Child.father -> Parent and Child.mother -> Parent.
      # We should offer some pattern matching.
      raise Exception('Multiple edges found to %s' % table_in)

    # Rename pivot to col1 (ie. pointers[0][1])

    translation = pointers[0][1]
    print("> Matching attribute found for {}: {}.{} points to {}.{}".format(column, *pointers[0]))
    return translation


