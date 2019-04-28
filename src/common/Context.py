
from .Frame import Frame
import pandas as pd

class Context(object):
  def __init__(self, globals, current, timeCol):
    self.current = None
    self.globals = globals
    self.original_columns = {name: val.columns for (name, val) in globals.items()}
    self.swapIn(current)
    self.timeCol = timeCol

    #
    self.cached_frame_pivots = {}

  def swapIn(self, current):
    assert current in self.globals, '%s not registered' % current
    oldCurrent = self.current
    self.current = current
    return oldCurrent

  @property
  def df(self):
    # print("fs is", self.current, self.globals)
    return self.globals[self.current]

  @df.setter
  def df(self, value):
    self.globals[self.current] = value

  def get_pivots_for_table(self, tableName):
    return self.graph.pivots[tableName]

  def get_pivots_for_frame(self, name):
    if name in self.cached_frame_pivots:
      print("returning pivotss for %s" % name, self.cached_frame_pivots[name])
    return self.cached_frame_pivots.get(name)

  def create_subframe(self, colName, pivots):
    """Creates a frame derived from the self.current frame"""
    return Frame(colName, self, self.current, pivots)

  def currHasColumn(self, colName):
    return colName in self.df.columns

  def merge_frame_with_df(self, frame, on=None, right_on=None, left_on=None):
    if on:
      if not set(frame.pivots).issubset(self.df.columns):
        raise Exception('Result can\'t be merged into current dataset: ', \
        frame.pivots, self.df.columns)
      pivots = on
      self.df = pd.merge(self.df, \
        frame.get_stripped(), \
        on=on, \
        how='left', \
        suffixes=(False, False))
    else:
      pivots = [left_on]
      frame.rename_pivot(right_on, '__JOIN__')
      self.df = pd.merge(self.df, \
        frame.get_stripped(), \
        left_on=left_on, \
        right_on='__JOIN__', \
        how='left', \
        suffixes=(False, False))
      self.df.drop('__JOIN__', axis=1, inplace=True)

    # self.cached_frame_pivots[frame.name] = self.graph.pivots[self.current]
    self.cached_frame_pivots[frame.name] = pivots

  def findGraphEdge(self, tableOut=None, colOut=None, tableIn=None, colIn=None):
    return self.graph.find_edge(tableOut, colOut, tableIn, colIn)

  def getGraphLeafInformation(self, current, column):
    return self.graph.get_leaf_information(current, column)
