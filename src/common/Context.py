
from .Frame import Frame
import pandas as pd

class Context(object):
  def __init__(self, globals, current, timeCol):
    self.current = None
    self.globals = globals
    self.swapIn(current)
    self.timeCol = timeCol

    self.pointers = []
    self.pivots = []

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

  def addRelationship(self, table1, col1, table2, col2):
    self.pointers.append((table1, col1, table2, col2))

  def getRelationshipOnField(self, table, field):
    # print("table", table, field)
    for rel in self.pointers:
      if rel[0] == table and rel[1] == field:
        return (rel[0], rel[1], rel[2], rel[3])
      elif rel[2] == table and rel[3] == field:
        return (rel[2], rel[3], rel[0], rel[1])
    return None

  def create_subframe(self, colName, pivots=None):
    """Creates a frame derived from the self.current frame"""
    if not pivots is None:
      if not set(pivots).issubset(self.pivots[self.current]):
        pass
        # print("self.pivots[self.current]", self.pivots[self.current], pivots)
    return Frame(colName, self, self.current, pivots)

  def currHasColumn(self, colName):
    return colName in self.df.columns

  def currMergeFrame(self, frame):
    # display("now is", result.get_stripped())
    # import builtins
    # builtins.fuck = result.get_stripped()
    # display(ctx.df)

    # FIXME Even if df of child is different (eg. Items.MEAN(Sales.item)), we are
    # merging based on the child's columns, without any warning or thinking about it
    # deeper

    if not set(frame.pivots).issubset(self.df.columns):
      raise Exception('Result can\'t be merged into current dataset: ', \
        frame.pivots, self.df.columns)


    if not self.currHasColumn(frame.name):
      self.df = pd.merge(self.df, \
        frame.get_stripped(), \
        on=list(frame.pivots), \
        how='left', \
        suffixes=(False, False))
    return self
