
from .Frame import Frame
import pandas as pd

class Context(object):
  def __init__(self, globals, current, timeCol):
    self.current = None
    self.globals = globals
    self.original_columns = {name: val.columns for (name, val) in globals.items()}
    self.swapIn(current)
    self.timeCol = timeCol
    self.cached_frames = {}

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

  def get_date_range(self):
    return sorted(self.globals['Output'][self.timeCol].unique())

  def get_pivots_for_table(self, tableName):
    return self.graph.pivots[tableName]

  def get_cached_frame(self, name):
    if not name in self.cached_frames:
      print("WTF\n\n\n\n\n %s" % name, self.cached_frames[name])
    return self.cached_frames.get(name)

  def create_subframe(self, colName, pivots):
    """Creates a frame derived from the self.current dataframe"""
    return Frame(colName, self.current, pivots)

  def currHasColumn(self, colName):
    return colName in self.df.columns

  def merge_frame_with_df(self, frame, on=None, right_on=None, left_on=None):
    if on:
      if not set(frame.pivots).issubset(self.df.columns):
        raise Exception('Result can\'t be merged into current dataset: ', \
        frame.pivots, self.df.columns)
      outer_pivots = on

      frame_df = frame.get_stripped()

      import builtins
      builtins.frame = frame

      how = 'left'
      # if 'FWD(MEAN_DIFF(Order_items{CMONTH(date)=CMONTH(order.date)}.SUM(quantity|CMONTH(order.date),product),CMONTH(date)),1,CMONTH(date))' in frame_df.columns:
      #   print(frame_df.info())
      #   print(self.df.info())
      #   sys.exit(0)
        # how = 'inner'

      # for pivot in on:
      #
      # if 'CMONTH(date)' in frame_df.columns:
      #   print("types", frame_df['CMONTH(date)'].dtype, self.df['CMONTH(date)'].dtype)
      #   frame_df['CMONTH(date)'] = frame_df['CMONTH(date)'].astype('int64')

      # display(frame_df)
      # display(self.df)

      # print("not is gonna get stuck", on, frame_df.dtypes, self.df.dtypes)
      self.df = pd.merge(self.df, \
        frame_df, \
        on=on, \
        how=how, \
        suffixes=(False, False))
    else:
      copied_frame = frame.copy()
      copied_frame.rename_pivot(right_on, '__JOIN__')

      copied_frame_df = copied_frame.get_stripped()

      columns_overlap = set(copied_frame_df.columns).intersection(self.df.columns)
      right_on = '__JOIN__'

      outer_pivots = [left_on]
      if columns_overlap:
        assert len(columns_overlap) == 1 # Just for debugging now, this should be deleted!
        overlap = columns_overlap.pop()
        outer_pivots += [overlap]
        right_on = [right_on, overlap]
        left_on = [left_on, overlap]

      
      self.df = pd.merge(self.df, \
        copied_frame_df, \
        left_on=left_on, \
        right_on=right_on, \
        how='left', \
        suffixes=(False, False))
      self.df.drop('__JOIN__', axis=1, inplace=True)

    if not frame.fillnan is None:
      self.df.fillna(value={ frame.name: frame.fillnan }, inplace=True)

    # self.cached_frames[frame.name] = self.graph.pivots[self.current]
    copied = frame.copy()
    copied.pivots = outer_pivots
    print("copying too", frame.name, outer_pivots)
    self.cached_frames[frame.name] = copied

    return copied

  def findGraphEdge(self, tableOut=None, colOut=None, tableIn=None, colIn=None):
    return self.graph.find_edge(tableOut, colOut, tableIn, colIn)

  def getGraphLeafInformation(self, current, column):
    return self.graph.get_leaf_information(current, column)
