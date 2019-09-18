
from .Frame import Frame
import pandas as pd

class Context(object):
  def __init__(self, graph, output):
    self.output = output
    self.graph = graph
    self.current = None
    self.swap_in('output')
    self.cached_frames = {}

  def swap_in(self, current):
    current = current.lower()
    assert current in self.graph.tables, f'\'{current}\' is not a registered table'
    old = self.current
    self.current = current
    return old

  @property
  def table(self):
    return self.graph.get_table(self.current)

  @property
  def df(self):
    return self.graph.get_table(self.current).get_dataframe()

  @df.setter
  def df(self, value):
    self.graph.get_table(self.current).set_dataframe(value)

  def get_date_range(self):
    return sorted(self.graph.get_table('output').get_dataframe()['__date__'].unique())

  def get_cached_frame(self, name):
    if not name in self.cached_frames:
      raise Exception()
    return self.cached_frames.get(name)

  def merge_frame_with_df(self, frame, on=None, right_on=None, left_on=None):
    
    outer_pivots = self.table.merge_frame(frame, on, right_on, left_on)

    copied = frame.copy()
    copied.pivots = outer_pivots
    self.cached_frames[frame.name] = copied

    return copied
