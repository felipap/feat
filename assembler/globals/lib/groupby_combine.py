
from datetime import datetime, timedelta
from dateutil import relativedelta
from timeit import default_timer as timer

import numpy as np
import pandas as pd

def split_respecting_boundaries(records, keys, num_split=10):
  """
  Split an array of records in `num_split` semi-even chunks, while respecting
  that records with the same keys should never be put in separate chunks.
  """
  
  sorted_df = records.sort_values(keys).to_dict('records')
  chunks = list(map(list, np.array_split(sorted_df, num_split)))

  def belong_together(row1, row2):
    return { k: row1[k] for k in keys } == { k: row2[k] for k in keys }

  # Go to intersections of chunks and bump to the lowest chunk any
  # set of rows (eg. user stats over time) that were split across
  # chunks lines.
  for i in range(1, len(chunks)+1):
    if len(chunks[-i]) == 0:
      # This is expected in some borderline cases (eg. very small df).
      continue
    if i == len(chunks):
      # First item, no split to its left.
      continue
    while len(chunks[-i]) > 0 and belong_together(chunks[-i-1][-1], chunks[-i][0]):
      chunks[-i-1].append(chunks[-i].pop(0))
  # Remove all empty chunks for good measure.
  return list(filter(lambda c: len(c) > 0, chunks))

def process_df_chunk(info):
  cols = info['cols']
  date_counts = info['date_counts']
  df = info['df']
  fn = info['fn']
  time_col = info['time_col']

  if len(df) == 0:
    return []

  def dogroup_flat(rows):    
    keys = { col: rows[0][col] for col in cols }
    asdict = { r[time_col]: r for r in rows }
    sparsedict = { d: asdict.get(d) for d in date_counts }
    results = fn(keys, sparsedict)

    ret = []
    for (tcount, row) in sparsedict.items():
      ret.append(dict(keys, _tcount_=tcount, _result_=results.get(tcount)))
    return ret

  result = []
  
  row_keys = df[0].keys()
  last = tuple(df[0][col] for col in cols)
  accumulated = []
  for el in df:
    this = tuple(el[col] for col in cols)
    if last != this:
      # print(last, this)
      # if len(accumulated) > 5:
      #   print("EUREKA")
      result.extend(dogroup_flat(accumulated))
      accumulated = [el]
      last = this
    else:
      accumulated.append(el)
  if len(accumulated) > 0:
    result.extend(dogroup_flat(accumulated))
  return result
    
def groupby_combine(df):
  """This is like groupby().apply(), but 1000x faster."""

  pass