
from datetime import datetime, timedelta
from dateutil import relativedelta
from timeit import default_timer as timer

import numpy as np
import pandas as pd

def split_respecting_boundaries(dataframe, keys, num_split=10):
  """
  Split an array of records in `num_split` semi-even chunks, while respecting
  that records with the same keys should never be put in separate chunks.
  """
  
  assert isinstance(dataframe, pd.DataFrame)
  
  sorted_df = dataframe.sort_values(keys).to_dict('records')
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

def process_df_chunk(chunk):
  columns = chunk['columns']
  date_counts = chunk['date_counts']
  records = chunk['records']
  function = chunk['function']
  time_col = chunk['time_col']

  if len(records) == 0:
    return []

  def dogroup_flat(rows):    
    keys = { col: rows[0][col] for col in columns }
    asdict = { r[time_col]: r for r in rows }
    datetovalue = { d: asdict.get(d) for d in date_counts }
    results = function(keys, datetovalue)

    ret = []
    for (tcount, row) in datetovalue.items():
      ret.append(dict(keys, _tcount_=tcount, _result_=results.get(tcount)))
    return ret

  groups = []
  # Loop records sorted by their key values. Use `last_key_values` and `this`
  # to accumulate all the records with the same key values.
  last_key_values = tuple(records[0][col] for col in columns)
  accumulated = []
  for record in records:
    this = tuple(record[col] for col in columns)
    if last_key_values == this:
      accumulated.append(record)
    else:
      groups.append(accumulated)
      accumulated = [record]
      last_key_values = this
  if accumulated:
    groups.append(accumulated)

  # For each group, call `dogroup_flat`, which wraps `function`.
  result = []
  for group in groups:
    result.extend(dogroup_flat(group))
  return result
    
def groupby_combine(df):
  """This is like groupby().apply(), but 1000x faster."""

  pass