"""
Functions related to number of occurences across time.
"""

import pandas as pd
from .lib.per_group import make_per_group

def get_window_values(start, n, rows):
  window = []
  for index in range(start, start+n):
    item = rows.get(index)
    if not item:
      window.append(None)
    elif item['_value_'] and pd.notna(item['_value_']):
      window.append(item['_value_'])
    else:
      window.append(None)
  return window

def until(rows):
  result = {}
  count = None
  for date in sorted(rows.keys()):
    row = rows[date]
    if not count is None:
      count += 1
    if row and pd.notna(row['_value_']):
      count = 0
    result[date] = count
  return result

def within(rows, space):
  space = int(space)
  
  # if rows[2604] or rows[2603]:
  #   print("here")
  
  result = {}
  for date in sorted(rows.keys()):
    window = get_window_values(date+1, space, rows)
    result[date] = any(window)
  return result


functions = {
  'FUTURE_UNTIL': make_per_group(until, fillna=-999),
  'FUTURE_WITHIN': make_per_group(within, fillna=False, num_args=2),
}
