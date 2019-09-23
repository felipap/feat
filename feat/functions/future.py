"""
Functions related to number of occurences across time.
"""

import pandas as pd
from .lib.per_group import make_per_group

def until(rows, args):
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

def within(rows, args):
  space = int(args[0])
  
  result = {}
  for date in sorted(rows.keys()):
    for i in range(space):
      other = rows.get(date+i+1)
      if not other:
        break
      if other['_value_'] and pd.notna(other['_value_']):
        result[date] = True
        break
    else:
      result[date] = False
  return result


functions = {
  'FUTURE_UNTIL': make_per_group(until, fillna=-999),
  'FUTURE_WITHIN': make_per_group(within, fillna=False, num_args=2),
}
