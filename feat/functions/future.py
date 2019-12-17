"""
Functions related to number of occurences across time.
"""

import pandas as pd
from .lib.per_group import make_per_group, get_window_values

def call_until(rows):
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


# class FutureWithin(PerGroupFunction):
#   number_args = [0,2] # 2 # 0
#   comment = "This is what this function does, man!"
#   fill_nan = False
  
#   def call(rows, space):
#     pass


def call_within(rows, space):
  space = int(space)
  
  # if rows[2604] or rows[2603]:
  #   print("here")
  
  result = {}
  for date in sorted(rows.keys()):
    # The first argument ot get_window_values is the ending index.
    window = get_window_values(date+space, space, rows)
    result[date] = any(window)
  return result


functions = {
  'FUTURE_UNTIL': make_per_group(call_until, fillna=-999),
  'FUTURE_WITHIN': make_per_group(call_within, fillna=False, num_args=2),
}


