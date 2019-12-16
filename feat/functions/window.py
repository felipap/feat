"""
Functions related to aggregating values of rolling windows.
"""

import numpy as np
import pandas as pd

from .lib.per_group import make_per_group, get_window_values


def call_window_sum(rows, size):
  """
  Propagates the value of the first non-empty row. Beware that such value
  (ie. rows[date]['_value']) might still be None itself, even though a further
  non-empty value might exist. That's just how this function is programmed.
  """
  size = int(size)
  print(size)

  first = None
  result = {}
  for date in sorted(rows.keys()):
    window = get_window_values(date, size, rows)

    if first is None:
      if rows[date] and rows[date]['_value_']:
        # and pd.notna(rows[date]['_value_']):
        first = rows[date]['_value_']
    result[date] = first
  return result


functions = {
  'WINDOW_SUM': make_per_group(call_window_sum, num_args=2),
}
