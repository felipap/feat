"""
Functions related to aggregating values of rolling windows.
"""

import numpy as np
import pandas as pd

from .lib.per_group import make_per_group, get_window_values


def call_window_sum(rows, size):
  size = int(size)
  result = {}
  for date in sorted(rows.keys()):
    window = get_window_values(date, size, rows)
    # If all values are None, window aggregation must be None. Otherwise, sum
    # non-None values.
    if all(v is None for v in window):
      result[date] = None
    else:
      result[date] = sum(value for value in window if value)
  return result


def call_window_last(rows, size):
  size = int(size)
  result = {}
  for date in sorted(rows.keys()):
    window = get_window_values(date, size, rows)
    for item in window[::-1]:
      # Will take latest item that is not bool-false and not np.na.
      if item and pd.notna(item):
        result[date] = item
        break
    else:
      result[date] = None
  return result


def call_window_first(rows, size):
  size = int(size)
  result = {}
  for date in sorted(rows.keys()):
    window = get_window_values(date, size, rows)
    for item in window:
      # Will take first item that is not bool-false and not np.na.
      if item and pd.notna(item):
        result[date] = item
        break
    else:
      result[date] = None
  return result


def call_window_count_notna(rows, size):
  size = int(size)
  result = {}
  for date in sorted(rows.keys()):
    window = get_window_values(date, size, rows)
    result[date] = len([value for value in window if value and pd.notna(value)])
  return result



functions = {
  'WINDOW_SUM': make_per_group(call_window_sum, num_args=2),
  'WINDOW_FIRST': make_per_group(call_window_first, num_args=2),
  'WINDOW_LAST': make_per_group(call_window_last, num_args=2),
  'WINDOW_COUNT_NOTNA': make_per_group(call_window_count_notna, num_args=2),
}
