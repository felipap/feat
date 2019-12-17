"""
Functions related to comparing.
"""

import numpy as np
import pandas as pd

from .lib.per_value import per_value
from .lib.per_group import make_per_group


def call_greaterthan(value, args):
  return value > args[1]


def call_changed(rows):
  """
  True when a value changes from yesterday to today.
  """

  result = {}
  last = 'ROFL ANDREWWWW' # A random thing.
  for date in sorted(rows.keys()):
    if rows[date] and rows[date]['_value_']:
      this = rows[date]['_value_'] 
      result[date] = last != this
      last = this
    else:
      result[date] = False
  return result


def call_first(rows):
  """
  Propagates the value of the first non-empty row. Beware that such value
  (ie. rows[date]['_value_']) might still be None itself, even though a further
  non-empty value might exist. That's just how this function is programmed.
  """

  first = None
  result = {}
  for date in sorted(rows.keys()):
    if first is None:
      if rows[date] and rows[date]['_value_']:
        # and pd.notna(rows[date]['_value_']):
        first = rows[date]['_value_']
    result[date] = first
  return result


def call_last(rows):
  """
  Propagates the value of the latest non-empty row. Beware that such value
  (ie. rows[date]['_value_']) might still be None itself, even though a further
  non-empty value might exist. That's just how this function is programmed.
  """

  last = None
  result = {}
  for date in sorted(rows.keys()):
    if rows[date] and rows[date]['_value_']:
      # and pd.notna(rows[date]['_value_']):
      last = rows[date]['_value_']
    result[date] = last
  return result



functions = {
  'GREATERTHAN': per_value(call_greaterthan, fillna=False, dtype=np.bool, num_args=2),
  'CP_CHANGED': make_per_group(call_changed, fillna=0),
  'FIRST': make_per_group(call_first),
  'LAST': make_per_group(call_last),
}
