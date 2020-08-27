"""
Functions related to number of occurences across time.
"""

import numpy as np
import pandas as pd
from ..lib.tblock import date_to_cmonth, cmonth_to_date

from .lib.per_group import make_per_group

def accumulate_foreach(rows, window_size=None):
  """
  Accumulate _value_ across dates.
  """

  if window_size is None:
    window_size = 100000
  
  window = []
  result = {}

  for date in sorted(rows.keys()):
    row = rows[date]
    if len(window) == window_size:
      window = window[1:]
    if row and pd.notna(row['_value_']):
      window.append(row['_value_'])
    else:
      window.append(0)
    result[date] = sum(window)
  return result

def csince_foreach(rows):
  """
  Count times since seen _value_ set.
  """
  
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

def timesince(rows):
  """
  For each time slot, count the difference between it and the value being looked
  at, or -9999 if that difference is negative.
  """

  # if type(rows[0]['_value_']) == int:
  #   raise Exception('Not expected value integer')

  # TODO assert continuity of key (ie. CMONTH(date)) values, otherwise this will break.

  count = None
  result = {}
  for date in sorted(rows.keys()):
    if not count is None:
      count += 1
      result[date] = count
    elif not rows[date] or date < date_to_cmonth(rows[date]['_value_']):
      result[date] = -9999
    else:
      count = 0
      result[date] = 0
  return result


def timesinceseen(rows):
  """
  Count the time since we last saw a non-null value.
  """

  count = None
  result = {}

  for date in sorted(rows.keys()):
    row = rows[date]

    if count is None:
      if row is None:
        count = None
      elif not row['_value_'] or pd.isna(row['_value_']):
        count = None
      else:
        count = 0
    else:
      if not row or not row['_value_'] or pd.isna(row['_value_']):
        count += 1
      else:
        count = 0
    result[date] = count
  return result

functions = {
  'ACCUMULATE': make_per_group(accumulate_foreach, num_args=2),
  'TIME_SINCE': make_per_group(timesince),
  'TIME_SINCE_SEEN': make_per_group(timesinceseen, fillna=-999),
  # 'CSINCE': make_per_group(csince_foreach, fillna=-999),
}
