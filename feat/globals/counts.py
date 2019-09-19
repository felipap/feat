"""
Functions related to number of occurences across time.
"""

import numpy as np
import pandas as pd
from ..lib.cmonth import date_to_cmonth, cmonth_to_date

from .lib.pergroup import make_pergroup

def accumulate_foreach(_, rows):
  """
  Accumulate _value_ across dates.
  """

  result = {}
  count = 0
  for date in sorted(rows.keys()):
    row = rows[date]
    if row and row['_value_']:
      count += row['_value_'] 
    result[date] = count
  return result

def csince_foreach(_, rows):
  """
  Count times since seen _value_ set.
  """
  
  result = {}
  count = None
  for date in sorted(rows.keys()):
    row = rows[date]
    if not count is None:
      count += 1
    if row and row['_value_']:
      count = 0
    result[date] = count
  return result

def timesince(_, rows):
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


def timesinceseen(_, rows):
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
  'ACCUMULATE': make_pergroup(accumulate_foreach),
  'TIME_SINCE': make_pergroup(timesince),
  'TIME_SINCE_SEEN': make_pergroup(timesinceseen, fillna=-999),
  'CSINCE': make_pergroup(csince_foreach, fillna=-99999),
}
