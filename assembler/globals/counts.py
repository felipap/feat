"""
Functions related to number of occurences across time.
"""

import numpy as np
import pandas as pd
from ..lib.cmonth import date_to_cmonth, cmonth_to_date

from .lib.pergroup import make_pergroup

def accumulate_foreach(_, rows):
  '''Accumulate _value_ across dates.'''

  result = {}
  count = 0
  for date in sorted(rows.keys()):
    row = rows[date]
    if row and row['_value_']:
      count += row['_value_'] 
    result[date] = count
  return result

def csince_foreach(_, rows):
  '''Count times since seen _value_ set.'''
  
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
  '''Accumulate _value_ across dates.'''

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

functions = {
  'ACCUMULATE': make_pergroup(accumulate_foreach),
  'TIME_SINCE': make_pergroup(timesince),
  'CSINCE': make_pergroup(csince_foreach, fillna=-99999),
}