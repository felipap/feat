"""
Functions related to number of occurences across time.
"""

import numpy as np
import pandas as pd

from .lib.pergroup import make_pergroup

def accumulate_foreach(keys, rows):
  result = {}
  count = 0
  for date in sorted(rows.keys()):
    row = rows[date]
    if row and row['_value_']:
      count += row['_value_'] 
    result[date] = count
  return result
accumulate = make_pergroup(accumulate_foreach)

def csince_foreach(key, rows):
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

csince = make_pergroup(csince_foreach, fillna=-99999)